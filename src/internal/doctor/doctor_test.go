package doctor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

// fakeDaemon serves the given JSON per path; a path missing from routes is 404.
func fakeDaemon(t *testing.T, health int, routes map[string]any) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(health)
			return
		}
		v, ok := routes[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(v)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func cluster(api string) Cluster {
	return Cluster{
		API:      api,
		HTTP:     &http.Client{Timeout: 2 * time.Second},
		Token:    "tok",
		Dial:     (&net.Dialer{}).DialContext,
		PeerHTTP: &http.Client{Timeout: 2 * time.Second},
		Now:      func() time.Time { return now },
	}
}

func refuseDial(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("connection timed out")
}

// find returns the first check whose text contains sub.
func find(t *testing.T, checks []Check, sub string) Check {
	t.Helper()
	for _, c := range checks {
		if strings.Contains(c.What, sub) {
			return c
		}
	}
	t.Fatalf("no check mentions %q in %+v", sub, checks)
	return Check{}
}

func TestDaemonNotRunning(t *testing.T) {
	c := cluster("")
	c.Token = ""
	got := c.Run(context.Background())
	if f := find(t, got, "daemon not running"); f.Level != Fail || f.Fix != "pipedpeer start" {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "no cluster secret"); f.Level != Fail || !strings.Contains(f.Fix, "pipedpeer connect") {
		t.Fatalf("got %+v", f)
	}
}

func TestDaemonRejectsToken(t *testing.T) {
	got := cluster(fakeDaemon(t, http.StatusUnauthorized, nil)).Run(context.Background())
	if f := find(t, got, "rejects this machine's secret"); f.Level != Fail {
		t.Fatalf("got %+v", f)
	}
}

func TestIntroducerNotAnswering(t *testing.T) {
	api := fakeDaemon(t, 204, map[string]any{
		"/v1/introducers": []introducerState{
			{Addr: "10.0.0.1:4244", LastReply: now.Add(-10 * time.Second), Members: 3},
			{Addr: "10.0.0.2:4244", LastErr: "no reply"},
			{Addr: "10.0.0.3:4244", LastReply: now.Add(-5 * time.Minute)},
		},
		"/v1/nodes": []node{},
	})
	got := cluster(api).Run(context.Background())
	if f := find(t, got, "10.0.0.1"); f.Level != OK || !strings.Contains(f.What, "3 members") {
		t.Fatalf("fresh introducer: %+v", f)
	}
	if f := find(t, got, "10.0.0.2"); f.Level != Fail || !strings.Contains(f.What, "never answered: no reply") {
		t.Fatalf("silent introducer: %+v", f)
	}
	if f := find(t, got, "10.0.0.3"); f.Level != Fail || !strings.Contains(f.What, "5m0s ago") {
		t.Fatalf("stale introducer: %+v", f)
	}
	if f := find(t, got, "no other machines"); f.Level != Warn {
		t.Fatalf("got %+v", f)
	}
}

func TestNoIntroducerRoute(t *testing.T) {
	got := cluster(fakeDaemon(t, 204, map[string]any{"/v1/nodes": []node{}})).Run(context.Background())
	if f := find(t, got, "does not report its introducers"); f.Level != Warn {
		t.Fatalf("got %+v", f)
	}
}

// The college-router case: the peer was found over UDP discovery but TCP to
// its daemon times out.
func TestTCPBlockedUDPPasses(t *testing.T) {
	api := fakeDaemon(t, 204, map[string]any{
		"/v1/introducers": []introducerState{},
		"/v1/nodes": []node{
			{NodeID: "self", Source: "self"},
			{NodeID: "abcdef0123456789", SSHEndpoint: "root@10.10.220.5:22", DaemonPort: 4242,
				State: "unknown", Source: "discovery", Capabilities: map[string]string{"hostname": "psipl-OptiPlex"}},
			{NodeID: "1111", Host: "10.0.0.9", DaemonPort: 4242, State: "unreachable", Source: "internet",
				Reason: "no route to host"},
		},
	})
	c := cluster(api)
	c.Dial = refuseDial
	got := c.Run(context.Background())
	f := find(t, got, "psipl-OptiPlex (abcdef01) at 10.10.220.5:4242")
	if f.Level != Fail || !strings.Contains(f.What, "TCP blocked but UDP passes") {
		t.Fatalf("lan peer: %+v", f)
	}
	f = find(t, got, "1111 at 10.0.0.9:4242")
	if f.Level != Fail || !strings.Contains(f.What, "no route to host") {
		t.Fatalf("internet peer: %+v", f)
	}
	for _, c := range got {
		if strings.Contains(c.What, "self") {
			t.Fatalf("self row listed as a peer: %+v", c)
		}
	}
}

func TestPeerHealthyUnhealthyAndClockSkew(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Date", now.Add(5*time.Minute).Format(http.TimeFormat))
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer peer.Close()
	host, port, _ := net.SplitHostPort(strings.TrimPrefix(peer.URL, "http://"))
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	api := fakeDaemon(t, 204, map[string]any{
		"/v1/introducers": []introducerState{},
		"/v1/nodes": []node{
			{NodeID: "aaaa", Host: host, DaemonPort: p, State: "healthy", Source: "internet"},
			{NodeID: "bbbb", Host: host, DaemonPort: p, State: "unknown", Source: "discovery", Reason: "token proof timed out"},
		},
		"/v1/peers/unreachable": []unreachable{{Peer: "cccc", Reason: "both behind symmetric NAT"}},
	})
	got := cluster(api).Run(context.Background())
	if f := find(t, got, "aaaa healthy"); f.Level != OK {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "bbbb is unknown: token proof timed out"); f.Level != Warn {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "aaaa clock is off by 5m0s"); f.Level != Warn || !strings.Contains(f.Fix, "timedatectl") {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "cccc has no direct path"); f.Level != Fail {
		t.Fatalf("got %+v", f)
	}
	var buf bytes.Buffer
	if code := Print(&buf, got); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(buf.String(), "✗ peer cccc has no direct path: both behind symmetric NAT — fix: ") {
		t.Fatalf("printed:\n%s", buf.String())
	}
}

func TestAllOKExitsZero(t *testing.T) {
	var buf bytes.Buffer
	if code := Print(&buf, []Check{{Level: OK, What: "fine"}, {Warn, "meh", "x"}}); code != 0 {
		t.Fatalf("exit %d", code)
	}
}

func machine(t *testing.T) Machine {
	return Machine{
		Firewall:  func(context.Context) (string, bool) { return "firewalld", false },
		Ports:     []string{"4242/tcp", "4243/udp"},
		Userns:    func() (bool, string) { return false, "sudo pipedpeer setup -y" },
		Crun:      func() bool { return true },
		Nix:       func() bool { return false },
		PCIRoot:   t.TempDir(),
		DriverDir: filepath.Join(t.TempDir(), "absent"),
		NvidiaSMI: func() bool { return false },
		DiskPath:  "/nix",
		FreeBytes: func(string) (uint64, error) { return 2 * gib, nil },
	}
}

func TestMachineChecks(t *testing.T) {
	m := machine(t)
	dev := filepath.Join(m.PCIRoot, "0000:01:00.0")
	if err := os.MkdirAll(dev, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(dev, "vendor"), []byte("0x10de\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dev, "class"), []byte("0x030000\n"), 0o644)

	got := m.Run(context.Background())
	if f := find(t, got, "firewalld blocks 4242/tcp, 4243/udp"); f.Level != Fail || f.Fix != "pipedpeer setup -y" {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "user namespaces refused"); f.Level != Fail {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "crun installed"); f.Level != OK {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "nix missing"); f.Level != Fail {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "driver is not loaded"); f.Level != Fail {
		t.Fatalf("got %+v", f)
	}
	if f := find(t, got, "2.0 GiB free"); f.Level != Fail {
		t.Fatalf("got %+v", f)
	}

	// Driver loaded, nvidia-smi present, Intel-only box, plenty of disk.
	m.DriverDir = t.TempDir()
	m.NvidiaSMI = func() bool { return true }
	if f := find(t, m.nvidia(), "NVIDIA GPU and driver"); f.Level != OK {
		t.Fatalf("got %+v", f)
	}
	m.PCIRoot = t.TempDir()
	if c := m.nvidia(); c != nil {
		t.Fatalf("no NVIDIA device should print nothing, got %+v", c)
	}
	m.FreeBytes = func(string) (uint64, error) { return 50 * gib, nil }
	if c := m.disk(); c.Level != OK {
		t.Fatalf("got %+v", c)
	}
}

func TestUninstallDryRunListsEverything(t *testing.T) {
	dir := t.TempDir()
	called := false
	never := func() error { called = true; return nil }
	steps := UninstallPlan(UninstallDeps{
		StopDaemon: never, CloseFirewall: never, RemoveRoot: never,
		Dirs:         []string{filepath.Join(dir, "data"), filepath.Join(dir, "state")},
		Crun:         "/home/u/.local/bin/crun",
		Binary:       "/usr/local/bin/pipedpeer",
		RemoveBinary: func(string) error { called = true; return nil },
	})
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := RunSteps(&buf, steps, true); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("dry run removed something")
	}
	if _, err := os.Stat(filepath.Join(dir, "data")); err != nil {
		t.Fatal("dry run deleted a dir")
	}
	want := []string{
		"would stop the daemon and remove its user service",
		"would close pipedpeer's firewall ports",
		"would remove the root parts",
		"would delete " + filepath.Join(dir, "data"),
		"would delete " + filepath.Join(dir, "state"),
		"would delete /home/u/.local/bin/crun (the crun setup installed)",
		"would delete /usr/local/bin/pipedpeer",
	}
	for _, w := range want {
		if !strings.Contains(buf.String(), w) {
			t.Fatalf("missing %q in:\n%s", w, buf.String())
		}
	}

	// A crun that is not setup's stays, with one line saying why.
	buf.Reset()
	steps = UninstallPlan(UninstallDeps{StopDaemon: never, CloseFirewall: never, RemoveRoot: never,
		CrunKeep: "/home/u/.local/bin/crun: it is not crun 1.29.1, the version setup installs"})
	if err := RunSteps(&buf, steps, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "would keep /home/u/.local/bin/crun: it is not crun 1.29.1") ||
		strings.Contains(buf.String(), "delete /home/u/.local/bin/crun") {
		t.Fatalf("foreign crun handled wrong:\n%s", buf.String())
	}
}

func TestUninstallCarriesOnPastFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ok := func() error { return nil }
	steps := UninstallPlan(UninstallDeps{
		StopDaemon: ok, CloseFirewall: func() error { return errors.New("sudo refused") }, RemoveRoot: ok,
		Dirs: []string{dir},
	})
	var buf bytes.Buffer
	err := RunSteps(&buf, steps, false)
	if err == nil || !strings.Contains(err.Error(), "sudo refused") {
		t.Fatalf("err = %v", err)
	}
	if _, serr := os.Stat(dir); !os.IsNotExist(serr) {
		t.Fatal("later step did not run after a failure")
	}
}
