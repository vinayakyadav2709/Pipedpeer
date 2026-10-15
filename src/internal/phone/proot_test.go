package phone

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// fakePhone is a Termux with no proot-distro on the machine: PREFIX, HOME and
// the marker live in temp dirs; it returns the rootfs marker path.
func fakePhone(t *testing.T) string {
	t.Helper()
	pre := t.TempDir()
	t.Setenv("PREFIX", pre)
	t.Setenv("TERMUX_VERSION", "0.118")
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	oldMarker, oldStep, oldLaunch, oldRun, oldLook, oldVer := marker, runStep, launch, runCmd, lookPython, pyVersionOf
	t.Cleanup(func() {
		marker, runStep, launch, runCmd, lookPython, pyVersionOf = oldMarker, oldStep, oldLaunch, oldRun, oldLook, oldVer
		pyOnce = sync.Once{}
	})
	// The guest's /etc/pipedpeer-phone; the host has none at this path.
	marker = "/etc/pipedpeer-phone-test"
	return filepath.Join(pre, "var", "lib", "proot-distro", "containers", Distro, "rootfs", marker)
}

func TestRoute(t *testing.T) {
	guest := fakePhone(t)
	if InProot() || ProotReady() {
		t.Fatal("no marker yet: neither in proot nor ready")
	}
	if got := Route(); !strings.Contains(got, "pool-only") {
		t.Errorf("Termux without the Debian: Route = %q, want the pool-only worker", got)
	}
	if err := os.MkdirAll(filepath.Dir(guest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(guest, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if !ProotReady() {
		t.Error("marker in the Debian: Termux must forward")
	}

	// Inside the Debian the marker is at its own path, and TERMUX_VERSION
	// must not make it think it is Termux.
	marker = guest
	if !InProot() || IsTermux() || ProotReady() {
		t.Errorf("inside: InProot=%v IsTermux=%v ProotReady=%v, want true false false", InProot(), IsTermux(), ProotReady())
	}
	if got := Route(); !strings.Contains(got, "full node") {
		t.Errorf("inside: Route = %q, want the full node", got)
	}
}

func TestSetupTermuxFullNode(t *testing.T) {
	guest := fakePhone(t)
	var ran []string
	runStep = func(w io.Writer, name string, args ...string) error {
		cmd := name + " " + strings.Join(args, " ")
		ran = append(ran, cmd)
		switch {
		case cmd == "proot-distro login "+Distro+" -- true":
			return errors.New("not installed")
		case strings.Contains(cmd, "/bin/sh -c"):
			if !strings.Contains(cmd, "nix-") || !strings.Contains(cmd, "/etc/pipedpeer-phone") {
				t.Errorf("guest script must install Nix and write the marker: %s", cmd)
			}
			if err := os.MkdirAll(filepath.Dir(guest), 0o755); err != nil {
				return err
			}
			return os.WriteFile(guest, nil, 0o644)
		}
		return nil
	}
	var forwarded []string
	launch = func(args []string) { forwarded = args }
	stopped := false

	if err := SetupTermux(io.Discard, []string{"setup", "-y"}, func() error { stopped = true; return nil }); err != nil {
		t.Fatal(err)
	}
	want := []string{"proot-distro list", "proot-distro login pipedpeer -- true", "proot-distro install --name pipedpeer debian"}
	for i, w := range want {
		if i >= len(ran) || ran[i] != w {
			t.Fatalf("commands = %q, want them to start with %q", ran, want)
		}
	}
	if strings.Join(forwarded, " ") != "setup -y" || !stopped {
		t.Errorf("forwarded %q, stopped the old daemon %v; want setup -y rerun in the Debian after the stop", forwarded, stopped)
	}
}

func TestSetupTermuxFallback(t *testing.T) {
	fakePhone(t)
	runStep = func(w io.Writer, name string, args ...string) error {
		if strings.Contains(strings.Join(args, " "), "/bin/sh -c") {
			return errors.New("nix installer failed")
		}
		return nil
	}
	launch = func([]string) { t.Error("must not forward when the full node failed") }
	var pkgs []string
	runCmd = func(name string, args ...string) ([]byte, error) {
		if name != "dpkg-query" && name != "apt-get" {
			pkgs = append(pkgs, name+" "+strings.Join(args, " "))
		}
		return nil, nil
	}
	lookPython = func() (string, error) { return "/bin/false", nil }
	pyVersionOf = func(string) (string, error) { return "3.14", nil }
	pyOnce = sync.Once{}
	var logs bytes.Buffer
	oldLog := log.Logger
	log.Logger = zerolog.New(&logs)
	defer func() { log.Logger = oldLog }()

	var out bytes.Buffer
	if err := SetupTermux(&out, []string{"setup"}, func() error { t.Error("no stop on fallback"); return nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "falling back to a pool-only worker") || !strings.Contains(logs.String(), "nix installer failed") {
		t.Errorf("log = %q, want the fallback line with its cause", logs.String())
	}
	if !strings.Contains(out.String(), "pool-only worker") || len(pkgs) == 0 || !strings.HasPrefix(pkgs[0], "pkg install -y python") {
		t.Errorf("output %q, pkg calls %q: want the Termux worker set up", out.String(), pkgs)
	}
}

func TestSandboxShell(t *testing.T) {
	pr := filepath.Join(t.TempDir(), "proot")
	if err := os.WriteFile(pr, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	old := sandboxProot
	defer func() { sandboxProot = old }()
	sandboxProot = func() string { return pr }
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	q := func(s string) string { return "'" + s + "'" }

	s, err := SandboxShell("/w/job", 1<<30, q)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ulimit -v 3145728 && exec ", "/proot' '-r' ", "'-w' '/w/job' '-b' '/w/job'", "'nice' '-n' '10'"} {
		if !strings.Contains(s, want) {
			t.Errorf("SandboxShell = %q, missing %q", s, want)
		}
	}
	if !strings.HasSuffix(s, " ") {
		t.Errorf("SandboxShell = %q: the command is appended, so it must end in a space", s)
	}
	if s, _ := SandboxShell("/w/job", 0, q); strings.Contains(s, "ulimit") {
		t.Errorf("no memory grant must mean no rlimit, got %q", s)
	}

	sandboxProot = func() string { return filepath.Join(t.TempDir(), "proot") }
	if _, err := SandboxShell("/w/job", 0, q); err == nil {
		t.Error("SandboxShell without proot must fail, not run the job unsandboxed")
	}
}

func TestDetachNeedsLauncher(t *testing.T) {
	t.Setenv(ctlEnv, "")
	if err := Detach([]string{"true"}); err == nil {
		t.Error("Detach outside a forwarded command must fail")
	}
}
