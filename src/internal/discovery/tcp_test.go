package discovery

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
)

// TestProbeHost confirms the TCP fallback parses a real daemon /health
// payload into a registrable node, and rejects non-daemon endpoints.
func TestProbeHost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"status":"ok","node_id":"tcp-node","capabilities":{"arch":"arm64","hostname":"worker9"}}`))
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	ip, portStr, _ := net.SplitHostPort(host)
	var port int
	for _, c := range portStr {
		port = port*10 + int(c-'0')
	}

	d := probeHost(ip, port)
	if d == nil {
		t.Fatal("expected probeHost to find the test daemon")
	}
	if d.NodeID != "tcp-node" {
		t.Fatalf("expected node_id tcp-node, got %q", d.NodeID)
	}
	if d.Arch != "arm64" || d.Hostname != "worker9" {
		t.Fatalf("expected caps parsed, got arch=%q hostname=%q", d.Arch, d.Hostname)
	}
	if !strings.HasSuffix(d.SSHEndpoint, ":22") || !strings.HasPrefix(d.SSHEndpoint, "root@") {
		t.Fatalf("unexpected ssh endpoint %q", d.SSHEndpoint)
	}
}

// TestProbeHostRejectsForeign confirms a plain HTTP server (no pipedpeer
// health shape) is not registered as a node.
func TestProbeHostRejectsForeign(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("<html>welcome</html>"))
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	ip, portStr, _ := net.SplitHostPort(host)
	var port int
	for _, c := range portStr {
		port = port*10 + int(c-'0')
	}
	if d := probeHost(ip, port); d != nil {
		t.Fatalf("expected nil for foreign server, got %+v", d)
	}
}

// TestLocalSubnetsExcludesLoopback keeps the sweep off 127.0.0.0/8.
func TestLocalSubnetsExcludesLoopback(t *testing.T) {
	for _, s := range localSubnets() {
		if s.ip.IsLoopback() {
			t.Fatalf("loopback subnet leaked into scan targets: %s", s.ip)
		}
	}
}

// TestTheSweepDoesNotLeakTheToken: the sweep knocks on every host of the
// subnet. One that answers /health like a daemon but cannot prove it holds
// the cluster token must not be handed it, and is not registered.
func TestTheSweepDoesNotLeakTheToken(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if err := authtoken.Set("s3cret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authtoken.Set("") })
	orig := http.DefaultTransport
	authtoken.Install()
	t.Cleanup(func() { http.DefaultTransport = orig })

	ln, err := net.Listen("tcp", "127.0.0.2:0") // not a loopback literal, so treated as remote
	if err != nil {
		t.Skipf("cannot listen on 127.0.0.2 (not Linux?): %v", err)
	}
	var leaked atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(authtoken.Header) != "" {
			leaked.Add(1)
		}
		w.Write([]byte(`{"status":"ok","node_id":"impostor","capabilities":{}}`))
	}))
	srv.Listener.Close()
	srv.Listener = ln
	srv.Start()
	defer srv.Close()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	for _, c := range portStr {
		port = port*10 + int(c-'0')
	}
	if d := probeHost("127.0.0.2", port); d != nil {
		t.Fatalf("registered a host that proved nothing: %+v", d)
	}
	if n := leaked.Load(); n != 0 {
		t.Fatalf("the sweep handed the token over %d time(s)", n)
	}
}
