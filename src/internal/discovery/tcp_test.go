package discovery

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
