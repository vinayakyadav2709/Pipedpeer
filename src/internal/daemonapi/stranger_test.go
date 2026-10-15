package daemonapi

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
)

// E6: on a site that reuses the peer's subnet, its LAN address is a stranger
// that answers but cannot prove it holds the token. While the internet path
// works, the candidate is dropped and not re-learnt; once nothing works, it
// may be learnt again.
func TestStrangerAtACandidateIsDroppedWhileAnotherPathWorks(t *testing.T) {
	s := nodeTestServer(t, "me")
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if err := authtoken.Set("s3cret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := authtoken.Set(""); err != nil {
			t.Errorf("clearing the test token: %v", err)
		}
	})
	old := http.DefaultTransport
	authtoken.Install()
	t.Cleanup(func() { http.DefaultTransport = old })

	ln, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Skipf("cannot listen on 127.0.0.2 (not Linux?): %v", err)
	}
	stranger := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(healthResponse{Status: "ok", NodeID: "peerA"}) // no proof
	}))
	stranger.Listener.Close()
	stranger.Listener = ln
	stranger.Start()
	t.Cleanup(stranger.Close)
	sHost, sPort := splitAddr(t, ln.Addr().String())

	fwd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(healthResponse{Status: "ok", NodeID: "peerA"})
	}))
	fHost, fPort := splitAddr(t, fwd.Listener.Addr().String())
	if err := s.AddPeer(fHost, fPort); err != nil {
		t.Fatal(err)
	}
	if err := s.store.SeePath("peerA", nodestore.PathLAN, sHost, sPort); err != nil {
		t.Fatal(err)
	}

	s.pollAllNodes()
	if n := rowOf(t, s, "peerA"); n.State != "healthy" || n.Host != fHost || n.Port != fPort {
		t.Fatalf("row %+v, want healthy at the internet path", n)
	}
	paths, err := s.store.Paths("peerA")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if p.Host == sHost && p.Port == sPort {
			t.Fatalf("stranger's address kept: %+v", paths)
		}
	}
	if !s.isStranger("peerA", sHost, sPort) {
		t.Fatal("stranger not remembered; gossip would re-add it every round")
	}

	fwd.Close()
	s.pollAllNodes()
	if s.isStranger("peerA", sHost, sPort) {
		t.Fatal("with no working path left, the refused candidate must be learnable again")
	}
}
