package daemonapi

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nodestore"
)

// nodeTestServer is a Server with a fresh node store, and TLS pins kept out of
// the developer's own state directory.
func nodeTestServer(t *testing.T, id string) *Server {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	s := New(id)
	store, err := nodestore.NewAt(filepath.Join(t.TempDir(), "nodes.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s.store = store
	return s
}

// fakePeer answers /health as nodeID and returns its host and port.
func fakePeer(t *testing.T, nodeID string) (string, int) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(healthResponse{Status: "ok", NodeID: nodeID,
			Capabilities: map[string]string{"hostname": nodeID + "-host"}})
	}))
	t.Cleanup(ts.Close)
	return splitAddr(t, ts.Listener.Addr().String())
}

// deadAddr is a local address nothing listens on: connection refused, fast.
func deadAddr(t *testing.T) (string, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return splitAddr(t, addr)
}

func splitAddr(t *testing.T, addr string) (string, int) {
	t.Helper()
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	return h, port
}

func rowOf(t *testing.T, s *Server, id string) nodestore.Node {
	t.Helper()
	nodes, err := s.store.ListAll()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if n.NodeID == id {
			return n
		}
	}
	t.Fatalf("no row for %s in %+v", id, nodes)
	return nodestore.Node{}
}

// /v1/nodes names the path a row is reached by, from the active path's kind.
func TestPathNameFromActiveKind(t *testing.T) {
	cases := []struct {
		node nodestore.Node
		want string
	}{
		{nodestore.Node{Host: "127.0.0.1", Source: "self"}, "self"},
		{nodestore.Node{Host: "127.0.0.1", Source: "internet"}, "internet"},
		{nodestore.Node{Host: "10.0.0.4", Source: "lan"}, "lan"},
		{nodestore.Node{Host: "10.0.0.4", Source: "gossip"}, "lan"},
		{nodestore.Node{Source: "lan"}, ""},
	}
	for _, tc := range cases {
		if got := pathName(tc.node); got != tc.want {
			t.Errorf("pathName(%+v) = %q, want %q", tc.node, got, tc.want)
		}
	}
}

// The poller tries the active path first, then lan before internet before
// gossip; self paths are never polled.
func TestCandidateOrder(t *testing.T) {
	got := candidateOrder([]nodestore.Path{
		{Kind: nodestore.PathGossip, Host: "g"},
		{Kind: nodestore.PathInternet, Host: "active", Active: true},
		{Kind: nodestore.PathSelf, Host: "self"},
		{Kind: nodestore.PathInternet, Host: "i"},
		{Kind: nodestore.PathLAN, Host: "l"},
	})
	want := []string{"active", "l", "i", "g"}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want hosts %v", got, want)
	}
	for i := range want {
		if got[i].Host != want[i] {
			t.Fatalf("order %d = %q, want %q (got %+v)", i, got[i].Host, want[i], got)
		}
	}
}

// E5e: LAN discovery reporting a peer at an address that does not answer must
// not replace the internet path that works. It used to overwrite the row's
// one address, and the row flapped unknown/unreachable.
func TestDiscoveryNeverReplacesAWorkingInternetPath(t *testing.T) {
	s := nodeTestServer(t, "me")
	fwdHost, fwdPort := fakePeer(t, "peerA")
	if err := s.AddPeer(fwdHost, fwdPort); err != nil {
		t.Fatal(err)
	}
	lanHost, lanPort := deadAddr(t)
	s.SetDiscoverFn(func() []NodeDiscovered {
		return []NodeDiscovered{{NodeID: "peerA", Host: lanHost, DaemonPort: lanPort}}
	})

	for i := 0; i < 2; i++ {
		s.pollAllNodes()
		n := rowOf(t, s, "peerA")
		if n.Host != fwdHost || n.Port != fwdPort || n.Source != "internet" || n.State != "healthy" {
			t.Fatalf("poll %d: row %s:%d %s %s, want the internet path %s:%d healthy",
				i, n.Host, n.Port, n.Source, n.State, fwdHost, fwdPort)
		}
	}
	paths, err := s.store.Paths("peerA")
	if err != nil {
		t.Fatal(err)
	}
	var sawLAN bool
	for _, p := range paths {
		if p.Kind == nodestore.PathLAN {
			sawLAN = true
			if p.Active || p.LastErr == "" {
				t.Fatalf("dead lan candidate %+v: want inactive with its error recorded", p)
			}
		}
	}
	if !sawLAN {
		t.Fatalf("lan candidate not kept as a candidate: %+v", paths)
	}
}

// A moved peer's new LAN address takes over only once it answers as that
// node; the internet path stays active until then.
func TestAnsweringLANPathTakesOverFromInternet(t *testing.T) {
	s := nodeTestServer(t, "me")
	fwdHost, fwdPort := fakePeer(t, "peerA")
	if err := s.AddPeer(fwdHost, fwdPort); err != nil {
		t.Fatal(err)
	}
	lanHost, lanPort := fakePeer(t, "peerA")
	s.SetDiscoverFn(func() []NodeDiscovered {
		return []NodeDiscovered{{NodeID: "peerA", Host: lanHost, DaemonPort: lanPort}}
	})
	s.pollAllNodes()
	n := rowOf(t, s, "peerA")
	if n.Host != lanHost || n.Port != lanPort || n.Source != "lan" {
		t.Fatalf("row %s:%d %s, want the answering lan path %s:%d", n.Host, n.Port, n.Source, lanHost, lanPort)
	}
}

// An address where another node answers is not this node's path any more.
func TestAnotherNodeAtTheAddressDropsThePath(t *testing.T) {
	s := nodeTestServer(t, "me")
	host, port := fakePeer(t, "newcomer")
	if err := s.store.SeePath("old", nodestore.PathLAN, host, port); err != nil {
		t.Fatal(err)
	}
	s.pollAllNodes()
	paths, err := s.store.Paths("old")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 0 {
		t.Fatalf("old node keeps %+v, want the address dropped", paths)
	}
	if n := rowOf(t, s, "old"); n.State == "healthy" {
		t.Fatalf("old node marked healthy by someone else's answer: %+v", n)
	}
}

// RemovePeer drops the forwarder's path only.
func TestRemovePeerDropsOnlyThatPath(t *testing.T) {
	s := nodeTestServer(t, "me")
	fwdHost, fwdPort := fakePeer(t, "peerA")
	if err := s.AddPeer(fwdHost, fwdPort); err != nil {
		t.Fatal(err)
	}
	if err := s.store.SeePath("peerA", nodestore.PathLAN, "10.9.9.9", 4242); err != nil {
		t.Fatal(err)
	}
	if err := s.RemovePeer(fwdHost, fwdPort); err != nil {
		t.Fatal(err)
	}
	paths, err := s.store.Paths("peerA")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0].Host != "10.9.9.9" {
		t.Fatalf("paths after RemovePeer = %+v, want only the lan one", paths)
	}
}

// A new peer whose first /health gets EOF is retried within a second or two,
// not at the next 10 s poll (test-net e2).
func TestAddPeerRetriesSoonAfterEOF(t *testing.T) {
	s := nodeTestServer(t, "me")
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close() // EOF for the first probe
			}
			return
		}
		_ = json.NewEncoder(w).Encode(healthResponse{Status: "ok", NodeID: "late"})
	}))
	t.Cleanup(ts.Close)
	host, port := splitAddr(t, ts.Listener.Addr().String())

	start := time.Now()
	if err := s.AddPeer(host, port); err != nil {
		t.Fatal(err)
	}
	for time.Since(start) < 3*time.Second {
		if ps, err := s.store.Paths("late"); err == nil && len(ps) == 1 && ps[0].Active {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer not adopted within 3 s after an EOF first probe (%d probes)", calls.Load())
}
