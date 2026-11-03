package daemonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// latePeer is a peer daemon that starts without a closure; it holds one once
// an import of it has come in (or failed, with refuse).
type latePeer struct {
	srv      *httptest.Server
	imports  atomic.Int32
	checks   atomic.Int32
	refuse   bool
	mu       sync.Mutex
	holds    bool
	importOK chan struct{}
}

func newLatePeer(t *testing.T, refuse bool) *latePeer {
	p := &latePeer{refuse: refuse, importOK: make(chan struct{}, 8)}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/store":
			p.checks.Add(1)
			p.mu.Lock()
			holds := p.holds
			p.mu.Unlock()
			writeJSON(w, http.StatusOK, map[string]bool{"cached": holds})
		case "/v1/store/import":
			p.imports.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			if p.refuse {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "no room"})
				return
			}
			p.mu.Lock()
			p.holds = true
			p.mu.Unlock()
			writeJSON(w, http.StatusOK, map[string]bool{"imported": true})
			p.importOK <- struct{}{}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// lateServer is an origin daemon running a job on storePath (it exists here
// and is in the closure cache) with peer as its one healthy peer.
func lateServer(t *testing.T, peer *latePeer) (*Server, string, *PeerHealth) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("l", 8))
	t.Cleanup(s.StopWarmWorkers)
	s.EnablePoolSpill()
	hostport := strings.TrimPrefix(peer.srv.URL, "http://")
	ph := &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), Status: "healthy", TotalCPUs: 4}
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{hostport: ph}
	s.peersMu.Unlock()
	storePath := filepath.Join(t.TempDir(), "nix", "store", "late-env")
	if err := os.MkdirAll(storePath, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := s.narCache.store(storePath, bytes.NewReader([]byte(storePath+"\n"))); err != nil {
		t.Fatal(err)
	}
	return s, storePath, ph
}

// pacingFor is the shim's request: from the job on this node, for its closure.
func pacingFor(t *testing.T, s *Server, storePath string) map[string]int64 {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/pool/stats?store="+url.QueryEscape(storePath), nil)
	req.RemoteAddr = "127.0.0.1:40000"
	rec := httptest.NewRecorder()
	s.handlePoolStats(rec, req)
	var out map[string]int64
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("pacing answer %q: %v", rec.Body.String(), err)
	}
	return out
}

// TestAPeerWithoutTheClosureIsSentItMidRun: seeding used to happen only at
// upload, so a machine that joined during a run was skipped by every chunk
// for good. The map's own pacing request now sends it the closure, says it
// is pending meanwhile, and lists it as a peer once it holds it.
func TestAPeerWithoutTheClosureIsSentItMidRun(t *testing.T) {
	peer := newLatePeer(t, false)
	s, storePath, _ := lateServer(t, peer)

	first := pacingFor(t, s, storePath)
	if first["peers"] != 0 || first["pending"] != 1 {
		t.Fatalf("a peer lacking the closure: want peers 0, pending 1; got %v", first)
	}
	select {
	case <-peer.importOK:
	case <-time.After(20 * time.Second):
		t.Fatal("the peer was never sent the closure")
	}
	deadline := time.Now().Add(20 * time.Second)
	var got map[string]int64
	for time.Now().Before(deadline) {
		if got = pacingFor(t, s, storePath); got["peers"] == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got["peers"] != 1 || got["pending"] != 0 || got["inflight"] < 1 {
		t.Fatalf("after the import: want peers 1, pending 0; got %v", got)
	}
	if n := peer.imports.Load(); n != 1 {
		t.Fatalf("the peer was sent the closure %d times; want once", n)
	}
}

// TestAPeerThatRefusesTheClosureIsNotResentEveryPacing: the shim asks once a
// second; a peer that failed the import is left alone for lateSeedRetry, and
// is not pending (the map does not wait on it).
func TestAPeerThatRefusesTheClosureIsNotResentEveryPacing(t *testing.T) {
	peer := newLatePeer(t, true)
	s, storePath, ph := lateServer(t, peer)

	pacingFor(t, s, storePath)
	deadline := time.Now().Add(20 * time.Second)
	for s.seedFor(*ph, storePath) != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for range 5 {
		if got := pacingFor(t, s, storePath); got["pending"] != 0 {
			t.Fatalf("a peer that refused the closure still counts as pending: %v", got)
		}
	}
	if n := peer.imports.Load(); n != 1 {
		t.Fatalf("a refusing peer was sent the closure %d times in a row; want once", n)
	}
}

// TestAHeldClosureIsNotAskedAboutEveryPacing: a yes is believed for heldTTL,
// so a map's per-second pacing does not cost a request per peer per second.
func TestAHeldClosureIsNotAskedAboutEveryPacing(t *testing.T) {
	peer := newLatePeer(t, false)
	peer.holds = true
	s, storePath, _ := lateServer(t, peer)

	for range 10 {
		if got := pacingFor(t, s, storePath); got["peers"] != 1 || got["pending"] != 0 {
			t.Fatalf("want peers 1, pending 0; got %v", got)
		}
	}
	if n := peer.checks.Load(); n > 2 {
		t.Fatalf("asked the peer about its closure %d times in 10 pacings", n)
	}
	if peer.imports.Load() != 0 {
		t.Fatal("a peer holding the closure was sent it")
	}
}

// TestOnlyTheJobsOwnPacingSeeds: a pacing request from another machine (the
// cluster stats poller asks without a store; nothing else should ask with
// one) starts no send.
func TestOnlyTheJobsOwnPacingSeeds(t *testing.T) {
	peer := newLatePeer(t, false)
	s, storePath, _ := lateServer(t, peer)
	req := httptest.NewRequest(http.MethodGet, "/v1/pool/stats?store="+url.QueryEscape(storePath), nil)
	req.RemoteAddr = "192.0.2.7:40000"
	rec := httptest.NewRecorder()
	s.handlePoolStats(rec, req)
	if strings.Contains(rec.Body.String(), "pending") {
		t.Fatalf("a remote pacing request was answered with pending: %s", rec.Body.String())
	}
	time.Sleep(100 * time.Millisecond)
	if peer.imports.Load() != 0 {
		t.Fatal("a remote pacing request started a closure send")
	}
}

// TestALANPeerTakesEveryPathFromTheSender: the seed's "which paths do you
// lack" asks a LAN peer not to fetch from the binary cache (it would pull the
// whole closure over the lab's shared uplink while the sender sits on the
// same switch); a peer behind an internet forwarder still may.
func TestALANPeerTakesEveryPathFromTheSender(t *testing.T) {
	if (&PeerHealth{Host: "10.20.1.114"}).substitutesItself() {
		t.Fatal("a LAN peer was asked to fetch from the binary cache")
	}
	if !(&PeerHealth{Host: "127.0.0.1"}).substitutesItself() {
		t.Fatal("a peer behind an internet forwarder may fetch from the binary cache")
	}
	var asked []bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Substitute bool `json:"substitute"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		asked = append(asked, req.Substitute)
		writeJSON(w, http.StatusOK, map[string][]string{"missing": {}})
	}))
	defer srv.Close()
	hostport := strings.TrimPrefix(srv.URL, "http://")
	for _, sub := range []bool{false, true} {
		if _, ok := peerMissingPaths("127.0.0.1", mustPort(t, hostport), []string{"/nix/store/a-x"}, sub); !ok {
			t.Fatal("no answer")
		}
	}
	if len(asked) != 2 || asked[0] || !asked[1] {
		t.Fatalf("substitute sent as %v, want [false true]", asked)
	}
}
