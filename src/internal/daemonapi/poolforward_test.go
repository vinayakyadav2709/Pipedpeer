package daemonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// countingServer serves s and counts the pool/map requests it receives.
func countingServer(s *Server, hits *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/pool/map" {
			hits.Add(1)
		}
		s.Handler().ServeHTTP(w, r)
	}))
}

func doubledItems(n int) []any {
	items := make([]any, n)
	for i := range items {
		items[i] = i + 1
	}
	return items
}

// TestOwnJobLeaseIsNotCountedAgainstItsPool: a job running here under a lease
// that fills the node posts pool work from its shim (loopback). That memory is
// the job's own, so the work runs here instead of being forwarded away.
func TestOwnJobLeaseIsNotCountedAgainstItsPool(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	fakeRun(t, storePath)

	s1 := New("node-" + strings.Repeat("e", 8))
	avail := memAvailBytes(t)
	s1.mu.Lock()
	s1.leases["job"] = &Lease{LeaseID: "job", State: LeaseRunning, MemBytes: avail - 100*1024*1024, JobID: "job-1"}
	s1.mu.Unlock()

	srv1 := httptest.NewServer(s1.Handler())
	defer srv1.Close()
	defer s1.StopWarmWorkers()

	status, results := postPoolMap(t, srv1.URL, storePath, map[string]any{
		"func_src": "def run(x):\n    return x * 2\n", "items": doubledItems(10),
		"required_mem": 300 * 1024 * 1024, // over s1's free memory less the job's lease
	})
	if status != http.StatusOK || len(results) != 10 {
		t.Fatalf("status %d, %d results; want 200 and 10 (the job's own lease admits its pool work)", status, len(results))
	}
}

// TestANoFanoutPartIsNeverForwarded: a part another node already split is
// terminal. When it does not fit it is refused (503), never forwarded on.
func TestANoFanoutPartIsNeverForwarded(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	fakeRun(t, storePath)

	s1 := New("node-" + strings.Repeat("g", 8))
	s2 := New("node-" + strings.Repeat("h", 8))
	avail := memAvailBytes(t)
	s1.mu.Lock()
	s1.leases["reserve"] = &Lease{LeaseID: "reserve", State: LeaseRunning, MemBytes: avail - 100*1024*1024}
	s1.mu.Unlock()

	var peerHits atomic.Int32
	srv1 := httptest.NewServer(s1.Handler())
	srv2 := countingServer(s2, &peerHits)
	defer srv1.Close()
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()
	peer := strings.TrimPrefix(srv2.URL, "http://")
	s1.pool.SetPeerFn(func(_, _ string, _ http.Header) []string { return []string{peer} })

	status, _ := postPoolMap(t, srv1.URL, storePath, map[string]any{
		"func_src": "def run(x):\n    return x * 2\n", "items": doubledItems(10),
		"required_mem": 300 * 1024 * 1024, "no_fanout": true,
	})
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", status)
	}
	if n := peerHits.Load(); n != 0 {
		t.Fatalf("a no_fanout part was forwarded %d time(s), want 0", n)
	}
}

// TestAForwardedChunkNeverReturnsToItsForwarder: s1 cannot admit the work and
// forwards it whole to s2. s2 splits it, but no part may go back to s1: s1
// sees exactly the one request it forwarded, and every result comes back.
func TestAForwardedChunkNeverReturnsToItsForwarder(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	fakeRun(t, storePath)

	s1 := New("node-" + strings.Repeat("i", 8))
	s2 := New("node-" + strings.Repeat("j", 8))
	avail := memAvailBytes(t)
	s1.mu.Lock()
	s1.leases["reserve"] = &Lease{LeaseID: "reserve", State: LeaseRunning, MemBytes: avail - 100*1024*1024}
	s1.mu.Unlock()

	var s1Hits, s2Hits atomic.Int32
	srv1 := countingServer(s1, &s1Hits)
	srv2 := countingServer(s2, &s2Hits)
	defer srv1.Close()
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()
	p1 := strings.TrimPrefix(srv1.URL, "http://")
	p2 := strings.TrimPrefix(srv2.URL, "http://")
	s1.pool.SetPeerFn(func(_, _ string, _ http.Header) []string { return []string{p2} })
	s2.pool.SetPeerFn(func(_, _ string, _ http.Header) []string { return []string{p1} })
	// s2 knows s1 by node id, as it does through its health poller.
	s2.peersMu.Lock()
	if s2.peerHealths == nil {
		s2.peerHealths = map[string]*PeerHealth{}
	}
	s2.peerHealths[p1] = &PeerHealth{NodeID: s1.nodeID}
	s2.peersMu.Unlock()

	status, results := postPoolMap(t, srv1.URL, storePath, map[string]any{
		"func_src": "def run(x):\n    return x * 2\n", "items": doubledItems(40),
		"required_mem": 300 * 1024 * 1024, "force": true,
	})
	if status != http.StatusOK || len(results) != 40 {
		t.Fatalf("status %d, %d results; want 200 and 40", status, len(results))
	}
	for i, v := range results {
		if v != int64((i+1)*2) {
			t.Fatalf("result %d: got %d, want %d", i, v, (i+1)*2)
		}
	}
	if n := s1Hits.Load(); n != 1 {
		t.Fatalf("forwarder received %d pool/map requests, want 1 (its own): work bounced back", n)
	}
	if n := s2Hits.Load(); n != 1 {
		t.Fatalf("peer received %d pool/map requests, want 1", n)
	}
}

// TestAPartSendsItsOwnRequiredMem: a part carries the shim's per-node estimate
// over its own items (2 x (globals + callable + largest item)), not the whole
// chunk's figure, which here is set by a 1 MB item the part does not hold.
func TestAPartSendsItsOwnRequiredMem(t *testing.T) {
	got := make(chan int64, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var hdr struct {
			RequiredMem int64 `json:"required_mem"`
		}
		nl := bytes.IndexByte(body, '\n')
		if nl < 0 || json.Unmarshal(body[:nl], &hdr) != nil {
			t.Errorf("bad frames header: %q", body)
		}
		got <- hdr.RequiredMem
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer peer.Close()

	whole := int64(2 * (10 + 20 + 1<<20)) // the chunk's estimate, set by its 1 MB item
	part := &chunk{
		frames: true, globals: make([]byte, 10), pickledFunc: strings.Repeat("f", 20),
		items:       []json.RawMessage{make([]byte, 100), make([]byte, 300)},
		requiredMem: whole,
	}
	pm := New("node-" + strings.Repeat("k", 8)).pool
	_, _, _ = pm.runRemote(strings.TrimPrefix(peer.URL, "http://"), "/nix/store/fake", part, 0)
	if mem := <-got; mem != 2*(10+20+300) {
		t.Fatalf("part sent required_mem %d, want %d (its own share, not the chunk's %d)", mem, 2*(10+20+300), whole)
	}

	// Compressed items are not their real size: the chunk's figure stands.
	part.itemsZlib = true
	_, _, _ = pm.runRemote(strings.TrimPrefix(peer.URL, "http://"), "/nix/store/fake", part, 0)
	if mem := <-got; mem != whole {
		t.Fatalf("zlib part sent required_mem %d, want the chunk's %d", mem, whole)
	}
}
