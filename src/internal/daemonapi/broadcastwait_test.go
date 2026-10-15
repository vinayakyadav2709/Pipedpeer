package daemonapi

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestASlowPeerNoLongerHoldsTheUploadHostage (M2): every run on a fresh lab
// waited ~600 s for one slow peer's closure import. The broadcast now returns
// after broadcastWait with a line for the late peer, and a second run of the
// same environment does not send that peer the closure again.
func TestASlowPeerNoLongerHoldsTheUploadHostage(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("w", 8))
	defer s.StopWarmWorkers()

	oldWait := broadcastWait
	broadcastWait = 200 * time.Millisecond
	defer func() { broadcastWait = oldWait }()

	release := make(chan struct{})
	var imports atomic.Int32
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/store":
			writeJSON(w, http.StatusOK, map[string]bool{"cached": false})
		case "/v1/store/import":
			imports.Add(1)
			<-release
			writeJSON(w, http.StatusOK, map[string]bool{"imported": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer peerSrv.Close()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	hostport := strings.TrimPrefix(peerSrv.URL, "http://")
	peer := &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), Status: "healthy"}
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{hostport: peer}
	s.peersMu.Unlock()

	storePath := filepath.Join(t.TempDir(), "nix", "store", "slow-env")
	if _, err := s.narCache.store(storePath, bytes.NewReader([]byte(storePath+"\n"))); err != nil {
		t.Fatal(err)
	}

	for run := 1; run <= 2; run++ {
		start := time.Now()
		got := s.broadcastClosure(storePath)
		if took := time.Since(start); took > 20*time.Second {
			t.Fatalf("run %d: broadcast waited %s for a peer that never finished", run, took)
		}
		if len(got) != 1 || !strings.Contains(got[0], hostport) || !strings.Contains(got[0], "still importing") {
			t.Fatalf("run %d: want one still-importing line naming the peer, got %q", run, got)
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for imports.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := imports.Load(); n != 1 {
		t.Fatalf("the peer was sent the closure %d times; want once", n)
	}

	close(release)
	released = true
	for s.seedFor(*peer, storePath) != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if s.seedFor(*peer, storePath) != nil {
		t.Fatal("the finished send is still registered as in flight")
	}
}

// TestAPeerImportingFromAnotherNodeIsNotSentTheClosure: a peer that says it
// is importing the closure already (from another node) gets no second copy.
func TestAPeerImportingFromAnotherNodeIsNotSentTheClosure(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("i", 8))
	defer s.StopWarmWorkers()

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/store":
			writeJSON(w, http.StatusOK, map[string]bool{"cached": false, "importing": true})
		default:
			t.Errorf("a peer importing the closure was sent %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer peerSrv.Close()
	hostport := strings.TrimPrefix(peerSrv.URL, "http://")
	peer := &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), Status: "healthy"}
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{hostport: peer}
	s.peersMu.Unlock()

	got := s.broadcastClosure("/nix/store/importing-env")
	if len(got) != 1 || !strings.Contains(got[0], "still importing") {
		t.Fatalf("want one still-importing line, got %q", got)
	}
}

// TestTheStoreCheckSaysWhenAnImportIsUnderway: the receiver half of the
// above.
func TestTheStoreCheckSaysWhenAnImportIsUnderway(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("c", 8))
	defer s.StopWarmWorkers()

	check := func() bool {
		rec := httptest.NewRecorder()
		s.handleStoreCheck(rec, httptest.NewRequest(http.MethodGet, "/v1/store?path=/nix/store/x-env&runnable=1", nil))
		return strings.Contains(rec.Body.String(), `"importing":true`)
	}
	done := s.markImporting("/nix/store/x-env")
	if !check() {
		t.Fatal("an import underway was not reported")
	}
	done()
	if check() {
		t.Fatal("a finished import is still reported")
	}
}

// closureFile makes a sparse file of n bytes to send as a closure archive.
func closureFile(t *testing.T, n int64) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "closure.nar")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(n); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return p
}

func peerAt(t *testing.T, srv *httptest.Server) *PeerHealth {
	hostport := strings.TrimPrefix(srv.URL, "http://")
	return &PeerHealth{Host: strings.Split(hostport, ":")[0], Port: mustPort(t, hostport), Status: "healthy"}
}

// TestAStalledClosureSendIsCut: a send that stops moving fails with the
// reason, well before any total timeout.
func TestAStalledClosureSendIsCut(t *testing.T) {
	old := closureSendStall
	closureSendStall = 300 * time.Millisecond
	defer func() { closureSendStall = old }()

	// The handler never reads the body; the server cannot see the client
	// leave until it does, so the test lets it go.
	done := make(chan struct{})
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/store/import" {
			http.NotFound(w, r)
			return
		}
		<-done
	}))
	defer peerSrv.Close()
	defer close(done)
	nar := closureFile(t, 64<<20)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("s", 8))
	defer s.StopWarmWorkers()
	start := time.Now()
	err := s.sendClosureOnce(peerAt(t, peerSrv), "/nix/store/stall-env", func() (string, bool) { return nar, true })
	if err == nil || !strings.Contains(err.Error(), "stalled") {
		t.Fatalf("want a stalled error, got %v", err)
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Fatalf("the stall took %s to be noticed", took)
	}
}

// TestASlowButMovingClosureSendFinishes: a send that runs several stall
// windows long but keeps moving is not cut.
func TestASlowButMovingClosureSendFinishes(t *testing.T) {
	old := closureSendStall
	closureSendStall = 300 * time.Millisecond
	defer func() { closureSendStall = old }()

	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/store/import" {
			http.NotFound(w, r)
			return
		}
		buf := make([]byte, 256<<10)
		for {
			if _, err := io.ReadFull(r.Body, buf); err != nil {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		writeJSON(w, http.StatusOK, map[string]bool{"imported": true})
	}))
	defer peerSrv.Close()
	nar := closureFile(t, 24<<20) // ~96 reads x 25 ms: several stall windows
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("node-" + strings.Repeat("s", 8))
	defer s.StopWarmWorkers()
	start := time.Now()
	if err := s.sendClosureOnce(peerAt(t, peerSrv), "/nix/store/slow-env", func() (string, bool) { return nar, true }); err != nil {
		t.Fatalf("a moving send was cut: %v", err)
	}
	if took := time.Since(start); took < closureSendStall {
		t.Fatalf("the send took %s, under one stall window: the test proves nothing", took)
	}
}
