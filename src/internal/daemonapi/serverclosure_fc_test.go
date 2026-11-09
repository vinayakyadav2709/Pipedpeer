package daemonapi

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestFCServer(t *testing.T, nodeID string) (*Server, func()) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	oldSU := systemUntrusted
	systemUntrusted = func(keys []string) ([]string, bool, error) {
		return nil, false, nil
	}
	s := New(nodeID)
	return s, func() {
		systemUntrusted = oldSU
		s.StopWarmWorkers()
	}
}

func startFCTestListener(t *testing.T, handler http.Handler) (net.Listener, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return ln, port
}

func cacheFCDummyNar(t *testing.T, s *Server, storePath string) {
	t.Helper()
	if _, err := s.narCache.store(storePath, bytes.NewReader([]byte(storePath+"\n"))); err != nil {
		t.Fatalf("cache dummy nar for %s: %v", storePath, err)
	}
}

func TestServerClosureFC_CaseTable(t *testing.T) {
	t.Run("no peers, poller finished its first cycle", func(t *testing.T) {
		s, cleanup := newTestFCServer(t, "node-fc-1")
		defer cleanup()
		s.markFirstPollDone()

		var polls atomic.Int32
		s.discoverFn = func() []NodeDiscovered {
			polls.Add(1)
			return nil
		}

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-1")
		start := time.Now()
		lines := s.broadcastClosure(storePath)
		elapsed := time.Since(start)

		if got := polls.Load(); got != 0 {
			t.Fatalf("case %q failed on input (targets=0, known=0, firstPollDone=true): expected 0 synchronous polls, got %d",
				"no peers, poller finished its first cycle", got)
		}
		if elapsed > 200*time.Millisecond {
			t.Fatalf("case %q failed: expected to return at once, took %s",
				"no peers, poller finished its first cycle", elapsed)
		}
		if len(lines) != 0 {
			t.Fatalf("case %q failed: expected empty refusal lines, got %v",
				"no peers, poller finished its first cycle", lines)
		}
	})

	t.Run("no peers, first cycle not finished", func(t *testing.T) {
		s, cleanup := newTestFCServer(t, "node-fc-2")
		defer cleanup()

		var polls atomic.Int32
		s.discoverFn = func() []NodeDiscovered {
			polls.Add(1)
			return nil
		}

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-2")
		lines := s.broadcastClosure(storePath)

		if got := polls.Load(); got != 1 {
			t.Fatalf("case %q failed on input (targets=0, known=0, firstPollDone=false): expected 1 synchronous poll, got %d",
				"no peers, first cycle not finished", got)
		}
		if len(lines) != 0 {
			t.Fatalf("case %q failed: expected empty refusal lines, got %v",
				"no peers, first cycle not finished", lines)
		}
		if !s.isFirstPollDone() {
			t.Fatalf("case %q failed: expected firstPollDone to be marked true after synchronous poll",
				"no peers, first cycle not finished")
		}
	})

	t.Run("peers known, none healthy", func(t *testing.T) {
		s, cleanup := newTestFCServer(t, "node-fc-3")
		defer cleanup()

		var polls atomic.Int32
		s.discoverFn = func() []NodeDiscovered {
			polls.Add(1)
			return nil
		}

		s.peersMu.Lock()
		s.peerHealths = map[string]*PeerHealth{
			"10.0.0.1:4242": {Host: "10.0.0.1", Port: 4242, Status: "unreachable"},
		}
		s.peersMu.Unlock()

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-3")
		lines := s.broadcastClosure(storePath)

		if got := polls.Load(); got != 0 {
			t.Fatalf("case %q failed on input (known=1, healthy=0): expected no synchronous poll, got %d",
				"peers known, none healthy", got)
		}
		if len(lines) != 0 {
			t.Fatalf("case %q failed: expected empty refusal lines, got %v",
				"peers known, none healthy", lines)
		}
	})

	t.Run("32 peers each answering after 50 ms", func(t *testing.T) {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(50 * time.Millisecond)
			writeJSON(w, http.StatusOK, map[string]bool{"cached": true})
		})
		_, port := startFCTestListener(t, handler)

		s, cleanup := newTestFCServer(t, "node-fc-4")
		defer cleanup()
		s.markFirstPollDone()

		s.peersMu.Lock()
		s.peerHealths = make(map[string]*PeerHealth)
		for i := 1; i <= 32; i++ {
			host := fmt.Sprintf("127.0.0.%d", i)
			hp := fmt.Sprintf("%s:%d", host, port)
			s.peerHealths[hp] = &PeerHealth{Host: host, Port: port, Status: "healthy"}
		}
		s.peersMu.Unlock()

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-4")
		start := time.Now()
		_ = s.broadcastClosure(storePath)
		elapsed := time.Since(start)

		if elapsed >= 500*time.Millisecond {
			t.Fatalf("case %q failed on 32 peers: all checks took %s, expected under 500ms",
				"32 peers each answering after 50 ms", elapsed)
		}
	})

	t.Run("one peer hanging to the 2 s timeout, 5 quick", func(t *testing.T) {
		hangingHost := "127.0.0.1"
		quickDone := make(chan struct{}, 5)
		hangRelease := make(chan struct{})
		t.Cleanup(func() { close(hangRelease) })

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host, _, _ := net.SplitHostPort(r.Host)
			if host == hangingHost {
				select {
				case <-hangRelease:
				case <-time.After(3 * time.Second):
				}
				writeJSON(w, http.StatusOK, map[string]bool{"cached": true})
				return
			}
			quickDone <- struct{}{}
			writeJSON(w, http.StatusOK, map[string]bool{"cached": true})
		})
		_, port := startFCTestListener(t, handler)

		s, cleanup := newTestFCServer(t, "node-fc-5")
		defer cleanup()
		s.markFirstPollDone()

		s.peersMu.Lock()
		s.peerHealths = make(map[string]*PeerHealth)
		s.peerHealths[fmt.Sprintf("%s:%d", hangingHost, port)] = &PeerHealth{
			Host: hangingHost, Port: port, Status: "healthy",
		}
		for i := 2; i <= 6; i++ {
			host := fmt.Sprintf("127.0.0.%d", i)
			hp := fmt.Sprintf("%s:%d", host, port)
			s.peerHealths[hp] = &PeerHealth{Host: host, Port: port, Status: "healthy"}
		}
		s.peersMu.Unlock()

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-5")
		start := time.Now()
		_ = s.broadcastClosure(storePath)
		elapsed := time.Since(start)

		if elapsed >= 2500*time.Millisecond {
			t.Fatalf("case %q failed: total time took %s, expected under 2.5s",
				"one peer hanging to the 2 s timeout, 5 quick", elapsed)
		}
		if len(quickDone) != 5 {
			t.Fatalf("case %q failed: expected 5 quick peers handled, got %d",
				"one peer hanging to the 2 s timeout, 5 quick", len(quickDone))
		}
	})

	t.Run("peer importing from another node", func(t *testing.T) {
		var importCalled atomic.Bool
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/store":
				writeJSON(w, http.StatusOK, map[string]bool{"cached": false, "importing": true})
			case "/v1/store/import":
				importCalled.Store(true)
				writeJSON(w, http.StatusOK, map[string]bool{"imported": true})
			default:
				http.NotFound(w, r)
			}
		})
		_, port := startFCTestListener(t, handler)

		s, cleanup := newTestFCServer(t, "node-fc-6")
		defer cleanup()
		s.markFirstPollDone()

		host := "127.0.0.1"
		hp := fmt.Sprintf("%s:%d", host, port)
		s.peersMu.Lock()
		s.peerHealths = map[string]*PeerHealth{
			hp: {Host: host, Port: port, Status: "healthy"},
		}
		s.peersMu.Unlock()

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-6")
		lines := s.broadcastClosure(storePath)

		if len(lines) != 1 || !strings.Contains(lines[0], "still importing") {
			t.Fatalf("case %q failed on peer=%s: expected one still importing line, got %v",
				"peer importing from another node", hp, lines)
		}
		if importCalled.Load() {
			t.Fatalf("case %q failed on peer=%s: closure send was called when peer was importing",
				"peer importing from another node", hp)
		}
	})

	t.Run("peer already being sent to (seedFor)", func(t *testing.T) {
		var importCalls atomic.Int32
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/store/import" {
				importCalls.Add(1)
			}
			writeJSON(w, http.StatusOK, map[string]bool{"cached": false})
		})
		_, port := startFCTestListener(t, handler)

		s, cleanup := newTestFCServer(t, "node-fc-7")
		defer cleanup()
		s.markFirstPollDone()

		host := "127.0.0.1"
		hp := fmt.Sprintf("%s:%d", host, port)
		peer := PeerHealth{Host: host, Port: port, Status: "healthy"}
		s.peersMu.Lock()
		s.peerHealths = map[string]*PeerHealth{hp: &peer}
		s.peersMu.Unlock()

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-7")
		cacheFCDummyNar(t, s, storePath)

		// Register seedFor in advance
		sd, started := s.startSeed(peer, storePath)
		if !started || sd == nil {
			t.Fatalf("case %q failed: could not pre-register seed", "peer already being sent to (seedFor)")
		}

		// Finish seed after short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			s.finishSeed(peer, storePath, sd, nil)
		}()

		lines := s.broadcastClosure(storePath)

		if len(lines) != 0 {
			t.Fatalf("case %q failed: expected no refusal lines, got %v",
				"peer already being sent to (seedFor)", lines)
		}
		if got := importCalls.Load(); got != 0 {
			t.Fatalf("case %q failed: expected no re-send for peer already in flight, got %d import calls",
				"peer already being sent to (seedFor)", got)
		}
	})

	t.Run("64 peers", func(t *testing.T) {
		var inFlight atomic.Int32
		var maxInFlight atomic.Int32
		var totalChecked atomic.Int32

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			totalChecked.Add(1)
			curr := inFlight.Add(1)
			defer inFlight.Add(-1)

			for {
				old := maxInFlight.Load()
				if curr <= old || maxInFlight.CompareAndSwap(old, curr) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			writeJSON(w, http.StatusOK, map[string]bool{"cached": true})
		})
		_, port := startFCTestListener(t, handler)

		s, cleanup := newTestFCServer(t, "node-fc-8")
		defer cleanup()
		s.markFirstPollDone()

		s.peersMu.Lock()
		s.peerHealths = make(map[string]*PeerHealth)
		for i := 1; i <= 64; i++ {
			host := fmt.Sprintf("127.0.0.%d", i)
			hp := fmt.Sprintf("%s:%d", host, port)
			s.peerHealths[hp] = &PeerHealth{Host: host, Port: port, Status: "healthy"}
		}
		s.peersMu.Unlock()

		storePath := filepath.Join(t.TempDir(), "nix", "store", "fc-case-8")
		_ = s.broadcastClosure(storePath)

		if got := totalChecked.Load(); got != 64 {
			t.Fatalf("case %q failed: expected 64 peers checked, got %d", "64 peers", got)
		}
		if max := maxInFlight.Load(); max > 16 {
			t.Fatalf("case %q failed: max concurrent checks was %d, expected at most 16",
				"64 peers", max)
		}
	})
}
