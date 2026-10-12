package daemonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestPoolMapCancelStopsPeerWork is FIX-CANCEL for Pool chunks: when the shim's
// request goes away (its job was killed), the origin drops the forward and the
// peer kills the worker running that chunk within 2 s. Nothing re-runs the
// chunk locally in its place.
func TestPoolMapCancelStopsPeerWork(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	s1 := New("node-" + strings.Repeat("a", 8))
	s2 := New("node-" + strings.Repeat("b", 8))
	srv1 := httptest.NewServer(s1.Handler())
	defer srv1.Close()
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	s1.pool.SetPeerFn(func(_, _ string) []string { return []string{strings.TrimPrefix(srv2.URL, "http://")} })

	pidFile := filepath.Join(t.TempDir(), "pid")
	src := fmt.Sprintf("def slow(x):\n    import os, time\n    open(%q, 'w').write(str(os.getpid()))\n    time.sleep(60)\n    return x\n", pidFile)
	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(strconv.Itoa(i)))
	}
	body, _ := json.Marshal(map[string]any{"func_src": src, "func_name": "slow", "items": items})

	// Posted from loopback, so s1 treats it as its own shim's request and
	// sends the work to the peer.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv1.URL+"/v1/pool/map", bytes.NewReader(body))
	req.Header.Set("X-Pipedpeer-Store", storePath)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()

	var pid int
	for deadline := time.Now().Add(30 * time.Second); pid == 0; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the chunk never started on a worker")
		}
		if b, err := os.ReadFile(pidFile); err == nil && len(b) > 0 {
			pid, _ = strconv.Atoi(string(b))
		}
	}
	s2.pool.mu.Lock()
	w2 := s2.pool.workers[storePath]
	s2.pool.mu.Unlock()
	if w2 == nil {
		t.Fatal("the chunk is not running on the peer")
	}
	// pid is the item's process, a child of the warm worker: it dying proves
	// the whole process group was killed, not just the worker.

	cancel()
	cancelledAt := time.Now()
	for syscall.Kill(pid, 0) == nil {
		if time.Since(cancelledAt) > 2*time.Second {
			t.Fatalf("peer worker pid %d still running 2 s after the cancel", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, s := range []*Server{s1, s2} {
		for s.pool.stats.snapshot()["chunks_cancelled"] != 1 {
			if time.Since(cancelledAt) > 2*time.Second {
				t.Fatalf("%s never counted the cancelled request", s.nodeID)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	// No local fallback: s1 never spawned a worker to redo the chunk.
	s1.pool.mu.Lock()
	_, s1Ran := s1.pool.workers[storePath]
	s1.pool.mu.Unlock()
	if s1Ran {
		t.Fatal("the origin re-ran the cancelled chunk locally")
	}
}
