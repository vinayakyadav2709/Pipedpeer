package daemonapi

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestJoinSeedServer(t *testing.T) (*Server, func()) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	oldSU := systemUntrusted
	systemUntrusted = func(keys []string) ([]string, bool, error) {
		return nil, false, nil
	}
	s := New("node-" + strings.Repeat("s", 8))
	return s, func() {
		systemUntrusted = oldSU
		s.StopWarmWorkers()
	}
}

func cacheDummyNar(t *testing.T, s *Server, storePath string) string {
	t.Helper()
	narPath, err := s.narCache.store(storePath, bytes.NewReader([]byte(storePath+"\n")))
	if err != nil {
		t.Fatalf("cache dummy nar for %s: %v", storePath, err)
	}
	return narPath
}

func setTestPeer(s *Server, host string, port int, status string) *PeerHealth {
	ph := &PeerHealth{
		Host:   host,
		Port:   port,
		Status: status,
	}
	s.peersMu.Lock()
	if s.peerHealths == nil {
		s.peerHealths = make(map[string]*PeerHealth)
	}
	s.peerHealths[net.JoinHostPort(host, strconv.Itoa(port))] = ph
	s.peersMu.Unlock()
	return ph
}

func TestJoinSeed_CaseTable(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "new healthy peer, running job with cached NAR",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-1"
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-1"] = &JobRecord{
					JobID:     "job-1",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				var broadcastStore atomic.Pointer[string]
				broadcastDone := make(chan struct{}, 5)
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					str := sp
					broadcastStore.Store(&str)
					broadcastDone <- struct{}{}
					return nil
				})

				ph := setTestPeer(s, "10.0.0.1", 4242, "healthy")
				s.seedJoiningPeers()

				select {
				case <-broadcastDone:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed on input peer=%s store=%s: broadcast was not called",
						"new healthy peer, running job with cached NAR", ph.addr(), storePath)
				}

				if got := broadcastCount.Load(); got != 1 {
					t.Fatalf("case %q failed on input peer=%s store=%s: expected 1 broadcast, got %d",
						"new healthy peer, running job with cached NAR", ph.addr(), storePath, got)
				}
				if sp := broadcastStore.Load(); sp == nil || *sp != storePath {
					t.Fatalf("case %q failed on input peer=%s store=%s: expected broadcast for %s, got %v",
						"new healthy peer, running job with cached NAR", ph.addr(), storePath, storePath, sp)
				}
			},
		},
		{
			name: "same peer set on the next poll",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-2"
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-2"] = &JobRecord{
					JobID:     "job-2",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				broadcastDone := make(chan struct{}, 5)
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					broadcastDone <- struct{}{}
					return nil
				})

				ph := setTestPeer(s, "10.0.0.2", 4242, "healthy")
				s.seedJoiningPeers()

				select {
				case <-broadcastDone:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed on initial poll for peer=%s store=%s",
						"same peer set on the next poll", ph.addr(), storePath)
				}

				// Next poll with same peer set
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if got := broadcastCount.Load(); got != 1 {
					t.Fatalf("case %q failed on input peer=%s store=%s: expected no extra broadcast, got total %d",
						"same peer set on the next poll", ph.addr(), storePath, got)
				}
			},
		},
		{
			name: "only finished jobs",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-3"
				cacheDummyNar(t, s, storePath)

				// First establish active job and attempted set
				s.jobsMu.Lock()
				s.jobs["job-3"] = &JobRecord{
					JobID:     "job-3",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					return nil
				})

				ph := setTestPeer(s, "10.0.0.3", 4242, "healthy")
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if !s.joinSeedHasAttemptedSet(storePath) {
					t.Fatalf("case %q failed on input peer=%s store=%s: expected attempted set to exist while running",
						"only finished jobs", ph.addr(), storePath)
				}

				// Now job finishes
				s.jobsMu.Lock()
				s.jobs["job-3"].Status = "finished"
				s.jobsMu.Unlock()

				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if s.joinSeedHasAttemptedSet(storePath) {
					t.Fatalf("case %q failed on input store=%s: expected attempted set to be removed",
						"only finished jobs", storePath)
				}
			},
		},
		{
			name: "job uploaded with skip-broadcast (DDP)",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-4"
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-4"] = &JobRecord{
					JobID:         "job-4",
					StorePath:     storePath,
					Status:        "running",
					CreatedAt:     time.Now(),
					SkipBroadcast: true,
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					return nil
				})

				ph := setTestPeer(s, "10.0.0.4", 4242, "healthy")
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if got := broadcastCount.Load(); got != 0 {
					t.Fatalf("case %q failed on input peer=%s store=%s: expected 0 broadcasts for skip_broadcast job, got %d",
						"job uploaded with skip-broadcast (DDP)", ph.addr(), storePath, got)
				}
			},
		},
		{
			name: "two running jobs, same store",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-5"
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-5a"] = &JobRecord{
					JobID:     "job-5a",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobs["job-5b"] = &JobRecord{
					JobID:     "job-5b",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				broadcastDone := make(chan struct{}, 5)
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					broadcastDone <- struct{}{}
					return nil
				})

				ph := setTestPeer(s, "10.0.0.5", 4242, "healthy")
				s.seedJoiningPeers()

				select {
				case <-broadcastDone:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed on input peer=%s store=%s: broadcast not called",
						"two running jobs, same store", ph.addr(), storePath)
				}
				time.Sleep(50 * time.Millisecond)

				if got := broadcastCount.Load(); got != 1 {
					t.Fatalf("case %q failed on input peer=%s store=%s: expected exactly 1 broadcast, got %d",
						"two running jobs, same store", ph.addr(), storePath, got)
				}
			},
		},
		{
			name: "two running jobs, different stores",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storeA := "/nix/store/test-case-6a"
				storeB := "/nix/store/test-case-6b"
				cacheDummyNar(t, s, storeA)
				cacheDummyNar(t, s, storeB)

				s.jobsMu.Lock()
				s.jobs["job-6a"] = &JobRecord{
					JobID:     "job-6a",
					StorePath: storeA,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobs["job-6b"] = &JobRecord{
					JobID:     "job-6b",
					StorePath: storeB,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var mu sync.Mutex
				seenStores := make(map[string]int)
				broadcastDone := make(chan string, 5)
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					mu.Lock()
					seenStores[sp]++
					mu.Unlock()
					broadcastDone <- sp
					return nil
				})

				ph := setTestPeer(s, "10.0.0.6", 4242, "healthy")
				s.seedJoiningPeers()

				for i := 0; i < 2; i++ {
					select {
					case <-broadcastDone:
					case <-time.After(300 * time.Millisecond):
						t.Fatalf("case %q failed on input peer=%s: timed out waiting for 2 broadcasts",
							"two running jobs, different stores", ph.addr())
					}
				}

				mu.Lock()
				defer mu.Unlock()
				if seenStores[storeA] != 1 || seenStores[storeB] != 1 {
					t.Fatalf("case %q failed on input peer=%s: expected one each for storeA and storeB, got %v",
						"two running jobs, different stores", ph.addr(), seenStores)
				}
			},
		},
		{
			name: "peer appears while a broadcast is in flight",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-7"
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-7"] = &JobRecord{
					JobID:     "job-7",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				blockFirst := make(chan struct{})
				firstStarted := make(chan struct{})
				var broadcastCount atomic.Int32
				broadcastDone := make(chan struct{}, 5)

				s.setJoinSeedBroadcastFn(func(sp string) []string {
					count := broadcastCount.Add(1)
					if count == 1 {
						close(firstStarted)
						<-blockFirst
					}
					broadcastDone <- struct{}{}
					return nil
				})

				peer1 := setTestPeer(s, "10.0.0.71", 4242, "healthy")
				s.seedJoiningPeers()

				select {
				case <-firstStarted:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed: first broadcast never started",
						"peer appears while a broadcast is in flight")
				}

				// Peer 2 appears while first broadcast is in flight
				peer2 := setTestPeer(s, "10.0.0.72", 4242, "healthy")
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				// Should still be only 1 broadcast started
				if got := broadcastCount.Load(); got != 1 {
					t.Fatalf("case %q failed on peer2=%s: expected none concurrent, got %d broadcasts",
						"peer appears while a broadcast is in flight", peer2.addr(), got)
				}
				// Peer2 must stay unattempted while broadcast was in flight
				if s.joinSeedAttempted(storePath, peer2.addr()) {
					t.Fatalf("case %q failed on peer2=%s: peer2 should stay outside attempted set while broadcast in flight",
						"peer appears while a broadcast is in flight", peer2.addr())
				}

				// Unblock first broadcast
				close(blockFirst)
				select {
				case <-broadcastDone:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed: first broadcast did not finish",
						"peer appears while a broadcast is in flight")
				}

				// Next poll must now take peer2
				s.seedJoiningPeers()
				select {
				case <-broadcastDone:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed: next poll did not trigger broadcast for peer2",
						"peer appears while a broadcast is in flight")
				}

				if got := broadcastCount.Load(); got != 2 {
					t.Fatalf("case %q failed on peer1=%s peer2=%s: expected 2 broadcasts total, got %d",
						"peer appears while a broadcast is in flight", peer1.addr(), peer2.addr(), got)
				}
			},
		},
		{
			name: "peer goes unhealthy then healthy during the job",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-8"
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-8"] = &JobRecord{
					JobID:     "job-8",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				broadcastDone := make(chan struct{}, 5)
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					broadcastDone <- struct{}{}
					return nil
				})

				ph := setTestPeer(s, "10.0.0.8", 4242, "healthy")
				s.seedJoiningPeers()

				select {
				case <-broadcastDone:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed: first broadcast never fired",
						"peer goes unhealthy then healthy during the job")
				}

				// Peer goes unhealthy
				setTestPeer(s, "10.0.0.8", 4242, "unreachable")
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if s.joinSeedAttempted(storePath, ph.addr()) {
					t.Fatalf("case %q failed: unhealthy peer did not leave attempted set",
						"peer goes unhealthy then healthy during the job")
				}

				// Peer becomes healthy again
				setTestPeer(s, "10.0.0.8", 4242, "healthy")
				s.seedJoiningPeers()

				select {
				case <-broadcastDone:
				case <-time.After(300 * time.Millisecond):
					t.Fatalf("case %q failed: broadcast did not fire after peer regained health",
						"peer goes unhealthy then healthy during the job")
				}

				if got := broadcastCount.Load(); got != 2 {
					t.Fatalf("case %q failed on peer=%s: expected 2 broadcasts total, got %d",
						"peer goes unhealthy then healthy during the job", ph.addr(), got)
				}
			},
		},
		{
			name: "peer healthy at upload",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-9"
				cacheDummyNar(t, s, storePath)

				ph := setTestPeer(s, "10.0.0.9", 4242, "healthy")

				// Simulate upload registration of peers
				cleanupUpload := s.registerJobPeers(storePath)
				if cleanupUpload != nil {
					cleanupUpload()
				}
				if !s.joinSeedAttempted(storePath, ph.addr()) {
					t.Fatalf("case %q failed on peer=%s store=%s: expected peer to be recorded in attempted set at upload",
						"peer healthy at upload", ph.addr(), storePath)
				}

				s.jobsMu.Lock()
				s.jobs["job-9"] = &JobRecord{
					JobID:     "job-9",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					return nil
				})

				// Background poll should not broadcast again to peer healthy at upload
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if got := broadcastCount.Load(); got != 0 {
					t.Fatalf("case %q failed on peer=%s store=%s: expected 0 extra broadcasts, got %d",
						"peer healthy at upload", ph.addr(), storePath, got)
				}
			},
		},
		{
			name: "uploaded job 11 min old",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-10"
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-10"] = &JobRecord{
					JobID:     "job-10",
					StorePath: storePath,
					Status:    "uploaded",
					CreatedAt: time.Now().Add(-11 * time.Minute),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					return nil
				})

				ph := setTestPeer(s, "10.0.0.10", 4242, "healthy")
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if got := broadcastCount.Load(); got != 0 {
					t.Fatalf("case %q failed on peer=%s store=%s: expected 0 broadcasts for 11 min old uploaded job, got %d",
						"uploaded job 11 min old", ph.addr(), storePath, got)
				}
			},
		},
		{
			name: "NAR no longer cached",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := "/nix/store/test-case-11"
				narPath := cacheDummyNar(t, s, storePath)
				// Remove the NAR file from disk so cache reports not found
				if err := os.Remove(narPath); err != nil {
					t.Fatalf("remove nar file %s: %v", narPath, err)
				}

				s.jobsMu.Lock()
				s.jobs["job-11"] = &JobRecord{
					JobID:     "job-11",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				var broadcastCount atomic.Int32
				s.setJoinSeedBroadcastFn(func(sp string) []string {
					broadcastCount.Add(1)
					return nil
				})

				ph := setTestPeer(s, "10.0.0.11", 4242, "healthy")
				// Should not panic, warning logged, peer stays unattempted
				s.seedJoiningPeers()
				time.Sleep(50 * time.Millisecond)

				if got := broadcastCount.Load(); got != 0 {
					t.Fatalf("case %q failed on peer=%s store=%s: expected 0 broadcasts when NAR not cached, got %d",
						"NAR no longer cached", ph.addr(), storePath, got)
				}
				if s.joinSeedAttempted(storePath, ph.addr()) {
					t.Fatalf("case %q failed on peer=%s store=%s: peer should stay unattempted when NAR not cached",
						"NAR no longer cached", ph.addr(), storePath)
				}
			},
		},
		{
			name: "broadcast calling pollAllNodes synchronously",
			run: func(t *testing.T) {
				s, cleanup := newTestJoinSeedServer(t)
				defer cleanup()

				storePath := filepath.Join(t.TempDir(), "nix", "store", "test-case-12")
				cacheDummyNar(t, s, storePath)

				s.jobsMu.Lock()
				s.jobs["job-12"] = &JobRecord{
					JobID:     "job-12",
					StorePath: storePath,
					Status:    "running",
					CreatedAt: time.Now(),
				}
				s.jobsMu.Unlock()

				// 0 peers initially, so broadcastClosure calls pollAllNodes synchronously at line 104
				done := make(chan struct{})
				go func() {
					_ = s.broadcastClosure(storePath)
					close(done)
				}()

				select {
				case <-done:
					// Succeeded without deadlock!
				case <-time.After(3 * time.Second):
					t.Fatalf("case %q failed on store=%s: deadlocked during synchronous pollAllNodes call",
						"broadcast calling pollAllNodes synchronously", storePath)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t)
		})
	}
}

func setupActiveJobServer(t *testing.T, storePath, jobID string) (*Server, func()) {
	t.Helper()
	s, cleanup := newTestJoinSeedServer(t)
	cacheDummyNar(t, s, storePath)
	s.jobsMu.Lock()
	s.jobs[jobID] = &JobRecord{JobID: jobID, StorePath: storePath, Status: "running", CreatedAt: time.Now()}
	s.jobsMu.Unlock()
	return s, cleanup
}

func TestJoinSeed_FixesCases(t *testing.T) {
	t.Run("registerJobPeers, peer joins before the cleanup runs, cleanup, poll", func(t *testing.T) {
		store := "/nix/store/test-fix-1"
		s, cleanup := setupActiveJobServer(t, store, "job-fix-1")
		defer cleanup()

		var broadcastCount atomic.Int32
		broadcastDone := make(chan struct{}, 5)
		s.setJoinSeedBroadcastFn(func(sp string) []string {
			broadcastCount.Add(1)
			broadcastDone <- struct{}{}
			return nil
		})

		peer1 := setTestPeer(s, "10.0.0.1", 4242, "healthy")
		peer1.NodeID = "node-1"
		cleanupUpload := s.registerJobPeers(store)

		peer2 := setTestPeer(s, "10.0.0.2", 4242, "healthy")
		peer2.NodeID = "node-2"

		cleanupUpload()
		s.seedJoiningPeers()

		select {
		case <-broadcastDone:
		case <-time.After(300 * time.Millisecond):
			t.Fatalf("case %q failed on peer2=%s store=%s: broadcast not called for joining peer",
				"registerJobPeers, peer joins before the cleanup runs, cleanup, poll", peer2.addr(), store)
		}
		if got := broadcastCount.Load(); got != 1 {
			t.Fatalf("case %q failed on peer2=%s: expected 1 broadcast for the joiner, got %d",
				"registerJobPeers, peer joins before the cleanup runs, cleanup, poll", peer2.addr(), got)
		}
	})

	t.Run("registerJobPeers, peer healthy at registration, cleanup, poll", func(t *testing.T) {
		store := "/nix/store/test-fix-2"
		s, cleanup := setupActiveJobServer(t, store, "job-fix-2")
		defer cleanup()

		var broadcastCount atomic.Int32
		s.setJoinSeedBroadcastFn(func(sp string) []string {
			broadcastCount.Add(1)
			return nil
		})

		peer1 := setTestPeer(s, "10.0.0.1", 4242, "healthy")
		peer1.NodeID = "node-1"
		cleanupUpload := s.registerJobPeers(store)
		cleanupUpload()

		s.seedJoiningPeers()
		time.Sleep(50 * time.Millisecond)

		if got := broadcastCount.Load(); got != 0 {
			t.Fatalf("case %q failed on peer=%s: expected no broadcast, got %d",
				"registerJobPeers, peer healthy at registration, cleanup, poll", peer1.addr(), got)
		}
	})

	t.Run("attempted peer's address changes, same node id, poll", func(t *testing.T) {
		store := "/nix/store/test-fix-3"
		s, cleanup := setupActiveJobServer(t, store, "job-fix-3")
		defer cleanup()

		var broadcastCount atomic.Int32
		s.setJoinSeedBroadcastFn(func(sp string) []string {
			broadcastCount.Add(1)
			return nil
		})

		peer1 := setTestPeer(s, "10.0.0.1", 4242, "healthy")
		peer1.NodeID = "node-1"
		cleanupUpload := s.registerJobPeers(store)
		cleanupUpload()

		s.peersMu.Lock()
		delete(s.peerHealths, "10.0.0.1:4242")
		s.peersMu.Unlock()
		peer1Moved := setTestPeer(s, "10.0.0.99", 4242, "healthy")
		peer1Moved.NodeID = "node-1"

		s.seedJoiningPeers()
		time.Sleep(50 * time.Millisecond)

		if got := broadcastCount.Load(); got != 0 {
			t.Fatalf("case %q failed on peer=%s: expected no broadcast when address changed for same node id, got %d",
				"attempted peer's address changes, same node id, poll", peer1Moved.addr(), got)
		}
	})

	t.Run("new node id on an address used earlier by another node", func(t *testing.T) {
		store := "/nix/store/test-fix-4"
		s, cleanup := setupActiveJobServer(t, store, "job-fix-4")
		defer cleanup()

		var broadcastCount atomic.Int32
		broadcastDone := make(chan struct{}, 5)
		s.setJoinSeedBroadcastFn(func(sp string) []string {
			broadcastCount.Add(1)
			broadcastDone <- struct{}{}
			return nil
		})

		peer1 := setTestPeer(s, "10.0.0.1", 4242, "healthy")
		peer1.NodeID = "node-1"
		cleanupUpload := s.registerJobPeers(store)
		cleanupUpload()

		peer2 := setTestPeer(s, "10.0.0.1", 4242, "healthy")
		peer2.NodeID = "node-2"

		s.seedJoiningPeers()

		select {
		case <-broadcastDone:
		case <-time.After(300 * time.Millisecond):
			t.Fatalf("case %q failed on peer2=%s nodeID=%s: expected broadcast for new node id",
				"new node id on an address used earlier by another node", peer2.addr(), peer2.NodeID)
		}
		if got := broadcastCount.Load(); got != 1 {
			t.Fatalf("case %q failed on peer2=%s: expected 1 broadcast for new node id, got %d",
				"new node id on an address used earlier by another node", peer2.addr(), got)
		}
	})
}
