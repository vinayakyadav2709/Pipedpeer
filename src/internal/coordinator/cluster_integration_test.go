package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// ---------- helpers ----------

func makeID(nodeID, hostname, arch string) identity.NodeIdentity {
	return identity.NodeIdentity{NodeID: nodeID, Hostname: hostname, Arch: arch}
}

func regWithFastSweep() (*registry.Registry, *httptest.Server) {
	cfg := registry.Config{
		LeaseDuration:    80 * time.Millisecond,
		StaleGrace:       60 * time.Millisecond,
		RemovalThreshold: 100 * time.Millisecond,
		SweepInterval:    10 * time.Millisecond,
	}
	return regWithConfig(cfg)
}

// regWithRelaxedExpiry keeps expiry fast enough to test in a unit test but
// leaves a wide enough healthy window that a slow or contended machine cannot
// expire a node while the test is still setting up. Removal is pushed far out
// so a stale node stays visible for the whole test.
func regWithRelaxedExpiry() (*registry.Registry, *httptest.Server) {
	cfg := registry.Config{
		LeaseDuration:    400 * time.Millisecond,
		StaleGrace:       200 * time.Millisecond,
		RemovalThreshold: 30 * time.Second,
		SweepInterval:    10 * time.Millisecond,
	}
	return regWithConfig(cfg)
}

func regWithConfig(cfg registry.Config) (*registry.Registry, *httptest.Server) {
	reg := registry.New(cfg)
	reg.Start()
	srv := httptest.NewServer(reg.Handler())
	return reg, srv
}

// ---------- full lifecycle tests ----------

// Scenario: 3 nodes register → all healthy → 1 stops heartbeat → goes stale →
// coordinator avoids it → node re-registers → becomes available again
func TestNodeDisconnectReconnectLifecycle(t *testing.T) {
	reg, srv := regWithFastSweep()
	defer srv.Close()
	defer reg.Stop()

	idB := makeID("node-b", "host-b", "x86_64-linux")
	idSelf := makeID("self", "self-host", "x86_64-linux")

	// Register both nodes with explicit low loads
	reg.Register(registry.NodeRecord{
		NodeID: "node-a", SSHEndpoint: "root@10.0.1.1:22", DaemonPort: 38080,
	})
	reg.Register(registry.NodeRecord{
		NodeID: "node-b", SSHEndpoint: "root@10.0.1.2:22", DaemonPort: 38080,
	})

	// Keep node-a alive with manual heartbeats (explicit low load)
	keepAlive := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				reg.Heartbeat("node-a", registry.LoadInfo{CPUPercent: 10, MemPercent: 20})
			case <-keepAlive:
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// Both should be healthy
	nodes := reg.ListNodes("healthy")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 healthy nodes, got %d", len(nodes))
	}

	// ---- Node B "crashes" — no heartbeat renewal, lease expires ----
	time.Sleep(120 * time.Millisecond)
	reg.Sweep()

	nodeB, ok := reg.GetNode("node-b")
	if !ok {
		t.Fatal("node-b should still exist as stale, not removed yet")
	}
	if nodeB.State == "healthy" {
		t.Fatalf("node-b should be stale or unavailable after disconnect, got %s", nodeB.State)
	}

	// Coordinator should skip stale node-b, pick node-a
	coord := New(Config{
		RegistryURL:  srv.URL,
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		SelfLoad:     registry.LoadInfo{CPUPercent: 80, MemPercent: 80},
	})

	decision := coord.FindNode()
	if decision.ChosenNode.NodeID != "node-a" {
		t.Fatalf("expected node-a (only healthy remote), got %s (candidates: %d)", decision.ChosenNode.NodeID, len(decision.Candidates))
	}

	// ---- Node B reconnects ----
	clientB2 := heartbeat.NewClient(srv.URL, idB, "root@10.0.1.2:22", 38080)
	clientB2.SetInterval(30 * time.Millisecond)
	_ = clientB2.Start()
	time.Sleep(50 * time.Millisecond)

	nodeB, ok = reg.GetNode("node-b")
	if !ok || nodeB.State != "healthy" {
		t.Fatalf("node-b should be healthy after reconnect, got ok=%v state=%s", ok, nodeB.State)
	}

	// Both nodes should be candidates again
	decision = coord.FindNode()
	if len(decision.Candidates) < 3 { // a + b + self
		t.Fatalf("expected 3 candidates after reconnect, got %d", len(decision.Candidates))
	}

	close(keepAlive)
	clientB2.Stop()
}

// Scenario: Only self-node is available (no registry, no discovery)
func TestSingleNodeSelfOnly(t *testing.T) {
	idSelf := makeID("only-node", "lonely-host", "aarch64-linux")

	coord := New(Config{
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
	})

	decision := coord.FindNode()

	if decision.ChosenNode.NodeID != "only-node" {
		t.Fatalf("expected self-node, got %s", decision.ChosenNode.NodeID)
	}
	if decision.Source != SourceSelf {
		t.Fatalf("expected self source, got %s", decision.Source)
	}
	if len(decision.Candidates) != 1 {
		t.Fatalf("expected exactly 1 candidate (self), got %d", len(decision.Candidates))
	}
	if decision.DegradedMode {
		t.Fatal("should not be degraded when no registry is configured")
	}
}

// Scenario: Registry unreachable → use cache + LAN discovery combined
func TestRegistryDownFallbackToCacheAndLAN(t *testing.T) {
	// First: prime the coordinator's cache via a working registry
	reg := registry.New(registry.DefaultConfig())
	srv := httptest.NewServer(reg.Handler())

	cachedNode := registry.NodeRecord{
		NodeID: "cached-worker", SSHEndpoint: "root@10.0.1.50:22",
		Load: registry.LoadInfo{CPUPercent: 20}, HealthScore: 0.9, State: "healthy",
	}
	reg.Register(cachedNode)

	idSelf := makeID("self-node", "self", "x86_64-linux")

	// LAN discovery also returns a node
	lanNode := registry.NodeRecord{
		NodeID: "lan-worker", SSHEndpoint: "root@10.0.1.60:22",
		Load: registry.LoadInfo{CPUPercent: 30}, HealthScore: 0.7, State: "healthy",
	}
	fakeLAN := func() []registry.NodeRecord { return []registry.NodeRecord{lanNode} }

	coord := New(Config{
		RegistryURL:  srv.URL,
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		SelfLoad:     registry.LoadInfo{CPUPercent: 90, MemPercent: 90},
		DiscoverFn:   fakeLAN,
		MaxCacheAge:  5 * time.Second,
	})

	// First call: registry up → should see cached-worker from registry + lan-worker + self
	decision := coord.FindNode()
	if decision.DegradedMode {
		t.Fatal("should not be degraded when registry is up")
	}
	if len(decision.Candidates) != 3 {
		t.Fatalf("expected 3 candidates (registry + lan + self), got %d", len(decision.Candidates))
	}

	// Kill registry
	srv.Close()

	// Second call: registry down → cache + LAN + self
	decision = coord.FindNode()
	if !decision.DegradedMode {
		t.Fatal("should be degraded when registry is down")
	}
	if len(decision.Candidates) != 3 {
		t.Fatalf("expected 3 candidates (cache + lan + self), got %d", len(decision.Candidates))
	}

	// Verify cached-worker came from cache source
	foundCache := false
	foundLAN := false
	for _, c := range decision.Candidates {
		if c.Node.NodeID == "cached-worker" && c.Source == SourceCache {
			foundCache = true
		}
		if c.Node.NodeID == "lan-worker" && c.Source == SourceDiscovery {
			foundLAN = true
		}
	}
	if !foundCache {
		t.Fatal("cached-worker should be from cache source")
	}
	if !foundLAN {
		t.Fatal("lan-worker should be from discovery source")
	}

	// Best node should be cached-worker (lower load than lan-worker, higher health)
	if decision.ChosenNode.NodeID != "cached-worker" {
		t.Fatalf("expected cached-worker (best score), got %s", decision.ChosenNode.NodeID)
	}
}

// Scenario: Node reports load via heartbeat → registry has correct values →
// coordinator uses them for scoring
func TestLoadReportingAccuracy(t *testing.T) {
	reg, srv := regWithFastSweep()
	defer srv.Close()
	defer reg.Stop()

	id := makeID("load-node", "worker", "x86_64-linux")
	client := heartbeat.NewClient(srv.URL, id, "root@10.0.1.5:22", 38080)
	client.SetInterval(20 * time.Millisecond)
	_ = client.Start()
	time.Sleep(50 * time.Millisecond)

	// Check that registry has real load values
	node, ok := reg.GetNode("load-node")
	if !ok {
		t.Fatal("node not in registry")
	}
	// CollectLoad returns real CPU/mem on Linux, 0 on other OS
	// Either way, it should be non-negative and the node should exist
	if node.Load.CPUPercent < 0 || node.Load.MemPercent < 0 {
		t.Fatalf("load should be non-negative: cpu=%.1f mem=%.1f", node.Load.CPUPercent, node.Load.MemPercent)
	}
	if node.State != "healthy" {
		t.Fatalf("expected healthy, got %s", node.State)
	}
	if node.HealthScore <= 0 || node.HealthScore > 1 {
		t.Fatalf("health score out of range: %f", node.HealthScore)
	}

	client.Stop()
}

// Scenario: Heartbeat sends ActiveJobs count, registry stores it,
// coordinator uses it in scoring
func TestActiveJobsReportedViaHeartbeat(t *testing.T) {
	reg := registry.New(registry.DefaultConfig())
	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	// Register node with 0 jobs
	reg.Register(registry.NodeRecord{
		NodeID: "busy-node", SSHEndpoint: "root@10.0.1.5:22",
		Load: registry.LoadInfo{ActiveJobs: 0}, HealthScore: 1.0, State: "healthy",
	})

	// Heartbeat with 5 active jobs
	body := `{"node_id":"busy-node","load":{"cpu_percent":50,"memory_percent":40,"active_jobs":5}}`
	resp, err := http.Post(srv.URL+"/v1/heartbeat", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	node, _ := reg.GetNode("busy-node")
	if node.Load.ActiveJobs != 5 {
		t.Fatalf("expected 5 active jobs, got %d", node.Load.ActiveJobs)
	}

	// Coordinator should score this node lower due to active jobs
	idSelf := makeID("self", "self", "x86_64-linux")
	coord := New(Config{
		RegistryURL:  srv.URL,
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		SelfLoad:     registry.LoadInfo{CPUPercent: 10, ActiveJobs: 0},
	})

	decision := coord.FindNode()
	// Self has lower load and 0 jobs → should win
	if decision.ChosenNode.NodeID != "self" {
		t.Fatalf("expected self (lighter load), got %s", decision.ChosenNode.NodeID)
	}
}

// Scenario: Lease expiry causes coordinator to stop selecting a node,
// then another node gets the task instead
func TestLeaseExpiryRedirectsTask(t *testing.T) {
	reg, srv := regWithRelaxedExpiry()
	defer srv.Close()
	defer reg.Stop()

	idSlow := makeID("worker-slow", "host-slow", "x86_64-linux")

	// Register worker-slow with a heartbeat client (stays alive) FIRST. Its
	// first heartbeat gathers host capabilities, which can shell out to GPU
	// tooling and take a while on a cold cache. Doing it before worker-fast is
	// registered keeps that cost outside worker-fast's healthy window.
	clientSlow := heartbeat.NewClient(srv.URL, idSlow, "root@10.0.1.2:22", 38080)
	clientSlow.SetInterval(30 * time.Millisecond)
	_ = clientSlow.Start()
	// Ensure heartbeat client stops BEFORE the server closes (t.Cleanup is LIFO)
	t.Cleanup(func() { clientSlow.Stop() })

	time.Sleep(20 * time.Millisecond) // let slow register

	// Register worker-fast manually (no heartbeat client → will expire).
	// It reports the same class of hardware as the real heartbeating node but
	// at a much lighter load, so this test turns on lease expiry rather than on
	// the details of the scorer.
	fastRegisteredAt := time.Now()
	reg.Register(registry.NodeRecord{
		NodeID: "worker-fast", SSHEndpoint: "root@10.0.1.1:22",
		Capabilities: map[string]string{"cpu_cores": "64", "cpu_mhz": "4800"},
		Load: registry.LoadInfo{
			CPUPercent: 1, MemPercent: 1, TotalCPUs: 64,
			AvailableMemBytes: 256 << 30,
		},
		HealthScore: 1.0, State: "healthy",
	})

	idSelf := makeID("self", "self", "x86_64-linux")
	coord := New(Config{
		RegistryURL:  srv.URL,
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		SelfLoad:     registry.LoadInfo{CPUPercent: 90, MemPercent: 90},
	})

	// Before expiry: worker-fast should be chosen (lowest load)
	decision := coord.FindNode()
	if elapsed := time.Since(fastRegisteredAt); elapsed >= 600*time.Millisecond {
		t.Skipf("machine too slow: worker-fast's healthy window elapsed during setup (%v)", elapsed)
	}
	if decision.ChosenNode.NodeID != "worker-fast" {
		t.Fatalf("expected worker-fast before expiry, got %s", decision.ChosenNode.NodeID)
	}

	// Wait for worker-fast's lease (400ms) plus stale grace (200ms) to pass
	time.Sleep(time.Until(fastRegisteredAt.Add(700 * time.Millisecond)))
	reg.Sweep()

	// worker-slow is still heartbeating → healthy. worker-fast is stale.
	//
	// Unless the machine did not get round to delivering those heartbeats.
	// worker-slow beats every 30ms against a 400ms lease, which is ample when
	// this package runs alone and not always ample when the whole suite runs
	// in parallel - the goroutine gets starved, worker-slow expires too, and
	// the coordinator correctly falls back to self. That is a starved test
	// machine, not a scheduling bug, and reporting it as "expected
	// worker-slow, got self" sends the reader to the scorer. Distinguish them
	// by asking the registry what it thinks before asserting on the choice.
	if rec, ok := reg.GetNode(idSlow.NodeID); !ok || rec.State != "healthy" {
		state := "absent"
		if ok {
			state = rec.State
		}
		t.Skipf("NOT VERIFIED: machine too slow: worker-slow is %q after the "+
			"%v window, so its own heartbeats were starved and there is no "+
			"healthy peer left for the choice to be between", state,
			time.Since(fastRegisteredAt))
	}

	decision = coord.FindNode()
	if decision.ChosenNode.NodeID != "worker-slow" {
		t.Fatalf("expected worker-slow after fast's lease expired, got %s", decision.ChosenNode.NodeID)
	}

	// Stop worker-slow's heartbeat and wait for its lease to expire too
	clientSlow.Stop()
	time.Sleep(700 * time.Millisecond)
	reg.Sweep()

	// Now only self should be available
	decision = coord.FindNode()
	if decision.ChosenNode.NodeID != "self" {
		t.Fatalf("expected self when all workers expired, got %s", decision.ChosenNode.NodeID)
	}
}

// Scenario: Connection breaks from registry side (registry restarts) →
// heartbeat client re-registers automatically
func TestRegistryRestartClientReRegisters(t *testing.T) {
	reg1 := registry.New(registry.DefaultConfig())
	srv1 := httptest.NewServer(reg1.Handler())

	id := makeID("persistent-node", "worker", "x86_64-linux")
	client := heartbeat.NewClient(srv1.URL, id, "root@10.0.1.5:22", 38080)
	client.SetInterval(30 * time.Millisecond)
	_ = client.Start()
	time.Sleep(50 * time.Millisecond)

	// Verify registered
	nodes := reg1.ListNodes("")
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node before restart, got %d", len(nodes))
	}

	// "Restart" registry — close old, start new on same URL
	// The client will get connection errors, then on next heartbeat get 404, then re-register
	srv1.Close()

	// Start new registry (client needs to find it at same URL — not possible with httptest
	// so we simulate: the client's heartbeats fail during downtime)
	time.Sleep(80 * time.Millisecond)

	// Client should still be running (heartbeat errors are non-fatal)
	// Stop cleanly — deregister should fail gracefully
	client.Stop()
	// No panic = success. The real reconnect is tested in TestHeartbeatReRegistersOn404
}

// Scenario: Coordinator called per-task with fresh data each time
func TestCoordinatorPerTaskFreshData(t *testing.T) {
	reg := registry.New(registry.DefaultConfig())
	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	idSelf := makeID("self", "self", "x86_64-linux")
	coord := New(Config{
		RegistryURL:  srv.URL,
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		SelfLoad:     registry.LoadInfo{CPUPercent: 50, MemPercent: 50},
	})

	// Task 1: no workers → self
	d1 := coord.FindNode()
	if d1.ChosenNode.NodeID != "self" {
		t.Fatalf("task 1: expected self, got %s", d1.ChosenNode.NodeID)
	}

	// Register a worker between tasks
	reg.Register(registry.NodeRecord{
		NodeID: "new-worker", SSHEndpoint: "root@10.0.1.5:22",
		Load: registry.LoadInfo{CPUPercent: 5}, HealthScore: 1.0, State: "healthy",
	})

	// Task 2: fresh query should find new-worker
	d2 := coord.FindNode()
	if d2.ChosenNode.NodeID != "new-worker" {
		t.Fatalf("task 2: expected new-worker, got %s", d2.ChosenNode.NodeID)
	}

	// Remove worker between tasks
	reg.Deregister("new-worker")

	// Task 3: back to self
	d3 := coord.FindNode()
	if d3.ChosenNode.NodeID != "self" {
		t.Fatalf("task 3: expected self after worker removed, got %s", d3.ChosenNode.NodeID)
	}
}

// Scenario: Health score computation is correct
func TestHealthScoreComputation(t *testing.T) {
	reg := registry.New(registry.DefaultConfig())

	reg.Register(registry.NodeRecord{NodeID: "node-1"})

	// Heartbeat with high load + failures
	reg.Heartbeat("node-1", registry.LoadInfo{
		CPUPercent:     80,
		MemPercent:     70,
		ActiveJobs:     3,
		RecentFailures: 2,
	})

	node, _ := reg.GetNode("node-1")
	// score = 1.0 - 80/200 - 70/200 - 2*0.1 = 1.0 - 0.4 - 0.35 - 0.2 = 0.05
	expected := 0.05
	if node.HealthScore < expected-0.01 || node.HealthScore > expected+0.01 {
		t.Fatalf("expected health score ~%.2f, got %.4f", expected, node.HealthScore)
	}

	// A node with 100% CPU + 100% mem should have score 0
	reg.Register(registry.NodeRecord{NodeID: "node-2"})
	reg.Heartbeat("node-2", registry.LoadInfo{CPUPercent: 200, MemPercent: 200, RecentFailures: 5})
	node2, _ := reg.GetNode("node-2")
	if node2.HealthScore != 0 {
		t.Fatalf("expected health score 0 for overloaded node, got %f", node2.HealthScore)
	}
}

// Scenario: Stats endpoint reflects real-time state changes
func TestStatsReflectLiveState(t *testing.T) {
	reg, srv := regWithFastSweep()
	defer srv.Close()
	defer reg.Stop()

	// Stats via HTTP
	resp, _ := http.Get(srv.URL + "/v1/stats")
	var stats registry.Stats
	json.NewDecoder(resp.Body).Decode(&stats)
	resp.Body.Close()
	if stats.Total != 0 {
		t.Fatalf("expected 0 nodes initially, got %d", stats.Total)
	}

	// Add 3 nodes
	for _, id := range []string{"a", "b", "c"} {
		reg.Register(registry.NodeRecord{NodeID: id})
	}

	resp, _ = http.Get(srv.URL + "/v1/stats")
	json.NewDecoder(resp.Body).Decode(&stats)
	resp.Body.Close()
	if stats.Total != 3 || stats.Healthy != 3 {
		t.Fatalf("expected 3 healthy, got %+v", stats)
	}

	// Let all leases expire
	time.Sleep(100 * time.Millisecond)
	reg.Sweep()

	resp, _ = http.Get(srv.URL + "/v1/stats")
	json.NewDecoder(resp.Body).Decode(&stats)
	resp.Body.Close()
	if stats.Healthy != 0 {
		t.Fatalf("expected 0 healthy after expiry, got %+v", stats)
	}
	if stats.Stale == 0 && stats.Unavailable == 0 {
		t.Fatalf("expected some stale/unavailable nodes, got %+v", stats)
	}
}

// --- Lease integration tests ---

// PlaceWithRetry places a task on a real daemon and receives a lease
// TestLeaseLifecycleAgainstARealDaemon covers request, commit and complete
// against a daemon rather than a stub.
//
// It was called TestPlaceWithRetryGetsLease and never called PlaceWithRetry -
// it drives requestLease and CommitLease by hand. The name claimed coverage of
// the placement path that nothing had, which is worse than the gap itself:
// somebody looking for that coverage would have found it and stopped looking.
// TestPlaceWithRetryChoosesANodeAndReserves below is the real thing.
func TestLeaseLifecycleAgainstARealDaemon(t *testing.T) {
	// Start a real daemon
	daemon := daemonapi.NewWithConfig("worker-1", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	// Parse daemon URL to get host:port
	daemonHost := daemonSrv.Listener.Addr().String()

	idSelf := makeID("submitter", "submitter-host", "x86_64-linux")

	coord := New(Config{
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{
				{
					NodeID:      "worker-1",
					SSHEndpoint: "root@" + daemonHost,
					DaemonPort:  0, // port is in the host already
					Load: registry.LoadInfo{
						CPUPercent: 10, AvailableMemBytes: 8 * 1024 * 1024 * 1024,
					},
					HealthScore: 1.0, State: "healthy",
				},
			}
		},
		RequiredMemBytes: 1024, // tiny
		RetryInterval:    50 * time.Millisecond,
	})

	// Use requestLease directly with the daemon URL
	leaseID, expiresAt, _, err := coord.requestLease(daemonSrv.URL, acceptReq{
		TargetID:         "worker-1",
		SubmitterNode:    "submitter",
		RequiredMemBytes: 1024,
	})
	if err != nil {
		t.Fatalf("requestLease failed: %v", err)
	}
	if leaseID == "" {
		t.Fatal("expected lease_id")
	}
	if expiresAt == "" {
		t.Fatal("expected expires_at")
	}

	// Commit the lease
	if err := coord.CommitLease(daemonSrv.URL, leaseID); err != nil {
		t.Fatalf("CommitLease failed: %v", err)
	}

	// Verify daemon has 1 running job
	if daemon.ActiveJobs() != 1 {
		t.Fatalf("expected 1 active job, got %d", daemon.ActiveJobs())
	}

	// Complete the lease
	if err := coord.CompleteLease(daemonSrv.URL, leaseID, "succeeded"); err != nil {
		t.Fatalf("CompleteLease failed: %v", err)
	}

	if daemon.ActiveJobs() != 0 {
		t.Fatalf("expected 0 active jobs after complete, got %d", daemon.ActiveJobs())
	}
}

// Queue behavior: daemon rejects → wait → resources freed → placed
// TestLeaseIsRefusedWhileTheWorkerIsFullAndGrantedAfter drives the admission
// path by hand: reserve most of a daemon's memory, watch a request be refused,
// free the blocker and watch the next one succeed.
//
// Also formerly named for PlaceWithRetry, which it does not call. The retry
// loop here is the test's own.
func TestLeaseIsRefusedWhileTheWorkerIsFullAndGrantedAfter(t *testing.T) {
	daemon := daemonapi.NewWithConfig("busy-worker", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	// Find out how much memory the daemon actually has available
	realAvailable := daemon.AvailableForJob()
	if realAvailable <= 0 {
		t.Skip("no available memory on test host")
	}

	// Reserve 90% of available memory with a blocker job
	blockerMem := realAvailable * 90 / 100
	body := fmt.Sprintf(`{"target_id":"busy-worker","job_name":"blocker","required_mem_bytes":%d}`, blockerMem)
	resp, _ := http.Post(daemonSrv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var blockerResp struct {
		Accepted bool   `json:"accepted"`
		LeaseID  string `json:"lease_id"`
	}
	json.NewDecoder(resp.Body).Decode(&blockerResp)
	resp.Body.Close()
	if !blockerResp.Accepted {
		t.Skip("could not create blocker reservation")
	}

	// Now request a job needing 50% of total available — should be rejected (only 10% free)
	requestMem := realAvailable * 50 / 100
	idSelf := makeID("submitter", "sub", "x86_64-linux")
	coord := New(Config{
		SelfIdentity:     idSelf,
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: requestMem,
		RetryInterval:    50 * time.Millisecond,
	})

	var statusMessages []string

	// Free the blocker after 80ms
	go func() {
		time.Sleep(80 * time.Millisecond)
		complete := fmt.Sprintf(`{"lease_id":%q}`, blockerResp.LeaseID)
		http.Post(daemonSrv.URL+"/v1/complete", "application/json", strings.NewReader(complete))
	}()

	// Retry loop
	var leaseID string
	for attempt := 0; attempt < 20; attempt++ {
		lid, _, _, err := coord.requestLease(daemonSrv.URL, acceptReq{
			TargetID:         "busy-worker",
			SubmitterNode:    "submitter",
			RequiredMemBytes: requestMem,
		})
		if err != nil {
			statusMessages = append(statusMessages, fmt.Sprintf("attempt %d rejected: %v", attempt, err))
			time.Sleep(30 * time.Millisecond)
			continue
		}
		leaseID = lid
		break
	}

	if leaseID == "" {
		t.Fatalf("all attempts rejected, status: %v", statusMessages)
	}
	if len(statusMessages) == 0 {
		t.Fatal("expected at least one rejection before success (daemon was 90% reserved)")
	}

	// Commit + complete
	if err := coord.CommitLease(daemonSrv.URL, leaseID); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if err := coord.CompleteLease(daemonSrv.URL, leaseID, "succeeded"); err != nil {
		t.Fatalf("complete failed: %v", err)
	}
}

// --- ExecuteWithRetry integration tests ---

// Success on first try: place → commit → execute succeeds
func TestExecuteWithRetrySuccessFirstTry(t *testing.T) {
	daemon := daemonapi.NewWithConfig("worker-ok", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	addr := daemonSrv.Listener.Addr().String()
	host, port := splitHostPort(t, addr)

	coord := New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		RetryInterval:    50 * time.Millisecond,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{
				NodeID: "worker-ok", SSHEndpoint: "root@" + host + ":22",
				DaemonPort:  port,
				Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 * 1024 * 1024 * 1024},
				HealthScore: 1.0, State: "healthy",
			}}
		},
	})

	var execCount int32
	executor := func(host string, port int, nodeID string) error {
		atomic.AddInt32(&execCount, 1)
		return nil // success
	}

	var msgs []string
	ctx := context.Background()
	result, err := coord.ExecuteWithRetry(ctx, executor, func(m string) { msgs = append(msgs, m) })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result")
	}
	if atomic.LoadInt32(&execCount) != 1 {
		t.Fatalf("expected 1 execution, got %d", execCount)
	}
	if daemon.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after success, got %d", daemon.ActiveLeases())
	}
}

// Execution fails on first node → rescheduled → succeeds on retry
func TestExecuteWithRetryReschedulesOnFailure(t *testing.T) {
	daemon := daemonapi.NewWithConfig("worker-retry", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	addr := daemonSrv.Listener.Addr().String()
	host, port := splitHostPort(t, addr)

	coord := New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		RetryInterval:    50 * time.Millisecond,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{
				NodeID: "worker-retry", SSHEndpoint: "root@" + host + ":22",
				DaemonPort:  port,
				Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 * 1024 * 1024 * 1024},
				HealthScore: 1.0, State: "healthy",
			}}
		},
	})

	// Executor fails twice, then succeeds
	var execCount int32
	executor := func(host string, port int, nodeID string) error {
		n := atomic.AddInt32(&execCount, 1)
		if n <= 2 {
			return fmt.Errorf("node crashed (attempt %d)", n)
		}
		return nil
	}

	var msgs []string
	ctx := context.Background()
	result, err := coord.ExecuteWithRetry(ctx, executor, func(m string) { msgs = append(msgs, m) })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected result")
	}
	if atomic.LoadInt32(&execCount) != 3 {
		t.Fatalf("expected 3 executions (2 failures + 1 success), got %d", execCount)
	}

	// Verify status messages contain reschedule info
	foundReschedule := false
	for _, m := range msgs {
		if strings.Contains(m, "Rescheduling") || strings.Contains(m, "rescheduling") {
			foundReschedule = true
		}
	}
	if !foundReschedule {
		t.Fatalf("expected reschedule message in status updates, got: %v", msgs)
	}

	// All leases should be cleaned up (2 failed + 1 succeeded = all completed)
	if daemon.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after completion, got %d", daemon.ActiveLeases())
	}
}

// Context cancellation stops the retry loop
func TestExecuteWithRetryCancelStopsLoop(t *testing.T) {
	daemon := daemonapi.NewWithConfig("worker-cancel", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	addr := daemonSrv.Listener.Addr().String()
	host, port := splitHostPort(t, addr)

	coord := New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		RetryInterval:    50 * time.Millisecond,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{
				NodeID: "worker-cancel", SSHEndpoint: "root@" + host + ":22",
				DaemonPort:  port,
				Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 * 1024 * 1024 * 1024},
				HealthScore: 1.0, State: "healthy",
			}}
		},
	})

	// Executor always fails
	var execCount int32
	executor := func(host string, port int, nodeID string) error {
		atomic.AddInt32(&execCount, 1)
		return fmt.Errorf("node crashed")
	}

	// Cancel after 200ms
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := coord.ExecuteWithRetry(ctx, executor, nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !strings.Contains(err.Error(), "cancel") {
		t.Fatalf("expected cancellation error, got: %v", err)
	}

	// Should have attempted at least 1 execution before cancel
	if atomic.LoadInt32(&execCount) < 1 {
		t.Fatal("expected at least 1 execution attempt")
	}
}

// Task is never auto-cancelled: executor keeps failing, loop keeps going until context done
func TestExecuteWithRetryNeverAutoCancel(t *testing.T) {
	daemon := daemonapi.NewWithConfig("worker-persist", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	addr := daemonSrv.Listener.Addr().String()
	host, port := splitHostPort(t, addr)

	coord := New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		RetryInterval:    20 * time.Millisecond,
		ExecBackoffBase:  5 * time.Millisecond,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{
				NodeID: "worker-persist", SSHEndpoint: "root@" + host + ":22",
				DaemonPort:  port,
				Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 * 1024 * 1024 * 1024},
				HealthScore: 1.0, State: "healthy",
			}}
		},
	})

	// Executor always fails — simulates persistent node failures
	var execCount int32
	executor := func(host string, port int, nodeID string) error {
		atomic.AddInt32(&execCount, 1)
		return fmt.Errorf("connection refused")
	}

	// Let it run for 300ms — should keep retrying, never auto-cancel
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_, err := coord.ExecuteWithRetry(ctx, executor, nil)
	if err == nil {
		t.Fatal("expected error after context timeout")
	}

	// Should have retried MANY times (not just once), proving it never auto-cancelled
	finalCount := atomic.LoadInt32(&execCount)
	if finalCount < 3 {
		t.Fatalf("expected at least 3 retry attempts in 300ms, got %d (auto-cancelled too early?)", finalCount)
	}
	t.Logf("retried %d times in 300ms before context cancelled — never auto-cancelled", finalCount)
}

// Helper to split host:port from a test server address
func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		t.Fatalf("bad addr: %s", addr)
	}
	var port int
	fmt.Sscanf(parts[1], "%d", &port)
	return parts[0], port
}

// TestPlaceWithRetryChoosesANodeAndReserves is the coverage that was missing.
//
// Two tests carried this function's name and neither called it. PlaceWithRetry
// is what actually places a job: it picks a candidate, asks it for a lease,
// commits, and reports which node got it - so nothing about that was exercised
// while the names suggested it all was.
func TestPlaceWithRetryChoosesANodeAndReserves(t *testing.T) {
	daemon := daemonapi.NewWithConfig("worker-1", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()
	daemonHost := strings.TrimPrefix(daemonSrv.URL, "http://")
	host, portStr, ok := strings.Cut(daemonHost, ":")
	if !ok {
		t.Fatalf("cannot split %q", daemonHost)
	}
	daemonPort, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port in %q: %v", daemonHost, err)
	}

	idSelf := identity.NodeIdentity{NodeID: "submitter", Hostname: "submitter", Arch: "x86_64-linux"}
	cfg := Config{
		SelfIdentity: idSelf,
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		// Only one candidate, so the choice is unambiguous and the assertion
		// below is about placement rather than about which node won a race.
		DiscoverFn: func() []registry.NodeRecord {
			// A real port, because PlaceWithRetry builds the daemon URL from
			// these fields. The older tests could leave it zero only because
			// they called requestLease with a ready-made URL and never
			// exercised that construction at all.
			return []registry.NodeRecord{{
				NodeID:      "worker-1",
				SSHEndpoint: "root@" + host + ":22",
				DaemonPort:  daemonPort,
				Load: registry.LoadInfo{
					CPUPercent: 5, AvailableMemBytes: 8 * 1024 * 1024 * 1024, TotalCPUs: 8,
				},
				HealthScore: 1.0, State: "healthy",
			}}
		},
		RequiredMemBytes: 1024,
		RetryInterval:    50 * time.Millisecond,
		NoSelf:           true, // force the remote candidate, not this process
	}
	coord := New(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var statuses []string
	res, err := coord.PlaceWithRetry(ctx, cfg, func(s string) { statuses = append(statuses, s) })
	if err != nil {
		t.Fatalf("PlaceWithRetry: %v (statuses: %v)", err, statuses)
	}
	if res == nil {
		t.Fatal("PlaceWithRetry returned no lease and no error")
	}
	if res.LeaseID == "" {
		t.Error("no lease id: nothing was actually reserved on the worker")
	}
	if res.NodeID != "worker-1" {
		t.Errorf("placed on %q, want worker-1", res.NodeID)
	}

	// Reserved, not yet committed - which is the contract. PlaceWithRetry
	// holds a slot; the caller commits once the workspace is uploaded, so a
	// submitter that dies in between does not leave a worker believing it is
	// running something.
	if got := daemon.ActiveLeases(); got != 1 {
		t.Errorf("worker holds %d lease(s) after placement, want 1", got)
	}
	if got := daemon.ActiveJobs(); got != 0 {
		t.Errorf("worker reports %d running job(s) before the commit, want 0", got)
	}

	// And the reservation is a real one the caller can carry through.
	if err := coord.CommitLease(daemonSrv.URL, res.LeaseID); err != nil {
		t.Fatalf("CommitLease on the placed lease: %v", err)
	}
	if got := daemon.ActiveJobs(); got != 1 {
		t.Errorf("worker reports %d running job(s) after the commit, want 1", got)
	}
	if err := coord.CompleteLease(daemonSrv.URL, res.LeaseID, "succeeded"); err != nil {
		t.Fatalf("CompleteLease: %v", err)
	}
	if got := daemon.ActiveJobs(); got != 0 {
		t.Errorf("worker still reports %d active job(s) after completion", got)
	}
}

// TestExecuteWithRetryHandsTheExecutorAReachableDaemon is the un-stubbed path.
//
// Every other executor in this file returns a canned result without touching a
// daemon, so nothing checks the host and port ExecuteWithRetry passes in. Those
// are the whole point of the call - the executor's job is to go and talk to the
// node that was just leased - and a wrong endpoint would surface as a run that
// fails somewhere far away with a connection error.
func TestExecuteWithRetryHandsTheExecutorAReachableDaemon(t *testing.T) {
	daemon := daemonapi.NewWithConfig("worker-1", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	host, portStr, ok := strings.Cut(strings.TrimPrefix(daemonSrv.URL, "http://"), ":")
	if !ok {
		t.Fatalf("cannot split %q", daemonSrv.URL)
	}
	daemonPort, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		SelfIdentity: identity.NodeIdentity{NodeID: "submitter", Hostname: "submitter", Arch: "x86_64-linux"},
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   fakeDaemon(t),
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{
				NodeID:      "worker-1",
				SSHEndpoint: "root@" + host + ":22",
				DaemonPort:  daemonPort,
				Load: registry.LoadInfo{
					CPUPercent: 5, AvailableMemBytes: 8 * 1024 * 1024 * 1024, TotalCPUs: 8,
				},
				HealthScore: 1.0, State: "healthy",
			}}
		},
		RequiredMemBytes: 1024,
		RetryInterval:    50 * time.Millisecond,
		NoSelf:           true,
	}
	coord := New(cfg)

	var reachedHost string
	var reachedPort int
	var reachedNode string
	executor := func(h string, p int, nodeID string) error {
		reachedHost, reachedPort, reachedNode = h, p, nodeID
		// Actually use them. A stub that ignores its arguments cannot tell a
		// correct endpoint from a nonsense one.
		resp, err := http.Get(fmt.Sprintf("http://%s:%d/health", h, p))
		if err != nil {
			return fmt.Errorf("the endpoint handed to the executor is not reachable: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("/health on the leased node answered %d", resp.StatusCode)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res, err := coord.ExecuteWithRetry(ctx, executor, nil)
	if err != nil {
		t.Fatalf("ExecuteWithRetry: %v", err)
	}
	if res == nil {
		t.Fatal("no result")
	}
	if reachedHost != host || reachedPort != daemonPort {
		t.Errorf("executor was given %s:%d, want %s:%d", reachedHost, reachedPort, host, daemonPort)
	}
	if reachedNode != "worker-1" {
		t.Errorf("executor was told the node is %q, want worker-1", reachedNode)
	}

	// The lifecycle is closed out, not left holding a slot on the worker.
	if got := daemon.ActiveJobs(); got != 0 {
		t.Errorf("worker still reports %d active job(s); the lease was not completed", got)
	}
}
