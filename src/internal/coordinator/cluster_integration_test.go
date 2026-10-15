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
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// ---------- helpers ----------

func makeID(nodeID, hostname, arch string) identity.NodeIdentity {
	return identity.NodeIdentity{NodeID: nodeID, Hostname: hostname, Arch: arch}
}

// Scenario: Only self-node is available (no discovery)
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

// A task that keeps failing stops after MaxExecAttempts with the last error;
// it used to retry forever.
func TestExecuteWithRetryGivesUpAfterThreeAttempts(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := coord.ExecuteWithRetry(ctx, executor, nil)
	if err == nil || !strings.Contains(err.Error(), "gave up after 3 failed attempts") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("got %v, want a give-up naming the last error", err)
	}
	if ctx.Err() != nil {
		t.Fatal("ran until the context ended instead of giving up")
	}
	if n := atomic.LoadInt32(&execCount); n != MaxExecAttempts {
		t.Fatalf("executed %d times, want %d", n, MaxExecAttempts)
	}
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

// A failure marked permanent ends the loop at once: the task is not placed
// again, the error reaches the caller unwrapped, and the lease is released.
func TestExecuteWithRetryStopsOnPermanentFailure(t *testing.T) {
	daemon := daemonapi.NewWithConfig("worker-perm", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemonSrv := httptest.NewServer(daemon.Handler())
	defer daemonSrv.Close()

	host, port := splitHostPort(t, daemonSrv.Listener.Addr().String())

	coord := New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		RetryInterval:    50 * time.Millisecond,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{
				NodeID: "worker-perm", SSHEndpoint: "root@" + host + ":22",
				DaemonPort:  port,
				Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 * 1024 * 1024 * 1024},
				HealthScore: 1.0, State: "healthy",
			}}
		},
	})

	scriptErr := fmt.Errorf("remote job exited with code 1")
	var execCount int32
	executor := func(host string, port int, nodeID string) error {
		atomic.AddInt32(&execCount, 1)
		return Permanent(scriptErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := coord.ExecuteWithRetry(ctx, executor, nil)
	if err != scriptErr {
		t.Fatalf("got error %v, want the executor's own error", err)
	}
	if n := atomic.LoadInt32(&execCount); n != 1 {
		t.Fatalf("executed %d times, want 1", n)
	}
	if daemon.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after a permanent failure, got %d", daemon.ActiveLeases())
	}
}

// Straight after start-up the cluster can look empty for a moment. A single
// look then reported "no eligible node" for a cluster that had one.
func TestFindNodeWithinWaitsForALateNode(t *testing.T) {
	var looks int32
	coord := New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		NoSelf:           true,
		DiscoverFn: func() []registry.NodeRecord {
			if atomic.AddInt32(&looks, 1) < 3 {
				return nil
			}
			return []registry.NodeRecord{{
				NodeID: "late-worker", SSHEndpoint: "root@10.0.0.9:22", DaemonPort: 4242,
				Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 * 1024 * 1024 * 1024},
				HealthScore: 1.0, State: "healthy",
			}}
		},
	})

	waited := 0
	d := coord.FindNodeWithin(context.Background(), 5*time.Second, 10*time.Millisecond, func(PlacementDecision, time.Duration) { waited++ })
	if d.ChosenNode.NodeID != "late-worker" {
		t.Fatalf("chose %q, want the node that appeared on the third look (reason: %s)", d.ChosenNode.NodeID, d.Reason)
	}
	if waited != 1 {
		t.Fatalf("waiting callback ran %d times, want once", waited)
	}

	// With nothing ever appearing it gives up when the window closes.
	empty := New(Config{
		SelfIdentity: makeID("submitter", "sub", "x86_64-linux"),
		SelfDaemon:   fakeDaemon(t),
		NoSelf:       true,
		DiscoverFn:   func() []registry.NodeRecord { return nil },
	})
	start := time.Now()
	if d := empty.FindNodeWithin(context.Background(), 100*time.Millisecond, 10*time.Millisecond, nil); d.ChosenNode.NodeID != "" {
		t.Fatalf("chose %q from an empty cluster", d.ChosenNode.NodeID)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("did not give up when the window closed")
	}
}
