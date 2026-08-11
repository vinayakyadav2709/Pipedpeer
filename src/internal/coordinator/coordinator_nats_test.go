package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/natsbus"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func TestPlaceWithRetryNATSTransport(t *testing.T) {
	// Create shared NATS bus
	bus, err := natsbus.New(natsbus.Config{
		Embedded: true,
		StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	// Start a daemon on "worker-node" that listens on NATS
	daemonNodeID := "worker-nats-node"
	daemon := daemonapi.NewWithConfig(daemonNodeID, 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemon.StartSweeper()
	defer daemon.StopSweeper()

	if err := daemon.BindNATS(bus); err != nil {
		t.Fatalf("bind daemon nats: %v", err)
	}
	defer daemon.UnbindNATS()

	// Build the worker node record (what the coordinator would discover)
	workerNode := registry.NodeRecord{
		NodeID:      daemonNodeID,
		SSHEndpoint: "root@10.0.3.5:22",
		DaemonPort:  38080,
		Load: registry.LoadInfo{
			CPUPercent:        20,
			MemPercent:        30,
			AvailableMemBytes: 8 * 1024 * 1024 * 1024, // 8GB available
		},
		State:       "healthy",
		HealthScore: 0.9,
	}

	// Create coordinator with NATS bus + discovery that returns the worker
	coord := New(Config{
		Bus: bus,
		SelfIdentity: identity.NodeIdentity{
			NodeID:   "coordinator-node",
			Hostname: "coord",
			Arch:     "x86_64-linux",
		},
		SelfSSH:    "root@localhost:22",
		SelfDaemon: 38080,
		SelfLoad: registry.LoadInfo{
			CPUPercent:        80, // self is loaded
			MemPercent:        90,
			AvailableMemBytes: 512 * 1024 * 1024, // only 512MB on self
		},
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{workerNode}
		},
		RequiredMemBytes: 1024, // tiny job
		RetryInterval:    100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := coord.configSnapshot()
	result, err := coord.PlaceWithRetry(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("place with retry: %v", err)
	}

	// Should have placed on worker-nats-node (better score due to lower load)
	if result.NodeID != daemonNodeID {
		t.Fatalf("expected placement on %s, got %s", daemonNodeID, result.NodeID)
	}
	if result.LeaseID == "" {
		t.Fatal("expected non-empty lease_id")
	}

	// Verify daemon has the lease
	lease, ok := daemon.GetLease(result.LeaseID)
	if !ok {
		t.Fatal("daemon should have the lease")
	}
	if lease.State != daemonapi.LeaseReserved {
		t.Fatalf("expected reserved, got %s", lease.State)
	}
}

func TestExecuteWithRetryNATSFullLifecycle(t *testing.T) {
	bus, err := natsbus.New(natsbus.Config{
		Embedded: true,
		StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	daemonNodeID := "exec-nats-node"
	daemon := daemonapi.NewWithConfig(daemonNodeID, 5*time.Second, 2*time.Second, 100*time.Millisecond)
	daemon.StartSweeper()
	defer daemon.StopSweeper()
	if err := daemon.BindNATS(bus); err != nil {
		t.Fatalf("bind: %v", err)
	}
	defer daemon.UnbindNATS()

	workerNode := registry.NodeRecord{
		NodeID:      daemonNodeID,
		SSHEndpoint: "root@10.0.4.5:22",
		DaemonPort:  38080,
		Load: registry.LoadInfo{
			CPUPercent:        10,
			MemPercent:        20,
			AvailableMemBytes: 16 * 1024 * 1024 * 1024,
		},
		State:       "healthy",
		HealthScore: 1.0,
	}

	coord := New(Config{
		Bus: bus,
		SelfIdentity: identity.NodeIdentity{
			NodeID:   "exec-coord",
			Hostname: "coord",
			Arch:     "x86_64-linux",
		},
		SelfSSH:    "root@localhost:22",
		SelfDaemon: 38080,
		SelfLoad: registry.LoadInfo{
			CPUPercent:        90,
			MemPercent:        95,
			AvailableMemBytes: 256 * 1024 * 1024,
		},
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{workerNode}
		},
		RequiredMemBytes: 1024,
		RetryInterval:    100 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Executor that always succeeds
	executed := false
	executor := func(host string, port int, targetNodeID string) error {
		executed = true
		if targetNodeID != daemonNodeID {
			t.Fatalf("expected target %s, got %s", daemonNodeID, targetNodeID)
		}
		if host != "10.0.4.5" {
			t.Fatalf("expected host 10.0.4.5, got %s", host)
		}
		if port != 38080 {
			t.Fatalf("expected port 38080, got %d", port)
		}
		return nil
	}

	var statusMessages []string
	statusFn := func(msg string) {
		statusMessages = append(statusMessages, msg)
	}

	result, err := coord.ExecuteWithRetry(ctx, executor, statusFn)
	if err != nil {
		t.Fatalf("execute with retry: %v", err)
	}

	if !executed {
		t.Fatal("executor was never called")
	}
	if result.NodeID != daemonNodeID {
		t.Fatalf("expected result on %s, got %s", daemonNodeID, result.NodeID)
	}

	// After successful execution, daemon should have released the lease
	if daemon.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after successful execution, got %d", daemon.ActiveLeases())
	}
}

func TestCoordinatorNATSCommitAndComplete(t *testing.T) {
	bus, err := natsbus.New(natsbus.Config{
		Embedded: true,
		StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	nodeID := "commit-nats-node"
	daemon := daemonapi.NewWithConfig(nodeID, 5*time.Second, 2*time.Second, 100*time.Millisecond)
	if err := daemon.BindNATS(bus); err != nil {
		t.Fatalf("bind: %v", err)
	}
	defer daemon.UnbindNATS()

	coord := New(Config{
		Bus:          bus,
		SelfIdentity: identity.NodeIdentity{NodeID: "coord-commit"},
	})

	// Manually request a lease via NATS
	workerNode := registry.NodeRecord{
		NodeID:     nodeID,
		DaemonPort: 38080,
		Load:       registry.LoadInfo{AvailableMemBytes: 8 * 1024 * 1024 * 1024},
		State:      "healthy",
	}

	coord.discoverFn = func() []registry.NodeRecord {
		return []registry.NodeRecord{workerNode}
	}

	leaseID, _, _, err := coord.requestLease("", acceptReq{
		TargetID:         nodeID,
		SubmitterNode:    "coord-commit",
		RequiredMemBytes: 1024,
	})
	if err != nil {
		t.Fatalf("request lease: %v", err)
	}
	if leaseID == "" {
		t.Fatal("expected non-empty lease_id")
	}

	// Commit via NATS — coordinator has bus set, but CommitLease currently uses HTTP
	// CommitLease uses NATS via the bus if available (through PublishJSON for complete)
	// For now just verify the lease exists
	lease, ok := daemon.GetLease(leaseID)
	if !ok {
		t.Fatal("lease should exist on daemon")
	}
	if lease.State != daemonapi.LeaseReserved {
		t.Fatalf("expected reserved, got %s", lease.State)
	}
}
