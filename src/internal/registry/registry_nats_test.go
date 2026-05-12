package registry

import (
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/natsbus"
)

// setupNATSRegistry creates a registry backed by NATS JetStream KV
// with an embedded NATS server.
func setupNATSRegistry(t *testing.T, cfg Config) *Registry {
	t.Helper()
	bus, err := natsbus.New(natsbus.Config{
		Embedded: true,
		StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}

	r, err := NewWithNATSKV(cfg, bus)
	if err != nil {
		bus.Close()
		t.Fatalf("create nats registry: %v", err)
	}

	t.Cleanup(func() {
		r.Stop()
		bus.Close()
	})

	return r
}

func TestNATSKVBackendType(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())
	if r.BackendType() != "nats_kv" {
		t.Fatalf("expected nats_kv backend, got %s", r.BackendType())
	}
}

func TestMemoryBackendType(t *testing.T) {
	r := New(DefaultConfig())
	if r.BackendType() != "memory" {
		t.Fatalf("expected memory backend, got %s", r.BackendType())
	}
}

func TestNATSKVRegisterAndQuery(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())

	rec := r.Register(NodeRecord{
		NodeID:      "nats-node-1",
		SSHEndpoint: "root@10.0.2.5:22",
		DaemonPort:  38080,
		Capabilities: map[string]string{"arch": "x86_64-linux"},
	})

	if rec.State != "healthy" {
		t.Fatalf("expected healthy, got %s", rec.State)
	}
	if rec.LeaseExpiry.IsZero() {
		t.Fatal("expected non-zero lease expiry")
	}

	nodes := r.ListNodes("")
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].NodeID != "nats-node-1" {
		t.Fatalf("expected nats-node-1, got %s", nodes[0].NodeID)
	}
	if nodes[0].SSHEndpoint != "root@10.0.2.5:22" {
		t.Fatalf("expected SSH endpoint root@10.0.2.5:22, got %s", nodes[0].SSHEndpoint)
	}
}

func TestNATSKVHeartbeat(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())

	r.Register(NodeRecord{NodeID: "hb-nats-node"})

	result, ok := r.Heartbeat("hb-nats-node", LoadInfo{CPUPercent: 45, MemPercent: 60, ActiveJobs: 3})
	if !ok {
		t.Fatal("heartbeat should succeed for registered node")
	}
	if result.Load.CPUPercent != 45 {
		t.Fatalf("expected cpu 45, got %f", result.Load.CPUPercent)
	}
	if result.Load.ActiveJobs != 3 {
		t.Fatalf("expected 3 active jobs, got %d", result.Load.ActiveJobs)
	}
	if result.State != "healthy" {
		t.Fatalf("expected healthy, got %s", result.State)
	}
}

func TestNATSKVHeartbeatUnknownNode(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())

	_, ok := r.Heartbeat("nonexistent", LoadInfo{})
	if ok {
		t.Fatal("heartbeat should fail for unregistered node")
	}
}

func TestNATSKVDeregister(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())
	r.Register(NodeRecord{NodeID: "dereg-node"})

	r.Deregister("dereg-node")

	nodes := r.ListNodes("")
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes after deregister, got %d", len(nodes))
	}
}

func TestNATSKVStats(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())
	r.Register(NodeRecord{NodeID: "stats-1"})
	r.Register(NodeRecord{NodeID: "stats-2"})
	r.Register(NodeRecord{NodeID: "stats-3"})

	stats := r.Stats()
	if stats.Total != 3 {
		t.Fatalf("expected 3 total, got %d", stats.Total)
	}
	if stats.Healthy != 3 {
		t.Fatalf("expected 3 healthy, got %d", stats.Healthy)
	}
}

func TestNATSKVReRegisterUpdatesEndpoint(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())

	r.Register(NodeRecord{NodeID: "update-node", SSHEndpoint: "root@10.0.1.5:22"})
	r.Register(NodeRecord{NodeID: "update-node", SSHEndpoint: "root@10.0.1.99:22"})

	node, ok := r.GetNode("update-node")
	if !ok {
		t.Fatal("node should exist")
	}
	if node.SSHEndpoint != "root@10.0.1.99:22" {
		t.Fatalf("expected updated endpoint, got %s", node.SSHEndpoint)
	}
}

func TestNATSKVLeaseExpiry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LeaseDuration = 20 * time.Millisecond
	cfg.StaleGrace = 30 * time.Millisecond
	cfg.RemovalThreshold = 50 * time.Millisecond
	cfg.SweepInterval = 5 * time.Millisecond

	r := setupNATSRegistry(t, cfg)
	r.Register(NodeRecord{NodeID: "expire-nats-node"})

	// Wait for lease to expire
	time.Sleep(25 * time.Millisecond)
	r.Sweep()

	node, ok := r.GetNode("expire-nats-node")
	if !ok {
		t.Fatal("node should still exist (stale)")
	}
	if node.State != "stale" {
		t.Fatalf("expected stale, got %s", node.State)
	}

	// Wait for grace period
	time.Sleep(35 * time.Millisecond)
	r.Sweep()

	node, ok = r.GetNode("expire-nats-node")
	if !ok {
		t.Fatal("node should still exist (unavailable)")
	}
	if node.State != "unavailable" {
		t.Fatalf("expected unavailable, got %s", node.State)
	}

	// Wait for removal threshold
	time.Sleep(55 * time.Millisecond)
	r.Sweep()

	_, ok = r.GetNode("expire-nats-node")
	if ok {
		t.Fatal("node should be removed after removal threshold")
	}
}

func TestNATSKVHealthScoreCalculation(t *testing.T) {
	r := setupNATSRegistry(t, DefaultConfig())
	r.Register(NodeRecord{NodeID: "score-node"})

	// Low load → high health score
	node, _ := r.Heartbeat("score-node", LoadInfo{CPUPercent: 10, MemPercent: 20})
	if node.HealthScore <= 0.5 {
		t.Fatalf("expected high health score for low load, got %f", node.HealthScore)
	}

	// High load → lower health score
	node, _ = r.Heartbeat("score-node", LoadInfo{CPUPercent: 90, MemPercent: 80, RecentFailures: 2})
	if node.HealthScore >= 0.5 {
		t.Fatalf("expected low health score for high load, got %f", node.HealthScore)
	}
}

func TestNATSKVPersistenceAcrossLookups(t *testing.T) {
	// Verify that data written via Put is immediately readable via Get
	r := setupNATSRegistry(t, DefaultConfig())

	r.Register(NodeRecord{
		NodeID:      "persist-node",
		SSHEndpoint: "root@10.0.5.5:22",
		DaemonPort:  39000,
		Capabilities: map[string]string{
			"arch":     "x86_64-linux",
			"hostname": "worker-5",
		},
	})

	// Immediate read-back
	node, ok := r.GetNode("persist-node")
	if !ok {
		t.Fatal("node should be readable immediately after register")
	}
	if node.DaemonPort != 39000 {
		t.Fatalf("expected daemon_port=39000, got %d", node.DaemonPort)
	}
	if node.Capabilities["hostname"] != "worker-5" {
		t.Fatalf("expected hostname=worker-5, got %s", node.Capabilities["hostname"])
	}
}
