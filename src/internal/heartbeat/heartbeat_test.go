package heartbeat

import (
	"testing"
	"time"

	"net/http/httptest"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func TestHeartbeatLifecycle(t *testing.T) {
	// Use real registry — no fake handlers
	reg := registry.New(registry.Config{
		LeaseDuration:    500 * time.Millisecond,
		StaleGrace:       500 * time.Millisecond,
		RemovalThreshold: 1 * time.Second,
		SweepInterval:    100 * time.Millisecond,
	})
	reg.Start()
	defer reg.Stop()
	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	id := identity.NodeIdentity{
		NodeID:   "test-heartbeat-node",
		Hostname: "test-host",
		Arch:     "x86_64-linux",
	}

	client := NewClient(srv.URL, id, "root@10.0.1.5:22", 38080)
	client.interval = 50 * time.Millisecond // fast for testing

	if err := client.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Wait for register + at least 2 heartbeats
	time.Sleep(150 * time.Millisecond)

	// Verify the node actually exists in the real registry
	nodes := reg.ListNodes("")
	found := false
	for _, n := range nodes {
		if n.NodeID == "test-heartbeat-node" {
			found = true
			if n.State != "healthy" {
				t.Fatalf("expected healthy state, got %s", n.State)
			}
		}
	}
	if !found {
		t.Fatalf("heartbeat client did not register with real registry")
	}

	client.Stop()

	// After Stop(), node should be deregistered from registry
	nodesAfter := reg.ListNodes("")
	for _, n := range nodesAfter {
		if n.NodeID == "test-heartbeat-node" {
			t.Fatalf("node should be deregistered after Stop(), but still found in registry")
		}
	}
}

func TestHeartbeatReRegistersOn404(t *testing.T) {
	// Use real registry. Simulate "forgotten node" by deregistering externally.
	reg := registry.New(registry.Config{
		LeaseDuration:    500 * time.Millisecond,
		StaleGrace:       500 * time.Millisecond,
		RemovalThreshold: 1 * time.Second,
		SweepInterval:    100 * time.Millisecond,
	})
	reg.Start()
	defer reg.Stop()
	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	id := identity.NodeIdentity{NodeID: "reregister-node", Hostname: "test", Arch: "x86_64-linux"}
	client := NewClient(srv.URL, id, "root@10.0.1.5:22", 38080)
	client.interval = 50 * time.Millisecond

	_ = client.Start()

	// Wait for initial registration + at least 1 heartbeat
	time.Sleep(80 * time.Millisecond)

	// Externally deregister the node — next heartbeat will get 404
	reg.Deregister("reregister-node")

	// Verify it's gone
	nodes := reg.ListNodes("")
	for _, n := range nodes {
		if n.NodeID == "reregister-node" {
			t.Fatalf("node should have been deregistered")
		}
	}

	// Wait for client to detect 404 and re-register
	time.Sleep(150 * time.Millisecond)

	// Node should be back in registry (re-registered after 404)
	nodes = reg.ListNodes("")
	found := false
	for _, n := range nodes {
		if n.NodeID == "reregister-node" {
			found = true
		}
	}
	if !found {
		t.Fatalf("client did not re-register after 404 — node not found in registry")
	}

	client.Stop()
}

func TestCollectLoadReturnsValues(t *testing.T) {
	load := CollectLoad(3, 1024*1024*100) // 3 jobs, 100MB reserved
	// On Linux: should return real values. On non-Linux: returns 0.
	// Either way, values should be non-negative.
	if load.CPUPercent < 0 || load.MemPercent < 0 {
		t.Fatalf("load values should be non-negative: cpu=%.1f mem=%.1f", load.CPUPercent, load.MemPercent)
	}
	if load.ActiveJobs != 3 {
		t.Fatalf("expected ActiveJobs=3, got %d", load.ActiveJobs)
	}
	if load.ReservedMemBytes != 1024*1024*100 {
		t.Fatalf("expected ReservedMemBytes=100MB, got %d", load.ReservedMemBytes)
	}
	if load.TotalCPUs < 1 {
		t.Fatalf("expected at least 1 CPU, got %d", load.TotalCPUs)
	}
}
