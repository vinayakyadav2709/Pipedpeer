package registry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRegisterAndQuery(t *testing.T) {
	r := New(DefaultConfig())

	rec := r.Register(NodeRecord{
		NodeID:       "node-1",
		SSHEndpoint:  "root@10.0.1.5:22",
		DaemonPort:   38080,
		Capabilities: map[string]string{"arch": "x86_64-linux"},
	})

	if rec.State != "healthy" {
		t.Fatalf("expected healthy state, got %s", rec.State)
	}
	if rec.LeaseExpiry.IsZero() {
		t.Fatal("expected non-zero lease expiry")
	}

	nodes := r.ListNodes("")
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].NodeID != "node-1" {
		t.Fatalf("expected node-1, got %s", nodes[0].NodeID)
	}
}

func TestHeartbeatRenewsLease(t *testing.T) {
	r := New(DefaultConfig())

	r.Register(NodeRecord{NodeID: "node-1", SSHEndpoint: "root@10.0.1.5:22"})

	time.Sleep(10 * time.Millisecond)

	result, ok := r.Heartbeat("node-1", LoadInfo{CPUPercent: 25, MemPercent: 40, ActiveJobs: 1})
	if !ok {
		t.Fatal("heartbeat should succeed for registered node")
	}
	if result.State != "healthy" {
		t.Fatalf("expected healthy after heartbeat, got %s", result.State)
	}
	if result.Load.CPUPercent != 25 {
		t.Fatalf("expected cpu 25, got %f", result.Load.CPUPercent)
	}
}

func TestHeartbeatUnknownNodeFails(t *testing.T) {
	r := New(DefaultConfig())

	_, ok := r.Heartbeat("nonexistent", LoadInfo{})
	if ok {
		t.Fatal("heartbeat should fail for unregistered node")
	}
}

func TestLeaseExpiry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LeaseDuration = 20 * time.Millisecond
	cfg.StaleGrace = 30 * time.Millisecond
	cfg.RemovalThreshold = 50 * time.Millisecond
	cfg.SweepInterval = 5 * time.Millisecond

	r := New(cfg)
	r.Register(NodeRecord{NodeID: "node-1", SSHEndpoint: "root@10.0.1.5:22"})

	// Wait for lease to expire
	time.Sleep(25 * time.Millisecond)
	r.sweep()

	node, ok := r.GetNode("node-1")
	if !ok {
		t.Fatal("node should still exist (just stale)")
	}
	if node.State != "stale" {
		t.Fatalf("expected stale, got %s", node.State)
	}

	// Wait for grace period
	time.Sleep(35 * time.Millisecond)
	r.sweep()

	node, ok = r.GetNode("node-1")
	if !ok {
		t.Fatal("node should still exist (unavailable)")
	}
	if node.State != "unavailable" {
		t.Fatalf("expected unavailable, got %s", node.State)
	}

	// Wait for removal
	time.Sleep(55 * time.Millisecond)
	r.sweep()

	_, ok = r.GetNode("node-1")
	if ok {
		t.Fatal("node should be removed after removal threshold")
	}
}

func TestDeregister(t *testing.T) {
	r := New(DefaultConfig())
	r.Register(NodeRecord{NodeID: "node-1", SSHEndpoint: "root@10.0.1.5:22"})

	r.Deregister("node-1")

	nodes := r.ListNodes("")
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes after deregister, got %d", len(nodes))
	}
}

func TestStats(t *testing.T) {
	r := New(DefaultConfig())
	r.Register(NodeRecord{NodeID: "node-1"})
	r.Register(NodeRecord{NodeID: "node-2"})

	stats := r.Stats()
	if stats.Total != 2 || stats.Healthy != 2 {
		t.Fatalf("expected 2 healthy, got %+v", stats)
	}
}

func TestListNodesFilterByState(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LeaseDuration = 10 * time.Millisecond
	cfg.StaleGrace = 10 * time.Minute // long grace so it stays stale
	r := New(cfg)

	r.Register(NodeRecord{NodeID: "node-1"})

	// Wait for node-1's lease to expire, then sweep to mark it stale
	time.Sleep(15 * time.Millisecond)
	r.Sweep()

	// Verify node-1 is stale
	node1, ok := r.GetNode("node-1")
	if !ok {
		t.Fatal("node-1 should still exist")
	}
	if node1.State != "stale" {
		t.Fatalf("expected stale, got %s", node1.State)
	}

	// Register a fresh healthy node
	r.Register(NodeRecord{NodeID: "node-2"})

	healthy := r.ListNodes("healthy")
	if len(healthy) != 1 || healthy[0].NodeID != "node-2" {
		t.Fatalf("expected only node-2 as healthy, got %v", healthy)
	}
}

func TestHTTPRegisterAndNodes(t *testing.T) {
	r := New(DefaultConfig())
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	// Register
	body := `{"node_id":"http-node","ssh_endpoint":"root@10.0.1.5:22","daemon_port":38080}`
	resp, err := http.Post(srv.URL+"/v1/register", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("register request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Query
	resp, err = http.Get(srv.URL + "/v1/nodes")
	if err != nil {
		t.Fatalf("nodes request failed: %v", err)
	}
	defer resp.Body.Close()

	var nodes []NodeRecord
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		t.Fatalf("decode nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "http-node" {
		t.Fatalf("unexpected nodes: %+v", nodes)
	}
}

func TestHTTPHeartbeatAndDeregister(t *testing.T) {
	r := New(DefaultConfig())
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	// Register first
	body := `{"node_id":"hb-node","ssh_endpoint":"root@10.0.1.5:22","daemon_port":38080}`
	resp, err := http.Post(srv.URL+"/v1/register", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	resp.Body.Close()

	// Heartbeat via HTTP
	hbBody := `{"node_id":"hb-node","load":{"cpu_percent":30,"memory_percent":50,"active_jobs":2}}`
	resp, err = http.Post(srv.URL+"/v1/heartbeat", "application/json", strings.NewReader(hbBody))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("heartbeat expected 200, got %d", resp.StatusCode)
	}

	// Verify load updated
	nodes := r.ListNodes("")
	if len(nodes) != 1 || nodes[0].Load.CPUPercent != 30 {
		t.Fatalf("heartbeat didn't update load: %+v", nodes)
	}

	// Deregister via HTTP
	deregBody := `{"node_id":"hb-node"}`
	resp, err = http.Post(srv.URL+"/v1/deregister", "application/json", strings.NewReader(deregBody))
	if err != nil {
		t.Fatalf("deregister: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("deregister expected 200, got %d", resp.StatusCode)
	}

	nodes = r.ListNodes("")
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes after deregister, got %d", len(nodes))
	}
}

func TestEmptyNodeListReturnsEmptySlice(t *testing.T) {
	r := New(DefaultConfig())
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/nodes")
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	defer resp.Body.Close()

	var nodes []NodeRecord
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nodes == nil {
		t.Fatal("expected empty slice, got nil")
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestReRegisterUpdatesEndpoint(t *testing.T) {
	r := New(DefaultConfig())

	r.Register(NodeRecord{NodeID: "node-1", SSHEndpoint: "root@10.0.1.5:22"})
	r.Register(NodeRecord{NodeID: "node-1", SSHEndpoint: "root@10.0.1.99:22"})

	nodes := r.ListNodes("")
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after re-register, got %d", len(nodes))
	}
	if nodes[0].SSHEndpoint != "root@10.0.1.99:22" {
		t.Fatalf("expected updated endpoint, got %s", nodes[0].SSHEndpoint)
	}
}

func TestHeartbeatUnknownNodeHTTPReturns404(t *testing.T) {
	r := New(DefaultConfig())
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	body := `{"node_id":"ghost","load":{}}`
	resp, err := http.Post(srv.URL+"/v1/heartbeat", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404 for unknown node, got %d", resp.StatusCode)
	}
}
