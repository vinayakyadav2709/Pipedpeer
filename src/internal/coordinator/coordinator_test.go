package coordinator

import (
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func testIdentity() identity.NodeIdentity {
	return identity.NodeIdentity{
		NodeID:   "self-node-uuid",
		Hostname: "test-host",
		Arch:     "x86_64-linux",
	}
}

func TestSelectsLeastLoadedNode(t *testing.T) {
	// Fake mDNS discovery returns two nodes
	fakeLAN := func() []registry.NodeRecord {
		return []registry.NodeRecord{
			{NodeID: "node-heavy", SSHEndpoint: "root@10.0.1.5:22", DaemonPort: 38080,
				Load: registry.LoadInfo{CPUPercent: 80, MemPercent: 70, ActiveJobs: 5}, HealthScore: 0.8, State: "healthy"},
			{NodeID: "node-light", SSHEndpoint: "root@10.0.1.6:22", DaemonPort: 38080,
				Load: registry.LoadInfo{CPUPercent: 10, MemPercent: 20, ActiveJobs: 0}, HealthScore: 1.0, State: "healthy"},
		}
	}

	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		SelfLoad:     registry.LoadInfo{CPUPercent: 50, MemPercent: 60, ActiveJobs: 3},
		DiscoverFn:   fakeLAN,
	})

	decision := c.FindNode()

	if decision.ChosenNode.NodeID != "node-light" {
		t.Fatalf("expected node-light (least loaded), got %s (scores: %v)", decision.ChosenNode.NodeID, decision.Candidates)
	}
	if decision.Source != SourceDiscovery {
		t.Fatalf("expected discovery source, got %s", decision.Source)
	}
}

func TestFallsBackToSelfWhenNoOtherNodes(t *testing.T) {
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		DiscoverFn:   func() []registry.NodeRecord { return nil },
	})

	decision := c.FindNode()

	if decision.ChosenNode.NodeID != "self-node-uuid" {
		t.Fatalf("expected self-node, got %s", decision.ChosenNode.NodeID)
	}
	if decision.Source != SourceSelf {
		t.Fatalf("expected self source, got %s", decision.Source)
	}
}

func TestSelfNodeUsesLoopback(t *testing.T) {
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
	})

	selfNode := registry.NodeRecord{NodeID: "self-node-uuid"}
	endpoint := c.ResolveEndpoint(selfNode)

	if endpoint != "root@localhost:22" {
		t.Fatalf("expected loopback for self, got %s", endpoint)
	}

	otherNode := registry.NodeRecord{NodeID: "other-node", SSHEndpoint: "root@10.0.1.5:22"}
	endpoint = c.ResolveEndpoint(otherNode)

	if endpoint != "root@10.0.1.5:22" {
		t.Fatalf("expected remote endpoint, got %s", endpoint)
	}
}

func TestRegistryIntegrationAndCacheFallback(t *testing.T) {
	// Start a real registry
	reg := registry.New(registry.DefaultConfig())
	srv := httptest.NewServer(reg.Handler())

	// Register a node in registry
	reg.Register(registry.NodeRecord{
		NodeID: "registry-node", SSHEndpoint: "root@10.0.1.100:22", DaemonPort: 38080,
		Load: registry.LoadInfo{CPUPercent: 5}, HealthScore: 1.0, State: "healthy",
	})

	// Self has higher load so registry-node wins
	selfLoad := registry.LoadInfo{CPUPercent: 50, MemPercent: 60, ActiveJobs: 3}

	c := New(Config{
		RegistryURL:  srv.URL,
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		SelfLoad:     selfLoad,
		DiscoverFn:   func() []registry.NodeRecord { return nil },
	})

	// First call: registry is up — should select registry-node (lower load)
	decision := c.FindNode()
	if decision.ChosenNode.NodeID != "registry-node" {
		t.Fatalf("expected registry-node, got %s", decision.ChosenNode.NodeID)
	}
	if decision.DegradedMode {
		t.Fatal("should not be degraded when registry is up")
	}

	// Shut down registry
	srv.Close()

	// Second call: registry is down — should use cache
	decision = c.FindNode()
	if decision.DegradedMode != true {
		t.Fatal("should be degraded when registry is down")
	}
	if decision.ChosenNode.NodeID != "registry-node" {
		t.Fatalf("expected registry-node from cache, got %s", decision.ChosenNode.NodeID)
	}
	if decision.Source != SourceCache {
		t.Fatalf("expected cache source, got %s", decision.Source)
	}
}

func TestMergeNodeDeduplication(t *testing.T) {
	list := []ScoredNode{
		{Node: registry.NodeRecord{NodeID: "node-1"}, Source: SourceDiscovery},
	}

	// Registry version should override discovery
	list = mergeNode(list, ScoredNode{
		Node:   registry.NodeRecord{NodeID: "node-1", SSHEndpoint: "updated"},
		Source: SourceRegistry,
	})

	if len(list) != 1 {
		t.Fatalf("expected 1 after dedup, got %d", len(list))
	}
	if list[0].Source != SourceRegistry {
		t.Fatalf("expected registry source after merge, got %s", list[0].Source)
	}
	if list[0].Node.SSHEndpoint != "updated" {
		t.Fatal("expected updated endpoint from registry version")
	}
}

func TestDiagnosticsRecordAllCandidates(t *testing.T) {
	fakeLAN := func() []registry.NodeRecord {
		return []registry.NodeRecord{
			{NodeID: "lan-1", SSHEndpoint: "root@10.0.1.5:22", HealthScore: 1.0, State: "healthy"},
			{NodeID: "lan-2", SSHEndpoint: "root@10.0.1.6:22", HealthScore: 1.0, State: "healthy"},
		}
	}

	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		DiscoverFn:   fakeLAN,
	})

	decision := c.FindNode()

	// Should have 3 candidates: lan-1, lan-2, self
	if len(decision.Candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(decision.Candidates))
	}
}

func TestStaleCacheReturnsSelf(t *testing.T) {
	c := New(Config{
		RegistryURL:  "http://127.0.0.1:1", // unreachable
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		MaxCacheAge:  1 * time.Millisecond, // very short
		DiscoverFn:   func() []registry.NodeRecord { return nil },
	})

	// Prime cache
	c.updateCache([]registry.NodeRecord{
		{NodeID: "cached-node", SSHEndpoint: "root@10.0.1.5:22", HealthScore: 1.0, State: "healthy"},
	})

	// Wait for cache to go stale
	time.Sleep(5 * time.Millisecond)

	decision := c.FindNode()

	// Cache is stale, registry unreachable, discovery returns nothing → self
	if decision.ChosenNode.NodeID != "self-node-uuid" {
		t.Fatalf("expected self-node when cache stale, got %s", decision.ChosenNode.NodeID)
	}
	if !decision.DegradedMode {
		t.Fatal("should be degraded when registry down")
	}
}

func TestDiscoveryAndRegistryOverlapDedup(t *testing.T) {
	// Same node found via both mDNS and registry
	reg := registry.New(registry.DefaultConfig())
	srv := httptest.NewServer(reg.Handler())
	defer srv.Close()

	sharedNode := registry.NodeRecord{
		NodeID: "shared-node", SSHEndpoint: "root@10.0.1.5:22", DaemonPort: 38080,
		Load: registry.LoadInfo{CPUPercent: 10}, HealthScore: 1.0, State: "healthy",
	}
	reg.Register(sharedNode)

	fakeLAN := func() []registry.NodeRecord {
		return []registry.NodeRecord{
			{NodeID: "shared-node", SSHEndpoint: "root@10.0.1.5:22", DaemonPort: 38080,
				Load: registry.LoadInfo{CPUPercent: 15}, HealthScore: 0.5, State: "healthy"},
		}
	}

	c := New(Config{
		RegistryURL:  srv.URL,
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		SelfLoad:     registry.LoadInfo{CPUPercent: 90, MemPercent: 90},
		DiscoverFn:   fakeLAN,
	})

	decision := c.FindNode()

	// Should have 2 candidates (shared-node deduped + self), not 3
	if len(decision.Candidates) != 2 {
		t.Fatalf("expected 2 candidates (deduped), got %d", len(decision.Candidates))
	}

	// Registry source should win over discovery
	for _, c := range decision.Candidates {
		if c.Node.NodeID == "shared-node" && c.Source != SourceRegistry {
			t.Fatalf("expected registry source for deduped node, got %s", c.Source)
		}
	}
}

func TestRegistryDownNoCacheFallsToSelf(t *testing.T) {
	c := New(Config{
		RegistryURL:  "http://127.0.0.1:1", // unreachable
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		DiscoverFn:   func() []registry.NodeRecord { return nil },
	})

	decision := c.FindNode()

	if decision.ChosenNode.NodeID != "self-node-uuid" {
		t.Fatalf("expected self when registry down + no cache, got %s", decision.ChosenNode.NodeID)
	}
	if !decision.DegradedMode {
		t.Fatal("should be degraded")
	}
	if decision.Source != SourceSelf {
		t.Fatalf("expected self source, got %s", decision.Source)
	}
}

func TestExplicitRemoteBypassesCoordinator(t *testing.T) {
	// When --remote is provided explicitly, coordinator shouldn't run
	// This tests the ResolveEndpoint for a non-self node
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
	})

	explicit := registry.NodeRecord{
		NodeID:      "explicit-node",
		SSHEndpoint: "root@192.168.1.100:22",
	}
	endpoint := c.ResolveEndpoint(explicit)

	if endpoint != "root@192.168.1.100:22" {
		t.Fatalf("explicit node should use its own endpoint, got %s", endpoint)
	}
}

// Gap 2: Coordinator rejects nodes with insufficient available memory
func TestCoordinatorRejectsInsufficientMemory(t *testing.T) {
	// Node A has only 500MB available, node B has 8GB available
	fakeLAN := func() []registry.NodeRecord {
		return []registry.NodeRecord{
			{
				NodeID: "low-mem-node", SSHEndpoint: "root@10.0.1.5:22", DaemonPort: 38080,
				Load: registry.LoadInfo{
					CPUPercent: 10, MemPercent: 90,
					AvailableMemBytes: 500 * 1024 * 1024, // 500MB
					TotalMemBytes:     8 * 1024 * 1024 * 1024,
				},
				HealthScore: 1.0, State: "healthy",
			},
			{
				NodeID: "high-mem-node", SSHEndpoint: "root@10.0.1.6:22", DaemonPort: 38080,
				Load: registry.LoadInfo{
					CPUPercent: 50, MemPercent: 40,
					AvailableMemBytes: 8 * 1024 * 1024 * 1024, // 8GB
					TotalMemBytes:     16 * 1024 * 1024 * 1024,
				},
				HealthScore: 1.0, State: "healthy",
			},
		}
	}

	c := New(Config{
		SelfIdentity:     testIdentity(),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       38080,
		SelfLoad:         registry.LoadInfo{CPUPercent: 90, MemPercent: 90, AvailableMemBytes: 200 * 1024 * 1024},
		RequiredMemBytes: 2 * 1024 * 1024 * 1024, // Need 2GB
		DiscoverFn:       fakeLAN,
	})

	decision := c.FindNode()

	// low-mem-node (500MB) should be rejected, self (200MB) should be rejected
	// high-mem-node (8GB) should be chosen
	if decision.ChosenNode.NodeID != "high-mem-node" {
		t.Fatalf("expected high-mem-node, got %s", decision.ChosenNode.NodeID)
	}

	// Verify low-mem-node was rejected with reason
	for _, cand := range decision.Candidates {
		if cand.Node.NodeID == "low-mem-node" {
			if !cand.Rejected {
				t.Fatal("low-mem-node should have been rejected")
			}
			if cand.RejectReason == "" {
				t.Fatal("rejected node should have a reason")
			}
		}
	}
}

// Coordinator with RequiredMemBytes=0 should not filter anyone
func TestCoordinatorNoMemReqSkipsFilter(t *testing.T) {
	fakeLAN := func() []registry.NodeRecord {
		return []registry.NodeRecord{
			{
				NodeID: "tiny-node", SSHEndpoint: "root@10.0.1.5:22", DaemonPort: 38080,
				Load: registry.LoadInfo{
					CPUPercent: 10, AvailableMemBytes: 100 * 1024 * 1024, // 100MB
				},
				HealthScore: 1.0, State: "healthy",
			},
		}
	}

	c := New(Config{
		SelfIdentity:     testIdentity(),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       38080,
		SelfLoad:         registry.LoadInfo{CPUPercent: 90, MemPercent: 90},
		RequiredMemBytes: 0, // no requirement
		DiscoverFn:       fakeLAN,
	})

	decision := c.FindNode()

	// tiny-node should be chosen (no mem filter applied)
	if decision.ChosenNode.NodeID != "tiny-node" {
		t.Fatalf("expected tiny-node when no mem req, got %s", decision.ChosenNode.NodeID)
	}

	// Verify tiny-node was NOT rejected (no mem filter when RequiredMemBytes=0)
	for _, cand := range decision.Candidates {
		if cand.Node.NodeID == "tiny-node" && cand.Rejected {
			t.Fatal("tiny-node should NOT be rejected when RequiredMemBytes=0")
		}
	}
}

// All remote nodes AND self rejected → no candidate chosen (task should queue)
func TestCoordinatorAllNodesRejectedNoneChosen(t *testing.T) {
	fakeLAN := func() []registry.NodeRecord {
		return []registry.NodeRecord{
			{
				NodeID: "small-node", SSHEndpoint: "root@10.0.1.5:22",
				Load: registry.LoadInfo{
					CPUPercent: 10, AvailableMemBytes: 100 * 1024 * 1024, // 100MB
				},
				HealthScore: 1.0, State: "healthy",
			},
		}
	}

	c := New(Config{
		SelfIdentity:     testIdentity(),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       38080,
		SelfLoad:         registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 50 * 1024 * 1024}, // self also low
		RequiredMemBytes: 10 * 1024 * 1024 * 1024,                                                // need 10GB, nobody has it
		DiscoverFn:       fakeLAN,
	})

	decision := c.FindNode()

	// All nodes rejected (including self) → no viable candidate
	allRejected := true
	for _, cand := range decision.Candidates {
		if !cand.Rejected {
			allRejected = false
		}
	}
	if !allRejected {
		t.Fatal("all candidates should be rejected when nobody has 10GB")
	}

	// Verify small-node WAS rejected
	for _, cand := range decision.Candidates {
		if cand.Node.NodeID == "small-node" {
			if !cand.Rejected {
				t.Fatal("small-node (100MB) should be rejected when 10GB required")
			}
			if cand.RejectReason == "" {
				t.Fatal("rejected node should have a reason")
			}
		}
	}

	// Verify self-node WAS also rejected
	for _, cand := range decision.Candidates {
		if cand.Node.NodeID == testIdentity().NodeID {
			if !cand.Rejected {
				t.Fatal("self-node (50MB) should also be rejected when 10GB required")
			}
		}
	}
}

// GPU scheduling tests

func gpuNode(id, gpuType string) registry.NodeRecord {
	return registry.NodeRecord{
		NodeID: id, SSHEndpoint: "root@10.0.1.5:22", DaemonPort: 38080,
		Capabilities: map[string]string{"gpu": gpuType, "gpu_count": "1"},
		Load:         registry.LoadInfo{CPUPercent: 10, MemPercent: 20, ActiveJobs: 0},
		HealthScore:  1.0, State: "healthy",
	}
}

func gpuNodeMulti(id, gpuType string, count int) registry.NodeRecord {
	return registry.NodeRecord{
		NodeID: id, SSHEndpoint: "root@10.0.1.5:22", DaemonPort: 38080,
		Capabilities: map[string]string{"gpu": gpuType, "gpu_count": strconv.Itoa(count)},
		Load:         registry.LoadInfo{CPUPercent: 10, MemPercent: 20, ActiveJobs: 0},
		HealthScore:  1.0, State: "healthy",
	}
}

func cpuNode(id string) registry.NodeRecord {
	return registry.NodeRecord{
		NodeID: id, SSHEndpoint: "root@10.0.1.6:22", DaemonPort: 38080,
		Load:        registry.LoadInfo{CPUPercent: 10, MemPercent: 20, ActiveJobs: 0},
		HealthScore: 1.0, State: "healthy",
	}
}

func TestGPUNodeChosenFirstWhenPreferGPU(t *testing.T) {
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		PreferGPU:    true,
		SelfLoad:     registry.LoadInfo{CPUPercent: 50, MemPercent: 60, ActiveJobs: 3},
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{
				gpuNode("gpu-node", "nvidia"),
				cpuNode("cpu-node"),
			}
		},
	})

	decision := c.FindNode()

	// GPU node should be chosen by FindNode (has higher score due to lower load + GPU preference in PlaceWithRetry)
	if decision.ChosenNode.NodeID != "gpu-node" {
		t.Fatalf("expected GPU node to be chosen, got %s (scores: gpu=%v, cpu=%v)",
			decision.ChosenNode.NodeID,
			scoreFor(decision.Candidates, "gpu-node"),
			scoreFor(decision.Candidates, "cpu-node"))
	}
}

func scoreFor(candidates []ScoredNode, id string) float64 {
	for _, c := range candidates {
		if c.Node.NodeID == id {
			return c.Score
		}
	}
	return -1
}

func TestRequireGPURejectsCPUNodes(t *testing.T) {
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		RequireGPU:   true,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{
				gpuNode("gpu-node", "nvidia"),
				cpuNode("cpu-node"),
			}
		},
	})

	decision := c.FindNode()

	if decision.ChosenNode.NodeID != "gpu-node" {
		t.Fatalf("expected gpu-node when RequireGPU=true, got %s", decision.ChosenNode.NodeID)
	}

	// CPU node should be rejected
	for _, cand := range decision.Candidates {
		if cand.Node.NodeID == "cpu-node" && !cand.Rejected {
			t.Fatal("cpu-node should be rejected when RequireGPU=true")
		}
	}
}

func TestRequireGPUWithNoGPUNodeFails(t *testing.T) {
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		RequireGPU:   true,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{
				cpuNode("only-cpu-1"),
				cpuNode("only-cpu-2"),
			}
		},
	})

	decision := c.FindNode()

	// No node should be chosen since all are CPU
	if decision.ChosenNode.NodeID != "" {
		t.Fatalf("expected no chosen node when RequireGPU=true and no GPU nodes, got %s", decision.ChosenNode.NodeID)
	}

	// Both CPU nodes should be rejected
	for _, cand := range decision.Candidates {
		if !cand.Rejected {
			t.Fatal("all candidates should be rejected when RequireGPU=true and no GPU nodes")
		}
	}
}

func TestPreferGPUFallsBackToCPU(t *testing.T) {
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		PreferGPU:    true,
		SelfLoad:     registry.LoadInfo{CPUPercent: 50, MemPercent: 60, ActiveJobs: 3},
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{
				cpuNode("only-cpu"),
			}
		},
	})

	decision := c.FindNode()

	// The discovered CPU node should be chosen (fallback behavior) over self (higher load)
	if decision.ChosenNode.NodeID != "only-cpu" {
		t.Fatalf("expected fallback to CPU when PreferGPU=true but no GPU nodes, got %s", decision.ChosenNode.NodeID)
	}

	// CPU node should not be rejected
	for _, cand := range decision.Candidates {
		if cand.Node.NodeID == "only-cpu" && cand.Rejected {
			t.Fatal("CPU node should not be rejected when PreferGPU=true")
		}
	}
}

func TestGPUPreferenceDoesNotAffectNoGPUScenario(t *testing.T) {
	// When no GPU is requested (no PreferGPU, no RequireGPU), all nodes are equal
	c := New(Config{
		SelfIdentity: testIdentity(),
		SelfSSH:      "root@localhost:22",
		SelfDaemon:   38080,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{
				gpuNode("gpu-node", "nvidia"),
				cpuNode("cpu-node"),
			}
		},
	})

	decision := c.FindNode()

	// Without PreferGPU, the choice is based on load alone (both have same load here)
	// So either is acceptable, just not rejected
	for _, cand := range decision.Candidates {
		if cand.Rejected {
			t.Fatal("neither node should be rejected when no GPU config is set")
		}
	}
}

// TestNoSelfExcludesLocalNodeFromAnySource: --no-self must exclude the local
// machine even when it surfaces via discovery or the daemon node store, not
// just via buildSelfNode. Otherwise "distribute to OTHER devices" silently
// keeps placing on the local machine.
func TestNoSelfExcludesLocalNodeFromAnySource(t *testing.T) {
	c := New(Config{
		SelfIdentity: testIdentity(), // NodeID "self-node-uuid"
		SelfSSH:      "root@localhost:22",
		NoSelf:       true,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{
				// The local machine also announces itself on the LAN.
				{NodeID: "self-node-uuid", SSHEndpoint: "root@localhost:22", DaemonPort: 38080, State: "healthy"},
				{NodeID: "remote-node", SSHEndpoint: "root@10.0.1.7:22", DaemonPort: 38080, State: "healthy"},
			}
		},
	})

	decision := c.FindNode()
	if decision.ChosenNode.NodeID == "self-node-uuid" {
		t.Fatalf("--no-self must not place on the local machine, got %s", decision.ChosenNode.NodeID)
	}
	if decision.ChosenNode.NodeID != "remote-node" {
		t.Fatalf("expected placement on the remote node, got %s", decision.ChosenNode.NodeID)
	}
	for _, cand := range decision.Candidates {
		if cand.Node.NodeID == "self-node-uuid" {
			t.Fatal("self node must not appear as a candidate under --no-self")
		}
	}
}
