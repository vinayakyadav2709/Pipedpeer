package coordinator

import (
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func mkNode(caps map[string]string, load registry.LoadInfo) registry.NodeRecord {
	return registry.NodeRecord{
		NodeID:       "n",
		Capabilities: caps,
		Load:         load,
		HealthScore:  1.0,
		State:        "healthy",
	}
}

// Between two equally loaded nodes, the one with more cores wins.
func TestScoreNodePrefersMoreCores(t *testing.T) {
	small := mkNode(map[string]string{"cpu_cores": "4"},
		registry.LoadInfo{CPUPercent: 50, MemPercent: 50, TotalCPUs: 4})
	big := mkNode(map[string]string{"cpu_cores": "32"},
		registry.LoadInfo{CPUPercent: 50, MemPercent: 50, TotalCPUs: 32})

	if scoreNode(big) <= scoreNode(small) {
		t.Fatalf("32-core node (%.4f) should outrank 4-core node (%.4f) at equal load",
			scoreNode(big), scoreNode(small))
	}
}

// Between identical machines, the faster clock wins.
func TestScoreNodePrefersFasterClockSpeed(t *testing.T) {
	slow := mkNode(map[string]string{"cpu_cores": "8", "cpu_mhz": "1800"},
		registry.LoadInfo{CPUPercent: 20, TotalCPUs: 8})
	fast := mkNode(map[string]string{"cpu_cores": "8", "cpu_mhz": "4800"},
		registry.LoadInfo{CPUPercent: 20, TotalCPUs: 8})

	if scoreNode(fast) <= scoreNode(slow) {
		t.Fatalf("4.8GHz node (%.4f) should outrank 1.8GHz node (%.4f)",
			scoreNode(fast), scoreNode(slow))
	}
}

// Load must outweigh raw capability: a saturated fast box loses to an idle
// modest one, or the scheduler would pile everything onto the best machine.
func TestScoreNodeLoadOutweighsCapability(t *testing.T) {
	busyBeast := mkNode(map[string]string{"cpu_cores": "64", "cpu_mhz": "4800"},
		registry.LoadInfo{CPUPercent: 95, MemPercent: 95, ActiveJobs: 8, TotalCPUs: 64})
	idleModest := mkNode(map[string]string{"cpu_cores": "4", "cpu_mhz": "2000"},
		registry.LoadInfo{CPUPercent: 2, MemPercent: 5, TotalCPUs: 4})

	if scoreNode(idleModest) <= scoreNode(busyBeast) {
		t.Fatalf("idle modest node (%.4f) should outrank saturated fast node (%.4f)",
			scoreNode(idleModest), scoreNode(busyBeast))
	}
}

// A node that reports no capability data must not be starved: it should score
// near an identically loaded node that does report, not far below it.
func TestScoreNodeUnknownCapabilityIsNeutral(t *testing.T) {
	silent := mkNode(nil, registry.LoadInfo{CPUPercent: 30, MemPercent: 30})
	average := mkNode(map[string]string{"cpu_cores": "16", "cpu_mhz": "2000"},
		registry.LoadInfo{
			CPUPercent: 30, MemPercent: 30, TotalCPUs: 16,
			AvailableMemBytes: 16 << 30,
		})

	diff := scoreNode(average) - scoreNode(silent)
	if diff < -0.05 || diff > 0.05 {
		t.Fatalf("node with no telemetry (%.4f) should score near an average node (%.4f); diff %.4f",
			scoreNode(silent), scoreNode(average), diff)
	}
}

// Health score scales everything: a degraded node loses to a healthy one at
// identical load.
func TestScoreNodeHealthScoreScales(t *testing.T) {
	healthy := mkNode(map[string]string{"cpu_cores": "8"}, registry.LoadInfo{CPUPercent: 10, TotalCPUs: 8})
	degraded := healthy
	degraded.HealthScore = 0.3

	if scoreNode(degraded) >= scoreNode(healthy) {
		t.Fatalf("degraded node (%.4f) should not outrank healthy node (%.4f)",
			scoreNode(degraded), scoreNode(healthy))
	}
}

// --- GPU scoring ---

func mkGPUNode(vram int64, computeCap string, gpus []registry.PerGPUInfo, util float64) registry.NodeRecord {
	return registry.NodeRecord{
		NodeID:       "g",
		Capabilities: map[string]string{"gpu": "nvidia", "gpu_compute_cap": computeCap},
		Load: registry.LoadInfo{
			GPUModel:       "test-gpu",
			GPUMemBytes:    vram,
			GPUUtilPercent: util,
			GPUs:           gpus,
		},
		HealthScore: 1.0,
	}
}

func TestScoreGPUNodeZeroWithoutGPU(t *testing.T) {
	cpuOnly := mkNode(map[string]string{"cpu_cores": "8"}, registry.LoadInfo{})
	if got := scoreGPUNode(cpuOnly); got != 0 {
		t.Fatalf("CPU-only node should score 0 on the GPU axis, got %.4f", got)
	}
}

func TestScoreGPUNodePrefersMoreVRAM(t *testing.T) {
	small := mkGPUNode(8<<30, "8.6", []registry.PerGPUInfo{{Index: 0, MemoryFreeBytes: 8 << 30}}, 0)
	large := mkGPUNode(48<<30, "8.6", []registry.PerGPUInfo{{Index: 0, MemoryFreeBytes: 48 << 30}}, 0)

	if scoreGPUNode(large) <= scoreGPUNode(small) {
		t.Fatalf("48GB GPU (%.4f) should outrank 8GB GPU (%.4f)",
			scoreGPUNode(large), scoreGPUNode(small))
	}
}

func TestScoreGPUNodePrefersHigherComputeCapability(t *testing.T) {
	older := mkGPUNode(24<<30, "6.1", []registry.PerGPUInfo{{Index: 0, MemoryFreeBytes: 24 << 30}}, 0)
	newer := mkGPUNode(24<<30, "9.0", []registry.PerGPUInfo{{Index: 0, MemoryFreeBytes: 24 << 30}}, 0)

	if scoreGPUNode(newer) <= scoreGPUNode(older) {
		t.Fatalf("sm_90 GPU (%.4f) should outrank sm_61 GPU (%.4f) at equal VRAM",
			scoreGPUNode(newer), scoreGPUNode(older))
	}
}

func TestScoreGPUNodePenalisesUtilisation(t *testing.T) {
	idle := mkGPUNode(24<<30, "8.6", []registry.PerGPUInfo{{Index: 0, MemoryFreeBytes: 24 << 30}}, 0)
	busy := mkGPUNode(24<<30, "8.6", []registry.PerGPUInfo{{Index: 0, MemoryFreeBytes: 24 << 30}}, 100)

	if scoreGPUNode(busy) >= scoreGPUNode(idle) {
		t.Fatalf("fully utilised GPU (%.4f) should not outrank an idle one (%.4f)",
			scoreGPUNode(busy), scoreGPUNode(idle))
	}
}

// --- Threshold helpers ---

func TestFreeCoresTracksUtilisation(t *testing.T) {
	n := mkNode(map[string]string{"cpu_cores": "16"}, registry.LoadInfo{CPUPercent: 75, TotalCPUs: 16})
	if got := freeCores(n); got < 3.9 || got > 4.1 {
		t.Fatalf("16 cores at 75%% should leave ~4 free, got %.2f", got)
	}
}

func TestFreeCoresNeverNegative(t *testing.T) {
	n := mkNode(map[string]string{"cpu_cores": "8"}, registry.LoadInfo{CPUPercent: 140, TotalCPUs: 8})
	if got := freeCores(n); got < 0 {
		t.Fatalf("free cores must not go negative, got %.2f", got)
	}
}

func TestBestFreeVRAMPicksLargestDevice(t *testing.T) {
	n := mkGPUNode(24<<30, "8.6", []registry.PerGPUInfo{
		{Index: 0, MemoryFreeBytes: 2 << 30},
		{Index: 1, MemoryFreeBytes: 20 << 30},
		{Index: 2, MemoryFreeBytes: 6 << 30},
	}, 0)
	if got := bestFreeVRAM(n); got != 20<<30 {
		t.Fatalf("expected the 20GiB device to be picked, got %d bytes", got)
	}
}

// With no per-device stats, fall back to the aggregate total-minus-used.
func TestBestFreeVRAMFallsBackToAggregate(t *testing.T) {
	n := registry.NodeRecord{
		Load: registry.LoadInfo{GPUModel: "x", GPUMemBytes: 16 << 30, GPUMemUsedBytes: 6 << 30},
	}
	if got := bestFreeVRAM(n); got != 10<<30 {
		t.Fatalf("expected 10GiB free from aggregate figures, got %d bytes", got)
	}
}

func TestHasGPUDetection(t *testing.T) {
	cases := []struct {
		name string
		rec  registry.NodeRecord
		want bool
	}{
		{"capability", registry.NodeRecord{Capabilities: map[string]string{"gpu": "nvidia"}}, true},
		{"load model", registry.NodeRecord{Load: registry.LoadInfo{GPUModel: "A100"}}, true},
		{"per-device", registry.NodeRecord{Load: registry.LoadInfo{GPUs: []registry.PerGPUInfo{{Index: 0}}}}, true},
		{"cpu only", registry.NodeRecord{Capabilities: map[string]string{"arch": "x86_64-linux"}}, false},
		{"empty", registry.NodeRecord{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasGPU(tc.rec); got != tc.want {
				t.Fatalf("hasGPU = %v, want %v", got, tc.want)
			}
		})
	}
}

// Repeated execution failures must back off, or a task that always fails
// hammers the cluster — and since each attempt rebuilds and exports a nix
// closure, an unthrottled loop can fill the disk.
func TestExecRetryBackoffGrowsAndCaps(t *testing.T) {
	first := execRetryBackoff(0, 1)
	second := execRetryBackoff(0, 2)
	if second <= first {
		t.Fatalf("backoff should grow: attempt 1 = %s, attempt 2 = %s", first, second)
	}

	capped := execRetryBackoff(0, 50)
	if capped > 2*time.Minute {
		t.Fatalf("backoff should be capped at 2m, got %s", capped)
	}
	if execRetryBackoff(0, 100) != capped {
		t.Fatalf("backoff should stay at the ceiling once reached, got %s then %s",
			capped, execRetryBackoff(0, 100))
	}
}
