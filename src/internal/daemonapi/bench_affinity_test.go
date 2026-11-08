package daemonapi

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestMeasureThroughputFollowsAffinity re-execs this test binary pinned to one
// CPU via taskset and verifies that measureThroughput reports Cores == 1. Go's
// runtime.NumCPU() follows cpuset/affinity, so this should pass even before any
// code change.
func TestMeasureThroughputFollowsAffinity(t *testing.T) {
	if _, err := exec.LookPath("taskset"); err != nil {
		t.Skip("taskset not on PATH")
	}
	if runtime.NumCPU() < 2 {
		t.Skip("need at least 2 CPUs to pin to one")
	}

	cmd := exec.Command("taskset", "-c", "0", os.Args[0], "-test.run=^TestAffinityChild$")
	cmd.Env = append(os.Environ(), "PP_AFFINITY_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("affinity child failed:\n%s\n%v", out, err)
	}
}

// TestAffinityChild is the child half of TestMeasureThroughputFollowsAffinity.
// It is only meaningful when re-exec'd under taskset -c 0 by the parent test.
func TestAffinityChild(t *testing.T) {
	if os.Getenv("PP_AFFINITY_CHILD") != "1" {
		t.Skip("only runs as re-exec child under taskset")
	}
	got := measureThroughput(50 * time.Millisecond)
	if got.Cores != 1 {
		t.Fatalf("expected Cores == 1 under taskset -c 0, got %d", got.Cores)
	}
}

// TestFanWidthFollowsAffinity checks that _fan_width uses os.sched_getaffinity
// rather than os.cpu_count(), so it respects the CPU cap imposed on a worker
// node. The test runs the _fan_width source (from chunkFanOut) under
// taskset -c 0 and expects the width to be 1, not the host's core count.
// It also verifies that PIPEDPEER_WORKER_PROCS still wins over affinity.
func TestFanWidthFollowsAffinity(t *testing.T) {
	if _, err := exec.LookPath("taskset"); err != nil {
		t.Skip("taskset not on PATH")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	// Same probe mechanism as TestFanWidthRespectsFreeMemory: exec the
	// chunkFanOut source into a namespace and call _fan_width.
	probe := `
import os, sys
ns = {"os": os, "sys": sys}
exec(sys.argv[1], ns)
fan = ns["_fan_width"]
print(fan(64, 0))
`

	// Baseline: taskset -c 0 must yield width 1.
	cmd := exec.Command("taskset", "-c", "0", python, "-c", probe, chunkFanOut)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("affinity probe failed: %v", err)
	}
	got := strings.TrimSpace(string(out))
	if got != "1" {
		t.Fatalf("expected _fan_width(64,0) == 1 under taskset -c 0, got %q (host cpu_count in use?)", got)
	}

	// Override: PIPEDPEER_WORKER_PROCS=3 must win even under a 1-CPU affinity mask.
	cmd2 := exec.Command("taskset", "-c", "0", python, "-c", probe, chunkFanOut)
	cmd2.Env = append(os.Environ(), "PIPEDPEER_WORKER_PROCS=3")
	out2, err := cmd2.Output()
	if err != nil {
		t.Fatalf("override probe failed: %v", err)
	}
	got2 := strings.TrimSpace(string(out2))
	if got2 != "3" {
		t.Fatalf("expected _fan_width(64,0) == 3 with PIPEDPEER_WORKER_PROCS=3, got %q", got2)
	}
}
