package daemonapi

import (
	"os/exec"
	"testing"
	"time"
)

func TestPeakMemTracker(t *testing.T) {
	// Spawn a process that outlives the sampling window: a subshell running
	// sleep keeps the tree alive so at least one 50ms sample lands.
	cmd := exec.Command("sh", "-c", "sleep 2")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start process: %v", err)
	}

	tracker := newPeakMemTracker(int32(cmd.Process.Pid), 50*time.Millisecond)
	// Tree walking can transiently miss a sample; wait until one lands (the
	// process lives 2s, well past the 3s deadline).
	deadline := time.Now().Add(3 * time.Second)
	var peak int64
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if peak = tracker.peak(); peak > 0 {
			break
		}
	}
	tracker.stop()
	_ = cmd.Wait()

	if peak <= 0 {
		t.Fatalf("expected peak > 0 bytes, got %d", peak)
	}
}

func TestTreeRSS(t *testing.T) {
	// A parent with a child: `sh -c 'sleep 1'` gives a shell + sleep.
	cmd := exec.Command("sh", "-c", "sleep 1")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start process: %v", err)
	}
	defer cmd.Process.Kill()

	time.Sleep(100 * time.Millisecond)
	kb := treeRSS(int32(cmd.Process.Pid))
	if kb <= 0 {
		t.Fatalf("expected rss > 0, got %d", kb)
	}
}
