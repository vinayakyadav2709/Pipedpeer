package daemonapi

import (
	"os/exec"
	"testing"
	"time"
)

func TestPeakMemTracker(t *testing.T) {
	// Spawn a process that grows: a subshell running yes to a pipe (holds
	// memory) with a sleep so sampling catches it. Keep it short.
	cmd := exec.Command("sh", "-c", "sleep 0.4 & wait")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start process: %v", err)
	}

	tracker := newPeakMemTracker(int32(cmd.Process.Pid), 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	peak := tracker.stop()
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
