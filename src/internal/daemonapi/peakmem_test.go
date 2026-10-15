package daemonapi

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
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

	tracker := newPeakMemTracker(int32(cmd.Process.Pid), 50*time.Millisecond, "")
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

func TestTreeMem(t *testing.T) {
	// A parent with a child: `sh -c 'sleep 1'` gives a shell + sleep.
	cmd := exec.Command("sh", "-c", "sleep 1")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start process: %v", err)
	}
	defer cmd.Process.Kill()

	time.Sleep(100 * time.Millisecond)
	kb := (&peakMemTracker{}).treeMem(int32(cmd.Process.Pid))
	if kb <= 0 {
		t.Fatalf("expected memory > 0, got %d", kb)
	}
}

// TestTreeMemCountsForkedPagesOnce: a parent touching 128 MiB and four forked
// children sharing it copy-on-write must read as ~128 MiB, not 5x (the
// RSS-sum bug that recorded 7.6 GB for a Pool job and then fit no node).
func TestTreeMemCountsForkedPagesOnce(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not installed")
	}
	script := `
import os, sys, time
b = b"x" * (128 << 20)
for _ in range(4):
    if os.fork() == 0:
        time.sleep(30); os._exit(0)
print("ready", flush=True)
time.sleep(30)
`
	cmd := exec.Command(py, "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start python3: %v", err)
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }()
	buf := make([]byte, 6)
	if _, err := io.ReadFull(out, buf); err != nil {
		t.Fatalf("waiting for ready: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // the last fork may still be settling

	mib := (&peakMemTracker{}).treeMem(int32(cmd.Process.Pid)) / 1024
	if mib < 100 || mib > 256 {
		t.Fatalf("tree memory %d MiB, want ~128 (shared pages counted once)", mib)
	}
}

func TestPeakMemTrackerCgroup(t *testing.T) {
	dir := t.TempDir()
	stat := "anon 4194304\nfile 1073741824\nkernel_stack 16384\nslab 1048576\n"
	os.WriteFile(filepath.Join(dir, "memory.stat"), []byte(stat), 0o644)
	tr := newPeakMemTracker(0, 10*time.Millisecond, dir)
	time.Sleep(100 * time.Millisecond)
	if got, want := tr.stop(), int64(4194304+16384+1048576); got != want {
		t.Fatalf("peak = %d, want %d (anon+kernel, no file cache)", got, want)
	}
}
