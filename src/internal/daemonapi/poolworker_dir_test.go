package daemonapi

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func TestKillRemovesWorkerScratchDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "warm-x")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()
	w := &poolWorker{cmd: cmd, dir: dir}
	w.kill()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("worker dir still there after kill: %v", err)
	}
	w.kill() // second kill is a no-op
}
