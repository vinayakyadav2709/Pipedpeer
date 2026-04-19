package app

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

func TestRunDetachedSyncWorkerReceivesNestedFiles(t *testing.T) {
	// Verify real SSH is available
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("ssh not available")
	}
	if out, err := exec.Command("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=2",
		"root@localhost", "echo", "ok").CombinedOutput(); err != nil {
		t.Skipf("SSH to localhost not available: %v (%s)", err, string(out))
	}

	tmp := t.TempDir()
	xdg := filepath.Join(tmp, "xdg")
	t.Setenv("XDG_DATA_HOME", xdg)

	// Create a fake "running" job on the remote side via real SSH
	remoteJobDir := "/tmp/pipedpeer-detach-test-" + filepath.Base(tmp)

	// Set up remote job directory with artifacts via real SSH
	setupCmd := strings.Join([]string{
		"rm -rf " + remoteJobDir,
		"mkdir -p " + remoteJobDir + "/work/artifacts/nested",
		"echo 'hello' > " + remoteJobDir + "/work/artifacts/nested/output.txt",
		// Create exit code file (job "completed")
		"echo 0 > " + remoteJobDir + "/exit_code",
		// Create stdout/stderr logs
		"echo 'detached-stdout' > " + remoteJobDir + "/stdout.log",
		"echo 'detached-stderr' > " + remoteJobDir + "/stderr.log",
		// Create done marker
		"touch " + remoteJobDir + "/done",
	}, " && ")
	if out, err := exec.Command("ssh", "-o", "StrictHostKeyChecking=accept-new",
		"root@localhost", setupCmd).CombinedOutput(); err != nil {
		t.Fatalf("failed to set up remote job dir via SSH: %v (%s)", err, string(out))
	}
	t.Cleanup(func() {
		exec.Command("ssh", "root@localhost", "rm -rf "+remoteJobDir).Run()
	})

	record, historyDir, err := jobhistory.NewRecord(filepath.Join(tmp, "train.py"), "root@localhost:22", "node-a", true, true)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	record.Status = "running_detached"
	if err := jobhistory.SaveRecord(historyDir, record); err != nil {
		t.Fatalf("save record: %v", err)
	}

	localRoot := filepath.Join(tmp, "workspace")
	if err := os.MkdirAll(localRoot, 0755); err != nil {
		t.Fatalf("mkdir localRoot: %v", err)
	}

	// Use REAL SSH to localhost
	err = RunDetachedSyncWorker(DetachedSyncWorkerOptions{
		User:       "root",
		Host:       "localhost",
		Port:       22,
		JobDir:     remoteJobDir,
		LocalRoot:  localRoot,
		HistoryDir: historyDir,
		TimeoutSec: 5,
	})
	if err != nil {
		t.Fatalf("run detached sync worker: %v", err)
	}

	outFile := filepath.Join(localRoot, "artifacts", "nested", "output.txt")
	if _, err := os.Stat(outFile); err != nil {
		t.Fatalf("expected synced nested output file: %v", err)
	}

	updated, err := jobhistory.ReadRecordAt(historyDir)
	if err != nil {
		t.Fatalf("read updated record: %v", err)
	}
	if updated.Status != "succeeded" {
		t.Fatalf("expected succeeded status, got %s", updated.Status)
	}
	if updated.ReceivedFiles == 0 {
		t.Fatalf("expected received files > 0")
	}

	stdoutRaw, _ := os.ReadFile(filepath.Join(historyDir, "stdout.log"))
	stderrRaw, _ := os.ReadFile(filepath.Join(historyDir, "stderr.log"))
	if !strings.Contains(string(stdoutRaw), "detached-stdout") {
		t.Fatalf("expected detached stdout capture, got: %s", string(stdoutRaw))
	}
	if !strings.Contains(string(stderrRaw), "detached-stderr") {
		t.Fatalf("expected detached stderr capture, got: %s", string(stderrRaw))
	}
}
