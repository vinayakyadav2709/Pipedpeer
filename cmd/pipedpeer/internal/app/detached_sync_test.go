package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

func TestRunDetachedSyncWorkerReceivesNestedFiles(t *testing.T) {
	tmp := t.TempDir()
	mockBin := filepath.Join(tmp, "mockbin")
	if err := os.MkdirAll(mockBin, 0755); err != nil {
		t.Fatalf("mkdir mockbin: %v", err)
	}

	sshScript := `#!/bin/sh
set -eu
args="$*"
if printf '%s' "$args" | grep -q '\[ -f '; then
  exit 0
fi
if printf '%s' "$args" | grep -q 'exit_code'; then
  echo 0
  exit 0
fi
if printf '%s' "$args" | grep -q 'stdout.log'; then
  echo detached-stdout
  exit 0
fi
if printf '%s' "$args" | grep -q 'stderr.log'; then
  echo detached-stderr
  exit 0
fi
if printf '%s' "$args" | grep -q 'tar -C'; then
  tmpd="$(mktemp -d)"
  mkdir -p "$tmpd/artifacts/nested"
  echo 'hello' > "$tmpd/artifacts/nested/output.txt"
  tar -C "$tmpd" -cf - .
  rm -rf "$tmpd"
  exit 0
fi
exit 0
`
	if err := writeExec(filepath.Join(mockBin, "ssh"), sshScript); err != nil {
		t.Fatalf("write mock ssh: %v", err)
	}

	t.Setenv("PATH", mockBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	xdg := filepath.Join(tmp, "xdg")
	t.Setenv("XDG_DATA_HOME", xdg)

	record, historyDir, err := jobhistory.NewRecord(filepath.Join(tmp, "train.py"), "root@localhost:2221", "node-a", true, true)
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

	err = RunDetachedSyncWorker(DetachedSyncWorkerOptions{
		User:       "root",
		Host:       "localhost",
		Port:       2221,
		JobDir:     "/tmp/pipedpeer/jobs/job-detached",
		LocalRoot:  localRoot,
		HistoryDir: historyDir,
		TimeoutSec: 2,
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
	if strings.TrimSpace(updated.ManifestPath) == "" {
		t.Fatalf("expected manifest path")
	}

	stdoutRaw, _ := os.ReadFile(filepath.Join(historyDir, "stdout.log"))
	stderrRaw, _ := os.ReadFile(filepath.Join(historyDir, "stderr.log"))
	if !strings.Contains(string(stdoutRaw), "detached-stdout") {
		t.Fatalf("expected detached stdout capture")
	}
	if !strings.Contains(string(stderrRaw), "detached-stderr") {
		t.Fatalf("expected detached stderr capture")
	}
}
