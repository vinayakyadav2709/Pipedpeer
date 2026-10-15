package warmup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestArgsCPUTorchWithoutNVIDIA(t *testing.T) {
	if got := strings.Join(Args("c.py", false), " "); got != "run c.py --check-only --gpu off" {
		t.Fatal(got)
	}
	if got := strings.Join(Args("c.py", true), " "); got != "run c.py --check-only" {
		t.Fatal(got)
	}
}

// TestLogEndsInDone: the detached run writes its output, then "done" (C19).
func TestLogEndsInDone(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	exe := filepath.Join(t.TempDir(), "fake")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\necho \"built in 1s: $*\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Start(exe, false); err != nil {
		t.Fatal(err)
	}
	var body []byte
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		body, _ = os.ReadFile(LogPath())
		if strings.HasSuffix(string(body), "done\n") {
			break
		}
	}
	if !strings.HasSuffix(string(body), "done\n") || !strings.Contains(string(body), "--gpu off") {
		t.Fatalf("log:\n%s", body)
	}
	if Running() {
		t.Fatal("finished warm-up still reported running")
	}
}
