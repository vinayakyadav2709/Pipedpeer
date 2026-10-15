package main

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Flags after the script belong to the script, as with python:
// `pipedpeer run -v t.py --epochs 3` gives t.py "--epochs 3".
func TestRunPassesScriptFlagsThrough(t *testing.T) {
	cmd := newRunCmd()
	if err := cmd.Flags().Parse([]string{"-v", "t.py", "--epochs", "3", "-v"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got, want := cmd.Flags().Args(), []string{"t.py", "--epochs", "3", "-v"}; !slices.Equal(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
	if v, _ := cmd.Flags().GetBool("verbose"); !v {
		t.Fatal("-v before the script did not set --verbose")
	}
}

// A missing script fails first, before any daemon start or placement
// (a typo used to reach three remote nodes before "file not found").
func TestRunMissingScriptFailsFirst(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "04_torch_ddp.y")
	cmd := newRunCmd()
	cmd.SetArgs([]string{"--remote", "--gpu", "force", missing})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatalf("err = %v, want a no-such-file error", err)
	}
	if err := checkScript(t.TempDir()); err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("directory: err = %v, want a not-a-script error", err)
	}
}
