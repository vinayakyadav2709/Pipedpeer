package daemonapi

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureCrunStateDirUnset(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_RUNTIME_DIR", "")
	got, err := EnsureCrunStateDir()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(state, "pipedpeer", "run")
	if got != want || os.Getenv("XDG_RUNTIME_DIR") != want {
		t.Fatalf("got %q (env %q), want %q", got, os.Getenv("XDG_RUNTIME_DIR"), want)
	}
	if info, err := os.Stat(want); err != nil || !info.IsDir() {
		t.Fatalf("%s not created: %v", want, err)
	}
}

func TestEnsureCrunStateDirGone(t *testing.T) {
	state := t.TempDir()
	t.Setenv("XDG_STATE_HOME", state)
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(state, "logged-out"))
	got, err := EnsureCrunStateDir()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Join(state, "pipedpeer", "run") {
		t.Fatalf("a runtime dir that no longer exists was kept: %q", got)
	}
}

func TestEnsureCrunStateDirKept(t *testing.T) {
	run := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", run)
	got, err := EnsureCrunStateDir()
	if err != nil || got != run || os.Getenv("XDG_RUNTIME_DIR") != run {
		t.Fatalf("an existing runtime dir must stay: got %q, %v", got, err)
	}
}
