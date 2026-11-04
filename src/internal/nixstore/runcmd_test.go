package nixstore

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

// Not private: RunCmd is exactly the old direct exec (no crun, no bundle).
func TestRunCmdSystemStoreIsDirectExec(t *testing.T) {
	resetPrivate()
	t.Cleanup(resetPrivate)
	t.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "0")
	t.Setenv("PIPEDPEER_NIX_ROOT", t.TempDir()) // no seeded store: not private
	if Private() {
		t.Skip("this machine has a system nix store that is not driveable; Private() is true")
	}
	want := []string{"/nix/store/x-run/bin/run", "w.py", "a"}
	cmd, cleanup, err := RunCmd(context.Background(), "", want...)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Fatalf("args = %v, want %v", cmd.Args, want)
	}
}

// Private: the command is crun over a bundle that runs argv unchanged with the
// private store bound at /nix.
func TestRunCmdPrivateRunsInsideTheBundle(t *testing.T) {
	if _, err := exec.LookPath("crun"); err != nil {
		t.Skip("no crun")
	}
	resetPrivate()
	t.Cleanup(resetPrivate)
	t.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "1")
	root := t.TempDir()
	t.Setenv("PIPEDPEER_NIX_ROOT", root)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	if err := os.MkdirAll(filepath.Join(root, "nix", "store"), 0o755); err != nil && !os.IsExist(err) {
		t.Fatal(err)
	}

	argv := []string{"/nix/store/x-run/bin/run", "runner.py", "in", "out"}
	cmd, cleanup, err := RunCmd(context.Background(), "", argv...)
	if err != nil {
		t.Skipf("private command not buildable here: %v", err)
	}
	defer cleanup()
	if filepath.Base(cmd.Args[0]) != "crun" || cmd.Args[1] != "run" || cmd.Args[2] != "--bundle" {
		t.Fatalf("not a crun run: %v", cmd.Args)
	}
	raw, err := os.ReadFile(filepath.Join(cmd.Args[3], "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Process struct{ Args []string } `json:"process"`
		Mounts  []struct{ Destination, Source string }
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec.Process.Args, argv) {
		t.Errorf("bundle args = %v, want %v", spec.Process.Args, argv)
	}
	bound := false
	for _, m := range spec.Mounts {
		if m.Destination == "/nix" && m.Source == NixDir() {
			bound = true
		}
	}
	if !bound {
		t.Errorf("private store not bound at /nix: %+v", spec.Mounts)
	}
}
