package phone

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixgen"
)

// TestForeignEnv: a job's recipe offered to a node of another system is built
// for that system with a fake nix; until it is built the node is not runnable
// for the job (never the pinned python), afterwards chunks run in it.
func TestForeignEnv(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	oldBuild, oldSys, oldHost := buildFlake, system, hostChunks
	t.Cleanup(func() { buildFlake, system, hostChunks = oldBuild, oldSys, oldHost })
	system = func() string { return "aarch64-linux" }
	hostChunks = func() bool { return true } // a phone: the pinned python is there

	release := make(chan error)
	var built string
	buildFlake = func(dir, sys string) error {
		if err := <-release; err != nil {
			return err
		}
		built = dir
		run := filepath.Join(dir, "result", "bin", "run")
		os.MkdirAll(filepath.Dir(run), 0o755)
		return os.WriteFile(run, []byte("#!/bin/sh\n"), 0o755)
	}
	const store = "/nix/store/abc123-x86-env"
	need := Need("3.14", true, "pandas")
	waitFor := func(want bool) {
		t.Helper()
		for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
			foreignMu.Lock()
			idle := len(foreignBusy) == 0
			foreignMu.Unlock()
			if idle && CanRun(store, need) == want {
				return
			}
		}
		t.Fatalf("CanRun never became %v", want)
	}

	if err := BuildForeign(store, nixgen.Recipe{Pkgs: []string{"pandas"}}); err != nil {
		t.Fatal(err)
	}
	if CanRun(store, need) {
		t.Fatal("runnable while its env is still building")
	}
	if _, err := PoolRunPath(store); err == nil || !strings.Contains(err.Error(), "still building") {
		t.Fatalf("PoolRunPath while building: %v", err)
	}

	release <- errors.New("fake nix failed")
	waitFor(false)
	if _, err := PoolRunPath(store); err == nil || !strings.Contains(err.Error(), "fake nix failed") {
		t.Fatalf("PoolRunPath after a failed build: %v", err)
	}

	// The next offer retries.
	if err := BuildForeign(store, nixgen.Recipe{Pkgs: []string{"pandas"}}); err != nil {
		t.Fatal(err)
	}
	release <- nil
	waitFor(true)
	flake, err := os.ReadFile(filepath.Join(built, "flake.nix"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(flake), "packages.aarch64-linux.default") || !strings.Contains(string(flake), "ps.pandas") {
		t.Fatalf("flake not built for this system with the job's packages:\n%s", flake)
	}
	if run, err := PoolRunPath(store); err != nil || run != filepath.Join(built, "result", "bin", "run") {
		t.Fatalf("PoolRunPath = %q, %v", run, err)
	}
	// Built once: a second offer starts no build (release would block).
	if err := BuildForeign(store, nixgen.Recipe{Pkgs: []string{"pandas"}}); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []struct {
		store string
		r     nixgen.Recipe
	}{
		{"/nix/store/../etc", nixgen.Recipe{}},
		{"/tmp/x-env", nixgen.Recipe{}},
		{store, nixgen.Recipe{Pkgs: []string{"x; builtins.readFile /etc/shadow"}}},
		{store, nixgen.Recipe{Python: "python3.withPackages"}},
	} {
		if err := BuildForeign(bad.store, bad.r); err == nil {
			t.Errorf("BuildForeign(%q, %+v) accepted", bad.store, bad.r)
		}
	}
}
