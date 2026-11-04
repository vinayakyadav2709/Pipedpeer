package phone

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func init() {
	// Private() memoises on first use; force it for the whole package. Paths
	// outside /nix are untouched by it.
	os.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "1")
}

const privStore = "/nix/store/00000000000000000000000000000000-run"

func seedPrivate(t *testing.T, withRun bool) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("PIPEDPEER_NIX_ROOT", root)
	if !withRun {
		return
	}
	bin := filepath.Join(root, "store", "00000000000000000000000000000000-run", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "run"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestPrivateStoreRunnable(t *testing.T) {
	stub(t, false, "3.12")
	seedPrivate(t, true)
	if !CanRun(privStore, http.Header{}) {
		t.Error("private store with closure in the private dir must be runnable")
	}
	if p, err := PoolRunPath(privStore); err != nil || p != filepath.Join(privStore, "bin", "run") {
		t.Errorf("PoolRunPath keeps the in-namespace path: %q, %v", p, err)
	}

	seedPrivate(t, false)
	if CanRun(privStore, http.Header{}) {
		t.Error("private store without the closure must not be runnable")
	}
	if _, err := PoolRunPath(privStore); err == nil {
		t.Error("private store without the closure must refuse")
	}
}

func TestPrivateStoreNonNixPathUnchanged(t *testing.T) {
	stub(t, false, "3.12")
	seedPrivate(t, false)
	if !CanRun(storeWithRun(t), http.Header{}) {
		t.Error("a path outside /nix is checked as is")
	}
}
