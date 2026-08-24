package setup

import (
	"os"
	"path/filepath"
	"testing"
)

// Fedora's nix-core RPM installs the nix binary and nothing else — no store.
// Checking PATH alone reported "nix ✓ ok" on such a machine, and the first job
// to build an environment then failed on a store that was never there.
func TestNixUsableRequiresTheStore(t *testing.T) {
	if !binaryCheck("nix")() {
		t.Skip("nix is not on PATH, so the store half cannot be exercised")
	}

	t.Run("store present", func(t *testing.T) {
		t.Setenv("NIX_STORE_DIR", t.TempDir())
		if !nixUsable() {
			t.Fatal("nix with an existing store should be usable")
		}
	})

	t.Run("store missing", func(t *testing.T) {
		t.Setenv("NIX_STORE_DIR", filepath.Join(t.TempDir(), "absent"))
		if nixUsable() {
			t.Fatal("nix without a store must not report usable")
		}
	})

	t.Run("store path is a file", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "store")
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Setenv("NIX_STORE_DIR", f)
		if nixUsable() {
			t.Fatal("a regular file is not a store")
		}
	})
}

func TestStoreDirHonoursOverride(t *testing.T) {
	t.Setenv("NIX_STORE_DIR", "")
	if got := storeDir(); got != "/nix/store" {
		t.Fatalf("default store dir: got %q, want /nix/store", got)
	}

	t.Setenv("NIX_STORE_DIR", "/somewhere/else/store")
	if got := storeDir(); got != "/somewhere/else/store" {
		t.Fatalf("override ignored: got %q", got)
	}
}

// Creating only the store root is not enough: nixUsable checks the store dir
// itself, so a createNixStore that stops at the root leaves setup reporting
// nix missing forever — install, "✓ installed", still missing, loop.
func TestCreateNixStoreSatisfiesTheCheck(t *testing.T) {
	if !binaryCheck("nix")() {
		t.Skip("nix is not on PATH")
	}
	t.Setenv("NIX_STORE_DIR", filepath.Join(t.TempDir(), "nix", "store"))

	if nixUsable() {
		t.Fatal("precondition: store should not exist yet")
	}
	if err := createNixStore(); err != nil {
		t.Fatalf("createNixStore: %v", err)
	}
	if !nixUsable() {
		t.Fatal("store created but nixUsable still false — setup would loop")
	}
}
