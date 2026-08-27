package setup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
)

// Fedora's nix-core RPM installs the nix binary and nothing else — no store.
// Checking PATH alone reported "nix ✓ ok" on such a machine, and the first job
// to build an environment then failed on a store that was never there.
func TestNixUsableRequiresTheStore(t *testing.T) {
	if !nixInstalled() {
		t.Skip("no nix installed anywhere on this machine, so the store half " +
			"cannot be exercised")
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
	if !nixInstalled() {
		t.Skip("no nix installed anywhere on this machine")
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

// TestSetupFindsNixOffPath is the guard for the regression that made the two
// tests above skip on a machine that plainly had nix: the check went through
// PATH, and PATH is empty in every context that is not a login shell. Setup
// then reported nix missing and offered to install a second one.
func TestSetupFindsNixOffPath(t *testing.T) {
	if nixstore.Private() {
		t.Skip("this machine uses a private store, which is usable regardless " +
			"of where a system nix is; nothing to discriminate here")
	}
	if !nixInstalled() {
		t.Skip("no nix installed anywhere on this machine")
	}

	home := t.TempDir()
	bin := filepath.Join(home, ".nix-profile", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "nix"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir()) // exactly what a systemd unit sees

	if binaryCheck("nix")() {
		t.Fatal("precondition: PATH was supposed to be empty of nix")
	}
	if !nixInstalled() {
		t.Error("nix is installed but was not found with an empty PATH, so " +
			"setup would report it missing on any machine running from a " +
			"unit file, cron, or ssh")
	}

	store := t.TempDir()
	t.Setenv("NIX_STORE_DIR", store)
	if !nixUsable() {
		t.Error("an installed nix with an existing store is usable; reporting " +
			"otherwise sends the user to install a second nix over the one " +
			"they already have")
	}
}
