package setup

import (
	"os"
	"path/filepath"
	"strings"
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

// TestSetupNeverDisablesSignatureCheckingSystemWide.
//
// The old repair appended `require-sigs = false` to the machine's nix
// configuration so pipedpeer could import an unsigned peer closure. That
// stops nix verifying signatures for everything on the machine - every
// channel, every substituter, every user - and it outlives pipedpeer.
//
// Closures now carry their signatures, so the narrow statement is enough.
// This guards the line reappearing: it would be one line in a diff, it would
// work, and nothing else in the system would notice.
func TestSetupNeverDisablesSignatureCheckingSystemWide(t *testing.T) {
	script := nixConfigRepair("pipedpeer-abc:AAAA= pipedpeer-def:BBBB=", "/etc/nix/nix.custom.conf")

	if strings.Contains(script, "require-sigs") {
		t.Fatalf("setup writes a require-sigs setting into the machine's nix config:\n%s", script)
	}
	if !strings.Contains(script, "extra-trusted-public-keys = pipedpeer-abc:AAAA= pipedpeer-def:BBBB=") {
		t.Errorf("setup does not trust the cluster's keys:\n%s", script)
	}
	if !strings.Contains(script, "/etc/nix/nix.custom.conf") {
		t.Errorf("setup edits the wrong file:\n%s", script)
	}
}

// TestSetupWithNoKeysYetChangesNothing.
//
// Appending "extra-trusted-public-keys = " with nothing after it would edit a
// system file as root to no effect, and leave a line that reads as if the
// cluster were trusted. Saying what is missing is the useful answer.
func TestSetupWithNoKeysYetChangesNothing(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	err := trustClusterKeys()
	if err == nil {
		t.Fatal("setup edited the nix config with no cluster keys to trust")
	}
	if !strings.Contains(err.Error(), "start the daemon") {
		t.Errorf("the error does not say what to do about it: %v", err)
	}
}
