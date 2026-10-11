package setup

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
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
	ran := fakeTrust(t, func(keys []string) ([]string, bool, error) { return keys, true, nil })
	err := trustClusterKeys()
	if err == nil || len(*ran) > 0 {
		t.Fatalf("setup edited the nix config with no cluster keys to trust (err=%v)", err)
	}
	if !errors.Is(err, errNoKeysYet) || !strings.Contains(err.Error(), "pipedpeer trust") {
		t.Errorf("the error does not say what to do about it: %v", err)
	}
}

// fakeTrust replaces system nix with untrusted and records every root script
// instead of running it.
func fakeTrust(t *testing.T, untrusted func([]string) ([]string, bool, error)) *[]string {
	t.Helper()
	su, rr, pe := systemUntrusted, runAsRoot, trustPollEvery
	t.Cleanup(func() { systemUntrusted, runAsRoot, trustPollEvery = su, rr, pe })
	var ran []string
	systemUntrusted = untrusted
	runAsRoot = func(script string) error { ran = append(ran, script); return nil }
	trustPollEvery = time.Millisecond
	return &ran
}

func writeClusterKeys(t *testing.T, keys ...string) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if err := nixsign.WriteTrustedKeys(keys); err != nil {
		t.Fatal(err)
	}
}

// TestTrustAsksForRootOnlyWhereNixConfDecides: `pipedpeer trust` is a no-op
// on a private store, a single-user nix, or one that already trusts the keys,
// and writes only the missing keys on a multi-user nix.
func TestTrustAsksForRootOnlyWhereNixConfDecides(t *testing.T) {
	const self, peer = "pipedpeer-self:AAAA=", "pipedpeer-peer:BBBB="
	cases := []struct {
		name      string
		untrusted func([]string) ([]string, bool, error)
		wantRoot  bool
		wantMsg   string
	}{
		{"single-user or private", func([]string) ([]string, bool, error) { return nil, false, nil }, false, "each import"},
		{"multi-user, trusted", func([]string) ([]string, bool, error) { return nil, true, nil }, false, "already trusts"},
		{"multi-user, peer missing", func([]string) ([]string, bool, error) { return []string{peer}, true, nil }, true, "1 more"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			writeClusterKeys(t, self, peer)
			ran := fakeTrust(t, c.untrusted)
			msg, err := Trust()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(msg, c.wantMsg) {
				t.Errorf("message %q does not say %q", msg, c.wantMsg)
			}
			if got := len(*ran) > 0; got != c.wantRoot {
				t.Fatalf("ran as root: %v, want %v (%v)", got, c.wantRoot, *ran)
			}
			if c.wantRoot && (!strings.Contains((*ran)[0], peer) || strings.Contains((*ran)[0], self)) {
				t.Errorf("root script should add only the missing key:\n%s", (*ran)[0])
			}
		})
	}
}

// TestJoinTrustsOnlyOnceAPeersKeyIsKnown: with only this node's own key,
// join must not spend a sudo prompt; with a peer's key it applies trust.
func TestJoinTrustsOnlyOnceAPeersKeyIsKnown(t *testing.T) {
	missingAll := func(keys []string) ([]string, bool, error) { return keys, true, nil }

	writeClusterKeys(t, "pipedpeer-self:AAAA=")
	ran := fakeTrust(t, missingAll)
	if err := TrustAfterJoin(5 * time.Millisecond); err != nil || len(*ran) > 0 {
		t.Fatalf("join asked for root with no peer key: err=%v ran=%v", err, *ran)
	}

	writeClusterKeys(t, "pipedpeer-self:AAAA=", "pipedpeer-peer:BBBB=")
	if err := TrustAfterJoin(0); err != nil || len(*ran) != 1 {
		t.Fatalf("join did not apply trust once a peer key was known: err=%v ran=%v", err, *ran)
	}

	ran = fakeTrust(t, func([]string) ([]string, bool, error) { return nil, false, nil })
	if err := TrustAfterJoin(0); err != nil || len(*ran) > 0 {
		t.Fatalf("join asked for root on a nix that takes keys per import: err=%v ran=%v", err, *ran)
	}
}
