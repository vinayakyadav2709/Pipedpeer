package setup

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/nixtrust"
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
	keys := nixtrust.FileContent([]string{"pipedpeer-abc:AAAA=", "pipedpeer-def:BBBB="})
	script := helperInstallScript("/home/u/.local/bin/pipedpeer", "u", "/etc/nix/nix.custom.conf")
	for _, s := range []string{keys, script, helperUninstallScript()} {
		if strings.Contains(s, "require-sigs") {
			t.Fatalf("setup writes a require-sigs setting into the machine's nix config:\n%s", s)
		}
	}
	if !strings.Contains(keys, "extra-trusted-public-keys = pipedpeer-abc:AAAA= pipedpeer-def:BBBB=") {
		t.Errorf("the helper does not trust the cluster's keys:\n%s", keys)
	}
}

// The root script installs a root-owned copy of the binary, a sudoers rule
// checked by visudo before it is moved into place, and the include line once,
// in the right file.
func TestTheHelperInstallScript(t *testing.T) {
	script := helperInstallScript("/home/o'brien/bin/pipedpeer", "obrien", "/etc/nix/nix.custom.conf")
	for _, want := range []string{
		"set -eu",
		`install -o root -g root -m 0755 '/home/o'\''brien/bin/pipedpeer' '/usr/local/libexec/.pipedpeer-nix-trust.new'`,
		"mv -f '/usr/local/libexec/.pipedpeer-nix-trust.new' '/usr/local/libexec/pipedpeer-nix-trust'",
		"printf '%s' 'obrien ALL=(root) NOPASSWD: /usr/local/libexec/pipedpeer-nix-trust *\n' > \"$tmp\"",
		`chmod 0440 "$tmp"`,
		`visudo -cf "$tmp"`,
		`mv -f "$tmp" '/etc/sudoers.d/pipedpeer'`,
		"grep -qxF '!include /etc/nix/pipedpeer-trusted-keys.conf' '/etc/nix/nix.custom.conf' 2>/dev/null || printf",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("install script lacks %q:\n%s", want, script)
		}
	}
	if strings.Index(script, "visudo") > strings.Index(script, "/etc/sudoers.d/pipedpeer'") {
		t.Errorf("the sudoers rule is moved into place before visudo checks it:\n%s", script)
	}
	if root := helperInstallScript("/usr/bin/pipedpeer", "", "/etc/nix/nix.conf"); strings.Contains(root, "sudoers") {
		t.Errorf("running as root needs no sudoers rule:\n%s", root)
	}
	un := helperUninstallScript()
	for _, want := range []string{nixtrust.SudoersFile, nixtrust.HelperPath, nixtrust.KeysFile, "grep -vxF '!include", "systemctl try-restart nix-daemon"} {
		if !strings.Contains(un, want) {
			t.Errorf("uninstall script lacks %q:\n%s", want, un)
		}
	}
}

// fakeTrust replaces system nix with untrusted and records every root script
// and helper call instead of running them. The helper starts not installed;
// a root script installs it.
func fakeTrust(t *testing.T, untrusted func([]string) ([]string, bool, error)) (ran, added *[]string) {
	t.Helper()
	su, rr, hr, ha := systemUntrusted, runAsRoot, helperReady, helperAdd
	t.Cleanup(func() { systemUntrusted, runAsRoot, helperReady, helperAdd = su, rr, hr, ha })
	ran, added = new([]string), new([]string)
	systemUntrusted = untrusted
	runAsRoot = func(script string) error { *ran = append(*ran, script); return nil }
	helperReady = func() bool { return len(*ran) > 0 }
	helperAdd = func(keys []string) error { *added = append(*added, keys...); return nil }
	return ran, added
}

func writeClusterKeys(t *testing.T, keys ...string) {
	t.Helper()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	if err := nixsign.WriteTrustedKeys(keys); err != nil {
		t.Fatal(err)
	}
}

// TestSetupWithNoKeysYetInstallsOnlyTheHelper: a fresh multi-user machine
// knows no keys, and must not abort; the helper is what lets the daemon add
// them later.
func TestSetupWithNoKeysYetInstallsOnlyTheHelper(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	ran, added := fakeTrust(t, func(keys []string) ([]string, bool, error) { return keys, true, nil })
	if err := trustClusterKeys(); err != nil {
		t.Fatal(err)
	}
	if len(*ran) != 1 || len(*added) != 0 {
		t.Fatalf("root scripts %d, keys added %v; want the helper installed and nothing added", len(*ran), *added)
	}
	if !acceptsPeerClosures() {
		t.Error("with the helper in place and no keys yet, setup should report nix-imports ok")
	}
}

// TestTrustAsksForRootOnlyWhereNixConfDecides: `pipedpeer trust` is a no-op
// on a private store or single-user nix, installs the helper once on a
// multi-user nix, and adds only the missing keys through it.
func TestTrustAsksForRootOnlyWhereNixConfDecides(t *testing.T) {
	const self, peer = "pipedpeer-self:AAAA=", "pipedpeer-peer:BBBB="
	cases := []struct {
		name      string
		untrusted func([]string) ([]string, bool, error)
		wantRoot  bool
		wantAdded []string
		wantMsg   string
	}{
		{"single-user or private", func([]string) ([]string, bool, error) { return nil, false, nil }, false, nil, "each import"},
		{"multi-user, trusted", func([]string) ([]string, bool, error) { return nil, true, nil }, true, nil, "already trusts"},
		{"multi-user, peer missing", func([]string) ([]string, bool, error) { return []string{peer}, true, nil }, true, []string{peer}, "1 more"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			writeClusterKeys(t, self, peer)
			ran, added := fakeTrust(t, c.untrusted)
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
			if strings.Join(*added, " ") != strings.Join(c.wantAdded, " ") {
				t.Errorf("helper added %v, want %v", *added, c.wantAdded)
			}
			// A second run finds the helper and asks for no root.
			if _, err := Trust(); err != nil || len(*ran) > 1 {
				t.Errorf("second trust: err=%v root scripts=%d", err, len(*ran))
			}
		})
	}
}

// TestJoinInstallsTheHelperOnce: join on a multi-user nix without the helper
// installs it (one prompt) and adds what is known; with it, join asks nothing.
func TestJoinInstallsTheHelperOnce(t *testing.T) {
	missingAll := func(keys []string) ([]string, bool, error) { return keys, true, nil }

	writeClusterKeys(t, "pipedpeer-self:AAAA=")
	ran, added := fakeTrust(t, missingAll)
	if err := TrustAfterJoin(); err != nil || len(*ran) != 1 || len(*added) != 1 {
		t.Fatalf("join did not install the helper and add the key: err=%v ran=%d added=%v", err, len(*ran), *added)
	}
	trusted := func([]string) ([]string, bool, error) { return nil, true, nil }
	systemUntrusted = trusted
	if err := TrustAfterJoin(); err != nil || len(*ran) != 1 {
		t.Fatalf("join asked for root again with the helper installed: err=%v ran=%d", err, len(*ran))
	}

	ran, _ = fakeTrust(t, func([]string) ([]string, bool, error) { return nil, false, nil })
	if err := TrustAfterJoin(); err != nil || len(*ran) > 0 {
		t.Fatalf("join asked for root on a nix that takes keys per import: err=%v ran=%v", err, *ran)
	}
}

// TestSetupPrefersTheSystemNix: an installed nix is used; with root
// available the system installer runs; only without root (or when forced)
// does nix go in a private store, and a failed installer falls back to it.
func TestSetupPrefersTheSystemNix(t *testing.T) {
	cases := []struct {
		force, system, root bool
		want                nixRoute
	}{
		{false, true, true, nixUseSystem},
		{false, true, false, nixUseSystem},
		{false, false, true, nixInstallSystem},
		{false, false, false, nixUsePrivate},
		{true, true, true, nixUsePrivate},
		{true, false, true, nixUsePrivate},
	}
	for _, c := range cases {
		if got := chooseNix(c.force, c.system, c.root); got != c.want {
			t.Errorf("chooseNix(force=%v, system=%v, root=%v) = %v, want %v", c.force, c.system, c.root, got, c.want)
		}
	}

	cbr, sp, id, mk := canBecomeRoot, seedPrivate, installDeterminate, makeNixStore
	t.Cleanup(func() { canBecomeRoot, seedPrivate, installDeterminate, makeNixStore = cbr, sp, id, mk })
	var did []string
	seedPrivate = func() error { did = append(did, "private"); return nil }
	installErr := errors.New("sudo: a password is required")
	installDeterminate = func() error { did = append(did, "determinate"); return installErr }
	makeNixStore = func() error { did = append(did, "store"); return nil }
	t.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "")
	if nixInstalled() {
		t.Skip("a nix is installed here, so the no-system-nix routes cannot be driven")
	}
	canBecomeRoot = func() bool { return true }
	if err := installNix(); err != nil || strings.Join(did, ",") != "determinate,private" {
		t.Errorf("with root and a failing installer: err=%v did=%v, want determinate then private", err, did)
	}
	did = nil
	canBecomeRoot = func() bool { return false }
	if err := installNix(); err != nil || strings.Join(did, ",") != "private" {
		t.Errorf("without root: err=%v did=%v, want private only", err, did)
	}
}
