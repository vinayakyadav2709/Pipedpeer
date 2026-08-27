package nixstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTheInstallerLocationsAreSearched.
//
// The home-directory case was the only one covered, and the location that
// actually bit was /nix/var/nix/profiles/default/bin - the multi-user and
// Determinate installer path, which is where the test machine had its nix and
// where every job failed with "nix not found in PATH". Emptying systemNixDirs
// left the old test green, because the home path is appended separately; the
// audit caught that.
func TestTheInstallerLocationsAreSearched(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	nix := filepath.Join(bin, "nix")
	if err := os.WriteFile(nix, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	restore := systemNixDirs
	systemNixDirs = []string{bin}
	defer func() { systemNixDirs = restore }()

	// Nothing on PATH, and no nix in the home directory either, so the only
	// way to find it is the installer location.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())

	got, err := SystemNix()
	if err != nil {
		t.Fatalf("a nix in an installer location was not found: %v", err)
	}
	if got != nix {
		t.Errorf("SystemNix() = %q, want %q", got, nix)
	}
}

// TestTheMultiUserPathIsOneOfThem, named explicitly because that is the one
// the test machine had and the one whose absence produced the failure.
func TestTheMultiUserPathIsOneOfThem(t *testing.T) {
	want := "/nix/var/nix/profiles/default/bin"
	for _, d := range systemNixDirs {
		if d == want {
			return
		}
	}
	t.Errorf("%s is not searched; it is where the multi-user and Determinate "+
		"installers put nix, and where every job failed with \"not found in "+
		"PATH\" on a machine that had it", want)
}

// TestSystemNixIsFoundOffPath covers a machine with nix installed and nothing
// on PATH, which is every non-login context: a systemd unit, cron, `ssh host
// command`, `docker exec`. The installer's profile script is the only thing
// that puts nix on PATH, and none of those run it.
//
// Observed on a machine with /nix/var/nix/profiles/default/bin/nix present:
// every job failed at "[4/7] Building locally... nix not found in PATH".
func TestSystemNixIsFoundOffPath(t *testing.T) {
	home := t.TempDir()
	bin := filepath.Join(home, ".nix-profile", "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	nix := filepath.Join(bin, "nix")
	if err := os.WriteFile(nix, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir()) // nothing on PATH

	got, err := SystemNix()
	if err != nil {
		t.Fatalf("an installed nix was not found: %v", err)
	}
	if got != nix {
		t.Errorf("SystemNix() = %q, want %q", got, nix)
	}
}

// TestPathStillWins. A nix the user put on PATH is the one they meant; the
// fallback must not reach past it to some other installation.
func TestPathStillWins(t *testing.T) {
	dir := t.TempDir()
	nix := filepath.Join(dir, "nix")
	if err := os.WriteFile(nix, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	got, err := SystemNix()
	if err != nil {
		t.Fatal(err)
	}
	if got != nix {
		t.Errorf("SystemNix() = %q, want the one on PATH at %q", got, nix)
	}
}

// TestMissingNixSaysWhereItLooked. "not found in PATH" sent the reader to
// their PATH, which was not the problem; the message has to name the places
// that were checked.
func TestMissingNixSaysWhereItLooked(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())

	if _, err := os.Stat("/nix/var/nix/profiles/default/bin/nix"); err == nil {
		t.Skip("NOT VERIFIED: this machine has a system nix, so there is no " +
			"missing-nix case to observe here")
	}
	_, err := SystemNix()
	if err == nil {
		t.Fatal("no nix anywhere, but SystemNix() succeeded")
	}
	if !strings.Contains(err.Error(), ".nix-profile") ||
		!strings.Contains(err.Error(), "/nix/var/nix/profiles") {
		t.Errorf("error %q does not name the directories that were searched, "+
			"so a reader cannot tell where to install or what to symlink", err)
	}
}

// TestSystemNixDirsAreAbsolute. A relative entry would resolve against the
// daemon's working directory, which is wherever it happened to be started.
func TestSystemNixDirsAreAbsolute(t *testing.T) {
	for _, dir := range systemNixDirs {
		if !filepath.IsAbs(dir) {
			t.Errorf("%q is not absolute", dir)
		}
	}
}
