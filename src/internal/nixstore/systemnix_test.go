package nixstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
