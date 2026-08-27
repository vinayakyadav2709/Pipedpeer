package nixstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCmdNamesTheMultiCallLink covers the mistake that made every export fail
// on a private store. nix dispatches on argv[0], but OCI takes args[0] as
// both the path and argv[0], so running <store>/bin/nix with argv[0]
// "nix-store" is not expressible: the command has to name bin/nix-store
// itself. Getting this wrong sends `nix-store -qR` to nix as `nix -qR`, which
// fails with "unrecognised flag '-q'" - a message that says nothing about the
// real cause.
func TestCmdNamesTheMultiCallLink(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PIPEDPEER_NIX_ROOT", root)
	t.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "1")
	resetPrivate()

	binDir := filepath.Join(root, "store", "abc123-nix-2.31.3", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nix", "nix-store"} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	got, err := NixBinDir()
	if err != nil {
		t.Fatalf("NixBinDir: %v", err)
	}
	// The path has to be the one seen inside the namespace, where the private
	// store is /nix. A host path here resolves to nothing in the sandbox.
	if want := "/nix/store/abc123-nix-2.31.3/bin"; got != want {
		t.Errorf("NixBinDir() = %q, want %q", got, want)
	}
	if strings.HasPrefix(got, root) {
		t.Errorf("NixBinDir() returned a host path %q; inside the namespace it does not exist", got)
	}

	// And the command must name the link, not the base binary.
	cmd, cleanup, err := Cmd("", "nix-store", "-qR", "/nix/store/x")
	if err != nil {
		t.Skipf("NOT VERIFIED: cannot build the namespace command here: %v", err)
	}
	defer cleanup()
	spec, err := os.ReadFile(filepath.Join(filepath.Dir(filepath.Dir(cmd.Args[3])), "config.json"))
	if err != nil {
		// crun's bundle path is the argument after --bundle.
		for i, a := range cmd.Args {
			if a == "--bundle" && i+1 < len(cmd.Args) {
				spec, err = os.ReadFile(filepath.Join(cmd.Args[i+1], "config.json"))
			}
		}
	}
	if err != nil {
		t.Fatalf("reading the generated bundle: %v", err)
	}
	if !strings.Contains(string(spec), "/bin/nix-store") {
		t.Errorf("bundle runs the wrong binary; nix-store would reach nix as `nix -qR`:\n%s", spec)
	}
}

// TestNixBinDirRefusesAnAmbiguousStore keeps a half-upgraded store from
// silently picking a version. Two nodes disagreeing about which nix they run
// is how store paths stop matching, and a store path that does not match is a
// cache miss that looks like a rebuild.
func TestNixBinDirRefusesAnAmbiguousStore(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PIPEDPEER_NIX_ROOT", root)
	for _, v := range []string{"aaa-nix-2.31.3", "bbb-nix-2.30.0"} {
		dir := filepath.Join(root, "store", v, "bin")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "nix"), []byte("x"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NixBinDir(); err == nil {
		t.Fatal("two nix versions in the store were accepted; one of them was picked silently")
	}
}

// TestHostNixDirFollowsTheStore checks the mount source the sandbox uses.
// Binding /nix when the store is private gives the job an empty or absent
// directory and every binary in the closure fails to resolve its interpreter.
func TestHostNixDirFollowsTheStore(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PIPEDPEER_NIX_ROOT", root)

	t.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "1")
	resetPrivate()
	if got := HostNixDir(); got != root {
		t.Errorf("with a private store HostNixDir() = %q, want %q", got, root)
	}

	t.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "")
	resetPrivate()
	if !Private() {
		if got := HostNixDir(); got != "/nix" {
			t.Errorf("with the system store HostNixDir() = %q, want /nix", got)
		}
	}
}

// resetPrivate clears the memoised answer so a test can choose one. Private()
// is settled once per process in production, which is what we want there and
// exactly what makes it untestable without this.
func resetPrivate() {
	privateOnce.Once = onceReset()
	privateOnce.private = false
}

// TestNixBuildCarriesTheExperimentalFlags. `nix build` is gated behind
// experimental features that a machine's nix.conf may not enable — the stock
// nixos/nix image does not — and the resulting error names the flake, not the
// configuration. Passing the flag per invocation keeps a system store, which
// belongs to the user, unedited.
func TestNixBuildCarriesTheExperimentalFlags(t *testing.T) {
	got := withExperimentalFeatures([]string{"nix", "build", ".#default"})
	want := []string{"nix", "--extra-experimental-features", "nix-command flakes", "build", ".#default"}
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

// The stable commands need nothing, and adding an unknown flag to them would
// break every closure transfer.
func TestNixStoreIsLeftAlone(t *testing.T) {
	in := []string{"nix-store", "-qR", "/nix/store/x"}
	if got := withExperimentalFeatures(in); len(got) != len(in) {
		t.Errorf("nix-store invocation was rewritten: %q", got)
	}
}

// A caller that sets the features itself means it; this is a default.
func TestAnExplicitSettingWins(t *testing.T) {
	in := []string{"nix", "--extra-experimental-features", "nix-command", "build"}
	if got := withExperimentalFeatures(in); len(got) != len(in) {
		t.Errorf("an explicit setting was overridden: %q", got)
	}
}
