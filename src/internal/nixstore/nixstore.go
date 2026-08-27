// Package nixstore lets pipedpeer keep a nix store without root.
//
// Creating /nix needs root, and on a machine the user does not administer
// there is no way to get it. So when the system store is unusable, the store
// lives in the user's own data directory and every nix command runs inside a
// user+mount namespace with that directory bound at /nix. crun builds the
// namespace, which pipedpeer already installs without privilege.
//
// The property that makes this safe for a cluster: a chroot store does not
// change store paths. A package built here is still
// /nix/store/<hash>-<name>, byte-identical to one built on a machine with a
// real /nix, so the store path stays a valid cache key between peers.
// Verified in a nix-less unprivileged container: nix bootstrapped from the
// release tarball, built hello from cache.nixos.org to the same path a normal
// machine produces, and the resulting binary ran.
package nixstore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Root is the private store's root directory; the store itself is at
// Root()/nix/store, which is what gets bound onto /nix.
func Root() string {
	if dir := os.Getenv("PIPEDPEER_NIX_ROOT"); dir != "" {
		return dir
	}
	return filepath.Join(userdir.Data(), "nix")
}

// NixDir is the directory bound at /nix inside the namespace. It is the root
// itself: nix is not told about a chroot store, it is simply given a /nix
// that happens to live in the user's data directory, so the layout under here
// is the ordinary store/ and var/ of any nix installation.
func NixDir() string { return Root() }

// systemNixDirs are where an installed nix lives when it is not on PATH.
//
// Every nix installer puts its profile on PATH from a login shell profile
// script, and nothing else. A daemon started by systemd, by cron, by
// `ssh host command`, or inside `docker exec` gets none of that, so a machine
// with a perfectly good nix reported "nix not found in PATH". Worse, the same
// lookup decides Private(): a node that had always used the system store
// would silently switch to a private one and re-download every closure it
// already held.
//
// Order matters only in that all of these are correct answers; the first that
// exists is used.
var systemNixDirs = []string{
	"/nix/var/nix/profiles/default/bin", // multi-user, and the Determinate installer
	"/run/current-system/sw/bin",        // NixOS
}

// SystemNix returns the path of an installed nix outside the private store.
func SystemNix() (string, error) {
	if p, err := exec.LookPath("nix"); err == nil {
		return p, nil
	}
	dirs := systemNixDirs
	if home, err := os.UserHomeDir(); err == nil {
		// The single-user installer, which puts it under the user's home.
		dirs = append(append([]string{}, dirs...), filepath.Join(home, ".nix-profile", "bin"))
	}
	for _, dir := range dirs {
		p := filepath.Join(dir, "nix")
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("nix not found on PATH or in %s", strings.Join(dirs, ", "))
}

var privateOnce struct {
	sync.Once
	private bool
}

// Private reports whether nix commands must run through the namespace.
//
// The system store wins whenever it works. Machines that already have nix
// keep behaving exactly as before - this path exists for machines that cannot
// have one, and switching a working node onto a second store would mean
// re-downloading every closure it already holds.
func Private() bool {
	privateOnce.Do(func() {
		if os.Getenv("PIPEDPEER_FORCE_PRIVATE_NIX") == "1" {
			privateOnce.private = true
			return
		}
		if _, err := os.Stat("/nix/store"); err == nil {
			if _, err := SystemNix(); err == nil {
				return // system store and a nix to drive it: nothing to do
			}
		}
		// A private store is only usable once it has been seeded with nix
		// itself; until then, callers should report nix as missing rather
		// than run commands that cannot work.
		if _, err := os.Stat(filepath.Join(NixDir(), "store")); err == nil {
			privateOnce.private = true
		}
	})
	return privateOnce.private
}

// NixBinDir finds the bin directory of the nix inside the private store. There is exactly one
// nix in a freshly seeded store; if a second ever appears, the newest by name
// is not a safe guess, so this refuses rather than picks.
func NixBinDir() (string, error) {
	matches, err := filepath.Glob(filepath.Join(NixDir(), "store", "*-nix-*"))
	if err != nil {
		return "", err
	}
	var found []string
	for _, m := range matches {
		bin := filepath.Join(m, "bin", "nix")
		if _, err := os.Stat(bin); err == nil {
			found = append(found, bin)
		}
	}
	switch len(found) {
	case 0:
		return "", fmt.Errorf("no nix in the private store at %s", NixDir())
	case 1:
		// Path as seen inside the namespace, where NixDir() is /nix.
		rel, err := filepath.Rel(NixDir(), filepath.Dir(found[0]))
		if err != nil {
			return "", err
		}
		return filepath.Join("/nix", rel), nil
	default:
		return "", fmt.Errorf("%d nix versions in %s; remove all but one", len(found), NixDir())
	}
}

// HostNixDir is the directory on this machine that must be mounted at /nix
// for a closure to run: the system store when there is one, and the private
// store otherwise.
func HostNixDir() string {
	if Private() {
		return NixDir()
	}
	return "/nix"
}

// Cmd builds a command that runs nix, transparently inside the namespace when
// the store is private. argv[0] selects nix's multi-call behaviour the same
// way the direct callers do, so "nix-store" and "nix" both work.
//
// dir is the working directory, "" meaning this process's. It is a parameter
// rather than a field the caller sets afterwards because with a private store
// the working directory has to be baked into the bundle: setting cmd.Dir
// would change where crun runs, not where nix runs, and `nix build` in the
// wrong directory fails with "could not find a flake.nix file".
//
// The returned cleanup must be called once the command has finished; it
// removes the generated bundle.
// withExperimentalFeatures enables the new CLI for the commands that need it.
//
// `nix build` is behind experimental flags that nix only enables when the
// machine's nix.conf says so, and a machine whose nix.conf does not is not a
// broken machine - the stock nixos/nix image is one. The failure is
// "experimental Nix feature 'nix-command' is disabled", which reads as a
// problem with the generated flake and sends the reader to the wrong file.
//
// A private store gets this written into its own nix.conf by ensureConf; a
// system store belongs to the user and is not ours to edit, so the flag is
// passed per invocation instead. The stable commands - every nix-store call -
// need nothing and are left alone.
func withExperimentalFeatures(argv []string) []string {
	if filepath.Base(argv[0]) != "nix" {
		return argv
	}
	for _, a := range argv {
		// An explicit setting from the caller wins; this is a default, not an
		// override.
		if strings.HasSuffix(a, "experimental-features") {
			return argv
		}
	}
	out := make([]string, 0, len(argv)+2)
	out = append(out, argv[0], "--extra-experimental-features", "nix-command flakes")
	return append(out, argv[1:]...)
}

func Cmd(dir string, argv ...string) (*exec.Cmd, func(), error) {
	if len(argv) == 0 {
		return nil, nil, fmt.Errorf("no command given")
	}
	argv = withExperimentalFeatures(argv)
	if !Private() {
		nixPath, err := SystemNix()
		if err != nil {
			return nil, nil, err
		}
		return &exec.Cmd{Path: nixPath, Args: argv, Dir: dir}, func() {}, nil
	}

	binDir, err := NixBinDir()
	if err != nil {
		return nil, nil, err
	}
	// Repair rather than assume. A store seeded by an older build has no
	// nix.conf, and the only symptom is every build failing with
	// "experimental Nix feature 'nix-command' is disabled" - which reads as a
	// problem with the flake, not with the store. Found on gcp1, on a store
	// seeded before this file existed.
	if err := ensureConf(); err != nil {
		return nil, nil, err
	}
	crun, err := exec.LookPath("crun")
	if err != nil {
		return nil, nil, fmt.Errorf("a private nix store needs crun to bind it at /nix: %w", err)
	}

	bundle, err := userdir.Scratch("nixexec-")
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { os.RemoveAll(bundle) }

	// argv[0] is nix's multi-call name, and OCI takes args[0] as both the
	// path and argv[0] - so running <store>/bin/nix with argv[0] "nix-store"
	// is not expressible. The nix package ships bin/nix-store and the rest as
	// links beside bin/nix, so naming the right link gets the right
	// behaviour. Without this, `nix-store -qR` reaches nix as `nix -qR` and
	// fails with "unrecognised flag '-q'".
	inside := append([]string{filepath.Join(binDir, argv[0])}, argv[1:]...)
	if err := writeBundle(bundle, inside, dir); err != nil {
		cleanup()
		return nil, nil, err
	}

	id := "pipedpeer-nix-" + filepath.Base(bundle)
	cmd := exec.Command(crun, "run", "--bundle", bundle, id)
	// crun needs somewhere to keep container state, and a daemon does not
	// always have XDG_RUNTIME_DIR set.
	if os.Getenv("XDG_RUNTIME_DIR") == "" {
		rundir := filepath.Join(userdir.State(), "run")
		_ = os.MkdirAll(rundir, 0o700)
		cmd.Env = append(os.Environ(), "XDG_RUNTIME_DIR="+rundir)
	}
	return cmd, cleanup, nil
}

// onceReset exists for tests: Private() memoises deliberately, and a test that
// exercises both branches needs to clear it.
func onceReset() sync.Once { return sync.Once{} }
