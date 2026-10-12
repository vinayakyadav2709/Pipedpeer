package nixstore

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// nixVersion is pinned. Two nodes have to agree on store paths for the cache
// key to mean anything, and "whatever was current the day you ran setup" is
// not agreement.
const nixVersion = "2.31.3"

// Seed populates a private store with nix itself, so a machine with no /nix
// and no root can still build and import closures.
//
// The release tarball is a nix store: unpacking its store/ directory into
// ours gives a nix whose own dependencies are already present, which is what
// makes the bootstrap terminate. Nothing here needs privilege - the store is
// an ordinary directory in the user's data dir, and the binaries only need
// /nix to exist inside the namespace, where Cmd puts it.
func Seed(progress func(string, ...any)) error {
	if progress == nil {
		progress = func(string, ...any) {}
	}
	storeDir := filepath.Join(NixDir(), "store")
	if _, err := NixBinDir(); err == nil {
		return nil // already seeded
	}
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return fmt.Errorf("creating the private store: %w", err)
	}

	url := fmt.Sprintf("https://releases.nixos.org/nix/nix-%s/nix-%s-x86_64-linux.tar.xz",
		nixVersion, nixVersion)
	progress("    → Fetching nix %s into %s (no root needed)...\n", nixVersion, NixDir())

	scratch, err := userdir.Scratch("nixseed-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)

	tarball := filepath.Join(scratch, "nix.tar.xz")
	if err := download(url, tarball); err != nil {
		return err
	}

	// tar is a setup prerequisite already, and shelling out avoids carrying an
	// xz decompressor for a path that runs once per machine.
	unpack := filepath.Join(scratch, "unpack")
	if err := os.MkdirAll(unpack, 0o755); err != nil {
		return err
	}
	if out, err := exec.Command("tar", "-xJf", tarball, "-C", unpack).CombinedOutput(); err != nil {
		return fmt.Errorf("unpacking nix: %v: %s", err, out)
	}

	inner, err := filepath.Glob(filepath.Join(unpack, "nix-*", "store"))
	if err != nil || len(inner) != 1 {
		return fmt.Errorf("unexpected layout in the nix release tarball")
	}
	// -a preserves the read-only modes store paths carry; -n so a partially
	// seeded store is completed rather than half-overwritten.
	if out, err := exec.Command("cp", "-an", inner[0]+"/.", storeDir+"/").CombinedOutput(); err != nil {
		return fmt.Errorf("populating the private store: %v: %s", err, out)
	}

	if _, err := NixBinDir(); err != nil {
		return fmt.Errorf("nix is not usable after seeding: %w", err)
	}
	if err := ensureConf(); err != nil {
		return err
	}
	// Force the private path on now that it can work, so the verification
	// below and everything after it go through the namespace.
	privateOnce.Do(func() {})
	privateOnce.private = true

	progress("    → Verifying nix runs from the private store...\n")
	cmd, cleanup, err := Cmd("", "nix", "--version")
	if err != nil {
		return err
	}
	defer cleanup()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nix from the private store does not run: %v: %s", err, out)
	}
	progress("    → %s", out)
	return nil
}

func download(url, dest string) error {
	client := &http.Client{Timeout: 15 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("downloading nix: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downloading nix: %s returned %s", url, resp.Status)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("downloading nix: %w", err)
	}
	return nil
}

// requiredConf is what a private store needs in its nix.conf, as key/value
// pairs so a store written by an older build can be repaired setting by
// setting rather than only when the file is missing entirely.
var requiredConf = [][2]string{
	// Without this the first build fails with "experimental Nix feature
	// 'nix-command' is disabled", which reads as a problem with the flake.
	{"experimental-features", "nix-command flakes"},
	// There is no nixbld group and no way to create one without root; builds
	// run as the invoking user, as a single-user nix does anyway.
	{"build-users-group", ""},
	// The namespace maps exactly one uid, so the kernel denies setgroups in
	// it. nix tries to drop supplementary groups before building and fails
	// with "setgroups failed"; it has nothing to drop here. Found on a real
	// Ubuntu machine - the unprivileged container this was first built in
	// does not hit it, because there the daemon was already root inside.
	{"require-drop-supplementary-groups", "false"},
	// nix's own build sandbox creates a second user namespace inside ours and
	// writes its uid_map, which a machine restricting unprivileged user
	// namespaces refuses ("writing file '/proc/N/uid_map': Operation not
	// permitted"). Turning it off costs build purity, not safety: the job
	// sandbox is a separate mechanism and is untouched, and nearly every path
	// arrives substituted from the binary cache rather than built here. Store
	// paths are computed from a derivation's inputs, so peers still agree on
	// them either way.
	{"sandbox", "false"},
}

// ensureConf makes sure the private store's nix.conf carries every setting
// the namespace requires, adding any that are missing and leaving anything
// else in the file alone.
func ensureConf() error {
	dir := filepath.Join(Root(), "etc", "nix")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, "nix.conf")

	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	body := string(existing)

	var add []string
	for _, kv := range requiredConf {
		if !hasSetting(body, kv[0]) {
			add = append(add, kv[0]+" = "+kv[1])
		}
	}
	if len(add) == 0 {
		return nil
	}
	if body != "" && !strings.HasSuffix(body, "\n") {
		body += "\n"
	}
	body += strings.Join(add, "\n") + "\n"
	return os.WriteFile(path, []byte(body), 0o644)
}

// hasSetting reports whether key is already assigned, ignoring comments so a
// commented-out line does not count as set.
func hasSetting(conf, key string) bool {
	for _, line := range strings.Split(conf, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(name) == key {
			return true
		}
	}
	return false
}

// ConfDir is where the private store keeps nix.conf, as seen inside the
// namespace.
func ConfDir() string { return "/nix/etc/nix" }
