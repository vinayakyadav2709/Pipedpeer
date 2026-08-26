package nixstore

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
	if err := writeConf(); err != nil {
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

// writeConf gives the private store its own nix.conf.
//
// A system install keeps this at /etc/nix, which here is bound read-only from
// the host and on a nix-less machine does not exist at all - so without this
// nix comes up with flakes disabled and the first build fails with
// "experimental Nix feature 'nix-command' is disabled". The bundle points
// NIX_CONF_DIR at this file.
func writeConf() error {
	dir := filepath.Join(Root(), "etc", "nix")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	conf := "experimental-features = nix-command flakes\n" +
		// There is no nixbld group and no way to create one without root;
		// builds run as the invoking user, which is what a single-user nix
		// does anyway.
		"build-users-group =\n"
	return os.WriteFile(filepath.Join(dir, "nix.conf"), []byte(conf), 0o644)
}

// ConfDir is where the private store keeps nix.conf, as seen inside the
// namespace.
func ConfDir() string { return "/nix/etc/nix" }
