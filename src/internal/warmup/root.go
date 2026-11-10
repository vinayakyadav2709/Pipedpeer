package warmup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// RootEnv names the link the warm-up's own run registers as an indirect Nix GC
// root for the environment it built. Start sets it for its child only; an
// ordinary run never sees it and adds no root. Not read by the shim.
const RootEnv = "PIPEDPEER_WARMUP_ROOT"

// RootPath is the fixed GC-root link: one, so only the current build's
// warm-up environment is held.
func RootPath() string { return filepath.Join(userdir.State(), "warmup-env") }

// rootLive: false only when a root link exists and no longer resolves (the
// environment was collected or deleted). No link means none could be
// registered (no nix here, a failed add), so the log alone decides. A private
// store's link points at /nix/store/... inside its namespace: HostPath maps it.
func rootLive() bool {
	target, err := os.Readlink(RootPath())
	if err != nil {
		return true
	}
	_, err = os.Stat(nixstore.HostPath(target))
	return err == nil
}

// addRoot makes link an indirect GC root of storePath. A var for tests, which
// must not reach a real Nix store.
var addRoot = func(link, storePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd, cleanup, err := nixstore.CmdContext(ctx, "", "nix-store", "--add-root", link, "--indirect", "-r", storePath)
	if err != nil {
		return err
	}
	defer cleanup()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("nix-store --add-root: %w: %s", err, out)
	}
	return nil
}

// RegisterRoot is called by `run --check-only` after it built storePath. When
// this run is the warm-up's (RootEnv set) it replaces the previous root with
// one for storePath. A failure is a warning: the warm-up still counts, it only
// stays open to a garbage collection.
func RegisterRoot(storePath string) {
	link := os.Getenv(RootEnv)
	if link == "" || storePath == "" {
		return
	}
	if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "pipedpeer: warm-up: removing the earlier GC root %s: %v (environment left without a new root)\n", link, err)
		return
	}
	if err := addRoot(link, storePath); err != nil {
		fmt.Fprintf(os.Stderr, "pipedpeer: warm-up: could not keep %s from garbage collection: %v\n", storePath, err)
	}
}
