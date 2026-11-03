package daemonapi

import (
	"os"
	"path/filepath"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// EnsureCrunStateDir gives crun a state root that exists for the daemon's
// whole life. Run as a user, crun keeps container state under
// $XDG_RUNTIME_DIR/crun, and without that variable it falls back to
// /run/crun, which only root may create: every job then fails with
// "create directory `/run/crun`". A daemon started by a systemd system unit
// has no XDG_RUNTIME_DIR, and one started from an ssh session keeps a path
// that logind deletes at that user's last logout. Every crun call the daemon
// makes (run, kill, state, delete) must agree on the root, so the variable is
// set once, for the process, before any job runs. A runtime dir that exists
// is left alone: it is where the user's systemd and session bus live.
// nixstore's sandbox does the same per command.
func EnsureCrunStateDir() (string, error) {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			return dir, nil
		}
	}
	dir := filepath.Join(userdir.State(), "run")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return dir, os.Setenv("XDG_RUNTIME_DIR", dir)
}
