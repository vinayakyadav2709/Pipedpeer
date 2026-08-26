// Package userdir resolves where pipedpeer keeps its files.
//
// The default used to be $TMPDIR. That is usually a tmpfs, which is RAM
// backed and sized to a fraction of it, so the daemon's own state competed
// with the jobs it was running for the same memory — a workspace with a big
// dataset once filled it and the daemon started rejecting uploads it had
// plenty of disk for. It is also wiped on reboot, which loses the node
// identity and the closure cache, so a restarted machine re-downloads
// gigabytes it already had.
//
// So: state that must survive a reboot goes under XDG_STATE_HOME, things
// that are expensive to rebuild but safe to lose go under XDG_CACHE_HOME,
// and only genuinely short-lived scratch stays in a temp dir — on disk,
// under the cache root, rather than in RAM.
package userdir

import (
	"os"
	"path/filepath"
)

const app = "pipedpeer"

func base(envVar, fallback string) string {
	if v := os.Getenv(envVar); v != "" {
		return filepath.Join(v, app)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		// No home: a temp dir is wrong for all the reasons above, but it is
		// better than failing to start, and this only happens in stripped
		// containers where nothing persists anyway.
		return filepath.Join(os.TempDir(), app)
	}
	return filepath.Join(home, fallback, app)
}

// State holds things that must survive a reboot: pid files, logs, the node
// identity, job history.
func State() string { return base("XDG_STATE_HOME", ".local/state") }

// Cache holds things that are expensive to rebuild but safe to lose: closure
// archives, parsed chunks.
func Cache() string { return base("XDG_CACHE_HOME", ".cache") }

// Data holds user-facing records: job directories, the node store.
func Data() string { return base("XDG_DATA_HOME", ".local/share") }

// Scratch makes a temporary directory that is not on a tmpfs. Exported
// closures reach several gigabytes, and building one in RAM is how a machine
// that had the disk for it runs out of memory instead.
func Scratch(prefix string) (string, error) {
	root := filepath.Join(Cache(), "scratch")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	return os.MkdirTemp(root, prefix)
}
