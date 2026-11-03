package userdir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ownerFile records the pid that made a scratch dir, so pruning can tell a
// dir a long-lived process (a warm worker, a running build) still uses from
// one a crashed or killed process left behind.
const ownerFile = ".owner"

// pruneKinds are the scratch dir name prefixes PruneScratch may remove.
var pruneKinds = []string{"build-", "warm-", "shards-"}

// ScratchMaxAge is how old a scratch dir must be before it is pruned.
const ScratchMaxAge = 24 * time.Hour

// PruneScratch removes build-*, warm-* and shards-* dirs under the scratch
// root that were last modified more than maxAge before now and whose owner
// process (the pid in ownerFile) is gone. Dirs with no owner file predate the
// marker and go by age alone. Each removal, and each failure, is passed to
// logf. It returns the removed paths.
func PruneScratch(maxAge time.Duration, now time.Time, logf func(string, ...any)) []string {
	root := filepath.Join(Cache(), "scratch")
	entries, err := os.ReadDir(root)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logf("scratch prune: cannot read %s: %v", root, err)
		}
		return nil
	}
	var removed []string
	for _, e := range entries {
		if !e.IsDir() || !hasKind(e.Name()) {
			continue
		}
		path := filepath.Join(root, e.Name())
		info, err := e.Info()
		if err != nil {
			logf("scratch prune: %s: %v", path, err)
			continue
		}
		if now.Sub(info.ModTime()) < maxAge || ownerAlive(path) {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			logf("scratch prune: cannot remove %s: %v", path, err)
			continue
		}
		logf("scratch prune: removed %s (unused for %s)", path, now.Sub(info.ModTime()).Round(time.Hour))
		removed = append(removed, path)
	}
	return removed
}

func hasKind(name string) bool {
	for _, k := range pruneKinds {
		if strings.HasPrefix(name, k) {
			return true
		}
	}
	return false
}

// ownerAlive reports whether the pid recorded in dir is a running process.
// A pid that is unreadable counts as not alive; a recycled pid keeps a dir
// longer than needed, which is the safe direction.
func ownerAlive(dir string) bool {
	b, err := os.ReadFile(filepath.Join(dir, ownerFile))
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return false
	}
	err = syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func markOwner(dir string) error {
	return os.WriteFile(filepath.Join(dir, ownerFile), []byte(fmt.Sprint(os.Getpid())), 0o644)
}
