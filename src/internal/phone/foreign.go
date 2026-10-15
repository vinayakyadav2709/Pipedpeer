package phone

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nixgen"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"github.com/rs/zerolog/log"
)

// Foreign environments: a node of another system (a phone among laptops)
// cannot run the submitter's closure, so the submitter's node sends the job's
// nixgen.Recipe and this node builds the same environment for its own system.
// Built envs are cached by flake hash under foreignDir()/<hash>, whose
// `result` out-link is also the nix GC root; foreignDir()/by-store/<closure
// basename> links the submitter's store path to it. Until the build is done
// CanRun answers not runnable for that store path (logged): a chunk never
// falls back to a python without the job's libraries.
//
// ponytail: built envs are never deleted; add an LRU sweep when phones fill up.

func foreignDir() string { return filepath.Join(userdir.Data(), "foreign-envs") }

var (
	foreignMu sync.Mutex
	// foreignBusy is the hash being built; one build at a time (a phone has
	// little memory and one network link). foreignFailed holds the last error
	// per hash, cleared when the next offer retries it.
	foreignBusy   = map[string]bool{}
	foreignFailed = map[string]string{}
	foreignLogged = map[string]string{} // store -> state last logged by CanRun
	foreignBuild  sync.Mutex

	// buildFlake builds dir's flake for system into dir/result. A variable so
	// tests use a fake nix.
	buildFlake = func(dir, system string) error {
		cmd, cleanup, err := nixstore.CmdContext(context.Background(), dir, "nix", "build",
			"path:.#packages."+system+".default", "--out-link", filepath.Join(dir, "result"),
			"--option", "build-users-group", "")
		if err != nil {
			return err
		}
		defer cleanup()
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("nix build: %v: %s", err, tail(out))
		}
		return nil
	}
	system = identity.System
)

// storeKey is the by-store link name for a submitter's store path, or "" when
// the path is not a plain /nix/store entry.
func storeKey(store string) string {
	base := filepath.Base(store)
	if !strings.HasPrefix(store, "/nix/store/") || filepath.Dir(store) != "/nix/store" || base == "." || base == ".." {
		return ""
	}
	return base
}

// BuildForeign maps the submitter's store path to recipe built for this
// node's system and starts that build in the background unless it is built
// or building. It returns at once.
func BuildForeign(store string, r nixgen.Recipe) error {
	key := storeKey(store)
	if key == "" {
		return fmt.Errorf("not a store path: %q", store)
	}
	sys := system()
	flake, err := r.Flake(sys)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(flake))
	hash := hex.EncodeToString(sum[:16])
	dir := filepath.Join(foreignDir(), hash)
	if err := os.MkdirAll(filepath.Join(foreignDir(), "by-store"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "flake.nix"), []byte(flake), 0o644); err != nil {
		return err
	}
	link := filepath.Join(foreignDir(), "by-store", key)
	if cur, _ := os.Readlink(link); cur != hash {
		os.Remove(link)
		if err := os.Symlink(hash, link); err != nil {
			return err
		}
	}
	if exists(filepath.Join(dir, "result", "bin", "run")) {
		return nil
	}
	foreignMu.Lock()
	if foreignBusy[hash] {
		foreignMu.Unlock()
		return nil
	}
	foreignBusy[hash] = true
	delete(foreignFailed, hash)
	foreignMu.Unlock()

	go func() {
		foreignBuild.Lock()
		defer foreignBuild.Unlock()
		log.Info().Str("store", store).Str("env", dir).Str("system", sys).
			Msg("building another system's job environment for this node; its chunks wait until it is built")
		err := buildFlake(dir, sys)
		foreignMu.Lock()
		delete(foreignBusy, hash)
		if err != nil {
			foreignFailed[hash] = err.Error()
		}
		foreignMu.Unlock()
		if err != nil {
			log.Error().Err(err).Str("store", store).Str("env", dir).
				Msg("cannot build the job environment for this node; it takes none of this job's chunks")
			return
		}
		log.Info().Str("store", store).Str("env", dir).Msg("job environment built; this node now takes its chunks")
	}()
	return nil
}

// foreignRun is the run wrapper built here for the submitter's store path:
// known is false when no recipe was ever offered for it; run is "" while the
// build is pending or failed (why says which).
func foreignRun(store string) (run, why string, known bool) {
	key := storeKey(store)
	if key == "" {
		return "", "", false
	}
	hash, err := os.Readlink(filepath.Join(foreignDir(), "by-store", key))
	if err != nil {
		return "", "", false
	}
	dir := filepath.Join(foreignDir(), hash)
	if r := filepath.Join(dir, "result", "bin", "run"); exists(r) {
		return r, "", true
	}
	foreignMu.Lock()
	defer foreignMu.Unlock()
	switch {
	case foreignBusy[hash]:
		why = "still building the job environment at " + dir
	case foreignFailed[hash] != "":
		why = "building the job environment failed: " + foreignFailed[hash]
	default:
		why = "the job environment at " + dir + " is not built (the next offer retries)"
	}
	return "", why, true
}

// logForeign logs why store is not runnable, once per store and reason.
func logForeign(store, why string) {
	foreignMu.Lock()
	seen := foreignLogged[store] == why
	foreignLogged[store] = why
	foreignMu.Unlock()
	if !seen {
		log.Warn().Str("store", store).Str("reason", why).Msg("phone: not runnable here yet")
	}
}
