package daemonapi

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Sending a closure in the form that keeps its signatures.
//
// The old transfer was `nix-store --export`, which does not serialise
// signatures, so a receiver had to be configured with `require-sigs = false`
// - switching the check off for everything it installs, not only for
// pipedpeer. This builds the same closure as a small binary cache instead,
// where the signature is a field in each path's metadata and survives the
// journey.
//
// Chosen per peer, never assumed: a peer that has not been upgraded says
// nothing about closure formats and keeps receiving the old one. Both
// directions have to work at once, because two machines are never updated in
// the same instant.

// buildSignedClosure is signedClosureFor, as a variable so a test can drive
// the choice of format without a real closure in this machine's store.
//
// Without it the two outcomes are indistinguishable from outside: a peer that
// cannot take signed closures and a closure that could not be packed both end
// up sending the old format, so a test cannot tell whether the peer's answer
// was consulted at all.
var buildSignedClosure = (*Server).signedClosureFor

// closureCacheDir is where this daemon keeps the cache it publishes from.
//
// One directory for the whole daemon rather than one per transfer: nix skips
// a path already there, so the second closure sharing python and numpy with
// the first pays for neither.
func closureCacheDir() (string, error) {
	dir := filepath.Join(userdir.Data(), "closure-cache")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

// signedClosureFor writes an archive of storePath for this peer, sending only
// the paths it says it lacks, and returns the file and a cleanup.
//
// Reports ok=false when this closure cannot be sent that way at all - the
// caller then falls back to the old format, which is worse but works.
func (s *Server) signedClosureFor(ph *PeerHealth, storePath string) (path string, cleanup func(), ok bool) {
	// The closure has to exist in the local store before anything can be
	// said about it: it may only ever have arrived here as a cached archive.
	_ = s.materializeClosure(storePath)

	paths, err := closurePaths(storePath)
	if err != nil || len(paths) == 0 {
		log.Info().Err(err).Str("store", storePath).
			Msg("cannot list closure paths; falling back to the unsigned transfer")
		return "", nil, false
	}

	// Signed before it is published: the signature is written into the
	// cache's metadata at publish time, so signing afterwards would leave the
	// archive carrying nothing.
	if err := nixsign.EnsureSigned(context.Background(), paths); err != nil {
		log.Info().Err(err).Msg("could not sign the closure; the peer may refuse it")
	}

	cacheDir, err := closureCacheDir()
	if err != nil {
		return "", nil, false
	}
	if err := narpack.Publish(context.Background(), cacheDir, []string{storePath}); err != nil {
		log.Info().Err(err).Str("store", storePath).
			Msg("cannot publish the closure locally; falling back to the unsigned transfer")
		return "", nil, false
	}

	// What the peer lacks. A peer that cannot answer gets everything: a
	// missing answer is not an empty one, and treating it as one would send
	// metadata with no archives and break the import on the far side.
	missing, answered := peerMissingPaths(ph.Host, ph.Port, paths)
	if !answered {
		missing = nil
		log.Info().Str("peer", fmt.Sprintf("%s:%d", ph.Host, ph.Port)).
			Msg("peer did not say which paths it lacks; sending the whole closure")
	} else if len(missing) == 0 {
		log.Info().Msg("peer already has every path; sending metadata only")
		missing = []string{}
	} else {
		log.Info().Int("missing", len(missing)).Int("closure", len(paths)).
			Str("peer", fmt.Sprintf("%s:%d", ph.Host, ph.Port)).
			Msg("sending only the store paths this peer lacks, with signatures")
	}

	dir, err := userdir.Scratch("signedclosure-*")
	if err != nil {
		return "", nil, false
	}
	cleanup = func() { os.RemoveAll(dir) }
	out := filepath.Join(dir, "closure.tar")
	f, err := os.Create(out)
	if err != nil {
		cleanup()
		return "", nil, false
	}
	if err := narpack.Pack(cacheDir, []string{storePath}, paths, missing, f); err != nil {
		f.Close()
		cleanup()
		log.Info().Err(err).Msg("cannot pack the closure; falling back to the unsigned transfer")
		return "", nil, false
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", nil, false
	}
	return out, cleanup, true
}
