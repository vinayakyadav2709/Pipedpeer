package daemonapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/peercache"
)

// The peer layer of the nix cache: this machine's own store, served to
// authenticated peers. It never asks other peers, so a request cannot loop
// through the cluster. The local /nix-cache layer fans out to these routes.

// maxPeerStoreJobs caps the nix processes the peer layer runs at once.
const maxPeerStoreJobs = 8

// peerStoreSem is the counting semaphore for those processes.
var peerStoreSem = make(chan struct{}, maxPeerStoreJobs)

// Seams for tests.
var (
	storeGlob = filepath.Glob

	// pathInfoCmd returns stdout of `nix path-info --json --sigs <path>`.
	// Never --json-format: the private store's nix predates it.
	pathInfoCmd = func(ctx context.Context, path string) ([]byte, error) {
		cmd, cleanup, err := nixstore.CmdContext(ctx, "", "nix", "path-info", "--json", "--sigs", path)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := cmd.Run(); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}

	// dumpCmd starts `nix-store --dump <path>`. The returned func waits for
	// the process and releases its resources; call it after reading.
	dumpCmd = func(ctx context.Context, path string) (io.ReadCloser, func() error, error) {
		cmd, cleanup, err := nixstore.CmdContext(ctx, "", "nix-store", "--dump", path)
		if err != nil {
			return nil, nil, err
		}
		rc, err := cmd.StdoutPipe()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		if err := cmd.Start(); err != nil {
			cleanup()
			return nil, nil, err
		}
		return rc, func() error {
			defer cleanup()
			return cmd.Wait()
		}, nil
	}

	ensureSigned = nixsign.EnsureSigned
)

// acquirePeerStore takes a semaphore slot or gives up with the request.
func acquirePeerStore(ctx context.Context) (release func(), err error) {
	select {
	case peerStoreSem <- struct{}{}:
		return func() { <-peerStoreSem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// localStorePath resolves a store hash to the full store path held here, or
// "" when there is none. Derivations, lock files and chroot dirs are not
// store objects.
func localStorePath(hash string) string {
	matches, err := storeGlob(nixstore.HostPath("/nix/store/" + hash + "-*"))
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Msg("peer store: glob failed")
		return ""
	}
	for _, m := range matches {
		base := filepath.Base(m)
		if strings.HasSuffix(base, ".drv") || strings.HasSuffix(base, ".lock") || strings.HasSuffix(base, ".chroot") {
			continue
		}
		return "/nix/store/" + base
	}
	return ""
}

// readPathInfo runs path-info for path and returns its entry.
func readPathInfo(ctx context.Context, path string) (peercache.PathInfo, error) {
	out, err := pathInfoCmd(ctx, path)
	if err != nil {
		return peercache.PathInfo{}, err
	}
	infos, err := peercache.ParsePathInfo(out)
	if err != nil {
		return peercache.PathInfo{}, err
	}
	for _, pi := range infos {
		if pi.Path == path {
			return pi, nil
		}
	}
	return peercache.PathInfo{}, fmt.Errorf("path-info returned no entry for %s", path)
}

// handlePeerNarinfo serves GET /v1/store/narinfo/{hash} from the local store.
func (s *Server) handlePeerNarinfo(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	if !peercache.ValidHash(hash) {
		http.Error(w, "bad store hash", http.StatusBadRequest)
		return
	}
	path := localStorePath(hash)
	if path == "" {
		log.Debug().Str("hash", hash).Msg("peer store: no such path here")
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()
	release, err := acquirePeerStore(ctx)
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: gave up waiting for a slot")
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	defer release()

	pi, err := readPathInfo(ctx, path)
	if err != nil {
		// An invalid path is a normal miss.
		log.Debug().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: path-info failed")
		http.NotFound(w, r)
		return
	}
	if len(pi.Sigs) == 0 && pi.CA == "" {
		if err := ensureSigned(ctx, []string{path}); err != nil {
			log.Warn().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: signing failed, serving unsigned")
		} else if again, err := readPathInfo(ctx, path); err != nil {
			log.Warn().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: path-info after signing failed, serving unsigned")
		} else {
			pi = again
		}
	}
	ni, err := peercache.Narinfo(pi)
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: cannot render narinfo")
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/x-nix-narinfo")
	_, _ = io.WriteString(w, ni.String())
}

// handlePeerNar serves GET /v1/store/nar/{hash}: the path as a zstd NAR.
func (s *Server) handlePeerNar(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	if !peercache.ValidHash(hash) {
		http.Error(w, "bad store hash", http.StatusBadRequest)
		return
	}
	path := localStorePath(hash)
	if path == "" {
		log.Debug().Str("hash", hash).Msg("peer store: no such path here")
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()
	release, err := acquirePeerStore(ctx)
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: gave up waiting for a slot")
		http.Error(w, "busy", http.StatusServiceUnavailable)
		return
	}
	defer release()

	rc, wait, err := dumpCmd(ctx, path)
	if err != nil {
		log.Warn().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: cannot start nar dump")
		http.Error(w, "cannot dump path", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/zstd")
	zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		_ = rc.Close()
		_ = wait()
		log.Warn().Err(err).Str("hash", hash).Str("path", path).Msg("peer store: cannot start encoder")
		http.Error(w, "encoder", http.StatusInternalServerError)
		return
	}
	_, copyErr := io.Copy(zw, rc)
	_ = rc.Close()
	waitErr := wait()
	closeErr := zw.Close()
	for _, e := range []error{copyErr, waitErr, closeErr} {
		if e != nil {
			// Headers are gone; the truncated stream fails the peer's nix check.
			log.Warn().Err(e).Str("hash", hash).Str("path", path).Msg("peer store: nar stream failed")
			return
		}
	}
}
