package daemonapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// handleStoreCheck reports whether the daemon already has a store path cached,
// so a submitter can skip re-uploading and re-importing the closure. With
// runnable=1 the answer only counts the store as present when it is actually
// materialised and executable (pool spill depends on that; the NAR cache alone
// would let a peer pass the check but fail at pool-map time).
//
// The runnable check looks at the store path directly, not the NAR cache: with
// a shared /nix/store volume the closure is on disk but never has a local NAR,
// and a broadcast must not push it again (ponytail: shared-store rig shortcut;
// a per-node store still works because bin/run is only present after import).
// closurePaths lists every store path a closure depends on, dependencies
// first. That ordering matters: nix-store --import refuses a path whose
// references are not present yet, so a filtered subset stays importable only
// because the order is preserved.
func closurePaths(storePath string) ([]string, error) {
	cmd, cleanup, err := nixstore.Cmd("", "nix-store", "-qR", storePath)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		// Carry nix's own words: "exit status 1" on its own sends you
		// looking in the wrong place.
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return strings.Fields(string(out)), nil
}

// cudaName matches the store path names of CUDA builds: nixpkgs' cuda*
// packages and the torch-cu126-* wheel set nixgen pins for GPU jobs.
var cudaName = regexp.MustCompile(`(^|-)cu(da|1[0-9]{2}(-|$))`)

// closureNeedsGPU reports whether a closure carries CUDA libraries, which
// nixgen only puts in one when the job asked for a GPU.
func closureNeedsGPU(paths []string) bool {
	for _, p := range paths {
		base := filepath.Base(p)
		if i := strings.IndexByte(base, '-'); i >= 0 && cudaName.MatchString(base[i+1:]) {
			return true
		}
	}
	return false
}

// exportPaths writes a zstd-compressed NAR carrying exactly the given store paths.
func exportPaths(paths []string, destPath string) error {
	if len(paths) == 0 {
		return fmt.Errorf("nothing to export")
	}
	_ = nixsign.EnsureSigned(context.Background(), paths)
	cmd, cleanup, err := nixstore.Cmd("", append([]string{"nix-store", "--export"}, paths...)...)
	if err != nil {
		return err
	}
	defer cleanup()
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()
	zw, err := tarcodec.Writer(f)
	if err != nil {
		return err
	}
	cmd.Stdout = zw
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		zw.Close()
		return err
	}
	return zw.Close()
}

// peerMissingPaths asks a peer which of these store paths it lacks. A peer
// that does not understand the question (older build, or any error) yields
// nil and the caller falls back to sending the closure whole.
func peerMissingPaths(host string, port int, paths []string) ([]string, bool) {
	body, err := json.Marshal(map[string]any{"paths": paths, "substitute": true})
	if err != nil {
		return nil, false
	}
	url := fmt.Sprintf("http://%s:%d/v1/store/missing", host, port)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var out struct {
		Missing []string `json:"missing"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return nil, false
	}
	return out.Missing, true
}

// handleStoreMissing answers which of a closure's store paths this node does
// not already have.
//
// Closures are transferred whole today, keyed on the top-level store path, so
// two environments that differ by one package re-send everything they have in
// common — python, numpy, the whole interpreter tree — because the top-level
// hash differs. Nix store paths are already content-addressed, which makes
// them exactly the units a sender should be allowed to skip: the equivalent
// of shipping only the layers a registry lacks.
func (s *Server) handleStoreMissing(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Paths []string `json:"paths"`
		// Files (additive) are workspace file hashes; the answer's
		// missing_files lists those the workspace CAS lacks.
		Files []string `json:"files"`
		// Substitute (additive): the asker is about to send; leave out what
		// the binary cache serves and fetch it here instead.
		Substitute bool `json:"substitute"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	// A store path is immutable once valid, so presence is the whole question
	// — but a directory alone is not presence: a fetch killed mid-unpack (a
	// slow substitute cancelled by awaitFetches, an OOM) leaves an invalid
	// one, and a sender told it is there skips it and breaks the import.
	var onDisk []string
	for _, p := range req.Paths {
		if pathExists(p) && strings.HasPrefix(p, "/nix/store/") {
			onDisk = append(onDisk, p)
		}
	}
	invalid := map[string]bool{}
	if len(onDisk) > 0 {
		bad, err := invalidStorePaths(onDisk)
		if err != nil {
			log.Warn().Err(err).Int("paths", len(onDisk)).
				Msg("cannot ask nix which store paths are valid; asking the sender for all of them")
			bad = onDisk
		}
		for _, p := range bad {
			invalid[p] = true
		}
	}
	missing := make([]string, 0, len(req.Paths))
	for _, p := range req.Paths {
		if !pathExists(p) || invalid[p] {
			missing = append(missing, p)
		}
	}
	if req.Substitute {
		missing = substituteMissing(missing)
	}
	resp := map[string]any{"missing": missing}
	if req.Files != nil {
		resp["missing_files"] = casMissing(s.casDir(), req.Files)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleStoreCheck(w http.ResponseWriter, r *http.Request) {
	storePath := r.URL.Query().Get("path")
	if storePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store path required"})
		return
	}
	path, cached := s.narCache.narFileFor(storePath)
	if r.URL.Query().Get("runnable") == "1" {
		cached = phone.CanRun(storePath, r.Header)
	}
	writeJSON(w, http.StatusOK, map[string]any{"cached": cached, "nar_path": path, "importing": s.isImporting(storePath)})
}

// handleStoreImport accepts a closure NAR for a store path without creating a
// job record. Peers receive closures this way during broadcast: the closure is
// content-addressed, so importing it on every node lets pool spill (and only
// pool spill) fan a task out to those nodes.
func (s *Server) handleStoreImport(w http.ResponseWriter, r *http.Request) {
	// Stream the parts, as handleJobUpload does. ParseMultipartForm held up
	// to 512 MiB of the archive in a growing buffer before spilling the rest
	// to os.TempDir(): a multi-GB CUDA closure took a receiver to ~1 GB RSS
	// and the kernel killed its daemon mid-broadcast. The sender writes the
	// fields before the archive, so the archive is handled as it arrives.
	mr, err := r.MultipartReader()
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse form: " + err.Error()})
		return
	}
	readField := func(p io.Reader) string {
		b, _ := io.ReadAll(io.LimitReader(p, 4096))
		return string(b)
	}
	var storePath string
	var partial bool
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse form: " + err.Error()})
			return
		}
		switch part.FormName() {
		case "store_path":
			storePath = readField(part)
		case "partial":
			partial = readField(part) == "1"
		case "nar":
			if storePath == "" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store_path required (before nar)"})
				return
			}
			status, body := s.importStoreNAR(r.Context(), part, storePath, partial)
			part.Close()
			writeJSON(w, status, body)
			return
		}
		part.Close()
	}
	if storePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store_path required"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "nar file required"})
}

// importStoreNAR imports one streamed closure archive for handleStoreImport
// and returns the status and JSON body to answer with.
// ctx is the request's: a submitter that hangs up stops nix mid-import.
func (s *Server) importStoreNAR(ctx context.Context, nar io.Reader, storePath string, partial bool) (int, any) {
	defer s.markImporting(storePath)()
	fail := func(status int, msg string) (int, any) {
		return status, map[string]string{"error": msg}
	}
	// Which format arrived is decided by the bytes, not by the form field.
	// The two ends are never upgraded in the same instant, and a stream read
	// as the wrong format fails somewhere deep inside nix with an error
	// about the archive rather than about the version.
	// Paths this node chose to fetch itself must land before their
	// dependents are imported: each branch waits (awaitFetches) once the
	// archive is in, and a fetch slower than the sender answers 409.
	br := bufio.NewReaderSize(nar, 1024)
	if narpack.IsArchive(br) {
		// The CLI's own upload arrives this way too, so this is the event
		// line `pipedpeer events` shows for most imports.
		start := time.Now()
		if err := s.importSignedClosure(ctx, br, storePath); err != nil {
			if errors.Is(err, errFetchSlow) {
				return fail(http.StatusConflict, err.Error())
			}
			return fail(http.StatusInternalServerError, err.Error())
		}
		logClosureImported(storePath, start, "signed")
		return http.StatusOK, map[string]any{"cached": false, "imported": true, "signed": true}
	}

	// A partial archive carries only the paths this node was missing, which
	// makes it useless as a cache entry: cached under the closure's key, it
	// would later be forwarded to a third node as if it were the whole
	// thing, and that node would be left with an unimportable fragment. So
	// import it and cache nothing. If this node ever needs to seed a peer,
	// ensureLocal re-exports the complete closure from its own store.
	if partial {
		if err := os.MkdirAll(s.narCache.dir, 0o755); err != nil {
			return fail(http.StatusInternalServerError, err.Error())
		}
		tmp, err := os.CreateTemp(s.narCache.dir, "partial-*.nar")
		if err != nil {
			return fail(http.StatusInternalServerError, err.Error())
		}
		defer os.Remove(tmp.Name())
		if _, err := io.Copy(tmp, br); err != nil {
			tmp.Close()
			return fail(http.StatusInternalServerError, err.Error())
		}
		tmp.Close()
		if err := awaitFetches(ctx); err != nil {
			return fail(http.StatusConflict, err.Error())
		}
		start := time.Now()
		if err := s.importNARFile(ctx, tmp.Name()); err != nil {
			return fail(http.StatusInternalServerError, "import: "+err.Error())
		}
		logClosureImported(storePath, start, "partial")
		return http.StatusOK, map[string]any{"cached": false, "imported": true}
	}

	// Cache the NAR and materialise the closure in the local nix store so
	// /v1/pool/map can actually run it (<storePath>/bin/run must exist).
	if _, err := s.narCache.store(storePath, br); err != nil {
		return fail(http.StatusInternalServerError, "cache nar: "+err.Error())
	}
	if err := awaitFetches(ctx); err != nil {
		return fail(http.StatusConflict, err.Error())
	}
	if err := s.materializeClosureCtx(ctx, storePath); err != nil {
		return fail(http.StatusInternalServerError, "materialise closure: "+err.Error())
	}
	return http.StatusOK, map[string]any{"cached": true}
}

// signedImport is narpack.Import, as a variable so a test can reach what
// happens AFTER a successful import.
//
// Everything interesting here is on that side - that nothing is cached, that
// the requested root is what gets imported - and a test cannot get there
// without a real store holding a real closure, which would make the test
// depend on this machine having built one.
var signedImport = narpack.Import

// senderKey is the manifest's signer key if it proves it belongs to the
// signer it names; an archive with none yields none (an older sender).
func senderKey(m narpack.Manifest) ([]string, error) {
	if m.SignerKey == "" && m.Signer == "" {
		return nil, nil
	}
	if err := nixsign.Verify(m.SignerKey, m.Signer); err != nil {
		return nil, fmt.Errorf("refusing a closure whose signer key does not belong to its sender: %w", err)
	}
	return []string{m.SignerKey}, nil
}

// importSignedClosure takes a closure whose signatures survived the journey
// and lets nix decide whether to accept it.
//
// Nothing is cached here, and that is deliberate. The archive holds only the
// paths this node was missing, so it is not the closure and must never be
// forwarded to a third node as if it were. When this node later has to seed a
// peer, it publishes from its own store, which is complete.
//
// No signature check is performed HERE either. The import is an ordinary
// substitution, so the machine's own nix configuration decides - which is the
// only place its owner can see and change the decision. If it says the
// closure lacks a trusted signature, that error is what the submitter sees.
//
// The one key added is the sender's own, named in the manifest: a fresh node
// has not polled the sender yet, so its trusted-keys file lacks it and the
// first closure from every new peer was refused. The request is already
// token-authenticated, which is the same bar a key learned by polling clears,
// and the key must prove it belongs to the node it names. It is trusted for
// this import only; the next poll records it for good.
//
// A multi-user nix-daemon ignores keys passed per import, so there the key
// goes through the same step the poll uses (warnUntrustedKeys): the root-owned
// key helper adds it to nix.conf before the copy when setup installed one,
// else the refusal names `pipedpeer trust`.
func (s *Server) importSignedClosure(ctx context.Context, r io.Reader, storePath string) error {
	dir, err := userdir.Scratch("signedimport-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	m, err := narpack.Unpack(r, dir)
	if err != nil {
		return fmt.Errorf("unpacking the closure: %w", err)
	}
	// The sender says what the roots are, but this node asked about
	// storePath; importing what the archive names rather than what was
	// requested would let a peer materialise something else entirely.
	roots := []string{storePath}
	if storePath == "" && len(m.Roots) > 0 {
		roots = m.Roots
	}
	senderKeys, err := senderKey(m)
	if err != nil {
		return err
	}
	s.warnUntrustedKeys(senderKeys)
	if err := awaitFetches(ctx); err != nil {
		return err
	}
	if err := signedImport(ctx, dir, roots, senderKeys...); err != nil {
		return err
	}
	log.Info().Str("store", storePath).Int("roots", len(m.Roots)).
		Msg("imported a closure whose signatures this machine checked")
	return nil
}

// materializeClosure imports a cached NAR into the local nix store, skipping
// the work when the closure's run entrypoint already exists there.
func (s *Server) materializeClosure(storePath string) error {
	return s.materializeClosureCtx(context.Background(), storePath)
}

// materializeClosureCtx is materializeClosure stopped by ctx.
func (s *Server) materializeClosureCtx(ctx context.Context, storePath string) error {
	if runEntry := filepath.Join(storePath, "bin", "run"); pathExists(runEntry) {
		return nil
	}
	narPath, ok := s.narCache.narFileFor(storePath)
	if !ok {
		return fmt.Errorf("no cached nar for %s", storePath)
	}
	f, err := os.Open(narPath)
	if err != nil {
		return err
	}
	defer f.Close()
	start := time.Now()
	out, err := importNAR(ctx, f)
	if err != nil {
		return fmt.Errorf("nix-store --import: %v: %s", err, string(out))
	}
	logClosureImported(storePath, start, "full")
	return nil
}

// logClosureImported is the "closure imported" event line (how = full |
// partial | signed) that `pipedpeer events` shows as environment work.
func logClosureImported(storePath string, start time.Time, how string) {
	log.Info().Str("store", storePath).Float64("secs", time.Since(start).Round(10*time.Millisecond).Seconds()).
		Str("how", how).Msg("closure imported")
}

// importNARFile imports an archive into the local nix store without caching
// it. Used for partial archives, which describe only what this node was
// missing and must never be mistaken for the whole closure.
func (s *Server) importNARFile(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if out, err := importNAR(ctx, f); err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}

// Workspace CAS: every workspace file this node has received, stored once by
// its sha256. A submitter asks which of its files are missing (the "files"
// field of /v1/store/missing) and tars only those; the rest are copied out of
// here into the job's work dir. Copied, not hardlinked: a job that edits a
// file in place must not change what the next job gets.
