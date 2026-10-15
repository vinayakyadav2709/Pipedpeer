package daemonapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// narCache content-addresses imported Nix closures by their store path. A Nix
// store path is already content-addressed (its hash reflects the closure), so
// the store path is the cache key. On a fan-out every task ships the same
// closure, so caching it once lets later tasks skip the upload and import.
//
// Cache layout:
//
//	<jobDir>/nar-cache/<sha256(storePath)>.nar   the exported NAR
//	<jobDir>/nar-cache/index.json                storePath → narFile
type narCache struct {
	dir  string
	mu   sync.Mutex
	byID map[string]string // storePath → absolute NAR path
}

func newNarCache() *narCache {
	base := defaultJobDir()
	dir := filepath.Join(base, "nar-cache")
	c := &narCache{dir: dir, byID: map[string]string{}}
	c.load()
	return c
}

func (c *narCache) load() {
	b, err := os.ReadFile(filepath.Join(c.dir, "index.json"))
	if err != nil {
		return
	}
	_ = json.Unmarshal(b, &c.byID)
}

func (c *narCache) save() {
	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return
	}
	b, _ := json.Marshal(c.byID)
	_ = os.WriteFile(filepath.Join(c.dir, "index.json"), b, 0644)
}

// narFileFor returns the cached NAR path for a store path, and whether it exists.
func (c *narCache) narFileFor(storePath string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	path, ok := c.byID[storePath]
	if !ok || path == "" {
		return "", false
	}
	if _, err := os.Stat(path); err != nil {
		delete(c.byID, storePath)
		return "", false
	}
	// Marked as used, so eviction removes what this node has stopped
	// sending rather than what it happened to cache first.
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	return path, true
}

// prune drops least-recently-used archives until the cache is under max.
//
// This cache had no bound at all: every closure this node ever exported or
// received stayed on disk forever, a compressed second copy of the store
// beside the real one. Removing an entry is safe - ensureLocal re-exports
// from this node's own store when a peer needs one again - so the cost of a
// wrong eviction is doing the export twice.
//
// Returns the bytes freed.
func (c *narCache) prune(max int64) int64 {
	if max <= 0 {
		return 0
	}
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return 0
	}
	type item struct {
		path string
		size int64
		used time.Time
	}
	var items []item
	var total int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".nar") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		items = append(items, item{filepath.Join(c.dir, e.Name()), info.Size(), info.ModTime()})
		total += info.Size()
	}
	if total <= max {
		return 0
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].used.Before(items[j].used) })

	c.mu.Lock()
	defer c.mu.Unlock()
	byPath := map[string]string{}
	for store, p := range c.byID {
		byPath[p] = store
	}
	var freed int64
	for _, it := range items {
		if total-freed <= max {
			break
		}
		if err := os.Remove(it.path); err != nil {
			continue
		}
		// The index has to lose it too, or narFileFor hands out a path that
		// is not there and the caller reports a cache hit for nothing.
		if store, ok := byPath[it.path]; ok {
			delete(c.byID, store)
		}
		freed += it.size
	}
	c.save()
	return freed
}

// store caches the NAR body for a store path and returns the cached path.
func (c *narCache) store(storePath string, src io.Reader) (string, error) {
	sum := sha256.Sum256([]byte(storePath))
	key := hex.EncodeToString(sum[:])

	if path, ok := c.narFileFor(storePath); ok {
		return path, nil
	}

	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return "", err
	}
	dst := filepath.Join(c.dir, key+".nar")
	out, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	defer out.Close()
	if _, err := io.Copy(out, src); err != nil {
		os.Remove(dst)
		return "", err
	}

	c.mu.Lock()
	c.byID[storePath] = dst
	c.save()
	c.mu.Unlock()

	// Bounded after the new entry is in place, so a cache already at its cap
	// makes room rather than refusing to take what it was just handed.
	if freed := c.prune(narpack.MaxBytes()); freed > 0 {
		log.Info().Str("freed", narpack.Human(freed)).
			Msg("evicted least-recently-used closure archives to stay under the cache cap")
	}
	return dst, nil
}

// ensureLocal returns a cached NAR for a store path, exporting it from this
// node's own Nix store when nothing was ever uploaded.
//
// The cache is only filled by an upload, and the submitter skips uploading
// whenever the target already has the closure — which is always true when the
// job lands on the machine that built it. That node then has the closure and
// no NAR, so it silently seeds none of its peers, none of them become
// eligible for spill, and every chunk runs at home while the receipt happily
// reports work "distributed" to a daemon that kept it. Exporting on demand
// closes that hole; the result is cached, so the cost is paid once.
func (c *narCache) ensureLocal(storePath string) (string, bool) {
	if path, ok := c.narFileFor(storePath); ok {
		return path, true
	}
	if storePath == "" {
		return "", false
	}
	if _, err := os.Stat(filepath.Join(storePath, "bin", "run")); err != nil {
		return "", false // not materialised here; nothing to export
	}
	qcmd, qcleanup, err := nixstore.Cmd("", "nix-store", "-qR", storePath)
	if err != nil {
		return "", false
	}
	out, err := qcmd.Output()
	qcleanup()
	if err != nil {
		return "", false
	}
	paths := strings.Fields(string(out))
	if len(paths) == 0 {
		return "", false
	}
	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return "", false
	}
	tmp, err := os.CreateTemp(c.dir, "export-*.nar")
	if err != nil {
		return "", false
	}
	defer os.Remove(tmp.Name())
	gz, err := tarcodec.Writer(tmp)
	if err != nil {
		tmp.Close()
		return "", false
	}
	// Signed before it leaves, so the far side can require a signature
	// instead of being configured to stop asking for one.
	_ = nixsign.EnsureSigned(context.Background(), paths)
	export, ecleanup, err := nixstore.Cmd("", append([]string{"nix-store", "--export"}, paths...)...)
	if err != nil {
		tmp.Close()
		return "", false
	}
	defer ecleanup()
	export.Stdout = gz
	export.Stderr = os.Stderr
	if err := export.Run(); err != nil {
		tmp.Close()
		return "", false
	}
	if err := gz.Close(); err != nil {
		tmp.Close()
		return "", false
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		tmp.Close()
		return "", false
	}
	dst, err := c.store(storePath, tmp)
	tmp.Close()
	if err != nil {
		return "", false
	}
	return dst, true
}

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
	missing := make([]string, 0, len(req.Paths))
	for _, p := range req.Paths {
		// A store path is immutable once it exists, so presence is the whole
		// question — there is no version of it that could be stale.
		if !pathExists(p) {
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
	writeJSON(w, http.StatusOK, map[string]any{"cached": cached, "nar_path": path})
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
	fail := func(status int, msg string) (int, any) {
		return status, map[string]string{"error": msg}
	}
	// Which format arrived is decided by the bytes, not by the form field.
	// The two ends are never upgraded in the same instant, and a stream read
	// as the wrong format fails somewhere deep inside nix with an error
	// about the archive rather than about the version.
	// Paths this node chose to fetch itself must land before their
	// dependents are imported.
	substituting.Wait()
	br := bufio.NewReaderSize(nar, 1024)
	if narpack.IsArchive(br) {
		// The CLI's own upload arrives this way too, so this is the event
		// line `pipedpeer events` shows for most imports.
		start := time.Now()
		if err := s.importSignedClosure(ctx, br, storePath); err != nil {
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

// casEntry is one workspace file the upload did not carry: where it goes in
// the work dir and which CAS object fills it. Wire type of the additive
// "workspace_cas" upload field.
type casEntry struct {
	Name string `json:"name"`
	SHA  string `json:"sha256"`
	Mode int64  `json:"mode"`
}

// casMaxBytes bounds the CAS; least recently used objects go first.
// ponytail: fixed cap, make it a setting if a dataset-heavy node needs more.
const casMaxBytes int64 = 4 << 30

func (s *Server) casDir() string {
	return filepath.Join(filepath.Dir(s.narCache.dir), "workspace-cas")
}

func validSHA(h string) bool {
	if len(h) != 64 {
		return false
	}
	_, err := hex.DecodeString(h)
	return err == nil
}

// casMissing lists the hashes the CAS lacks, touching the ones it has so a
// prune between the question and the upload does not take them.
func casMissing(dir string, shas []string) []string {
	missing := make([]string, 0)
	now := time.Now()
	for _, h := range shas {
		if !validSHA(h) {
			missing = append(missing, h)
			continue
		}
		if err := os.Chtimes(filepath.Join(dir, h), now, now); err != nil {
			missing = append(missing, h)
		}
	}
	return missing
}

// casWriter stores one file into the CAS as it is written, by its sha256.
type casWriter struct {
	dir string
	tmp *os.File
	h   hash.Hash
}

func newCASWriter(dir string) (*casWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, "incoming-*")
	if err != nil {
		return nil, err
	}
	return &casWriter{dir: dir, tmp: tmp, h: sha256.New()}, nil
}

func (c *casWriter) Write(p []byte) (int, error) {
	c.h.Write(p)
	return c.tmp.Write(p)
}

// commit names the object by its hash; abort drops it.
func (c *casWriter) commit() error {
	if err := c.tmp.Close(); err != nil {
		os.Remove(c.tmp.Name())
		return err
	}
	return os.Rename(c.tmp.Name(), filepath.Join(c.dir, hex.EncodeToString(c.h.Sum(nil))))
}

func (c *casWriter) abort() {
	c.tmp.Close()
	os.Remove(c.tmp.Name())
}

// materializeFromCAS copies the files an upload left out from the CAS into
// workDir, stamping them as uploaded like extracted ones.
func materializeFromCAS(casDir, workDir string, entries []casEntry, uploaded map[string]FileStamp) error {
	cleanWork := filepath.Clean(workDir) + string(os.PathSeparator)
	for _, e := range entries {
		target := filepath.Clean(filepath.Join(workDir, e.Name))
		if !strings.HasPrefix(target, cleanWork) || !validSHA(e.SHA) {
			return fmt.Errorf("workspace file %q: bad name or hash", e.Name)
		}
		if err := copyFileMode(filepath.Join(casDir, e.SHA), target, os.FileMode(e.Mode).Perm()); err != nil {
			return fmt.Errorf("workspace file %q from the cache: %w", e.Name, err)
		}
		now := time.Now()
		_ = os.Chtimes(filepath.Join(casDir, e.SHA), now, now)
		if info, err := os.Stat(target); err == nil {
			uploaded[strings.TrimPrefix(target, cleanWork)] = stampOf(info)
		}
	}
	return nil
}

func copyFileMode(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// pruneCAS removes least recently used objects until the CAS is under max.
func pruneCAS(dir string, max int64) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		log.Warn().Err(err).Str("dir", dir).Msg("workspace cache: cannot list for pruning")
		return
	}
	type obj struct {
		path string
		size int64
		used time.Time
	}
	var objs []obj
	var total int64
	for _, e := range ents {
		if info, err := e.Info(); err == nil && info.Mode().IsRegular() {
			objs = append(objs, obj{filepath.Join(dir, e.Name()), info.Size(), info.ModTime()})
			total += info.Size()
		}
	}
	if total <= max {
		return
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].used.Before(objs[j].used) })
	for _, o := range objs {
		if total <= max {
			break
		}
		if err := os.Remove(o.path); err != nil {
			log.Warn().Err(err).Str("path", o.path).Msg("workspace cache: prune failed")
			continue
		}
		total -= o.size
	}
}

// Receiver-side substitution: store paths cache.nixos.org already serves are
// left out of a /v1/store/missing answer and fetched by this node itself,
// in parallel with the sender packing and sending the rest. An import waits
// for those fetches, since nix refuses a path whose references are absent.
// A failed fetch is logged; the import then fails on the missing reference
// and the sender falls back to sending the closure whole.

// binaryCache is where substitutable paths are looked up.
var binaryCache = "https://cache.nixos.org"

// substitutable reports which paths binaryCache serves; a variable so tests
// need no network.
var substitutable = narinfoOnCache

// realise fetches paths from the node's substituters; a variable for tests.
var realise = func(ctx context.Context, paths []string) error {
	cmd, cleanup, err := nixstore.CmdContext(ctx, "", append([]string{"nix-store", "-r"}, paths...)...)
	if err != nil {
		return err
	}
	defer cleanup()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// substituting tracks fetches in flight; importStoreNAR waits for them.
// ponytail: one global wait, not per closure; key by root if unrelated
// imports start waiting on each other's fetches.
var substituting sync.WaitGroup

// narinfoOnCache asks binaryCache for each path's narinfo, 16 at a time, all
// within 2 s so an offline node answers the missing question promptly.
func narinfoOnCache(paths []string) map[string]bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	found := make(map[string]bool)
	var mu sync.Mutex
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var failures atomic.Int64
	for _, p := range paths {
		hashPart, _, ok := strings.Cut(filepath.Base(p), "-")
		if !ok || !strings.HasPrefix(p, "/nix/store/") {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			req, err := http.NewRequestWithContext(ctx, http.MethodHead, binaryCache+"/"+hashPart+".narinfo", nil)
			if err != nil {
				failures.Add(1)
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				failures.Add(1)
				return
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				mu.Lock()
				found[p] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if n := failures.Load(); n > 0 {
		log.Warn().Int64("paths", n).Str("cache", binaryCache).
			Msg("binary cache lookup failed; those paths are asked of the sender")
	}
	return found
}

// substituteMissing removes from missing the paths binaryCache serves and
// starts fetching them, returning what the sender must still send.
func substituteMissing(missing []string) []string {
	if len(missing) == 0 {
		return missing
	}
	found := substitutable(missing)
	if len(found) == 0 {
		return missing
	}
	rest := make([]string, 0, len(missing)-len(found))
	var fetch []string
	for _, p := range missing {
		if found[p] {
			fetch = append(fetch, p)
		} else {
			rest = append(rest, p)
		}
	}
	substituting.Add(1)
	go func() {
		defer substituting.Done()
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		if err := realise(ctx, fetch); err != nil {
			log.Warn().Err(err).Int("paths", len(fetch)).
				Msg("fetching paths from the binary cache failed; the import will ask for the whole closure")
			return
		}
		log.Info().Int("paths", len(fetch)).Float64("secs", time.Since(start).Seconds()).
			Msg("fetched paths from the binary cache instead of the sender")
	}()
	return rest
}
