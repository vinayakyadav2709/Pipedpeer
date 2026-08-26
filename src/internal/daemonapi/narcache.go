package daemonapi

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
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
	return path, true
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
	nixPath, err := exec.LookPath("nix")
	if err != nil {
		return "", false
	}
	out, err := (&exec.Cmd{Path: nixPath, Args: []string{"nix-store", "-qR", storePath}}).Output()
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
	gz := gzip.NewWriter(tmp)
	export := &exec.Cmd{
		Path:   nixPath,
		Args:   append([]string{"nix-store", "--export"}, paths...),
		Stdout: gz,
		Stderr: os.Stderr,
	}
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
	nixPath, err := exec.LookPath("nix")
	if err != nil {
		return nil, err
	}
	out, err := (&exec.Cmd{Path: nixPath, Args: []string{"nix-store", "-qR", storePath}}).Output()
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(out)), nil
}

// exportPaths writes a gzipped NAR carrying exactly the given store paths.
func exportPaths(paths []string, destPath string) error {
	if len(paths) == 0 {
		return fmt.Errorf("nothing to export")
	}
	nixPath, err := exec.LookPath("nix")
	if err != nil {
		return err
	}
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	cmd := &exec.Cmd{
		Path:   nixPath,
		Args:   append([]string{"nix-store", "--export"}, paths...),
		Stdout: gz,
		Stderr: os.Stderr,
	}
	return cmd.Run()
}

// peerMissingPaths asks a peer which of these store paths it lacks. A peer
// that does not understand the question (older build, or any error) yields
// nil and the caller falls back to sending the closure whole.
func peerMissingPaths(host string, port int, paths []string) ([]string, bool) {
	body, err := json.Marshal(map[string]any{"paths": paths})
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
	writeJSON(w, http.StatusOK, map[string]any{"missing": missing})
}

func (s *Server) handleStoreCheck(w http.ResponseWriter, r *http.Request) {
	storePath := r.URL.Query().Get("path")
	if storePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store path required"})
		return
	}
	path, cached := s.narCache.narFileFor(storePath)
	if r.URL.Query().Get("runnable") == "1" {
		cached = pathExists(filepath.Join(storePath, "bin", "run"))
	}
	writeJSON(w, http.StatusOK, map[string]any{"cached": cached, "nar_path": path})
}

// handleStoreImport accepts a closure NAR for a store path without creating a
// job record. Peers receive closures this way during broadcast: the closure is
// content-addressed, so importing it on every node lets pool spill (and only
// pool spill) fan a task out to those nodes.
func (s *Server) handleStoreImport(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(512 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to parse form: " + err.Error()})
		return
	}
	storePath := r.FormValue("store_path")
	if storePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store_path required"})
		return
	}
	narFile, _, err := r.FormFile("nar")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "nar file required: " + err.Error()})
		return
	}
	defer narFile.Close()

	// A partial archive carries only the paths this node was missing, which
	// makes it useless as a cache entry: cached under the closure's key, it
	// would later be forwarded to a third node as if it were the whole
	// thing, and that node would be left with an unimportable fragment. So
	// import it and cache nothing. If this node ever needs to seed a peer,
	// ensureLocal re-exports the complete closure from its own store.
	partial := r.FormValue("partial") == "1"
	if partial {
		if err := os.MkdirAll(s.narCache.dir, 0o755); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		tmp, err := os.CreateTemp(s.narCache.dir, "partial-*.nar")
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		defer os.Remove(tmp.Name())
		if _, err := io.Copy(tmp, narFile); err != nil {
			tmp.Close()
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		tmp.Close()
		if err := s.importNARFile(tmp.Name()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "import: " + err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"cached": false, "imported": true})
		return
	}

	// Cache the NAR and materialise the closure in the local nix store so
	// /v1/pool/map can actually run it (<storePath>/bin/run must exist).
	if _, err := s.narCache.store(storePath, narFile); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "cache nar: " + err.Error()})
		return
	}
	if err := s.materializeClosure(storePath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "materialise closure: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cached": true})
}

// materializeClosure imports a cached NAR into the local nix store, skipping
// the work when the closure's run entrypoint already exists there.
func (s *Server) materializeClosure(storePath string) error {
	if runEntry := filepath.Join(storePath, "bin", "run"); pathExists(runEntry) {
		return nil
	}
	narPath, ok := s.narCache.narFileFor(storePath)
	if !ok {
		return fmt.Errorf("no cached nar for %s", storePath)
	}
	nixPath, err := exec.LookPath("nix")
	if err != nil {
		return fmt.Errorf("nix not found in PATH")
	}
	f, err := os.Open(narPath)
	if err != nil {
		return err
	}
	defer f.Close()
	out, err := importNAR(nixPath, f)
	if err != nil {
		return fmt.Errorf("nix-store --import: %v: %s", err, string(out))
	}
	return nil
}

// importNARFile imports an archive into the local nix store without caching
// it. Used for partial archives, which describe only what this node was
// missing and must never be mistaken for the whole closure.
func (s *Server) importNARFile(path string) error {
	nixPath, err := exec.LookPath("nix")
	if err != nil {
		return fmt.Errorf("nix not found in PATH")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if out, err := importNAR(nixPath, f); err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}
