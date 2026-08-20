package daemonapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
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
