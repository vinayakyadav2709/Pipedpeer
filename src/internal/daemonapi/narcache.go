package daemonapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-chi/chi/v5"
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
// so a submitter can skip re-uploading and re-importing the closure.
func (s *Server) handleStoreCheck(w http.ResponseWriter, r *http.Request) {
	storePath := chi.URLParam(r, "storePath")
	if storePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "store path required"})
		return
	}
	path, cached := s.narCache.narFileFor(storePath)
	writeJSON(w, http.StatusOK, map[string]any{"cached": cached, "nar_path": path})
}
