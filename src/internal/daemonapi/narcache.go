package daemonapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/pipedpeer/pipedpeer/internal/narpack"
	"github.com/pipedpeer/pipedpeer/internal/nixsign"
	"github.com/pipedpeer/pipedpeer/internal/nixstore"
	"github.com/pipedpeer/pipedpeer/internal/tarcodec"
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
