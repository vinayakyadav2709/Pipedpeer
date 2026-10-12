package daemonapi

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The NAR cache had no bound either: every closure this node exported or
// received stayed forever, a compressed second copy of the store beside the
// real one. Evicting is safe - ensureLocal re-exports from this node's own
// store when a peer needs one again - so a wrong eviction costs one export.

func TestTheNarCacheStaysUnderItsCap(t *testing.T) {
	dir := t.TempDir()
	c := &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}

	// Three closures, oldest used first.
	for i, name := range []string{"oldest", "middle", "newest"} {
		if _, err := c.store("/nix/store/"+name, bytes.NewReader(make([]byte, 1000))); err != nil {
			t.Fatal(err)
		}
		path, ok := c.narFileFor("/nix/store/" + name)
		if !ok {
			t.Fatalf("%s was not cached", name)
		}
		when := time.Now().Add(time.Duration(i-72) * time.Hour)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatal(err)
		}
	}

	freed := c.prune(2200)
	if freed == 0 {
		t.Fatal("nothing was evicted from a cache over its cap")
	}
	if _, ok := c.narFileFor("/nix/store/oldest"); ok {
		t.Error("the least recently used archive survived")
	}
	if _, ok := c.narFileFor("/nix/store/newest"); !ok {
		t.Error("the most recently used archive was evicted")
	}
}

// TestAnEvictedArchiveIsNotStillClaimedAsCached.
//
// narFileFor answers from an index. Deleting the file without the index entry
// leaves the cache reporting a hit and handing out a path that is not there -
// which would be read as "this peer already has the closure" and skip sending
// it, so the job would fail on the far side rather than transfer.
func TestAnEvictedArchiveIsNotStillClaimedAsCached(t *testing.T) {
	dir := t.TempDir()
	c := &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}
	if _, err := c.store("/nix/store/gone", bytes.NewReader(make([]byte, 5000))); err != nil {
		t.Fatal(err)
	}
	if freed := c.prune(10); freed == 0 {
		t.Fatal("nothing was evicted")
	}
	if path, ok := c.narFileFor("/nix/store/gone"); ok {
		t.Fatalf("the cache still claims %q after evicting it", path)
	}
	// And the claim must not come back from disk on the next start.
	fresh := &narCache{dir: c.dir, byID: map[string]string{}}
	fresh.load()
	if _, ok := fresh.byID["/nix/store/gone"]; ok {
		t.Error("the saved index still lists an archive that was deleted")
	}
}

func TestTheNarCacheUnderItsCapIsLeftAlone(t *testing.T) {
	dir := t.TempDir()
	c := &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}
	if _, err := c.store("/nix/store/keep", bytes.NewReader(make([]byte, 100))); err != nil {
		t.Fatal(err)
	}
	if freed := c.prune(1 << 20); freed != 0 {
		t.Errorf("evicted %d bytes from a cache under its cap", freed)
	}
	if _, ok := c.narFileFor("/nix/store/keep"); !ok {
		t.Error("an archive was removed from a cache under its cap")
	}
}
