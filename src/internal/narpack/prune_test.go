package narpack

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The cache is a compressed copy of every closure this node has sent, and
// nothing removed it: a machine shipping a few environments a day grew a
// second store beside its real one until the disk filled. Everything here is
// derived data - anything evicted is republished from this node's own store -
// so the only cost of a wrong eviction is compressing it again.

// put writes one cache entry of a given size and last-use time.
func put(t *testing.T, dir, hash string, size int, used time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "nar"), 0o755); err != nil {
		t.Fatal(err)
	}
	nar := filepath.Join(dir, "nar", hash+".nar.zst")
	if err := os.WriteFile(nar, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	ni := filepath.Join(dir, hash+".narinfo")
	body := "StorePath: /nix/store/" + hash + "-thing\nURL: nar/" + hash + ".nar.zst\n"
	if err := os.WriteFile(ni, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(ni, used, used); err != nil {
		t.Fatal(err)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// TestPruneEvictsTheLeastRecentlyUsedFirst.
func TestPruneEvictsTheLeastRecentlyUsedFirst(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	put(t, dir, "aaaa", 1000, now.Add(-72*time.Hour)) // oldest
	put(t, dir, "bbbb", 1000, now.Add(-1*time.Hour))
	put(t, dir, "cccc", 1000, now) // newest

	freed, err := Prune(dir, 2200)
	if err != nil {
		t.Fatal(err)
	}
	if freed == 0 {
		t.Fatal("nothing was evicted from a cache over its cap")
	}
	if exists(filepath.Join(dir, "aaaa.narinfo")) {
		t.Error("the least recently used entry survived")
	}
	for _, keep := range []string{"bbbb", "cccc"} {
		if !exists(filepath.Join(dir, keep+".narinfo")) {
			t.Errorf("%s was evicted although newer entries should go last", keep)
		}
	}
	size, err := Size(dir)
	if err != nil {
		t.Fatal(err)
	}
	if size > 2200 {
		t.Errorf("cache is %d bytes, over the 2200 cap", size)
	}
}

// An entry's archive is what costs the disk; evicting the metadata and
// leaving a multi-megabyte archive behind would satisfy no cap at all.
func TestPruneRemovesTheArchiveNotJustTheMetadata(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "aaaa", 5000, time.Now().Add(-time.Hour))
	if _, err := Prune(dir, 10); err != nil {
		t.Fatal(err)
	}
	if exists(filepath.Join(dir, "nar", "aaaa.nar.zst")) {
		t.Fatal("the archive was left behind, so the cap frees nothing")
	}
}

// A cache under its cap must not be touched: eviction costs a republish, and
// doing it for no reason turns every transfer into a recompression.
func TestPruneLeavesACacheUnderTheCapAlone(t *testing.T) {
	dir := t.TempDir()
	put(t, dir, "aaaa", 100, time.Now().Add(-100*time.Hour))
	freed, err := Prune(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if freed != 0 {
		t.Errorf("evicted %d bytes from a cache well under its cap", freed)
	}
	if !exists(filepath.Join(dir, "aaaa.narinfo")) {
		t.Error("an entry was removed from a cache under its cap")
	}
}

// TestTouchSavesWhatIsStillBeingSent.
//
// Files are written when a path is first published and never rewritten, so
// eviction by file age would throw away the environment every job ships for
// being old, and keep something sent once months ago. Recording use is what
// makes it least-recently-USED rather than oldest.
func TestTouchSavesWhatIsStillBeingSent(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-90 * time.Hour)
	put(t, dir, "aaaa", 1000, old) // published long ago, still shipped daily
	put(t, dir, "bbbb", 1000, time.Now().Add(-time.Hour))

	Touch(dir, []string{"/nix/store/aaaa-thing"})

	if _, err := Prune(dir, 1100); err != nil {
		t.Fatal(err)
	}
	if !exists(filepath.Join(dir, "aaaa.narinfo")) {
		t.Error("the entry this node is still sending was evicted for being old")
	}
	if exists(filepath.Join(dir, "bbbb.narinfo")) {
		t.Error("the entry nobody has asked for survived")
	}
}

// A corrupt entry still occupies disk. If it could not be evicted it would
// pin the cache over its cap for good.
func TestACorruptEntryIsStillEvictable(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "junk.narinfo"),
		make([]byte, 4000), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Prune(dir, 10); err != nil {
		t.Fatal(err)
	}
	if exists(filepath.Join(dir, "junk.narinfo")) {
		t.Fatal("an unparseable entry cannot be evicted, so the cap can never be met")
	}
}

func TestMaxBytesIsOverridable(t *testing.T) {
	t.Setenv(MaxBytesEnv, "12345")
	if got := MaxBytes(); got != 12345 {
		t.Errorf("MaxBytes() = %d, want 12345", got)
	}
	t.Setenv(MaxBytesEnv, "not-a-number")
	if got := MaxBytes(); got != DefaultMaxBytes {
		t.Errorf("a nonsense override gave %d, want the default", got)
	}
}
