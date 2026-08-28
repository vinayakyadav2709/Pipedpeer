package narpack

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Bounding the cache this daemon publishes from.
//
// The cache is a copy of every closure this node has ever sent, compressed.
// Nothing removed it, so a machine that ships a few environments a day grows
// a second store alongside its real one and eventually fills the disk. That
// was true of the older NAR cache too, and adding a second unbounded copy of
// the same data is what makes it worth fixing rather than inheriting.
//
// Deleting from here is always safe. The cache is derived data: whatever is
// removed is republished from this node's own store the next time a peer
// needs it, at the cost of compressing it again. So the only thing a wrong
// eviction costs is time.

// DefaultMaxBytes is how much this node will keep published.
//
// Large enough for several ordinary environments and one big one - a CUDA
// torch closure is about 6.6 GB unpacked and rather less compressed - and
// small enough to be a bound rather than a formality. A machine with a
// different idea of "large" can say so.
const DefaultMaxBytes int64 = 8 << 30 // 8 GiB

// MaxBytesEnv overrides DefaultMaxBytes, in bytes.
const MaxBytesEnv = "PIPEDPEER_CLOSURE_CACHE_MAX"

// MaxBytes is the cap this node applies.
func MaxBytes() int64 {
	if v := os.Getenv(MaxBytesEnv); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return DefaultMaxBytes
}

// entry is one cached path: its metadata file, its archive, and their size.
type entry struct {
	narinfo string
	nar     string
	size    int64
	used    time.Time
}

// Touch records that these store paths were just sent.
//
// Eviction is by last use, and a file is only written when it is first
// published - so without this the cache would evict by age, and the
// environment every job ships would be thrown away for being old while
// something sent once in March survived. Failures are ignored: a cache entry
// that cannot be touched is a slightly worse eviction decision, not an error
// worth failing a transfer over.
func Touch(cacheDir string, paths []string) {
	index, err := Index(cacheDir)
	if err != nil {
		return
	}
	now := time.Now()
	for _, p := range paths {
		ni, ok := index[p]
		if !ok {
			continue
		}
		_ = os.Chtimes(filepath.Join(cacheDir, ni.Name), now, now)
	}
}

// Prune removes least-recently-used entries until the cache is under max.
//
// Returns how many bytes were freed. A cache already under the cap is left
// alone and costs one directory read.
func Prune(cacheDir string, max int64) (int64, error) {
	if max <= 0 {
		return 0, nil
	}
	entries, total, err := scan(cacheDir)
	if err != nil {
		return 0, err
	}
	// An early exit to skip the sort, not the guard: the loop below stops on
	// the same condition, so removing this changes nothing but speed.
	if total <= max {
		return 0, nil
	}

	// Oldest use first. A stable sort so two entries with the same timestamp
	// - which happens, since a closure's paths are published together - are
	// removed in a predictable order rather than an arbitrary one.
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].used.Before(entries[j].used)
	})

	var freed int64
	for _, e := range entries {
		if total-freed <= max {
			break
		}
		// The archive first: an entry whose narinfo survives without its
		// archive would be offered to a peer and fail on the far side, which
		// is worse than not having it. Removing the archive first means a
		// crash in between leaves a dangling narinfo, and Pack already fails
		// loudly on a missing archive rather than sending a broken closure.
		if e.nar != "" {
			_ = os.Remove(e.nar)
		}
		if err := os.Remove(e.narinfo); err != nil {
			continue
		}
		freed += e.size
	}
	return freed, nil
}

// scan lists what is in the cache and what it costs.
func scan(cacheDir string) ([]entry, int64, error) {
	names, err := os.ReadDir(cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	var out []entry
	var total int64
	for _, e := range names {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".narinfo") {
			continue
		}
		path := filepath.Join(cacheDir, e.Name())
		info, err := e.Info()
		if err != nil {
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		ni, perr := ParseNarinfo(e.Name(), f)
		f.Close()

		ent := entry{narinfo: path, size: info.Size(), used: info.ModTime()}
		if perr == nil && ni.URL != "" {
			ent.nar = filepath.Join(cacheDir, filepath.FromSlash(ni.URL))
			if ns, err := os.Stat(ent.nar); err == nil {
				ent.size += ns.Size()
			} else {
				ent.nar = ""
			}
		}
		// A narinfo that cannot be parsed still takes up room and still has
		// to be evictable, or a corrupt entry would pin the cache over its
		// cap forever.
		total += ent.size
		out = append(out, ent)
	}
	return out, total, nil
}

// Size reports what the cache currently occupies, for reporting.
func Size(cacheDir string) (int64, error) {
	_, total, err := scan(cacheDir)
	return total, err
}

// Human renders a byte count the way a person reads one.
func Human(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
