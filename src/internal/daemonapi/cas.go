package daemonapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

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
