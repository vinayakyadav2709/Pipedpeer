package console

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

const (
	metadataFile = "metadata.json"
	logTailBytes = 64 << 10
)

// The files `pipedpeer job --output` prints, in the order it prints them.
var jobLogFiles = []string{"stdout.log", "stderr.log", "remote_logs.txt"}

// jobRecord is one stored job with its start and finish times parsed.
type jobRecord struct {
	jobhistory.Record

	started  time.Time
	finished time.Time
}

// historyStore reads the job history directory. Records are re-parsed only
// when their file changes, since the views poll.
type historyStore struct {
	dir string
	// selfHost replaces a loopback run host, so jobs that ran on this machine
	// are named like every other node instead of by 127.0.0.1.
	selfHost string

	mu    sync.Mutex
	cache map[string]cachedRecord
}

type cachedRecord struct {
	modTime time.Time
	size    int64
	record  jobRecord
}

func newHistoryStore(dir, selfHost string) *historyStore {
	return &historyStore{dir: dir, selfHost: selfHost, cache: map[string]cachedRecord{}}
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// load returns every readable record, newest first. A missing history
// directory is an empty history.
func (h *historyStore) load() ([]jobRecord, error) {
	entries, err := os.ReadDir(h.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	seen := make(map[string]bool, len(entries))
	records := make([]jobRecord, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		rec, ok := h.readLocked(e.Name())
		if !ok {
			continue
		}
		seen[e.Name()] = true
		records = append(records, rec)
	}
	for id := range h.cache {
		if !seen[id] {
			delete(h.cache, id)
		}
	}
	sort.SliceStable(records, func(i, j int) bool { return records[i].started.After(records[j].started) })
	return records, nil
}

func (h *historyStore) readLocked(id string) (jobRecord, bool) {
	path := filepath.Join(h.dir, id, metadataFile)
	info, err := os.Stat(path)
	if err != nil {
		return jobRecord{}, false
	}
	if c, ok := h.cache[id]; ok && c.size == info.Size() && c.modTime.Equal(info.ModTime()) {
		return c.record, true
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return jobRecord{}, false
	}
	var rec jobRecord
	if err := json.Unmarshal(raw, &rec); err != nil || rec.ID == "" {
		return jobRecord{}, false
	}
	rec.started = parseTime(rec.StartedAt)
	rec.finished = parseTime(rec.FinishedAt)
	if h.selfHost != "" && isLoopbackHost(rec.RunHost) {
		rec.RunHost = h.selfHost
	}
	h.cache[id] = cachedRecord{modTime: info.ModTime(), size: info.Size(), record: rec}
	return rec, true
}

// get returns one record and its directory.
func (h *historyStore) get(id string) (jobRecord, string, bool) {
	if id == "" || id != filepath.Base(id) || strings.HasPrefix(id, ".") {
		return jobRecord{}, "", false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	rec, ok := h.readLocked(id)
	return rec, filepath.Join(h.dir, id), ok
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func unixMilli(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func (r jobRecord) view() JobView {
	return JobView{
		ID:                r.ID,
		Status:            r.Status,
		Error:             r.Error,
		StartedAt:         unixMilli(r.started),
		FinishedAt:        unixMilli(r.finished),
		DurationMs:        r.DurationMs,
		ScriptPath:        r.ScriptPath,
		JobName:           r.JobName,
		JobSet:            r.JobSet,
		RunHost:           r.RunHost,
		TargetID:          r.TargetID,
		PlacementSource:   r.PlacementSource,
		PlacementReason:   r.PlacementReason,
		CandidateCount:    r.CandidateCount,
		DegradedMode:      r.DegradedMode,
		PeakMemBytes:      r.PeakMemBytes,
		EstimatedMemBytes: r.EstimatedMemBytes,
		EstimationTier:    r.EstimationTier,
		Stage:             r.Stage,
		Detached:          r.Detached,
		Isolate:           r.Isolate,
	}
}

// matches reports whether the record contains q, case-insensitively, in any
// field a person would search by.
func (r jobRecord) matches(q string) bool {
	if q == "" {
		return true
	}
	q = strings.ToLower(q)
	for _, field := range []string{r.ID, r.ScriptPath, r.JobName, r.JobSet, r.RunHost, r.TargetID, r.Status, r.Error} {
		if strings.Contains(strings.ToLower(field), q) {
			return true
		}
	}
	return false
}

// logTail returns the end of a job's stored output, at most logTailBytes:
// stdout, then stderr and the remote logs under a heading each.
func logTail(dir string) string {
	var b strings.Builder
	for _, name := range jobLogFiles {
		text := readTail(filepath.Join(dir, name), logTailBytes)
		if strings.TrimSpace(text) == "" {
			continue
		}
		if name != jobLogFiles[0] {
			if b.Len() > 0 && !strings.HasSuffix(b.String(), "\n") {
				b.WriteByte('\n')
			}
			b.WriteString("--- " + strings.TrimSuffix(name, filepath.Ext(name)) + " ---\n")
		}
		b.WriteString(text)
	}
	return trimToTail(b.String(), logTailBytes)
}

// trimToTail keeps the last limit bytes of text, starting at a line boundary.
func trimToTail(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	text = strings.ToValidUTF8(text[len(text)-limit:], "")
	if nl := strings.IndexByte(text, '\n'); nl >= 0 {
		text = text[nl+1:]
	}
	return text
}

func readTail(path string, limit int64) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return ""
	}
	if info.Size() > limit {
		if _, err := f.Seek(-limit, io.SeekEnd); err != nil {
			return ""
		}
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return ""
	}
	return strings.ToValidUTF8(string(raw), "")
}
