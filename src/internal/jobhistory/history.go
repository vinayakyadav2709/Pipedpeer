package jobhistory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type Record struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
	StartedAt      string `json:"started_at"`
	FinishedAt     string `json:"finished_at,omitempty"`
	DurationMs     int64  `json:"duration_ms,omitempty"`
	ScriptPath     string `json:"script_path"`
	Remote         string `json:"remote"`
	TargetID       string `json:"target_id"`
	Detached       bool   `json:"detached"`
	Isolate        bool   `json:"isolate"`
	JobName        string `json:"job_name,omitempty"`
	JobSet         string `json:"job_set,omitempty"` // groups fan-out tasks from one map run
	RemoteJobDir   string `json:"remote_job_dir,omitempty"`
	StorePath      string `json:"store_path,omitempty"`
	RunPath        string `json:"run_path,omitempty"`
	RunHost        string `json:"run_host,omitempty"`
	LocalSyncRoot  string `json:"local_sync_root,omitempty"`
	ReceivedFiles  int    `json:"received_files,omitempty"`
	NewFiles       int    `json:"new_files,omitempty"`
	UpdatedFiles   int    `json:"updated_files,omitempty"`
	DeletedFiles   int    `json:"deleted_files,omitempty"`
	UnchangedFiles int    `json:"unchanged_files,omitempty"`
	ManifestPath   string `json:"manifest_path,omitempty"`
	// Coordinator placement diagnostics
	PlacementSource string `json:"placement_source,omitempty"` // "registry", "cache", "discovery", "self", "explicit"
	DegradedMode    bool   `json:"degraded_mode,omitempty"`
	CandidateCount  int    `json:"candidate_count,omitempty"`
	PlacementReason string `json:"placement_reason,omitempty"`
	// Strategy and Candidates are the coordinator's whole decision: every
	// node it considered and what it saw, for `pipedpeer job --explain`.
	Strategy   string      `json:"strategy,omitempty"`
	Candidates []Candidate `json:"candidates,omitempty"`
	// Resource estimation and tracking
	PeakMemBytes      int64  `json:"peak_mem_bytes,omitempty"`
	EstimatedMemBytes int64  `json:"estimated_mem_bytes,omitempty"`
	EstimationTier    string `json:"estimation_tier,omitempty"` // "user", "historical", "filesize", "import"
	// Live stage tracking
	Stage string `json:"stage,omitempty"` // "building", "exporting", "uploading", "executing"
	// Background runs: where the job keeps running after the submitter
	// detached, so attach can find it again.
	RemoteJobID    string `json:"remote_job_id,omitempty"`
	DaemonEndpoint string `json:"daemon_endpoint,omitempty"` // host:port
	// Collected: the job's end was brought back here (output log, results
	// in LocalSyncRoot, record finalized) by whoever saw it end first: the
	// daemon's watcher or attach.
	Collected bool `json:"collected,omitempty"`
	// ExitCode is the script's exit code as its node reported it.
	ExitCode int `json:"exit_code,omitempty"`
	// Stages is the run's timeline: every step, where it ran, and how long
	// it took, the environment build included.
	Stages []Stage `json:"stages,omitempty"`
}

type JobSummary struct {
	ID         string
	Status     string
	Stage      string
	Detached   bool
	JobName    string
	JobSet     string
	TargetID   string
	ScriptPath string
	Remote     string
	StartedAt  string
	DurationMs int64
}

func BaseDir() string {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "pipedpeer", "jobs")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(userdir.Data(), "jobs")
	}
	return filepath.Join(home, ".local", "share", "pipedpeer", "jobs")
}

func NewRecord(scriptPath, remote, targetID string, detached, isolate bool) (Record, string, error) {
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	dir := filepath.Join(BaseDir(), id)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return Record{}, "", err
	}
	r := Record{
		ID:         id,
		Status:     StateRunning,
		StartedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		ScriptPath: scriptPath,
		Remote:     remote,
		TargetID:   targetID,
		Detached:   detached,
		Isolate:    isolate,
	}
	if err := SaveRecord(dir, r); err != nil {
		return Record{}, "", err
	}
	return r, dir, nil
}

func SaveRecord(dir string, r Record) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "metadata.json"), b, 0644)
}

// UpdateStage writes the current stage to the record file.
func UpdateStage(dir, stage string) {
	r, err := ReadRecordAt(dir)
	if err != nil {
		return
	}
	r.Stage = stage
	_ = SaveRecord(dir, r)
}

func Finalize(dir string, r Record, runErr error) error {
	return FinalizeAt(dir, r, runErr, time.Now())
}

// FinalizeAt is Finalize for a run that ended at finished rather than now: a
// detached job collected after it ended.
func FinalizeAt(dir string, r Record, runErr error, finished time.Time) error {
	finished = finished.UTC()
	r.FinishedAt = finished.Format(time.RFC3339Nano)
	start, err := time.Parse(time.RFC3339Nano, r.StartedAt)
	if err == nil {
		r.DurationMs = finished.Sub(start).Milliseconds()
	}
	if errors.Is(runErr, context.Canceled) {
		r.Status = StateCancelled
		r.Error = runErr.Error()
	} else if runErr != nil {
		r.Status = StateFailed
		r.Error = runErr.Error()
	} else {
		r.Status = StateSucceeded
	}
	// Preserve stage written by UpdateStage during pipeline execution
	if existing, err := ReadRecordAt(dir); err == nil && existing.Stage != "" {
		r.Stage = existing.Stage
	}
	return SaveRecord(dir, r)
}

// WriteManifest stores the list of files a job sent back alongside the job's
// other history, and returns the path it was written to.
func WriteManifest(dir string, manifest any) (string, error) {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "results-manifest.json")
	if err := os.WriteFile(path, b, 0644); err != nil {
		return "", err
	}
	return path, nil
}

// CollectedOutput is the file, in a record's directory, holding a detached
// job's whole output as its node sent it, one frame per line.
const CollectedOutput = "output.jsonl"

// Lock takes an exclusive lock on a record's directory, waiting for it, so
// two processes never finish the same detached job twice. Call unlock when
// done.
func Lock(dir string) (unlock func(), err error) {
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("lock %s: %w", dir, err)
	}
	return func() { f.Close() }, nil // closing the file drops the lock
}

func SaveText(dir, name, content string) error {
	return os.WriteFile(filepath.Join(dir, name), []byte(content), 0644)
}

func CopyFileTo(dir, srcPath, dstName string) error {
	b, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}
	return SaveText(dir, dstName, string(b))
}

func ReadRecord(id string) (Record, string, error) {
	dir := filepath.Join(BaseDir(), id)
	r, err := ReadRecordAt(dir)
	if err != nil {
		return Record{}, "", err
	}
	return r, dir, nil
}

// ShortID is the id a person types: the last 8 digits of the nanosecond id.
// Not the first 8 — those are the same for every job started within the same
// 100 seconds, which is every job of a finale.
func ShortID(id string) string {
	if len(id) > 8 {
		return id[len(id)-8:]
	}
	return id
}

// Resolve finds a record by its full id, or by a prefix or suffix (ShortID)
// that matches exactly one record.
func Resolve(id string) (Record, string, error) {
	if id == "" {
		return Record{}, "", fmt.Errorf("job id is required")
	}
	if r, dir, err := ReadRecord(id); err == nil {
		return r, dir, nil
	}
	entries, err := os.ReadDir(BaseDir())
	if err != nil && !os.IsNotExist(err) {
		return Record{}, "", err
	}
	var matches []string
	for _, e := range entries {
		if e.IsDir() && (strings.HasPrefix(e.Name(), id) || strings.HasSuffix(e.Name(), id)) {
			matches = append(matches, e.Name())
		}
	}
	switch len(matches) {
	case 0:
		return Record{}, "", fmt.Errorf("job %s not found in this machine's history (%s); attach works only on the machine that started the job", id, BaseDir())
	case 1:
		return ReadRecord(matches[0])
	default:
		return Record{}, "", fmt.Errorf("job id %s matches %d jobs; give more digits", id, len(matches))
	}
}

func ReadRecordAt(dir string) (Record, error) {
	b, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		return Record{}, err
	}
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return Record{}, err
	}
	return r, nil
}

func List(limit int) ([]JobSummary, error) {
	entries, err := os.ReadDir(BaseDir())
	if err != nil {
		if os.IsNotExist(err) {
			return []JobSummary{}, nil
		}
		return nil, err
	}

	items := make([]JobSummary, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		r, _, err := ReadRecord(e.Name())
		if err != nil {
			continue
		}
		items = append(items, JobSummary{
			ID:         r.ID,
			Status:     r.Status,
			Stage:      r.Stage,
			Detached:   r.Detached,
			JobName:    r.JobName,
			JobSet:     r.JobSet,
			TargetID:   r.TargetID,
			ScriptPath: r.ScriptPath,
			Remote:     r.Remote,
			StartedAt:  r.StartedAt,
			DurationMs: r.DurationMs,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].StartedAt > items[j].StartedAt
	})

	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func ReadOptionalText(dir, name string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// QueryPeakMem returns the PeakMemBytes from the last N runs of the same script.
// Only returns non-zero values. Result is unsorted.
func QueryPeakMem(scriptPath string, limit int) []int64 {
	entries, err := os.ReadDir(BaseDir())
	if err != nil {
		return nil
	}

	var peaks []int64
	// Iterate in reverse (newest first)
	for i := len(entries) - 1; i >= 0 && len(peaks) < limit; i-- {
		e := entries[i]
		if !e.IsDir() {
			continue
		}
		r, _, err := ReadRecord(e.Name())
		if err != nil {
			continue
		}
		if r.ScriptPath == scriptPath && r.PeakMemBytes > 0 {
			peaks = append(peaks, r.PeakMemBytes)
		}
	}
	return peaks
}

// IsRecordDir reports whether dir holds a history record. The history shares
// its directory with the daemon's own job directories, NAR cache and state
// file, so a pruner that removed every old directory there deleted whatever
// the daemon kept beside it.
func IsRecordDir(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "metadata.json"))
	return err == nil
}

// Prune removes history entries whose directory has not been modified within
// olderThan (dir mtime ≈ when the run finished and its artifacts were
// written). Only directories holding a record are considered. Returns the
// number of entries removed.
func Prune(olderThan time.Duration) (int, error) {
	entries, err := os.ReadDir(BaseDir())
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	cutoff := time.Now().Add(-olderThan)
	removed := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(BaseDir(), e.Name())
		if !IsRecordDir(dir) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			if err := os.RemoveAll(dir); err != nil {
				return removed, err
			}
			removed++
		}
	}
	return removed, nil
}
