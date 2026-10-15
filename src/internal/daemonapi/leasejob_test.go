package daemonapi

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSweeperKeepsAJobBoundLeaseWhileTheJobRuns: a detached or crashed
// submitter renews nothing, so a lease bound to a running job must outlive
// the running-lease TTL, and go once the job has ended.
func TestSweeperKeepsAJobBoundLeaseWhileTheJobRuns(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("self")
	defer s.StopWarmWorkers()
	s.state = &state{path: filepath.Join(t.TempDir(), "state.json")}
	stale := time.Now().Add(-time.Hour)
	s.leases = map[string]*Lease{
		"bound":   {LeaseID: "bound", State: LeaseRunning, CreatedAt: stale, RenewedAt: stale},
		"unbound": {LeaseID: "unbound", State: LeaseRunning, CreatedAt: stale, RenewedAt: stale},
	}
	s.jobs = map[string]*JobRecord{"job-1": {JobID: "job-1", Status: "running"}}
	s.SetRunningLeaseTTL(time.Minute)

	s.bindLease("bound", "job-1")
	s.sweepExpired()
	if _, ok := s.GetLease("bound"); !ok {
		t.Fatal("lease of a running job was swept past the TTL")
	}
	if _, ok := s.GetLease("unbound"); ok {
		t.Fatal("unrenewed lease with no job survived the TTL")
	}

	s.jobsMu.Lock()
	s.jobs["job-1"].Status = "done"
	s.jobsMu.Unlock()
	s.sweepExpired()
	if _, ok := s.GetLease("bound"); ok {
		t.Fatal("lease outlived its finished job and the TTL")
	}
}

// TestReleaseJobLeaseFreesTheSlot: the job's end releases its lease at once.
func TestReleaseJobLeaseFreesTheSlot(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("self")
	defer s.StopWarmWorkers()
	s.state = &state{path: filepath.Join(t.TempDir(), "state.json")}
	s.leases = map[string]*Lease{"l": {LeaseID: "l", State: LeaseRunning, CreatedAt: time.Now()}}
	s.bindLease("l", "job-2")
	s.releaseJobLease("job-2", "done")
	if _, ok := s.GetLease("l"); ok {
		t.Fatal("lease survived its job's end")
	}
}

// TestPruneDropsFinishedJobsADayAfterTheyFinished: every finished job's dir,
// detached or not, lives FinishedJobRetention past its finish (Finding 25).
func TestPruneDropsFinishedJobsADayAfterTheyFinished(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("self")
	// Daemon job dirs are UUIDs; the pruner leaves any other name alone.
	const detached, plain, recent = "00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"
	defer s.StopWarmWorkers()
	finished := time.Now().Add(-FinishedJobRetention - time.Hour)
	for _, id := range []string{detached, plain, recent} {
		if err := os.MkdirAll(filepath.Join(s.jobDir, id), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s.jobsMu.Lock()
	s.jobs[detached] = &JobRecord{JobID: detached, Status: "done", Detached: true, FinishedAt: finished}
	s.jobs[plain] = &JobRecord{JobID: plain, Status: "failed", FinishedAt: finished}
	s.jobs[recent] = &JobRecord{JobID: recent, Status: "done", FinishedAt: time.Now()}
	s.jobsMu.Unlock()

	n, err := s.PruneCompletedJobDirs(JobRetention)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("pruned %d dirs, want 2", n)
	}
	for _, id := range []string{detached, plain} {
		if _, err := os.Stat(filepath.Join(s.jobDir, id)); !os.IsNotExist(err) {
			t.Fatalf("%s survived a day past its finish: %v", id, err)
		}
	}
	if _, err := os.Stat(filepath.Join(s.jobDir, recent)); err != nil {
		t.Fatalf("a job that just finished was pruned: %v", err)
	}
}

// TestPruneNeverTouchesARunningJob (PRUNE-EDGE): a job running longer than
// JobRetention keeps its dir, even when every file in it is old.
func TestPruneNeverTouchesARunningJob(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("self")
	defer s.StopWarmWorkers()
	const id = "00000000-0000-4000-8000-000000000004"
	dir := filepath.Join(s.jobDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * JobRetention)
	if err := os.Chtimes(dir, old, old); err != nil {
		t.Fatal(err)
	}
	s.jobsMu.Lock()
	s.jobs[id] = &JobRecord{JobID: id, Status: "running", CreatedAt: old}
	s.jobsMu.Unlock()

	if n, err := s.PruneCompletedJobDirs(JobRetention); err != nil || n != 0 {
		t.Fatalf("pruned %d (err %v); a running job's dir must stay", n, err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("running job dir removed: %v", err)
	}
}

// TestPruneUsesTheNewestFileNotTheDirMtime (PRUNE-EDGE): appending to a file
// inside a job dir leaves the dir's own mtime old; the newest file decides.
func TestPruneUsesTheNewestFileNotTheDirMtime(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("self")
	defer s.StopWarmWorkers()
	const fresh, stale = "00000000-0000-4000-8000-000000000005", "00000000-0000-4000-8000-000000000006"
	old := time.Now().Add(-2 * JobRetention)
	for _, id := range []string{fresh, stale} {
		dir := filepath.Join(s.jobDir, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		f := filepath.Join(dir, jobOutputFile)
		if err := os.WriteFile(f, []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if id == stale {
			if err := os.Chtimes(f, old, old); err != nil {
				t.Fatal(err)
			}
		}
		// The dir looks a fortnight old either way.
		if err := os.Chtimes(dir, old, old); err != nil {
			t.Fatal(err)
		}
	}

	n, err := s.PruneCompletedJobDirs(JobRetention)
	if err != nil || n != 1 {
		t.Fatalf("pruned %d (err %v), want 1", n, err)
	}
	if _, err := os.Stat(filepath.Join(s.jobDir, fresh)); err != nil {
		t.Fatalf("dir with a fresh file pruned: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.jobDir, stale)); !os.IsNotExist(err) {
		t.Fatalf("dir with only old files survived: %v", err)
	}
}
