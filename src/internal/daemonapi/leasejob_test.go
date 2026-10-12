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

// TestPruneDropsADetachedJobADayAfterItFinished: a detached job's dir lives
// DetachedJobRetention past its finish, while an ordinary job dir that recent
// stays for JobRetention.
func TestPruneDropsADetachedJobADayAfterItFinished(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s := New("self")
	// Daemon job dirs are UUIDs; the pruner leaves any other name alone.
	const detached, plain, recent = "00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"
	defer s.StopWarmWorkers()
	finished := time.Now().Add(-DetachedJobRetention - time.Hour)
	for _, id := range []string{detached, plain, recent} {
		if err := os.MkdirAll(filepath.Join(s.jobDir, id), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	s.jobsMu.Lock()
	s.jobs[detached] = &JobRecord{JobID: detached, Status: "done", Detached: true, FinishedAt: finished}
	s.jobs[plain] = &JobRecord{JobID: plain, Status: "done", FinishedAt: finished}
	s.jobs[recent] = &JobRecord{JobID: recent, Status: "done", Detached: true, FinishedAt: time.Now()}
	s.jobsMu.Unlock()

	n, err := s.PruneCompletedJobDirs(JobRetention)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d dirs, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(s.jobDir, detached)); !os.IsNotExist(err) {
		t.Fatalf("detached job dir survived a day past its finish: %v", err)
	}
	for _, id := range []string{plain, recent} {
		if _, err := os.Stat(filepath.Join(s.jobDir, id)); err != nil {
			t.Fatalf("%s was pruned: %v", id, err)
		}
	}
}
