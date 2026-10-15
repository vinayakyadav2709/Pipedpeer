package daemonapi

import (
	"testing"
	"time"
)

// A lease that expired before its job arrived (a cold build outlived the
// TTL) is taken again when the job binds it, held while the job runs and
// released when it ends: a running job is never left without a lease.
func TestBindLeaseRetakesExpiredLease(t *testing.T) {
	s := NewWithConfig("rebind-node", 5*time.Second, 2*time.Second, time.Hour)
	s.SetRunningLeaseTTL(time.Nanosecond)
	s.jobsMu.Lock()
	s.jobs["job-1"] = &JobRecord{JobID: "job-1", Status: "running"}
	s.jobsMu.Unlock()

	s.bindLease("gone-lease", "job-1")
	time.Sleep(time.Millisecond)
	s.sweepExpired()
	if got := s.ActiveJobs(); got != 1 {
		t.Fatalf("running leases after bind + sweep = %d, want 1", got)
	}

	s.releaseJobLease("job-1", "done")
	if got := s.ActiveLeases(); got != 0 {
		t.Fatalf("leases after the job ended = %d, want 0", got)
	}
}
