package daemonapi

import (
	"testing"
	"time"
)

// TestQueueDepthCountsPromisedWork.
//
// The field has been in the wire format since the registry was written and
// populated nowhere, so every node reported zero and the number told a reader
// nothing. A node with three jobs queued behind the one it is running is a
// worse choice than an idle one with the same core count.
func TestQueueDepthCountsPromisedWork(t *testing.T) {
	s := New("self")
	if got := s.QueueDepth(); got != 0 {
		t.Errorf("an idle daemon reports a queue of %d", got)
	}

	s.mu.Lock()
	s.leases = map[string]*Lease{
		"a": {LeaseID: "a", State: LeaseReserved},
		"b": {LeaseID: "b", State: LeaseReserved},
		"c": {LeaseID: "c", State: LeaseRunning},
	}
	s.mu.Unlock()

	if got := s.QueueDepth(); got != 2 {
		t.Errorf("queue depth is %d; two are reserved and one is already "+
			"running, which is not queued", got)
	}
}

// TestRecentFailuresCountsRecentOnes.
//
// The registry's health score has subtracted 0.1 per recent failure since it
// was written, against a field nothing ever set — so the term was always zero
// and a node that failed every job scored the same as one that failed none.
func TestRecentFailuresCountsRecentOnes(t *testing.T) {
	s := New("self")
	now := time.Now()

	s.jobsMu.Lock()
	s.jobs = map[string]*JobRecord{
		"fresh":  {JobID: "fresh", Status: "failed", CreatedAt: now.Add(-time.Minute)},
		"also":   {JobID: "also", Status: "failed", CreatedAt: now.Add(-2 * time.Minute)},
		"stale":  {JobID: "stale", Status: "failed", CreatedAt: now.Add(-2 * time.Hour)},
		"worked": {JobID: "worked", Status: "succeeded", CreatedAt: now.Add(-time.Minute)},
	}
	s.jobsMu.Unlock()

	if got := s.RecentFailures(); got != 2 {
		t.Errorf("counted %d recent failures, want 2 — an hours-old failure is "+
			"not a reason to avoid a node that has been fine since, and a job "+
			"that succeeded is not a failure at all", got)
	}
}

// TestHealthReportsBothFields, since populating them and not sending them
// leaves the registry scoring against zero exactly as before.
func TestHealthReportsBothFields(t *testing.T) {
	s := New("self")
	s.mu.Lock()
	s.leases = map[string]*Lease{"a": {LeaseID: "a", State: LeaseReserved}}
	s.mu.Unlock()
	s.jobsMu.Lock()
	s.jobs = map[string]*JobRecord{
		"f": {JobID: "f", Status: "failed", CreatedAt: time.Now()},
	}
	s.jobsMu.Unlock()

	h := s.healthSnapshot()
	if h.Load.QueueDepth != 1 {
		t.Errorf("health reports a queue of %d, want 1", h.Load.QueueDepth)
	}
	if h.Load.RecentFailures != 1 {
		t.Errorf("health reports %d recent failures, want 1", h.Load.RecentFailures)
	}
}
