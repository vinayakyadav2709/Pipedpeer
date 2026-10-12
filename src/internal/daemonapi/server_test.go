package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Lease lifecycle tests ---

func TestLeaseLifecycleAcceptCommitComplete(t *testing.T) {
	s := NewWithConfig("test-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	s.StartSweeper()
	defer s.StopSweeper()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// 1. Accept → should return lease_id + expiry
	body := `{"target_id":"test-node","job_name":"train-1","submitter_node":"node-a"}`
	resp, err := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	if !acceptResp.Accepted {
		t.Fatalf("expected accepted, got rejected: %s", acceptResp.Reason)
	}
	if acceptResp.LeaseID == "" {
		t.Fatal("expected non-empty lease_id")
	}
	if acceptResp.ExpiresAt == "" {
		t.Fatal("expected non-empty expires_at")
	}

	// Verify lease is in "reserved" state
	lease, ok := s.GetLease(acceptResp.LeaseID)
	if !ok {
		t.Fatal("lease not found after accept")
	}
	if lease.State != LeaseReserved {
		t.Fatalf("expected reserved state, got %s", lease.State)
	}
	if s.ActiveLeases() != 1 {
		t.Fatalf("expected 1 lease, got %d", s.ActiveLeases())
	}
	if s.ActiveJobs() != 0 {
		t.Fatalf("expected 0 active jobs (still reserved), got %d", s.ActiveJobs())
	}

	// 2. Commit → transitions to running
	commitBody := fmt.Sprintf(`{"lease_id":%q}`, acceptResp.LeaseID)
	resp, err = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	if err != nil {
		t.Fatal(err)
	}
	var commitResp commitResponse
	json.NewDecoder(resp.Body).Decode(&commitResp)
	resp.Body.Close()

	if !commitResp.Committed {
		t.Fatalf("expected committed, got rejected: %s", commitResp.Reason)
	}

	lease, _ = s.GetLease(acceptResp.LeaseID)
	if lease.State != LeaseRunning {
		t.Fatalf("expected running state after commit, got %s", lease.State)
	}
	if s.ActiveJobs() != 1 {
		t.Fatalf("expected 1 active job after commit, got %d", s.ActiveJobs())
	}

	// 3. Complete → removes lease
	completeBody := fmt.Sprintf(`{"lease_id":%q,"status":"succeeded"}`, acceptResp.LeaseID)
	resp, err = http.Post(srv.URL+"/v1/complete", "application/json", strings.NewReader(completeBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after complete, got %d", s.ActiveLeases())
	}
	if s.ActiveJobs() != 0 {
		t.Fatalf("expected 0 active jobs after complete, got %d", s.ActiveJobs())
	}
	if s.ReservedMem() != 0 {
		t.Fatalf("expected 0 reserved mem after complete, got %d", s.ReservedMem())
	}
}

func TestLeaseExpiryAutoReleasesReservation(t *testing.T) {
	// Lease duration 50ms, grace 20ms, sweep every 10ms
	s := NewWithConfig("expiry-node", 50*time.Millisecond, 20*time.Millisecond, 10*time.Millisecond)
	s.StartSweeper()
	defer s.StopSweeper()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Accept a job with 1GB reservation
	body := `{"target_id":"expiry-node","job_name":"temp-job","required_mem_bytes":1073741824}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	if !acceptResp.Accepted {
		t.Fatalf("accept failed: %s", acceptResp.Reason)
	}
	if s.ReservedMem() != 1073741824 {
		t.Fatalf("expected 1GB reserved, got %d", s.ReservedMem())
	}

	// Wait for lease to expire + grace + sweep
	time.Sleep(100 * time.Millisecond)

	// Lease should be auto-released by sweeper
	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after expiry, got %d", s.ActiveLeases())
	}
	if s.ReservedMem() != 0 {
		t.Fatalf("expected 0 reserved mem after expiry, got %d", s.ReservedMem())
	}
}

func TestCommitAfterExpiryRejected(t *testing.T) {
	// Lease 30ms, grace 10ms
	s := NewWithConfig("expired-node", 30*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond)
	s.StartSweeper()
	defer s.StopSweeper()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"expired-node","job_name":"late-job"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	// Wait past expiry + grace
	time.Sleep(60 * time.Millisecond)

	// Commit should fail
	commitBody := fmt.Sprintf(`{"lease_id":%q}`, acceptResp.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	var commitResp commitResponse
	json.NewDecoder(resp.Body).Decode(&commitResp)
	resp.Body.Close()

	if commitResp.Committed {
		t.Fatal("commit should be rejected after lease expiry")
	}
	if resp.StatusCode != http.StatusGone && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 410 Gone or 404, got %d", resp.StatusCode)
	}
}

func TestEarlyCommitWithinGracePeriod(t *testing.T) {
	// Lease 50ms, grace 200ms — commit shortly after lease expires but within grace
	s := NewWithConfig("grace-node", 50*time.Millisecond, 200*time.Millisecond, 500*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"grace-node","job_name":"grace-job"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	// Wait past lease duration but within grace
	time.Sleep(80 * time.Millisecond)

	// Commit should succeed (within grace period)
	commitBody := fmt.Sprintf(`{"lease_id":%q}`, acceptResp.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	var commitResp commitResponse
	json.NewDecoder(resp.Body).Decode(&commitResp)
	resp.Body.Close()

	if !commitResp.Committed {
		t.Fatalf("commit within grace period should succeed, got rejected: %s", commitResp.Reason)
	}
}

func TestEarlyCommitBeforeExpiry(t *testing.T) {
	s := NewWithConfig("early-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"early-node","job_name":"early-job"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	// Commit immediately (well before expiry)
	commitBody := fmt.Sprintf(`{"lease_id":%q}`, acceptResp.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	var commitResp commitResponse
	json.NewDecoder(resp.Body).Decode(&commitResp)
	resp.Body.Close()

	if !commitResp.Committed {
		t.Fatalf("early commit should succeed: %s", commitResp.Reason)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestCancelReleasesReservation(t *testing.T) {
	s := NewWithConfig("cancel-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"cancel-node","job_name":"cancel-job","submitter_node":"submitter-a","required_mem_bytes":536870912}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	if s.ReservedMem() != 536870912 {
		t.Fatalf("expected 512MB reserved, got %d", s.ReservedMem())
	}

	// Cancel by submitter
	cancelBody := fmt.Sprintf(`{"lease_id":%q,"submitter_node":"submitter-a"}`, acceptResp.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/cancel", "application/json", strings.NewReader(cancelBody))
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel should succeed, got %d", resp.StatusCode)
	}
	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after cancel, got %d", s.ActiveLeases())
	}
	if s.ReservedMem() != 0 {
		t.Fatalf("expected 0 reserved mem after cancel, got %d", s.ReservedMem())
	}
}

func TestCancelByNonSubmitterRejected(t *testing.T) {
	s := NewWithConfig("auth-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"auth-node","job_name":"auth-job","submitter_node":"real-submitter"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	// Cancel by different node → should be rejected
	cancelBody := fmt.Sprintf(`{"lease_id":%q,"submitter_node":"imposter"}`, acceptResp.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/cancel", "application/json", strings.NewReader(cancelBody))
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cancel by non-submitter should return 403, got %d", resp.StatusCode)
	}

	// Lease should still exist
	if s.ActiveLeases() != 1 {
		t.Fatalf("lease should not be cancelled by non-submitter, got %d leases", s.ActiveLeases())
	}
}

func TestRunningLeaseNotSwept(t *testing.T) {
	// Lease 30ms, sweep every 10ms — but committed leases should NOT be swept
	s := NewWithConfig("running-node", 30*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	s.StartSweeper()
	defer s.StopSweeper()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"running-node","job_name":"long-job"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	// Commit immediately
	commitBody := fmt.Sprintf(`{"lease_id":%q}`, acceptResp.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	resp.Body.Close()

	// Wait well past lease duration + grace
	time.Sleep(80 * time.Millisecond)

	// Lease should still exist (running leases are not swept)
	if s.ActiveLeases() != 1 {
		t.Fatalf("running lease should not be swept, got %d leases", s.ActiveLeases())
	}
	if s.ActiveJobs() != 1 {
		t.Fatalf("expected 1 active job, got %d", s.ActiveJobs())
	}
}

// --- Admission control tests ---

func TestAdmissionRejectsInsufficientMemory(t *testing.T) {
	s := New("mem-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Request 100TB
	body := `{"target_id":"mem-node","job_name":"huge-job","required_mem_bytes":109951162777600}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var result acceptResponse
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
	if result.Accepted {
		t.Fatal("should not accept 100TB job")
	}
	if !strings.Contains(result.Reason, "insufficient memory") {
		t.Fatalf("expected insufficient memory reason, got: %s", result.Reason)
	}
	if s.ActiveLeases() != 0 {
		t.Fatalf("no lease should be created on rejection, got %d", s.ActiveLeases())
	}
}

func TestStackedReservationsExhaust(t *testing.T) {
	s := NewWithConfig("stack-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Accept two jobs that each need ~40% of available memory
	available := s.AvailableForJob()
	chunk := available * 40 / 100

	body1 := fmt.Sprintf(`{"target_id":"stack-node","job_name":"job-1","required_mem_bytes":%d}`, chunk)
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body1))
	var resp1 acceptResponse
	json.NewDecoder(resp.Body).Decode(&resp1)
	resp.Body.Close()
	if !resp1.Accepted {
		t.Fatalf("job-1 should be accepted: %s", resp1.Reason)
	}

	body2 := fmt.Sprintf(`{"target_id":"stack-node","job_name":"job-2","required_mem_bytes":%d}`, chunk)
	resp, _ = http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body2))
	var resp2 acceptResponse
	json.NewDecoder(resp.Body).Decode(&resp2)
	resp.Body.Close()
	if !resp2.Accepted {
		t.Fatalf("job-2 should be accepted: %s", resp2.Reason)
	}

	// Third job: same chunk → should be rejected (80% already reserved)
	body3 := fmt.Sprintf(`{"target_id":"stack-node","job_name":"job-3","required_mem_bytes":%d}`, chunk)
	resp, _ = http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body3))
	var resp3 acceptResponse
	json.NewDecoder(resp.Body).Decode(&resp3)
	resp.Body.Close()
	if resp3.Accepted {
		t.Fatal("job-3 should be rejected (insufficient memory after 2 reservations)")
	}

	// Complete job-1 → resources freed
	complete1 := fmt.Sprintf(`{"lease_id":%q}`, resp1.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/complete", "application/json", strings.NewReader(complete1))
	resp.Body.Close()

	// Retry job-3 → should succeed now
	resp, _ = http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body3))
	var resp3b acceptResponse
	json.NewDecoder(resp.Body).Decode(&resp3b)
	resp.Body.Close()
	if !resp3b.Accepted {
		t.Fatalf("job-3 retry should succeed after job-1 completed: %s", resp3b.Reason)
	}
}

func TestAcceptRejectsWrongTargetID(t *testing.T) {
	s := New("my-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"wrong-node"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for wrong target, got %d", resp.StatusCode)
	}
}

func TestAcceptRequiresTargetID(t *testing.T) {
	s := New("some-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(`{}`))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing target_id, got %d", resp.StatusCode)
	}
}

func TestDoubleCommitRejected(t *testing.T) {
	s := NewWithConfig("double-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"double-node","job_name":"test"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var acceptResp acceptResponse
	json.NewDecoder(resp.Body).Decode(&acceptResp)
	resp.Body.Close()

	// First commit
	commitBody := fmt.Sprintf(`{"lease_id":%q}`, acceptResp.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	var cr1 commitResponse
	json.NewDecoder(resp.Body).Decode(&cr1)
	resp.Body.Close()
	if !cr1.Committed {
		t.Fatal("first commit should succeed")
	}

	// Second commit
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	var cr2 commitResponse
	json.NewDecoder(resp.Body).Decode(&cr2)
	resp.Body.Close()
	if cr2.Committed {
		t.Fatal("double commit should be rejected")
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for double commit, got %d", resp.StatusCode)
	}
}

// --- Concurrency tests ---

func TestConcurrentAcceptRace(t *testing.T) {
	s := NewWithConfig("race-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	count := 20
	var wg sync.WaitGroup
	accepted := make(chan string, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"target_id":"race-node","job_name":"race-%d","required_mem_bytes":1024}`, i)
			resp, err := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
			if err != nil {
				return
			}
			var ar acceptResponse
			json.NewDecoder(resp.Body).Decode(&ar)
			resp.Body.Close()
			if ar.Accepted {
				accepted <- ar.LeaseID
			}
		}(i)
	}
	wg.Wait()
	close(accepted)

	leaseIDs := make([]string, 0)
	for id := range accepted {
		leaseIDs = append(leaseIDs, id)
	}

	if len(leaseIDs) != count {
		t.Fatalf("expected %d accepted, got %d", count, len(leaseIDs))
	}
	if s.ActiveLeases() != count {
		t.Fatalf("expected %d leases, got %d", count, s.ActiveLeases())
	}
	if s.ReservedMem() != int64(count)*1024 {
		t.Fatalf("expected %d reserved, got %d", count*1024, s.ReservedMem())
	}

	// Verify all lease IDs are unique
	seen := make(map[string]bool)
	for _, id := range leaseIDs {
		if seen[id] {
			t.Fatalf("duplicate lease ID: %s", id)
		}
		seen[id] = true
	}
}

// --- Health endpoint ---

func TestHealthReportsLeaseInfo(t *testing.T) {
	s := NewWithConfig("health-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Accept + commit a job
	body := `{"target_id":"health-node","job_name":"health-job","required_mem_bytes":536870912}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var ar acceptResponse
	json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()

	commitBody := fmt.Sprintf(`{"lease_id":%q}`, ar.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	resp.Body.Close()

	// Check health
	resp, _ = http.Get(srv.URL + "/health")
	var health map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&health)
	resp.Body.Close()

	if health["node_id"] != "health-node" {
		t.Fatalf("expected node_id=health-node, got %v", health["node_id"])
	}
	if health["active_jobs"] != float64(1) {
		t.Fatalf("expected active_jobs=1, got %v", health["active_jobs"])
	}
	if health["active_leases"] != float64(1) {
		t.Fatalf("expected active_leases=1, got %v", health["active_leases"])
	}
	if health["reserved_mem"] != float64(536870912) {
		t.Fatalf("expected reserved_mem=536870912, got %v", health["reserved_mem"])
	}
}

func TestAcceptWithoutResourceReqAlwaysAccepted(t *testing.T) {
	s := New("compat-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"compat-node","job_name":"simple-job"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var ar acceptResponse
	json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()

	if !ar.Accepted {
		t.Fatalf("job without resource req should always be accepted: %s", ar.Reason)
	}
	if ar.LeaseID == "" {
		t.Fatal("should still return a lease_id")
	}
}

func TestCompleteByJobNameBackwardsCompat(t *testing.T) {
	s := NewWithConfig("compat-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"compat-node","job_name":"compat-job"}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var ar acceptResponse
	json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()

	// Commit
	commitBody := fmt.Sprintf(`{"lease_id":%q}`, ar.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commitBody))
	resp.Body.Close()

	// Complete using job_name instead of lease_id (backwards compat)
	resp, _ = http.Post(srv.URL+"/v1/complete", "application/json", strings.NewReader(`{"job_name":"compat-job"}`))
	resp.Body.Close()

	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after complete by job_name, got %d", s.ActiveLeases())
	}
}

// --- Edge case tests ---

func TestCommitWithUnknownLeaseID(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"lease_id":"nonexistent-lease-id"}`
	resp, _ := http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(body))
	var cr commitResponse
	json.NewDecoder(resp.Body).Decode(&cr)
	resp.Body.Close()

	if cr.Committed {
		t.Fatal("commit with unknown lease_id should fail")
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown lease_id, got %d", resp.StatusCode)
	}
}

func TestCommitWithEmptyLeaseID(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"lease_id":""}`
	resp, _ := http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(body))
	var cr commitResponse
	json.NewDecoder(resp.Body).Decode(&cr)
	resp.Body.Close()

	if cr.Committed {
		t.Fatal("commit with empty lease_id should fail")
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for empty lease_id, got %d", resp.StatusCode)
	}
}

func TestCommitWithInvalidJSON(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, _ := http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader("not-json"))
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d", resp.StatusCode)
	}
}

func TestCancelNonExistentLease(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"lease_id":"ghost-lease","submitter_node":"sub"}`
	resp, _ := http.Post(srv.URL+"/v1/cancel", "application/json", strings.NewReader(body))
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for non-existent lease, got %d", resp.StatusCode)
	}
}

func TestAcceptWithInvalidJSON(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader("garbage"))
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d", resp.StatusCode)
	}
}

func TestAcceptWithGetMethodRejected(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, _ := http.Get(srv.URL + "/v1/accept")
	resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET on /v1/accept, got %d", resp.StatusCode)
	}
}

func TestCompleteNonExistentLeaseIsNoOp(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"lease_id":"does-not-exist","status":"succeeded"}`
	resp, _ := http.Post(srv.URL+"/v1/complete", "application/json", strings.NewReader(body))
	resp.Body.Close()

	// Complete is idempotent — non-existent lease returns 200 OK (no-op)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for complete on non-existent lease, got %d", resp.StatusCode)
	}
	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases, got %d", s.ActiveLeases())
	}
}

func TestCompleteOnReservedLeaseReleasesIt(t *testing.T) {
	s := NewWithConfig("edge-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	// Accept but do NOT commit
	body := `{"target_id":"edge-node","job_name":"reserved-job","required_mem_bytes":1024}`
	resp, _ := http.Post(srv.URL+"/v1/accept", "application/json", strings.NewReader(body))
	var ar acceptResponse
	json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()

	if s.ActiveLeases() != 1 {
		t.Fatalf("expected 1 lease after accept, got %d", s.ActiveLeases())
	}

	// Complete directly without committing first
	completeBody := fmt.Sprintf(`{"lease_id":%q,"status":"cancelled"}`, ar.LeaseID)
	resp, _ = http.Post(srv.URL+"/v1/complete", "application/json", strings.NewReader(completeBody))
	resp.Body.Close()

	// Should release the lease even though it was never committed
	if s.ActiveLeases() != 0 {
		t.Fatalf("expected 0 leases after completing reserved lease, got %d", s.ActiveLeases())
	}
	if s.ReservedMem() != 0 {
		t.Fatalf("expected 0 reserved mem, got %d", s.ReservedMem())
	}
}

func TestCancelWithInvalidJSON(t *testing.T) {
	s := New("edge-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, _ := http.Post(srv.URL+"/v1/cancel", "application/json", strings.NewReader("not-json"))
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d", resp.StatusCode)
	}
}

// TestRewriteHostKeepsUserAndPort covers the endpoint rewrite behind
// self-advertisement. Peers dial the endpoint, so publishing a routable Host
// beside an unroutable SSHEndpoint fixes nothing.
func TestRewriteHostKeepsUserAndPort(t *testing.T) {
	for _, tc := range []struct{ in, host, want string }{
		{"root@192.168.0.201:22", "100.67.24.101", "root@100.67.24.101:22"},
		{"192.168.0.201:38080", "100.67.24.101", "100.67.24.101:38080"},
		{"root@192.168.0.201", "100.67.24.101", "root@100.67.24.101"},
		{"", "100.67.24.101", ""},                              // nothing to rewrite
		{"root@192.168.0.201:22", "", "root@192.168.0.201:22"}, // no better answer
	} {
		if got := rewriteHost(tc.in, tc.host); got != tc.want {
			t.Errorf("rewriteHost(%q, %q) = %q, want %q", tc.in, tc.host, got, tc.want)
		}
	}
}
