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

// postAccept sends an admission request and returns the decoded reply plus the
// HTTP status.
func postAccept(t *testing.T, url, body string) (acceptResponse, int) {
	t.Helper()
	resp, err := http.Post(url+"/v1/accept", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("accept request: %v", err)
	}
	defer resp.Body.Close()

	var out acceptResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode accept response: %v", err)
	}
	return out, resp.StatusCode
}

// A node configured with a cap of N accepts exactly N tasks and rejects the
// next one with 503, so the orchestrator moves on to another machine.
func TestMaxConcurrentJobsRejectsBeyondCap(t *testing.T) {
	const cap = 5

	s := NewWithConfig("cap-node", 30*time.Second, 2*time.Second, time.Second)
	s.SetMaxConcurrentJobs(cap)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"cap-node","submitter_node":"node-a"}`

	for i := 0; i < cap; i++ {
		out, status := postAccept(t, srv.URL, body)
		if !out.Accepted {
			t.Fatalf("accept %d/%d was rejected: %s", i+1, cap, out.Reason)
		}
		if status != http.StatusOK {
			t.Fatalf("accept %d/%d returned status %d, want 200", i+1, cap, status)
		}
	}

	out, status := postAccept(t, srv.URL, body)
	if out.Accepted {
		t.Fatalf("accept %d exceeded the cap of %d but was accepted", cap+1, cap)
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("over-cap accept returned status %d, want 503", status)
	}
	if !strings.Contains(out.Reason, "concurrency limit") {
		t.Fatalf("expected a concurrency-limit reason, got %q", out.Reason)
	}
	if s.ActiveLeases() != cap {
		t.Fatalf("expected %d leases held, got %d", cap, s.ActiveLeases())
	}
}

// Completing a task frees a slot, so the node accepts work again.
func TestMaxConcurrentJobsFreesSlotOnComplete(t *testing.T) {
	s := NewWithConfig("cap-node-2", 30*time.Second, 2*time.Second, time.Second)
	s.SetMaxConcurrentJobs(1)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"cap-node-2","submitter_node":"node-a"}`

	first, _ := postAccept(t, srv.URL, body)
	if !first.Accepted {
		t.Fatalf("first accept rejected: %s", first.Reason)
	}

	if blocked, status := postAccept(t, srv.URL, body); blocked.Accepted || status != http.StatusServiceUnavailable {
		t.Fatalf("second accept should have been capped, got accepted=%v status=%d", blocked.Accepted, status)
	}

	complete := fmt.Sprintf(`{"lease_id":%q,"status":"succeeded"}`, first.LeaseID)
	resp, err := http.Post(srv.URL+"/v1/complete", "application/json", strings.NewReader(complete))
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	resp.Body.Close()

	after, status := postAccept(t, srv.URL, body)
	if !after.Accepted {
		t.Fatalf("accept after completion was rejected: %s (status %d)", after.Reason, status)
	}
}

// A cap of 0 means unlimited — the documented default.
func TestMaxConcurrentJobsZeroIsUnlimited(t *testing.T) {
	s := NewWithConfig("uncapped-node", 30*time.Second, 2*time.Second, time.Second)
	s.SetMaxConcurrentJobs(0)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"uncapped-node","submitter_node":"node-a"}`
	for i := 0; i < 20; i++ {
		if out, _ := postAccept(t, srv.URL, body); !out.Accepted {
			t.Fatalf("uncapped node rejected accept %d: %s", i+1, out.Reason)
		}
	}
	if s.ActiveLeases() != 20 {
		t.Fatalf("expected 20 leases, got %d", s.ActiveLeases())
	}
}

// The cap must hold under concurrent submitters, not just sequential ones —
// this is the case a naive check-then-insert would get wrong.
func TestMaxConcurrentJobsHoldsUnderConcurrentSubmitters(t *testing.T) {
	const cap = 5
	const submitters = 40

	s := NewWithConfig("race-cap-node", 30*time.Second, 2*time.Second, time.Second)
	s.SetMaxConcurrentJobs(cap)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
	)
	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, _ := postAccept(t, srv.URL, `{"target_id":"race-cap-node","submitter_node":"node-a"}`)
			if out.Accepted {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if accepted > cap {
		t.Fatalf("cap of %d was exceeded: %d concurrent accepts succeeded", cap, accepted)
	}
	if s.ActiveLeases() > cap {
		t.Fatalf("node holds %d leases, above its cap of %d", s.ActiveLeases(), cap)
	}
}

// /v1/jobs is what the cluster-wide task view reads: it must report each live
// lease with the fields the CLI renders.
func TestJobsEndpointListsLiveLeases(t *testing.T) {
	s := NewWithConfig("jobs-node", 30*time.Second, 2*time.Second, time.Second)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	accept, _ := postAccept(t, srv.URL,
		`{"target_id":"jobs-node","job_name":"train-mnist","submitter_node":"submitter-1"}`)
	if !accept.Accepted {
		t.Fatalf("accept rejected: %s", accept.Reason)
	}

	resp, err := http.Get(srv.URL + "/v1/jobs")
	if err != nil {
		t.Fatalf("get jobs: %v", err)
	}
	defer resp.Body.Close()

	var payload struct {
		NodeID string  `json:"node_id"`
		Jobs   []Lease `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}

	if payload.NodeID != "jobs-node" {
		t.Fatalf("expected node_id jobs-node, got %q", payload.NodeID)
	}
	if len(payload.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(payload.Jobs))
	}

	job := payload.Jobs[0]
	if job.LeaseID != accept.LeaseID {
		t.Fatalf("expected lease %s, got %s", accept.LeaseID, job.LeaseID)
	}
	if job.JobName != "train-mnist" {
		t.Fatalf("expected job_name train-mnist, got %q", job.JobName)
	}
	if job.SubmitterNode != "submitter-1" {
		t.Fatalf("expected submitter-1, got %q", job.SubmitterNode)
	}
	if job.State != LeaseReserved {
		t.Fatalf("expected reserved state, got %s", job.State)
	}
}

// A node with no GPU must refuse a GPU-requiring task rather than accepting it
// and failing at run time.
func TestAcceptRejectsGPURequestWithoutGPU(t *testing.T) {
	s := NewWithConfig("cpu-only-node", 30*time.Second, 2*time.Second, time.Second)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	out, status := postAccept(t, srv.URL,
		`{"target_id":"cpu-only-node","submitter_node":"node-a","require_gpu":true,"required_gpu_mem_bytes":1099511627776}`)

	// A machine with a real GPU may legitimately accept; only assert the
	// no-GPU path, which is what CI runs on.
	if out.Accepted {
		t.Skip("this machine has a GPU large enough to satisfy the request")
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("GPU rejection returned status %d, want 503", status)
	}
	if !strings.Contains(out.Reason, "GPU") {
		t.Fatalf("expected a GPU-related reason, got %q", out.Reason)
	}
}

// A submitter that dies mid-run must not hold its slot forever: without this,
// a crashed CLI permanently consumes part of the node's concurrency budget.
func TestAbandonedRunningLeaseIsReaped(t *testing.T) {
	s := NewWithConfig("reap-node", 50*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	s.SetRunningLeaseTTL(80 * time.Millisecond)
	s.SetMaxConcurrentJobs(1)
	s.StartSweeper()
	defer s.StopSweeper()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := `{"target_id":"reap-node","submitter_node":"doomed"}`
	accept, _ := postAccept(t, srv.URL, body)
	if !accept.Accepted {
		t.Fatalf("accept rejected: %s", accept.Reason)
	}

	commit := fmt.Sprintf(`{"lease_id":%q}`, accept.LeaseID)
	resp, err := http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commit))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	resp.Body.Close()

	// The submitter now "dies" — no further renewals.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && s.ActiveLeases() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if s.ActiveLeases() != 0 {
		t.Fatalf("abandoned running lease was never reaped: %d leases still held", s.ActiveLeases())
	}

	if after, _ := postAccept(t, srv.URL, body); !after.Accepted {
		t.Fatalf("node should accept work again after reaping: %s", after.Reason)
	}
}

// A submitter that keeps renewing must keep its slot, however long the job
// runs — reaping must never cut off live work.
func TestRenewedRunningLeaseSurvives(t *testing.T) {
	s := NewWithConfig("renew-node", 50*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond)
	s.SetRunningLeaseTTL(80 * time.Millisecond)
	s.StartSweeper()
	defer s.StopSweeper()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	accept, _ := postAccept(t, srv.URL, `{"target_id":"renew-node","submitter_node":"alive"}`)
	commit := fmt.Sprintf(`{"lease_id":%q}`, accept.LeaseID)
	resp, err := http.Post(srv.URL+"/v1/commit", "application/json", strings.NewReader(commit))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	resp.Body.Close()

	// Renew across a span several times the TTL.
	renew := fmt.Sprintf(`{"lease_id":%q}`, accept.LeaseID)
	for i := 0; i < 15; i++ {
		time.Sleep(20 * time.Millisecond)
		r, err := http.Post(srv.URL+"/v1/renew", "application/json", strings.NewReader(renew))
		if err != nil {
			t.Fatalf("renew %d: %v", i, err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("renew %d returned %d — lease was reaped while still live", i, r.StatusCode)
		}
	}

	if s.ActiveJobs() != 1 {
		t.Fatalf("renewed job should still be running, got %d active jobs", s.ActiveJobs())
	}
}

// Renewing an unknown lease must fail, so a submitter learns its lease is gone
// and can reschedule instead of assuming success.
func TestRenewUnknownLeaseFails(t *testing.T) {
	s := New("renew-unknown-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/renew", "application/json",
		strings.NewReader(`{"lease_id":"does-not-exist"}`))
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown lease, got %d", resp.StatusCode)
	}
}
