package daemonapi

import (
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// A submitter killed mid-upload never sends /v1/complete. The node must free
// the lease the upload named at once, not after the running-lease TTL (a GPU
// node refused the next run "no GPU available" for 5 min).
func TestBrokenUploadReleasesLease(t *testing.T) {
	s := NewWithConfig("test-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	s.jobDir = t.TempDir()
	s.mu.Lock()
	s.leases["lease-up"] = &Lease{LeaseID: "lease-up", State: LeaseRunning, CreatedAt: time.Now(), RenewedAt: time.Now()}
	s.mu.Unlock()
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)
	go func() {
		mp.WriteField("lease_id", "lease-up")
		w, _ := mp.CreateFormFile("workspace", "workspace.tar")
		w.Write(make([]byte, 1024))
		pw.CloseWithError(io.ErrUnexpectedEOF) // the submitter dies
	}()
	if resp, err := http.Post(srv.URL+"/v1/jobs/upload", mp.FormDataContentType(), pr); err == nil {
		resp.Body.Close()
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		_, held := s.leases["lease-up"]
		s.mu.Unlock()
		if !held {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("lease still held after the upload broke")
}

// The admitted figure is the lease's reservation, not half the ceiling: a run
// with no estimate gets a fixed cap that is not twice anything.
func TestGrantableMemoryUsesLeaseReservation(t *testing.T) {
	s := NewWithConfig("test-node", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	spare := s.AvailableForJob()
	if spare <= 0 {
		t.Skip("no spare memory reading on this machine")
	}
	requested := spare * 4
	s.mu.Lock()
	s.leases["l"] = &Lease{LeaseID: "l", State: LeaseRunning, MemBytes: spare * 3}
	s.mu.Unlock()
	// The reservation was admitted, so the ceiling never drops below it.
	if got := s.grantableMemory(requested, "l", "job"); got < spare*3 {
		t.Fatalf("granted %d, below the lease's admitted %d", got, spare*3)
	}
}
