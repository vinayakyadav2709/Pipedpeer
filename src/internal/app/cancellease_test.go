package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Ctrl+C mid-upload must free the node's lease at once: nothing on the node
// is bound to it yet, and the friend's GPU node refused runs for 5 min.
func TestCancelledUploadReleasesLease(t *testing.T) {
	released := make(chan string, 1)
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/complete":
			var req struct {
				LeaseID string `json:"lease_id"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			released <- req.LeaseID
		case "/v1/jobs/upload":
			select { // a closure transfer that takes minutes
			case <-r.Context().Done():
			case <-stop:
			}
		}
	}))
	t.Cleanup(func() { close(stop); srv.Close() })
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	dir := t.TempDir()
	ws := filepath.Join(dir, "ws.tar")
	script := filepath.Join(dir, "job.py")
	for _, f := range []string{ws, script} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	env := &Environment{WorkspaceTar: ws, StorePath: "/nix/store/x-run", ScriptRel: "job.py"}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	err := RunTask(ctx, env, Task{Script: script, Options: Options{DaemonHost: u.Hostname(), DaemonPort: port, LeaseID: "lease-1"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	select {
	case id := <-released:
		if id != "lease-1" {
			t.Fatalf("released %q, want lease-1", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lease not released after the cancel")
	}
}
