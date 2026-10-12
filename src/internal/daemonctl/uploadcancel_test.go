package daemonctl

import (
	"context"
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

// Ctrl+C during the closure upload waited for the whole transfer (45 s to
// 6 min). A cancelled ctx must return at once.
func TestUploadJobReturnsOnCancel(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select { // a node that never answers
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	ws := filepath.Join(t.TempDir(), "ws.tar")
	if err := os.WriteFile(ws, make([]byte, 1024), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	start := time.Now()
	_, err := UploadJob(ctx, u.Hostname(), port, ws, "", "/nix/store/x", "job.py", UploadOptions{LeaseID: "l"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("returned after %s", d)
	}
}
