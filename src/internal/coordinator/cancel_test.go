package coordinator

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func cancelTestCoord(t *testing.T) (*Coordinator, *daemonapi.Server) {
	daemon := daemonapi.NewWithConfig("worker-c", 5*time.Second, 2*time.Second, 100*time.Millisecond)
	srv := httptest.NewServer(daemon.Handler())
	t.Cleanup(srv.Close)
	host, port := splitHostPort(t, srv.Listener.Addr().String())
	return New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfSSH:          "root@localhost:22",
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		RetryInterval:    50 * time.Millisecond,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{
				NodeID: "worker-c", SSHEndpoint: "root@" + host + ":22", DaemonPort: port,
				Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 << 30},
				HealthScore: 1.0, State: "healthy",
			}}
		},
	}), daemon
}

func TestExecuteWithRetryCancelledStopsAtOnce(t *testing.T) {
	coord, daemon := cancelTestCoord(t)
	ctx, cancel := context.WithCancel(context.Background())
	var n int32
	start := time.Now()
	_, err := coord.ExecuteWithRetry(ctx, func(string, int, string) error {
		atomic.AddInt32(&n, 1)
		cancel()
		return errors.New("killed")
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if n != 1 || time.Since(start) > time.Second {
		t.Fatalf("retried: n=%d after %s", n, time.Since(start))
	}
	if daemon.ActiveLeases() != 0 {
		t.Fatalf("lease left behind: %d", daemon.ActiveLeases())
	}
}

func TestExecuteWithRetryDetachedKeepsLease(t *testing.T) {
	coord, daemon := cancelTestCoord(t)
	var n int32
	res, err := coord.ExecuteWithRetry(context.Background(), func(string, int, string) error {
		atomic.AddInt32(&n, 1)
		return daemonctl.ErrDetached
	}, nil)
	if !errors.Is(err, daemonctl.ErrDetached) || res == nil {
		t.Fatalf("want result and ErrDetached, got %v, %v", res, err)
	}
	if n != 1 {
		t.Fatalf("retried: n=%d", n)
	}
	if daemon.ActiveLeases() != 1 {
		t.Fatalf("lease should stay for the node to release, got %d", daemon.ActiveLeases())
	}
}
