package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonapi"
	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// workers starts one real daemon per id and a coordinator that discovers
// them all and never places on itself.
func workers(t *testing.T, ids ...string) *Coordinator {
	t.Helper()
	var recs []registry.NodeRecord
	for _, id := range ids {
		d := daemonapi.NewWithConfig(id, 5*time.Second, 2*time.Second, 100*time.Millisecond)
		srv := httptest.NewServer(d.Handler())
		t.Cleanup(srv.Close)
		host, port := splitHostPort(t, srv.Listener.Addr().String())
		recs = append(recs, registry.NodeRecord{
			NodeID: id, SSHEndpoint: "root@" + host + ":22", DaemonPort: port,
			Load:        registry.LoadInfo{CPUPercent: 10, AvailableMemBytes: 8 << 30},
			HealthScore: 1.0, State: "healthy",
		})
	}
	return New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 1024,
		RetryInterval:    20 * time.Millisecond,
		ExecBackoffBase:  time.Millisecond,
		NoSelf:           true,
		DiscoverFn:       func() []registry.NodeRecord { return recs },
	})
}

// recorder is an executor that fails per node as told and records the order.
type recorder struct {
	mu    sync.Mutex
	ran   []string
	fails func(nodeID string) error
}

func (r *recorder) exec(_ string, _ int, nodeID string) error {
	r.mu.Lock()
	r.ran = append(r.ran, nodeID)
	r.mu.Unlock()
	return r.fails(nodeID)
}

// A node refusing the closure (#6) used to fail the whole run; the job must
// move to the next node, and fail only when none is left, naming the fix.
func TestClosureRefusalPlacesOnTheNextNode(t *testing.T) {
	refusal := Permanent(fmt.Errorf("upload: %w", &daemonctl.ClosureRefusedError{Reason: "lacks a signature by a trusted key"}))
	coord := workers(t, "worker-a", "worker-b")
	r := &recorder{fails: func(id string) error {
		if id == "worker-a" {
			return refusal
		}
		return nil
	}}
	// Put worker-a first so it is tried, whatever the scores say.
	coord.tried = map[string]int{"worker-b": 1}
	var msgs []string
	res, err := coord.ExecuteWithRetry(context.Background(), r.exec, func(m string) { msgs = append(msgs, m) })
	if err != nil || res.NodeID != "worker-b" {
		t.Fatalf("got %v, %v; want success on worker-b (ran %v)", res, err, r.ran)
	}
	if !strings.Contains(strings.Join(msgs, "\n"), "pipedpeer trust` on worker-a") {
		t.Errorf("no fix line for the refusing node in %q", msgs)
	}

	// The only node refuses: fail at once, not queue forever.
	only := workers(t, "worker-c")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r = &recorder{fails: func(string) error { return refusal }}
	_, err = only.ExecuteWithRetry(ctx, r.exec, nil)
	if err == nil || ctx.Err() != nil || !strings.Contains(err.Error(), "pipedpeer trust` on worker-c") {
		t.Fatalf("got %v (ctx %v), want a failure naming the fix", err, ctx.Err())
	}
	if len(r.ran) != 1 {
		t.Errorf("ran %d times on a node that refused, want 1", len(r.ran))
	}
}

// A retry after a failure goes to a node not yet tried (#29).
func TestRetryPrefersAnUntriedNode(t *testing.T) {
	coord := workers(t, "worker-a", "worker-b")
	r := &recorder{fails: func(string) error { return fmt.Errorf("killed by signal 9") }}
	_, err := coord.ExecuteWithRetry(context.Background(), r.exec, nil)
	if err == nil || !strings.Contains(err.Error(), "signal 9") {
		t.Fatalf("got %v, want the last error", err)
	}
	if len(r.ran) != MaxExecAttempts || r.ran[0] == r.ran[1] {
		t.Fatalf("ran on %v, want %d attempts with the second on another node", r.ran, MaxExecAttempts)
	}
}

// buildSelfNode left this machine's memory ceiling unknown (#10), so a job
// too big for every node including this one still queued.
func TestSelfNodeCarriesTheDaemonsCeiling(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"load": registry.LoadInfo{MemCeilingBytes: 1500 << 20}})
	}))
	defer ts.Close()
	port, _ := strconv.Atoi(ts.URL[strings.LastIndex(ts.URL, ":")+1:])
	c := New(Config{SelfIdentity: makeID("self", "laptop", "x86_64-linux"), SelfDaemon: port})
	if got := c.buildSelfNode().Load.MemCeilingBytes; got != 1500<<20 {
		t.Fatalf("self ceiling %d, want %d", got, 1500<<20)
	}
}

// The wait is minutes long, so it reports why nodes are busy as it goes (#11).
func TestFindNodeWithinReportsProgress(t *testing.T) {
	defer func(old time.Duration) { WaitProgressEvery = old }(WaitProgressEvery)
	WaitProgressEvery = 20 * time.Millisecond
	coord := New(Config{
		SelfIdentity:     makeID("submitter", "sub", "x86_64-linux"),
		SelfDaemon:       fakeDaemon(t),
		RequiredMemBytes: 4 << 30,
		NoSelf:           true,
		DiscoverFn: func() []registry.NodeRecord {
			return []registry.NodeRecord{{NodeID: "busy-node-uuid", Capabilities: map[string]string{"hostname": "pc1"},
				Load: registry.LoadInfo{AvailableMemBytes: 1 << 30, ActiveJobs: 1}, State: "healthy"}}
		},
	})
	var lines []string
	coord.FindNodeWithin(context.Background(), 200*time.Millisecond, 5*time.Millisecond, func(d PlacementDecision, _ time.Duration) {
		lines = append(lines, d.RejectionLine())
	})
	if len(lines) < 3 || !strings.Contains(lines[len(lines)-1], "pc1: insufficient memory") {
		t.Fatalf("progress lines %q, want several naming pc1's memory", lines)
	}
}
