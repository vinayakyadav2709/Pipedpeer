package coordinator

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// deadPort is a localhost port nothing listens on.
func deadPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// FIX-REQUEUE: with no node able to take the job, PlaceWithRetry used to
// retry every RetryInterval forever. It must give up once the placement
// window passes, with a plain error naming why.
func TestPlaceWithRetryGivesUpAfterThePlacementWindow(t *testing.T) {
	rec := registry.NodeRecord{
		NodeID: "gone", SSHEndpoint: "root@127.0.0.1:22", DaemonPort: deadPort(t),
		Capabilities: map[string]string{"hostname": "gone-host"},
		HealthScore:  1.0, State: "healthy",
	}
	cfg := Config{
		SelfIdentity:    makeID("submitter", "sub", "x86_64-linux"),
		NoSelf:          true,
		RetryInterval:   20 * time.Millisecond,
		PlacementWindow: 300 * time.Millisecond,
		DiscoverFn:      func() []registry.NodeRecord { return []registry.NodeRecord{rec} },
	}
	c := New(cfg)
	// A wide outer bound: a loaded CI runner must not turn a pass into a hang.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	res, err := c.PlaceWithRetry(ctx, cfg, nil)
	if err == nil {
		t.Fatalf("placed on %+v, want an error", res)
	}
	if ctx.Err() != nil {
		t.Fatalf("PlaceWithRetry ran until the outer timeout: %v", err)
	}
	if !strings.Contains(err.Error(), "no node took this job within 300ms") {
		t.Fatalf("error = %q, want the window named", err)
	}
	if took := time.Since(start); took < 300*time.Millisecond {
		t.Fatalf("gave up after %s, before the window passed", took)
	}
}

// A node with a role (pool-only, introducer-only) takes no jobs: it is
// rejected with the reason, and a cluster of only such nodes fails at once.
func TestNodesWithARoleTakeNoJobs(t *testing.T) {
	recs := []registry.NodeRecord{
		{NodeID: "pool", SSHEndpoint: "root@127.0.0.1:22", DaemonPort: deadPort(t), HealthScore: 1, State: "healthy",
			Capabilities: map[string]string{"hostname": "phone", heartbeat.RoleCapability: heartbeat.RolePoolOnly}},
		{NodeID: "intro", SSHEndpoint: "root@127.0.0.1:22", DaemonPort: deadPort(t), HealthScore: 1, State: "healthy",
			Capabilities: map[string]string{"hostname": "vps", heartbeat.RoleCapability: heartbeat.RoleIntroducerOnly}},
	}
	cfg := Config{
		SelfIdentity:  makeID("submitter", "sub", "x86_64-linux"),
		NoSelf:        true,
		RetryInterval: 20 * time.Millisecond,
		DiscoverFn:    func() []registry.NodeRecord { return recs },
	}
	c := New(cfg)

	d := c.FindNode()
	if d.ChosenNode.NodeID != "" {
		t.Fatalf("chose %s, a node with a role", d.ChosenNode.NodeID)
	}
	for _, cand := range d.Candidates {
		if !cand.Rejected || !strings.Contains(cand.RejectReason, "takes no jobs") {
			t.Fatalf("candidate %s: rejected=%v reason=%q", cand.Node.NodeID, cand.Rejected, cand.RejectReason)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := c.PlaceWithRetry(ctx, cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "no node in the cluster can take this job") {
		t.Fatalf("PlaceWithRetry = %v, want an immediate no-node error", err)
	}
}
