package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func ddpTestNode(id, state string) registry.NodeRecord {
	return registry.NodeRecord{NodeID: id, State: state, SSHEndpoint: "root@10.0.0." + id + ":22",
		DaemonPort: 38080, Capabilities: map[string]string{"hostname": "pc-" + id}}
}

func anyNode(registry.NodeRecord) bool { return true }

// TestAMachineWhoseRankDiedRejoinsWhenItComesBack: not while it is still
// down, at once when it was seen leaving and is back, and after a wait when
// its rank died on a machine that never left (it might die again).
func TestAMachineWhoseRankDiedRejoinsWhenItComesBack(t *testing.T) {
	t0 := time.Now()
	st := newDDPUpState([]registry.NodeRecord{ddpTestNode("1", "healthy"), ddpTestNode("2", "healthy"), ddpTestNode("3", "healthy")})
	st.started("1")
	st.started("2") // node 3 was healthy but placement left it out
	st.ended("2", true, t0)

	if _, ok := st.pick([]registry.NodeRecord{ddpTestNode("1", "healthy"), ddpTestNode("2", "stale"), ddpTestNode("3", "healthy")}, t0, anyNode); ok {
		t.Fatal("picked a machine while its rank's machine is down, or one placement rejected")
	}
	n, ok := st.pick([]registry.NodeRecord{ddpTestNode("1", "healthy"), ddpTestNode("2", "healthy"), ddpTestNode("3", "healthy")}, t0.Add(time.Second), anyNode)
	if !ok || n.NodeID != "2" {
		t.Fatalf("machine 2 came back and was not picked (got %q)", n.NodeID)
	}
	st.took("2")
	st.started("2")
	if _, ok := st.pick([]registry.NodeRecord{ddpTestNode("2", "healthy")}, t0.Add(time.Second), anyNode); ok {
		t.Error("a machine already running a rank was picked again")
	}

	// Died on a healthy machine: wait out ddpRejoinAfter.
	st.ended("2", true, t0)
	nodes := []registry.NodeRecord{ddpTestNode("2", "healthy")}
	if _, ok := st.pick(nodes, t0.Add(time.Second), anyNode); ok {
		t.Error("restarted straight into the death it just had")
	}
	if _, ok := st.pick(nodes, t0.Add(ddpRejoinAfter), anyNode); !ok {
		t.Error("never given another rank")
	}
	st.took("2")
	st.ended("2", true, t0)
	if _, ok := st.pick(nodes, t0.Add(time.Hour), anyNode); ok {
		t.Errorf("given more than %d ranks in one run", ddpMaxRejoins)
	}
}

// TestANewMachineJoinsButOnlyOfTheRingsKind.
func TestANewMachineJoinsButOnlyOfTheRingsKind(t *testing.T) {
	st := newDDPUpState([]registry.NodeRecord{ddpTestNode("1", "healthy")})
	st.started("1")
	gpu := ddpTestNode("9", "healthy")
	gpu.Capabilities["gpu"] = "RTX 3060"
	cpu := ddpTestNode("8", "healthy")
	wantGPU := func(n registry.NodeRecord) bool { return ddpHasGPU(n) }
	n, ok := st.pick([]registry.NodeRecord{ddpTestNode("1", "healthy"), cpu, gpu}, time.Now(), wantGPU)
	if !ok || n.NodeID != "9" {
		t.Fatalf("got %q, want the new GPU machine", n.NodeID)
	}
}

// TestARingThatCannotGrowSaysSoOnce: a script that slices its own tensors
// cannot take a rank mid-run. One line, no rank started, no crash.
func TestARingThatCannotGrowSaysSoOnce(t *testing.T) {
	old := ddpWatchEvery
	ddpWatchEvery = time.Millisecond
	defer func() { ddpWatchEvery = old }()

	var out bytes.Buffer
	started := 0
	g := &ddpGrower{
		st:       newDDPUpState(nil),
		group:    "g",
		want:     anyNode,
		nextRank: func() int { return 2 },
		start:    func(int, registry.NodeRecord) bool { started++; return true },
		out:      &out,
		fetch: func(int) ([]registry.NodeRecord, error) {
			return []registry.NodeRecord{ddpTestNode("7", "healthy")}, nil
		},
		check: func(string, int, string, int) error {
			return &ddpJoinRefused{msg: "this script does not train from a DataLoader the shim can reshard (it slices its own tensors), so a rank cannot join mid-run", final: true}
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g.run(ctx) // returns on the final refusal, not the timeout
	if ctx.Err() != nil {
		t.Fatal("kept watching after the ring said it cannot grow")
	}
	if started != 0 {
		t.Errorf("started %d rank(s) on a ring that cannot grow", started)
	}
	if got := strings.Count(out.String(), "\n"); got != 1 || !strings.Contains(out.String(), "pc-7 is free, but this script") {
		t.Errorf("want one line naming the machine and the reason, got:\n%s", out.String())
	}
}

// TestAJoinerIsAddedToTheRing: the grower starts the next rank index on the
// new machine once the lead daemon agrees.
func TestAJoinerIsAddedToTheRing(t *testing.T) {
	old := ddpWatchEvery
	ddpWatchEvery = time.Millisecond
	defer func() { ddpWatchEvery = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := make(chan int, 1)
	g := &ddpGrower{
		st:       newDDPUpState(nil),
		want:     anyNode,
		nextRank: func() int { return 3 },
		start:    func(rank int, n registry.NodeRecord) bool { got <- rank; cancel(); return true },
		out:      &bytes.Buffer{},
		fetch: func(int) ([]registry.NodeRecord, error) {
			return []registry.NodeRecord{ddpTestNode("7", "healthy")}, nil
		},
		check: func(string, int, string, int) error { return nil },
	}
	g.run(ctx)
	select {
	case r := <-got:
		if r != 3 {
			t.Errorf("started rank %d, want 3", r)
		}
	default:
		t.Fatal("no rank started on the new machine")
	}
}

// TestAJoinedRankCountsAndALateOneIsStopped: a rank added mid-run is waited
// for like any other; one still waiting to join when the rest of the ring
// has finished is stopped, and that is not a failure.
func TestAJoinedRankCountsAndALateOneIsStopped(t *testing.T) {
	old := ddpJoinGrace
	ddpJoinGrace = 20 * time.Millisecond
	defer func() { ddpJoinGrace = old }()

	results := make(chan ddpRankResult)
	added := make(chan ddpAdded)
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, added, func(int, error) {})
	}()
	added <- ddpAdded{rank: 2, host: "pc-2", cancel: func() {}}
	results <- ddpRankResult{rank: 2} // joined and finished
	lateCtx, lateCancel := context.WithCancel(context.Background())
	added <- ddpAdded{rank: 3, host: "pc-3", cancel: lateCancel}
	results <- ddpRankResult{rank: 0}
	results <- ddpRankResult{rank: 1}
	<-lateCtx.Done() // the grace ran out and the late joiner was stopped
	results <- ddpRankResult{rank: 3, err: context.Canceled}

	if err := <-done; err != nil {
		t.Fatalf("the run failed with %v", err)
	}
	if !strings.Contains(out.String(), "rank 3 on pc-3: the ring finished before it could join") {
		t.Errorf("the late joiner was not named:\n%s", out.String())
	}
	if strings.Contains(out.String(), "died") {
		t.Errorf("a stopped joiner was reported dead:\n%s", out.String())
	}
}
