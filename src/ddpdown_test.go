package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
)

// exited is the error a rank's app.RunTask returns when its script exited
// with code.
func exited(code int) error {
	return fmt.Errorf("execution failed: %w", &daemonctl.RemoteExitError{Code: code})
}

// TestARankDyingMidRunDoesNotFailTheRing: rank 1 fails while the others are
// still training; the run carries on, says which rank and where, tells the
// lead daemon once, and succeeds when the survivors finish.
func TestARankDyingMidRunDoesNotFailTheRing(t *testing.T) {
	results := make(chan ddpRankResult, 3)
	var downs []int
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &out, results,
			[]string{"laptop-a", "pc-2", "laptop-b"}, nil, nil,
			func(rank int, err error) { downs = append(downs, rank) })
	}()

	results <- ddpRankResult{rank: 1, err: errors.New("connection reset")}
	time.Sleep(20 * time.Millisecond) // survivors still running
	results <- ddpRankResult{rank: 0}
	results <- ddpRankResult{rank: 2}

	if err := <-done; err != nil {
		t.Fatalf("the ring failed with %v; two of three ranks finished", err)
	}
	if !strings.Contains(out.String(), "rank 1 on pc-2 died; continuing on 2") {
		t.Errorf("no message naming the dead rank and its host:\n%s", out.String())
	}
	if len(downs) != 1 || downs[0] != 1 {
		t.Errorf("lead daemon told about ranks %v, want [1]", downs)
	}
}

// TestEveryRankFailingFailsTheRun, naming the right rank rather than the
// order results arrived in.
func TestEveryRankFailingFailsTheRun(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 1, err: errors.New("ImportError")}
	results <- ddpRankResult{rank: 0, err: errors.New("ImportError")}
	var out bytes.Buffer
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, nil, func(int, error) {})
	if err == nil || !strings.Contains(err.Error(), "rank 1 on b") {
		t.Fatalf("got %v, want the first failure, rank 1 on b", err)
	}
}

// TestCancelIsNotADeath: Ctrl+C stops every rank, and none of them should be
// reported down or the run called a success.
func TestCancelIsNotADeath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 0, err: context.Canceled}
	results <- ddpRankResult{rank: 1}
	called := false
	err := ddpAwaitRanks(ctx, &bytes.Buffer{}, results, []string{"a", "b"}, nil, nil, func(int, error) { called = true })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want the cancel", err)
	}
	if called {
		t.Error("a cancelled rank was reported down")
	}
}

// TestAScriptFailureOnOneRankFailsTheRun: rank 0 crashing at checkpoint save
// is the script failing, not a lost machine. The ring still carries on (the
// others must not stall), but the run must not exit 0.
func TestAScriptFailureOnOneRankFailsTheRun(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 0, err: exited(1)}
	results <- ddpRankResult{rank: 1}
	var out bytes.Buffer
	var downs []int
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, nil,
		func(rank int, err error) { downs = append(downs, rank) })
	if err == nil || !strings.Contains(err.Error(), "rank 0 on a") {
		t.Fatalf("got %v, want a failure naming rank 0 on a", err)
	}
	if len(downs) != 1 {
		t.Errorf("lead daemon told about %v, want [0] so the ring does not stall", downs)
	}
	// A rank killed by a signal (machine stopping), or a node that could not
	// start the script (126/127), is a loss, not a failure.
	for _, code := range []int{137, 126, 127} {
		results <- ddpRankResult{rank: 1, err: exited(code)}
		results <- ddpRankResult{rank: 0}
		if err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, nil, func(int, error) {}); err != nil {
			t.Errorf("a rank that exited %d failed the run: %v", code, err)
		}
	}
}

// TestAJoinersCrashDoesNotFailTheRun: a rank added mid-run that exits 1 while
// joining (the lead refused it, a shape mismatch) never touched the training;
// the ranks that trained finish and the run succeeds.
func TestAJoinersCrashDoesNotFailTheRun(t *testing.T) {
	results := make(chan ddpRankResult)
	added := make(chan ddpAdded)
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, added, func(int, error) {})
	}()
	added <- ddpAdded{rank: 2, host: "pc-2", cancel: func() {}}
	results <- ddpRankResult{rank: 2, err: exited(1)}
	results <- ddpRankResult{rank: 0}
	results <- ddpRankResult{rank: 1}
	if err := <-done; err != nil {
		t.Fatalf("a joiner's crash failed a run that trained fine: %v", err)
	}
	if !strings.Contains(out.String(), "rank 2 on pc-2") {
		t.Errorf("the joiner's failure was not reported:\n%s", out.String())
	}
}

// TestNoRankJoinsAfterTheScriptFailed: once a rank's script has failed the
// run is lost, so a rank the grower starts afterwards is stopped at once
// rather than run on (possibly the same failing node).
func TestNoRankJoinsAfterTheScriptFailed(t *testing.T) {
	results := make(chan ddpRankResult)
	added := make(chan ddpAdded)
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, added, func(int, error) {})
	}()
	results <- ddpRankResult{rank: 0, err: exited(1)}
	cancelled := false
	added <- ddpAdded{rank: 2, host: "a", cancel: func() { cancelled = true }}
	results <- ddpRankResult{rank: 2, err: context.Canceled}
	results <- ddpRankResult{rank: 1}
	err := <-done
	if !cancelled {
		t.Error("a rank started after the script failed was left running")
	}
	if err == nil || !strings.Contains(err.Error(), "rank 0 on a: the script failed") {
		t.Fatalf("got %v, want rank 0's script failure", err)
	}
}

// TestARerunUnderTheSameNameGetsAFreshGroup: the lead daemon remembers a
// group's dropped ranks for ten minutes, so a re-run must not reuse it.
func TestARerunUnderTheSameNameGetsAFreshGroup(t *testing.T) {
	now := time.Now()
	a, b := ddpGroupName("train", now), ddpGroupName("train", now.Add(time.Second))
	if a == b || !strings.HasPrefix(a, "ddp-train-") {
		t.Errorf("groups %q and %q: want distinct, prefixed ddp-train-", a, b)
	}
}

// TestTheLeadDyingEndsTheRun: rank 0's machine hosts the rendezvous and the
// sync, so the ring cannot carry on without it. Say so instead of
// "continuing on 1", don't post to the dead lead, and fail the run.
func TestTheLeadDyingEndsTheRun(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 0, err: errors.New("connection lost")}
	results <- ddpRankResult{rank: 1}
	var out bytes.Buffer
	called := false
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"gpu1", "gpu2"}, nil, nil,
		func(int, error) { called = true })
	if err == nil || !strings.Contains(err.Error(), "leads the ring") {
		t.Fatalf("got %v, want a failure saying the lead's machine died", err)
	}
	if strings.Contains(out.String(), "continuing") || !strings.Contains(out.String(), "rank 0 on gpu1 died") {
		t.Errorf("wrong message for a dead lead:\n%s", out.String())
	}
	if called {
		t.Error("the dead lead daemon was told a rank is down")
	}
}

// TestARankThatMissedTheStartIsALossNotAFailure: the lead turned rank 1 away
// (exit 75) while rank 0 trained; the run succeeds and says why. A rank still
// running a grace after another finished (its closure still arriving) is
// stopped instead of holding the run open.
func TestARankThatMissedTheStartIsALossNotAFailure(t *testing.T) {
	results := make(chan ddpRankResult, 2)
	results <- ddpRankResult{rank: 1, err: exited(ddpMissedStartExit)}
	results <- ddpRankResult{rank: 0}
	var out bytes.Buffer
	if err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, nil, func(int, error) {}); err != nil {
		t.Fatalf("a rank that missed the start failed the run: %v", err)
	}
	if !strings.Contains(out.String(), "rank 1 on b was not ready when training started") {
		t.Errorf("no plain message for the late rank:\n%s", out.String())
	}

	old := ddpJoinGrace
	ddpJoinGrace = 50 * time.Millisecond
	t.Cleanup(func() { ddpJoinGrace = old })
	results = make(chan ddpRankResult, 2)
	stopped := make(chan struct{})
	stop := []context.CancelFunc{nil, func() { close(stopped); results <- ddpRankResult{rank: 1, err: context.Canceled} }}
	results <- ddpRankResult{rank: 0}
	out.Reset()
	if err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, stop, nil, func(int, error) {}); err != nil {
		t.Fatalf("stopping a straggler failed the run: %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Error("the straggler was never stopped")
	}
	if !strings.Contains(out.String(), "rank 1 on b: the ring finished without it") {
		t.Errorf("no message for the stopped straggler:\n%s", out.String())
	}
}
