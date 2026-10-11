package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

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
			[]string{"laptop-a", "pc-2", "laptop-b"}, nil,
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
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, func(int, error) {})
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
	err := ddpAwaitRanks(ctx, &bytes.Buffer{}, results, []string{"a", "b"}, nil, func(int, error) { called = true })
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
	results <- ddpRankResult{rank: 0, err: errors.New("execution failed: remote job exited with code 1")}
	results <- ddpRankResult{rank: 1}
	var out bytes.Buffer
	var downs []int
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil,
		func(rank int, err error) { downs = append(downs, rank) })
	if err == nil || !strings.Contains(err.Error(), "rank 0 on a") {
		t.Fatalf("got %v, want a failure naming rank 0 on a", err)
	}
	if len(downs) != 1 {
		t.Errorf("lead daemon told about %v, want [0] so the ring does not stall", downs)
	}
	// A rank killed by a signal (machine stopping) is a loss, not a failure.
	results <- ddpRankResult{rank: 1, err: errors.New("execution failed: remote job exited with code 137")}
	results <- ddpRankResult{rank: 0}
	if err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, nil, func(int, error) {}); err != nil {
		t.Errorf("a signalled rank failed the run: %v", err)
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
