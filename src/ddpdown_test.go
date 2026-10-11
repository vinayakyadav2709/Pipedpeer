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
			[]string{"laptop-a", "pc-2", "laptop-b"},
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
	err := ddpAwaitRanks(context.Background(), &out, results, []string{"a", "b"}, func(int, error) {})
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
	err := ddpAwaitRanks(ctx, &bytes.Buffer{}, results, []string{"a", "b"}, func(int, error) { called = true })
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want the cancel", err)
	}
	if called {
		t.Error("a cancelled rank was reported down")
	}
}
