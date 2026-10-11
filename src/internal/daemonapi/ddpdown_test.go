package daemonapi

import (
	"strings"
	"testing"
)

// TestADeadRankIsDroppedAndItsShareMoves: the two survivors of a ring of
// three are each told once, at the same sync, that rank 1 is gone and its
// share is now theirs; an open round that has both survivors' gradients
// completes at once instead of waiting out the 45 s partial timeout.
func TestADeadRankIsDroppedAndItsShareMoves(t *testing.T) {
	s := &Server{ddp: newDDPBoard()}
	b := s.ddp

	// Round 7 is open: ranks 0 and 2 have posted, rank 1 never will.
	acc := newDDPAccumulator(ddpF32, 1)
	for range 2 {
		if err := acc.addSegmented(make([]byte, 4), []float64{0}, 1); err != nil {
			t.Fatal(err)
		}
	}
	e := &ddpEntry{world: 3, acc: acc, seen: map[int]bool{0: true, 2: true}, done: make(chan struct{})}
	b.entries["g/7"] = e
	b.run("g").lastSeq = 7

	if left := s.markRankDown("g", 1, 3, "connection reset"); left != 2 {
		t.Fatalf("%d ranks left, want 2", left)
	}
	select {
	case <-e.done:
	default:
		t.Fatal("the open round still waits for the dead rank")
	}
	if e.result == nil {
		t.Error("the open round completed without a result")
	}

	if got := b.noteFor("g", 0, 7); got != "" {
		t.Errorf("rank 0 told at sync 7, before the agreed sync 8: %s", got)
	}
	for _, rank := range []int{0, 2} {
		note := b.noteFor("g", rank, 8)
		if !strings.Contains(note, `"weights": [0.5`) || !strings.Contains(note, ",0.000000,") {
			t.Errorf("rank %d got %q, want shares 0.5/0/0.5", rank, note)
		}
		if again := b.noteFor("g", rank, 9); again != "" {
			t.Errorf("rank %d told twice", rank)
		}
	}
	if b.dropCount("g") != 1 {
		t.Errorf("dropCount = %d, want 1", b.dropCount("g"))
	}
	// Idempotent: the submitter may report the same death twice.
	if left := s.markRankDown("g", 1, 3, ""); left != 2 {
		t.Errorf("a repeated report changed the count to %d", left)
	}
}

// TestSharesWithoutTheDead renormalises what is left.
func TestSharesWithoutTheDead(t *testing.T) {
	got := sharesWithout([]float64{0.5, 0.3, 0.2}, map[int]bool{0: true})
	if got[0] != 0 || got[1] < 0.599 || got[1] > 0.601 || got[2] < 0.399 || got[2] > 0.401 {
		t.Errorf("got %v, want [0 0.6 0.4]", got)
	}
}
