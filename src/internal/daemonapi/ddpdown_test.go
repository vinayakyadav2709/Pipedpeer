package daemonapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

// TestARankDownBeforeTheFirstSyncIsKept: a rank that dies before the ring
// ever synced still creates the run; the sweeper must not take it (and the
// drop with it) as a run nobody has touched for ages.
func TestARankDownBeforeTheFirstSyncIsKept(t *testing.T) {
	s := New("test-node")
	s.markRankDown("g", 1, 3, "machine lost")
	s.ddp.sweep(10 * time.Minute)
	r := s.ddp.runs["g"]
	if r == nil || !r.dropped[1] {
		t.Fatal("the sweeper forgot a run whose rank died before the first sync")
	}
}

// TestARoundDoesNotCloseOnADeadRanksContribution: rank 1 contributes, then
// dies. The round must still wait for rank 0, whose own gradient goes into
// the result, rather than closing on the dead rank alone.
func TestARoundDoesNotCloseOnADeadRanksContribution(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	dead := make(chan int, 1)
	go func() {
		code, _, _ := ddpReduceFull(t, ts, "g", 1, 1, 2, []float32{3})
		dead <- code
	}()
	// Wait until rank 1's contribution is in.
	for deadline := time.Now().Add(5 * time.Second); ; {
		s.ddp.mu.Lock()
		e := s.ddp.entries["g/1"]
		in := e != nil && e.seen[1]
		s.ddp.mu.Unlock()
		if in {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rank 1's contribution never arrived")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.markRankDown("g", 1, 2, "machine lost")

	s.ddp.mu.Lock()
	e := s.ddp.entries["g/1"]
	closed := e == nil || e.result != nil
	s.ddp.mu.Unlock()
	if closed {
		t.Fatal("the round closed on the dead rank's contribution alone")
	}

	code, _, got := ddpReduceFull(t, ts, "g", 1, 0, 2, []float32{1})
	if code != http.StatusOK || len(got) != 1 {
		t.Fatalf("rank 0: HTTP %d %v", code, got)
	}
	// Both gradients: the live rank's own, and the dead rank's batch, which
	// was real data computed on the same model.
	if got[0] != 2 {
		t.Errorf("rank 0 got %v, want the mean of both ranks, 2", got[0])
	}
	if c := <-dead; c != http.StatusOK {
		t.Errorf("the dead rank's open request ended with HTTP %d", c)
	}
}
