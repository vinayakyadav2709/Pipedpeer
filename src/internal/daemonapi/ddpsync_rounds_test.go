package daemonapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestARoundCompletesWhenARankNeverArrives is the fault margin. One machine
// dying used to stall every other rank for three minutes and then fail the
// whole run, which on a cluster of any size makes long training a lottery.
func TestARoundCompletesWhenARankNeverArrives(t *testing.T) {
	old := ddpPartialAfter
	ddpPartialAfter = 300 * time.Millisecond
	defer func() { ddpPartialAfter = old }()

	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// Two of three ranks show up; the third has died.
	type result struct {
		code int
		head map[string]any
		vals []float32
	}
	out := make(chan result, 2)
	for _, r := range []int{0, 1} {
		go func(rank int) {
			code, head, vals := ddpReduceFull(t, ts, "g", 1, rank, 3, []float32{2, 4})
			out <- result{code, head, vals}
		}(r)
	}

	for i := 0; i < 2; i++ {
		select {
		case got := <-out:
			if got.code != http.StatusOK {
				t.Fatalf("status %d: a round with a dead rank should still complete", got.code)
			}
			if len(got.vals) != 2 || got.vals[0] != 2 || got.vals[1] != 4 {
				t.Errorf("averaged %v, want the mean of the ranks that answered", got.vals)
			}
			if n, _ := got.head["ranks"].(float64); int(n) != 2 {
				t.Errorf("reply says %v ranks; it must say how many actually "+
					"contributed, or a run training on half its machines looks normal",
					got.head["ranks"])
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a round with one dead rank never completed")
		}
	}
}

// TestALateRankGetsTheSameResult. A rank excluded for being slow must receive
// the identical average the others applied, or it walks away with a different
// model and every later step compounds the difference. Three ranks: a round
// that closes on one rank alone is a dead ring, not a slow rank
// (TestEveryPeerGoneAbortsTheSurvivor).
func TestALateRankGetsTheSameResult(t *testing.T) {
	old := ddpPartialAfter
	ddpPartialAfter = 300 * time.Millisecond
	defer func() { ddpPartialAfter = old }()

	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	done := make(chan []float32, 2)
	for _, r := range []int{0, 1} {
		go func(rank int) {
			_, _, vals := ddpReduceFull(t, ts, "g", 1, rank, 3, []float32{10, 20})
			done <- vals
		}(r)
	}

	var early []float32
	for i := 0; i < 2; i++ {
		select {
		case early = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the first ranks never completed")
		}
	}

	// The straggler turns up after the round was closed.
	code, _, late := ddpReduceFull(t, ts, "g", 1, 2, 3, []float32{999, 999})
	if code != http.StatusOK {
		t.Fatalf("a late rank got status %d instead of the round's result", code)
	}
	if len(late) != len(early) {
		t.Fatalf("late rank got %d values, early got %d", len(late), len(early))
	}
	for i := range early {
		if late[i] != early[i] {
			t.Errorf("element %d: late rank got %v, the others applied %v — the ranks "+
				"now hold different models", i, late[i], early[i])
		}
	}
}

// TestAFullRoundReportsEveryRank. The partial path must not report a short
// count when nothing was missing, or every healthy run looks degraded.
func TestAFullRoundReportsEveryRank(t *testing.T) {
	s := New("test-node")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	out := make(chan map[string]any, 2)
	for _, r := range []int{0, 1} {
		go func(rank int) {
			_, head, _ := ddpReduceFull(t, ts, "g", 1, rank, 2, []float32{1, 1})
			out <- head
		}(r)
	}
	for i := 0; i < 2; i++ {
		head := <-out
		if n, _ := head["ranks"].(float64); int(n) != 2 {
			t.Errorf("a complete round reported %v of 2 ranks", head["ranks"])
		}
	}
}
