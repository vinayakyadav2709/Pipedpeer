package main

import (
	"math"
	"strings"
	"testing"
)

// TestTruncatedRingSharesStillSumToOne.
//
// Placement normalises over everything it admitted, then the ring is cut to
// --ddp N. Shares that summed to less than one would hand out less than the
// whole dataset, and every epoch would train on a fraction of it without
// anything saying so.
func TestTruncatedRingSharesStillSumToOne(t *testing.T) {
	// Placement admitted four, the user asked for two.
	got := ddpNormaliseWeights([]float64{0.4, 0.3})
	var total float64
	for _, w := range got {
		total += w
	}
	if math.Abs(total-1) > 1e-9 {
		t.Errorf("shares sum to %v, want 1 — %v%% of the dataset would go untrained",
			total, 100*(1-total))
	}
	// Proportions must survive the renormalisation.
	if math.Abs(got[0]/got[1]-0.4/0.3) > 1e-9 {
		t.Errorf("renormalising changed the ratio: %v", got)
	}
}

// TestUnusableSharesFallBackToEqual. A zero or negative share is not a small
// share, it is a broken one, and guessing at it would silently mis-split the
// dataset. nil means "no shares", which is the path every ring ran before.
func TestUnusableSharesFallBackToEqual(t *testing.T) {
	for _, w := range [][]float64{
		{0.5, 0},
		{0.5, -0.5},
		{},
	} {
		if got := ddpNormaliseWeights(w); got != nil {
			t.Errorf("shares %v produced %v rather than falling back to equal", w, got)
		}
	}
}

// TestUnevenAgreesWithTheShimsThreshold. The run prints "shares: ..." only
// when it believes they are uneven, and the shim reshards only when it
// believes the same. Two different thresholds means a run that announces one
// thing and does another.
func TestUnevenAgreesWithTheShimsThreshold(t *testing.T) {
	if ddpEvenTolerance != 0.02 {
		t.Errorf("tolerance is %v; the shim's _parse_weights uses 0.02 and the "+
			"two must match", ddpEvenTolerance)
	}
	if ddpUneven([]float64{0.5, 0.5}) {
		t.Error("equal shares reported as uneven")
	}
	if ddpUneven([]float64{0.505, 0.495}) {
		t.Error("shares within measurement noise reported as uneven")
	}
	if !ddpUneven([]float64{0.75, 0.25}) {
		t.Error("a 3:1 split was not reported as uneven")
	}
	if ddpUneven([]float64{1}) {
		t.Error("a ring of one was reported as uneven")
	}
}

// TestWeightListIsPreciseEnoughToPartition. Shares are rendered as text for
// the shim; too few digits and the cumulative offsets drift, which shows up
// as a shard boundary off by samples.
func TestWeightListIsPreciseEnoughToPartition(t *testing.T) {
	got := ddpWeightList([]float64{1.0 / 3, 1.0 / 3, 1.0 / 3})
	parts := strings.Split(got, ",")
	if len(parts) != 3 {
		t.Fatalf("rendered %q, want three shares", got)
	}
	for _, p := range parts {
		if len(p) < 6 {
			t.Errorf("share %q carries too few digits to place a boundary", p)
		}
	}
}
