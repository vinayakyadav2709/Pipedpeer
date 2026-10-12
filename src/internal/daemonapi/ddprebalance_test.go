package daemonapi

import (
	"math"
	"strings"
	"testing"
	"time"
)

func reports(specs ...[3]float64) map[int]rankReport {
	m := map[int]rankReport{}
	for i, s := range specs {
		m[i] = rankReport{StepMillis: s[0], SyncMillis: s[1], Samples: int(s[2])}
	}
	return m
}

// TestSharesFollowWhatTheRingActuallyDid.
//
// Placement measures a CPU loop for a quarter of a second on an idle machine.
// What a rank is worth for this job, while everything else on the box is also
// running, is a different number — and it moves.
func TestSharesFollowWhatTheRingActuallyDid(t *testing.T) {
	// Both take 100 ms a step, but rank 0 covered three times the samples —
	// so it is three times the machine, whatever the probe said.
	r := planRebalance(reports(
		[3]float64{100, 5, 3000},
		[3]float64{100, 5, 1000},
	), []float64{0.5, 0.5}, 2)

	if len(r.Weights) != 2 {
		t.Fatalf("no reshare proposed for a 3:1 ring on even shares (%+v)", r)
	}
	if math.Abs(r.Weights[0]-0.75) > 0.01 {
		t.Errorf("rank 0 got %.3f, want 0.75 — it did three of every four samples",
			r.Weights[0])
	}
	if !strings.Contains(r.Why, "measured") {
		t.Errorf("the reason does not say the shares were measured: %q", r.Why)
	}
}

// TestSmallDriftIsLeftAlone. Measurement wobbles by a few percent from one
// round to the next; resharding on that has every rank re-slicing its data
// every epoch for nothing.
func TestSmallDriftIsLeftAlone(t *testing.T) {
	r := planRebalance(reports(
		[3]float64{100, 5, 1030},
		[3]float64{100, 5, 970},
	), []float64{0.5, 0.5}, 2)
	if r.Weights != nil {
		t.Errorf("a 3%% wobble triggered a reshare: %v", r.Weights)
	}
	if r.Drop != -1 {
		t.Errorf("a 3%% wobble dropped rank %d", r.Drop)
	}
}

// TestNoVerdictUntilEveryRankHasReported. The fastest rank reports first, and
// judging the ring on its number alone compares it against itself.
func TestNoVerdictUntilEveryRankHasReported(t *testing.T) {
	r := planRebalance(reports([3]float64{100, 5, 3000}), nil, 3)
	if r.Weights != nil || r.Drop != -1 {
		t.Errorf("a verdict was reached on one rank of three: %+v", r)
	}
}

// TestARankThatHasNotMeasuredItselfStopsTheVerdict, rather than having its
// work reshared to everyone else on no evidence.
func TestARankThatHasNotMeasuredItselfStopsTheVerdict(t *testing.T) {
	r := planRebalance(reports(
		[3]float64{100, 5, 3000},
		[3]float64{0, 0, 0},
	), []float64{0.5, 0.5}, 2)
	if r.Weights != nil || r.Drop != -1 {
		t.Errorf("a rank with no measurement was still judged: %+v", r)
	}
}

// TestAFreeloaderIsDropped.
//
// Every rank costs a gradient exchange per step. A rank contributing almost
// nothing still costs the full exchange, so the ring is faster without it —
// which is the case placement cannot see, because it runs before a byte of
// the model has moved.
func TestAFreeloaderIsDropped(t *testing.T) {
	// Two strong ranks and one contributing under 1%, with sync costing
	// 50 ms a step — the figures a real sync-bound ring reported.
	r := planRebalance(reports(
		[3]float64{100, 50, 5000},
		[3]float64{100, 50, 5000},
		[3]float64{100, 50, 40},
	), []float64{0.4, 0.4, 0.2}, 3)

	if r.Drop != 2 {
		t.Fatalf("dropped rank %d, want 2 — it contributes 0.4%% and costs a "+
			"full exchange every step (%+v)", r.Drop, r)
	}
	if !strings.Contains(r.Why, "rank 2") {
		t.Errorf("the reason does not name the rank: %q", r.Why)
	}
	for _, want := range []string{"sync", "compute"} {
		if !strings.Contains(r.Why, want) {
			t.Errorf("the reason does not give the %s side of the trade: %q", want, r.Why)
		}
	}
}

// TestARankThatEarnsItsPlaceIsKept even when it is the slowest, because
// slowest is not the same as not worth having.
func TestARankThatEarnsItsPlaceIsKept(t *testing.T) {
	// The slow rank is half the others rather than a rounding error, and sync
	// is cheap.
	r := planRebalance(reports(
		[3]float64{100, 2, 4000},
		[3]float64{100, 2, 4000},
		[3]float64{100, 2, 2000},
	), []float64{0.4, 0.4, 0.2}, 3)
	if r.Drop != -1 {
		t.Errorf("dropped rank %d, which carries a fifth of the work for a 2 ms "+
			"exchange", r.Drop)
	}
}

// TestARingOfTwoIsNeverCutToOne. That is not a rebalance, it is abandoning
// the run the user asked to distribute.
func TestARingOfTwoIsNeverCutToOne(t *testing.T) {
	r := planRebalance(reports(
		[3]float64{100, 500, 5000},
		[3]float64{100, 500, 1},
	), []float64{0.5, 0.5}, 2)
	if r.Drop != -1 {
		t.Errorf("a ring of two dropped rank %d, leaving one", r.Drop)
	}
}

// TestRankZeroIsNeverDropped. It holds the blackboard every other rank posts
// to; removing it does not leave a smaller ring, it leaves no ring.
func TestRankZeroIsNeverDropped(t *testing.T) {
	r := planRebalance(reports(
		[3]float64{100, 50, 1},
		[3]float64{100, 50, 5000},
		[3]float64{100, 50, 5000},
	), []float64{0.3, 0.35, 0.35}, 3)
	if r.Drop == 0 {
		t.Error("rank 0 was dropped; every other rank syncs through it")
	}
}

// TestSharesSumToOne, or the shards derived from them cover less than the
// dataset and every epoch silently trains on a fraction of it.
func TestSharesSumToOne(t *testing.T) {
	r := planRebalance(reports(
		[3]float64{100, 5, 5000},
		[3]float64{100, 5, 3000},
		[3]float64{100, 5, 2000},
	), []float64{0.34, 0.33, 0.33}, 3)
	if r.Weights == nil {
		t.Fatal("no reshare proposed for a 5:3:2 ring on even shares")
	}
	var total float64
	for _, w := range r.Weights {
		total += w
	}
	if math.Abs(total-1) > 1e-9 {
		t.Errorf("shares sum to %v, want 1", total)
	}
}

// TestTheRingIsRefittedOnACadence, not on every step. A reshare costs an
// epoch boundary, and deciding every sync would have the ranks re-slicing
// their data constantly — the same mistake that logged one line 41 times.
func TestTheRingIsRefittedOnACadence(t *testing.T) {
	b := newDDPBoard()
	old := rebalanceEvery
	rebalanceEvery = 5
	defer func() { rebalanceEvery = old }()

	for rank := 0; rank < 2; rank++ {
		b.observe("g", rank, rankReport{StepMillis: 100, SyncMillis: 2,
			Samples: 3000 - rank*2000})
	}

	said := 0
	for i := 0; i < 12; i++ {
		if b.rebalanceFor("g", 2) != "" {
			said++
		}
	}
	if said == 0 {
		t.Fatal("twelve rounds at a cadence of five said nothing at all")
	}
	if said > 2 {
		t.Errorf("said something %d times in twelve rounds; the cadence is five "+
			"and once the shares are right there is nothing more to say", said)
	}
}

// TestADroppedRankIsToldOnlyOnce. Repeating the verdict every cadence would
// have the rank leave, then be told to leave again for the rest of the run.
func TestADroppedRankIsToldOnlyOnce(t *testing.T) {
	b := newDDPBoard()
	old := rebalanceEvery
	rebalanceEvery = 1
	defer func() { rebalanceEvery = old }()

	// Two strong ranks and a freeloader, sync-bound.
	for _, r := range []struct {
		rank    int
		samples int
	}{{0, 5000}, {1, 5000}, {2, 40}} {
		b.observe("g", r.rank, rankReport{StepMillis: 100, SyncMillis: 50, Samples: r.samples})
	}

	drops := 0
	for i := 0; i < 6; i++ {
		if strings.Contains(b.rebalanceFor("g", 3), `"drop"`) {
			drops++
		}
	}
	if drops != 1 {
		t.Errorf("the ring was told to drop a rank %d times, want once", drops)
	}
	if b.dropCount("g") != 1 {
		t.Errorf("dropCount is %d after one drop", b.dropCount("g"))
	}
}

// TestADroppedRankIsNotWaitedFor.
//
// A rank told to leave stops posting. If the round still expected it, every
// remaining step would wait out the full partial timeout — 45 seconds each —
// for a gradient that is never coming, which would make dropping a freeloader
// far worse than keeping it.
func TestADroppedRankIsNotWaitedFor(t *testing.T) {
	e := &ddpEntry{world: 3}
	if got := e.expectedCount(); got != 3 {
		t.Errorf("a ring of three with nobody dropped expects %d", got)
	}
	e.expected = 2
	if got := e.expectedCount(); got != 2 {
		t.Errorf("after a drop the round expects %d, want 2", got)
	}
	// A nonsense value must not make a round wait for ranks that do not exist.
	e.expected = 99
	if got := e.expectedCount(); got != 3 {
		t.Errorf("an out-of-range expected count gave %d, want the world size", got)
	}
}

// TestRunStateOutlivesASyncRound. An entry is one round; anything kept there
// is forgotten between steps, which is how a per-run decision came to be
// remade every step.
func TestRunStateOutlivesASyncRound(t *testing.T) {
	b := newDDPBoard()
	b.observe("g", 0, rankReport{StepMillis: 100, SyncMillis: 2, Samples: 1000})
	b.observe("g", 1, rankReport{StepMillis: 100, SyncMillis: 2, Samples: 1000})
	if got := len(b.runs["g"].reports); got != 2 {
		t.Fatalf("the run remembers %d reports, want 2", got)
	}
	b.observe("g", 0, rankReport{StepMillis: 90, SyncMillis: 2, Samples: 1100})
	if got := b.runs["g"].reports[0].Samples; got != 1100 {
		t.Errorf("a rank's later report did not replace its earlier one: %d", got)
	}
}

// TestFinishedRunsAreForgotten. One entry per training run, kept forever, is
// a daemon holding every ring it has ever served.
func TestFinishedRunsAreForgotten(t *testing.T) {
	b := newDDPBoard()
	b.observe("old", 0, rankReport{StepMillis: 1, Samples: 1})
	b.runs["old"].seen = time.Now().Add(-2 * time.Hour)
	b.observe("fresh", 0, rankReport{StepMillis: 1, Samples: 1})

	b.sweep(time.Hour)
	if _, ok := b.runs["old"]; ok {
		t.Error("a run last seen two hours ago is still held")
	}
	if _, ok := b.runs["fresh"]; !ok {
		t.Error("a run that synced just now was forgotten mid-flight")
	}
}
