package daemonapi

import (
	"strings"
	"testing"
	"time"
)

// TestOneSlowRankIsNamed.
//
// With equal batches - which is what runs today - a step costs what the
// slowest rank costs. A rank several times slower than the rest is not
// contributing its share, it is deciding how long every step takes, and
// nothing said so: the loss came out right and the run was simply slow.
func TestOneSlowRankIsNamed(t *testing.T) {
	// A GPU rank and a CPU rank, at the ratio measured on real hardware:
	// 1.5s against 56.2s for the same model and batch.
	steps := map[int]float64{0: 25, 1: 937}
	msg, ok := pacedBy(steps, 2)
	if !ok {
		t.Fatal("a rank 37x slower than the other was not reported")
	}
	if !strings.Contains(msg, "rank 1") {
		t.Errorf("the message does not name the slow rank: %s", msg)
	}
	if !strings.Contains(msg, "937") || !strings.Contains(msg, "25") {
		t.Errorf("the message does not give both step times: %s", msg)
	}
}

// TestABalancedRingIsNotComplainedAbout. Ranks on identical hardware vary by
// a few percent from load alone, and a warning on every run would be noise
// that teaches people to ignore the one that matters.
func TestABalancedRingIsNotComplainedAbout(t *testing.T) {
	for _, steps := range []map[int]float64{
		{0: 100, 1: 100, 2: 100},
		{0: 100, 1: 104, 2: 97},
		{0: 100, 1: 150}, // slower, but not a different class of device
	} {
		if msg, ok := pacedBy(steps, len(steps)); ok {
			t.Errorf("a balanced ring %v was reported as paced: %s", steps, msg)
		}
	}
}

// TestSilentUntilEveryRankHasReported. The fastest rank arrives first, and
// judging the ring on its number alone would compare it against itself.
func TestSilentUntilEveryRankHasReported(t *testing.T) {
	if _, ok := pacedBy(map[int]float64{0: 25}, 3); ok {
		t.Error("a verdict was reached on one rank of three")
	}
	if _, ok := pacedBy(map[int]float64{0: 25, 1: 937}, 3); ok {
		t.Error("a verdict was reached on two ranks of three")
	}
	if _, ok := pacedBy(map[int]float64{0: 25, 1: 937, 2: 30}, 3); !ok {
		t.Error("no verdict once every rank had reported")
	}
}

// TestASingleRankRingHasNoPace. One rank cannot wait for anybody.
func TestASingleRankRingHasNoPace(t *testing.T) {
	if _, ok := pacedBy(map[int]float64{0: 999}, 1); ok {
		t.Error("a ring of one was reported as having a straggler")
	}
}

// TestNonsenseTimingsAreIgnored rather than producing a division by zero or a
// verdict about a rank that never measured anything.
func TestNonsenseTimingsAreIgnored(t *testing.T) {
	for _, steps := range []map[int]float64{
		{0: 0, 1: 0},
		{0: 0, 1: 500},
	} {
		if msg, ok := pacedBy(steps, 2); ok {
			t.Errorf("timings %v produced a verdict: %s", steps, msg)
		}
	}
}

// TestTheVerdictSaysWhatDroppingItWouldSave, since "one rank is slow" without
// a number gives nobody a reason to act.
func TestTheVerdictSaysWhatDroppingItWouldSave(t *testing.T) {
	msg, ok := pacedBy(map[int]float64{0: 20, 1: 30, 2: 900}, 3)
	if !ok {
		t.Fatal("no verdict on an obviously paced ring")
	}
	// Without rank 2 the rest average 25 ms.
	if !strings.Contains(msg, "25 ms") {
		t.Errorf("the message does not say what the ring would cost without "+
			"the straggler: %s", msg)
	}
}

// TestRankTimesAreOrderedByRank. A map iterates in a different order every
// time, so without sorting the same ring prints a different line each run and
// two runs cannot be compared by eye.
func TestRankTimesAreOrderedByRank(t *testing.T) {
	got := rankTimes(map[int]float64{2: 900, 0: 20, 1: 30})
	want := "rank 0 20 ms, rank 1 30 ms, rank 2 900 ms"
	if got != want {
		t.Errorf("rankTimes gave %q, want %q", got, want)
	}
}

// TestTheRingIsReportedOncePerRun, not once per sync. The flag lived on the
// per-round entry at first, so a 90-step run logged the same line 41 times.
func TestTheRingIsReportedOncePerRun(t *testing.T) {
	b := newDDPBoard()
	b.measured["ddp-abc"] = time.Now()
	if _, said := b.measured["ddp-abc"]; !said {
		t.Fatal("a reported group was not remembered")
	}
	if _, said := b.measured["ddp-other"]; said {
		t.Error("an unrelated group was treated as already reported")
	}
}

// TestReportedGroupsAreForgotten. One key per training run, kept forever,
// is a daemon holding every group name it has ever seen.
func TestReportedGroupsAreForgotten(t *testing.T) {
	b := newDDPBoard()
	b.measured["old"] = time.Now().Add(-2 * time.Hour)
	b.measured["fresh"] = time.Now()
	b.sweep(time.Hour)
	if _, ok := b.measured["old"]; ok {
		t.Error("a group reported two hours ago is still held")
	}
	if _, ok := b.measured["fresh"]; !ok {
		t.Error("a group reported just now was dropped, so its run would " +
			"report its ring a second time")
	}
}
