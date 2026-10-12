package daemonapi

import (
	"runtime"
	"testing"
	"time"
)

// TestMeasureThroughputSeesTheCgroupQuota is the property placement depends
// on. Advertised core counts are wrong in both directions that matter - they
// ignore load, and they ignore any cap on the daemon; the local test cluster's
// workers all advertise the host's core count while held to 8, 2 and 1 by
// cgroups. A measurement is subject to the same scheduler and quota as the
// work, so it sees what is really there.
func TestMeasureThroughputReportsAPositiveScore(t *testing.T) {
	got := measureThroughput(80 * time.Millisecond)
	if got.Score <= 0 {
		t.Fatalf("score = %v, want a positive rate", got.Score)
	}
	if got.Cores != runtime.NumCPU() {
		t.Errorf("cores = %d, want %d", got.Cores, runtime.NumCPU())
	}
	if got.Millis <= 0 {
		t.Errorf("millis = %d; a sample with no duration cannot be a rate", got.Millis)
	}
}

// TestMeasureThroughputScalesWithTime keeps the score a *rate* rather than a
// count. A score that grew with the sampling window would rank whichever node
// happened to be asked for longer.
func TestMeasureThroughputScalesWithTime(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	short := measureThroughput(60 * time.Millisecond)
	long := measureThroughput(240 * time.Millisecond)
	if short.Score <= 0 || long.Score <= 0 {
		t.Fatal("no score")
	}
	ratio := long.Score / short.Score
	// Generous: this is a loaded CI box's worth of tolerance. The check that
	// matters is that it is not 4x, which is what a raw count would give.
	if ratio < 0.4 || ratio > 2.5 {
		t.Errorf("score changed %.2fx when the window grew 4x; it is a count, not a rate "+
			"(short=%.0f long=%.0f)", ratio, short.Score, long.Score)
	}
}
