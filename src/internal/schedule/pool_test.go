package schedule

import "testing"

func TestPartSecFloorAndOverheadShare(t *testing.T) {
	if got := PartSec(0, 0); got != 0.25 {
		t.Errorf("PartSec(0,0) = %v, want the 0.25 s floor", got)
	}
	if got := PartSec(0.2, 0.01); got < 1.79 || got > 1.81 {
		t.Errorf("PartSec(0.2, 0.01) = %v, want 9x setup = 1.8", got)
	}
	if got := PartSec(0.01, 0.1); got < 0.89 || got > 0.91 {
		t.Errorf("PartSec(0.01, 0.1) = %v, want 9x rtt = 0.9", got)
	}
}

func TestDepthPerNode(t *testing.T) {
	cases := []struct {
		lead, part float64
		want       int
	}{
		{0.15, 1.35, 2}, // LAN: double-buffered
		{1.28, 1.8, 2},  // internet, long parts
		{1.28, 0.6, 3},  // internet, tail parts: capped at 3
		{10, 0.25, 3},   // never deeper than 3
		{0, 1, 1},       // no lead time: nothing to cover
		{1, 0, 1},       // degenerate part length
	}
	for _, c := range cases {
		if got := Depth(c.lead, c.part); got != c.want {
			t.Errorf("Depth(%v, %v) = %d, want %d", c.lead, c.part, got, c.want)
		}
	}
}

func TestBacklog(t *testing.T) {
	if got := Backlog([]float64{2, 1}, []float64{0.5, 3}); got != 1.5 {
		t.Errorf("Backlog = %v, want 1.5 (an overdue part counts 0, never negative)", got)
	}
}

func TestBackupWorth(t *testing.T) {
	// 10 s part, 2 s in: 8 s left; a fresh copy needing 3 s saves 5 s.
	if ok, gain := BackupWorth(10, 2, 3, 0.1); !ok || gain != 5 {
		t.Errorf("BackupWorth(10,2,3) = %v %v, want true 5", ok, gain)
	}
	// Nearly done: a backup cannot beat it.
	if ok, _ := BackupWorth(10, 9.9, 3, 0.1); ok {
		t.Error("a part 0.1 s from done got a backup")
	}
	// Overdue: assume as long again as expected (at least the overrun).
	if ok, gain := BackupWorth(2, 6, 1, 0.1); !ok || gain != 3 {
		t.Errorf("overdue BackupWorth = %v %v, want true 3", ok, gain)
	}
	// The gain must beat the backup's own setup.
	if ok, _ := BackupWorth(10, 2, 7.5, 1); ok {
		t.Error("a 0.5 s gain beat a 1 s setup")
	}
}
