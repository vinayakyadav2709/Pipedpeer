package jobhistory

import (
	"strings"
	"testing"
	"time"
)

func TestTimelineSummaryTotalAndWaterfall(t *testing.T) {
	t0 := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	var tl Timeline
	tl.Add("nix build", "local", t0, 4*time.Second)
	tl.Add("upload", "node-a", t0.Add(4*time.Second), 500*time.Millisecond)
	tl.Add("script", "node-a", t0.Add(5*time.Second), 2*time.Second)

	if got, want := Summary(tl.Stages), "nix build 4.0s · upload 500ms · script 2.0s"; got != want {
		t.Errorf("Summary = %q, want %q", got, want)
	}
	// From the first start to the last end, gaps included.
	if got := Total(tl.Stages); got != 7*time.Second {
		t.Errorf("Total = %v, want 7s", got)
	}

	var b strings.Builder
	WriteWaterfall(&b, tl.Stages, 70)
	out := b.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 5 { // header, three stages, total
		t.Fatalf("waterfall has %d lines, want 5:\n%s", len(lines), out)
	}
	// Bars sit on a shared axis: a later stage's bar starts further right.
	if strings.Index(lines[3], "█") <= strings.Index(lines[1], "█") {
		t.Errorf("the script's bar does not start after the build's:\n%s", out)
	}
	if !strings.Contains(lines[3], "+5.0s") || !strings.Contains(lines[4], "7.0s") {
		t.Errorf("offsets or total missing:\n%s", out)
	}
}

func TestWaterfallWithNoStages(t *testing.T) {
	var b strings.Builder
	WriteWaterfall(&b, nil, 40)
	if !strings.Contains(b.String(), "no timeline") {
		t.Errorf("an empty timeline should say so, got %q", b.String())
	}
}

func TestBeginEnd(t *testing.T) {
	var tl Timeline
	end := tl.Begin("step", "local")
	time.Sleep(20 * time.Millisecond)
	end()
	if len(tl.Stages) != 1 || tl.Stages[0].Ms < 20 {
		t.Fatalf("stages = %+v, want one of at least 20ms", tl.Stages)
	}
}
