package jobhistory

import (
	"strings"
	"testing"
)

func TestExplanationShowsWinnerRejectionsAndScores(t *testing.T) {
	r := Record{
		Strategy:        "smart",
		PlacementReason: "score=0.812, source=discovery",
		Candidates: []Candidate{
			{NodeID: "bbbbbbbb-2", Host: "box-b", Source: "discovery", Score: 0.55, CPUPercent: 70, Cores: 8, FreeCores: 2.4},
			{NodeID: "aaaaaaaa-1", Host: "box-a", Source: "discovery", Score: 0.812, Chosen: true, CPUPercent: 5, Cores: 16, FreeCores: 15.2},
			{NodeID: "cccccccc-3", Host: "box-c", Rejected: true, Reason: "GPU required but node has no GPU"},
		},
	}
	var b strings.Builder
	WriteExplanation(&b, r)
	out := b.String()
	for _, want := range []string{"3 node(s) considered, 2 eligible", "→ aaaaaaaa", "0.812",
		"rejected: GPU required but node has no GPU", "15.2/16", "Chosen: score=0.812"} {
		if !strings.Contains(out, want) {
			t.Errorf("explanation lacks %q:\n%s", want, out)
		}
	}
	// The winner is listed first.
	if strings.Index(out, "aaaaaaaa") > strings.Index(out, "bbbbbbbb") {
		t.Errorf("the chosen node is not listed first:\n%s", out)
	}
}

func TestExplanationForALocalRun(t *testing.T) {
	var b strings.Builder
	WriteExplanation(&b, Record{PlacementSource: "self"})
	if !strings.Contains(b.String(), "Ran on this machine") {
		t.Errorf("a local run should say no placement was made, got:\n%s", b.String())
	}
}
