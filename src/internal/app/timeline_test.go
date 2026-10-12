package app

import (
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/daemonctl"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
)

func TestExecStagesSplitTheNodesAccount(t *testing.T) {
	start := time.Now()
	var tl jobhistory.Timeline
	addExecStages(&tl, "node-a", start, 5*time.Second,
		&daemonctl.ExecTiming{ImportMs: 1000, SetupMs: 300, RunMs: 3000})

	var names []string
	for _, s := range tl.Stages {
		names = append(names, s.Name)
	}
	want := []string{"closure import", "sandbox setup", "script", "stream + wrap-up"}
	if len(names) != len(want) {
		t.Fatalf("stages %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("stages %v, want %v", names, want)
		}
	}
	if tl.Stages[3].Ms != 700 {
		t.Errorf("leftover = %dms, want 700ms (5s minus the node's 4.3s)", tl.Stages[3].Ms)
	}

	// An older daemon sends no timing: one stage for the whole execution.
	var old jobhistory.Timeline
	addExecStages(&old, "node-a", start, 2*time.Second, nil)
	if len(old.Stages) != 1 || old.Stages[0].Name != "execute" || old.Stages[0].Ms != 2000 {
		t.Errorf("without timing got %+v, want one 2s execute stage", old.Stages)
	}
}
