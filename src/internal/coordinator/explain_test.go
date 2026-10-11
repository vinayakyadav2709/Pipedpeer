package coordinator

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func TestExplainCandidatesMarksTheWinner(t *testing.T) {
	win := registry.NodeRecord{NodeID: "win", SSHEndpoint: "root@10.0.0.2:22",
		Capabilities: map[string]string{"cpu_cores": "16"},
		Load:         registry.LoadInfo{CPUPercent: 5, AvailableMemBytes: 8 << 30}, HealthScore: 1}
	d := PlacementDecision{
		ChosenNode: win,
		Candidates: []ScoredNode{
			{Node: win, Score: 0.9, Source: SourceDiscovery},
			{Node: registry.NodeRecord{NodeID: "lose", SSHEndpoint: "root@10.0.0.3:22"}, Score: 0.4, Source: SourceRegistry},
			{Node: registry.NodeRecord{NodeID: "no"}, Rejected: true, RejectReason: "arch mismatch"},
		},
	}
	got := ExplainCandidates(d)
	if len(got) != 3 {
		t.Fatalf("got %d candidates, want 3", len(got))
	}
	if !got[0].Chosen || got[1].Chosen || got[2].Chosen {
		t.Errorf("chosen flags = %v %v %v, want only the winner", got[0].Chosen, got[1].Chosen, got[2].Chosen)
	}
	if got[0].Host != "10.0.0.2" || got[0].Cores != 16 || got[0].Score != 0.9 {
		t.Errorf("winner row = %+v", got[0])
	}
	if !got[2].Rejected || got[2].Reason != "arch mismatch" {
		t.Errorf("rejected row = %+v", got[2])
	}
}
