package coordinator

import (
	"errors"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func node(name string, ceiling int64, gpu bool) ScoredNode {
	n := registry.NodeRecord{NodeID: name + "-0000-uuid", Capabilities: map[string]string{"hostname": name}}
	n.Load.MemCeilingBytes = ceiling
	if gpu {
		n.Load.GPUModel = "RTX"
	}
	return ScoredNode{Node: n, Rejected: true, RejectReason: "insufficient memory"}
}

// TestAJobNoNodeCanEverHoldFailsFast: a 1.6 GiB job on 1.4 GiB containers
// queued forever; it must fail at once, naming the need and the nodes.
func TestAJobNoNodeCanEverHoldFailsFast(t *testing.T) {
	c := New(Config{RequiredMemBytes: 1681996185})
	d := PlacementDecision{Candidates: []ScoredNode{node("pc1", 1500<<20, false), node("pc2", 1500<<20, false)}}
	err := c.neverFits(d)
	var p *permanentError
	if err == nil || !errors.As(err, &p) {
		t.Fatalf("got %v, want a permanent error", err)
	}
	for _, want := range []string{"1.6 GiB of memory", "pc1 allows at most 1.5 GiB", "pc2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q missing from %q", want, err.Error())
		}
	}

	// One node big enough, or one whose ceiling is unknown: keep queueing.
	d.Candidates = append(d.Candidates, node("laptop", 0, false))
	if err := c.neverFits(d); err != nil {
		t.Errorf("a node with an unknown ceiling might fit, got %v", err)
	}

	// --gpu force with no GPU anywhere.
	g := New(Config{RequireGPU: true})
	if err := g.neverFits(PlacementDecision{Candidates: []ScoredNode{node("pc1", 0, false)}}); err == nil ||
		!strings.Contains(err.Error(), "a GPU") || !strings.Contains(err.Error(), "pc1 has no GPU") {
		t.Errorf("got %v, want a failure naming the GPU need", err)
	}
	if err := g.neverFits(PlacementDecision{Candidates: []ScoredNode{node("laptop", 0, true)}}); err != nil {
		t.Errorf("a GPU node exists, got %v", err)
	}
}

func TestEmptyRoundRobinPickIsNamed(t *testing.T) {
	line := rejectionLine(PlacementDecision{Candidates: []ScoredNode{node("pc1", 0, false)}})
	if line != "pc1: insufficient memory" {
		t.Errorf("got %q", line)
	}
}
