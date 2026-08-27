package ddpplace

import (
	"context"
	"testing"
)

// TestSecondGPUOnANodeGetsARank. A two-GPU machine handed one rank trains on
// one GPU and leaves the other idle for the whole run, which is the largest
// waste a heterogeneous cluster can produce and the reason this exists.
func TestSecondGPUOnANodeGetsARank(t *testing.T) {
	cands := []Candidate{
		{NodeID: "two-gpu", Host: "a", Cores: 16, MemBytes: 32 << 30, HasGPU: true, Slots: 2},
	}
	plan := Select(context.Background(), cands, Options{ProbeMillis: 1})

	if len(plan.Chosen) != 2 {
		t.Fatalf("a two-GPU node produced %d rank(s), want 2 (rejections: %v)",
			len(plan.Chosen), reasons(plan))
	}
	slots := map[int]bool{}
	for _, c := range plan.Chosen {
		if c.Candidate.NodeID != "two-gpu" {
			t.Errorf("rank placed on %s, want two-gpu", c.Candidate.NodeID)
		}
		slots[c.Slot] = true
	}
	if !slots[0] || !slots[1] {
		t.Errorf("ranks pinned devices %v; both taking one device is not two "+
			"ranks of work, it is two ranks queueing on one GPU", slots)
	}
}

// TestOneSlotIsUnchangedBehaviour. Slots left unset must place exactly as
// before, or every existing single-GPU cluster changes shape on upgrade.
func TestOneSlotIsUnchangedBehaviour(t *testing.T) {
	cands := []Candidate{
		{NodeID: "a", Host: "a", Cores: 16, MemBytes: 16 << 30},
		{NodeID: "b", Host: "b", Cores: 16, MemBytes: 16 << 30},
	}
	plan := Select(context.Background(), cands, Options{ProbeMillis: 1})
	if len(plan.Chosen) != 2 {
		t.Fatalf("got %d ranks, want 2", len(plan.Chosen))
	}
	for _, c := range plan.Chosen {
		if c.Slot != 0 {
			t.Errorf("%s got slot %d; a node with one slot has only device 0",
				c.Candidate.NodeID, c.Slot)
		}
	}
}

// TestRanksOnOneNodeShareItsMemory. The working-set gate decides whether a
// rank fits. Giving each rank on a node the node's whole free figure admits
// two that between them cannot fit, and the failure then arrives at run time
// as an OOM instead of here as a refusal.
func TestRanksOnOneNodeShareItsMemory(t *testing.T) {
	// 12 GB free, two slots: 6 GB each, and the model needs 8 GB.
	cands := []Candidate{
		{NodeID: "tight", Host: "a", Cores: 16, MemBytes: 12 << 30, HasGPU: true, Slots: 2},
		{NodeID: "roomy", Host: "b", Cores: 16, MemBytes: 32 << 30, HasGPU: true, Slots: 1},
	}
	plan := Select(context.Background(), cands, Options{
		ProbeMillis:     1,
		WorkingSetBytes: 8 << 30,
	})
	for _, c := range plan.Chosen {
		if c.Candidate.NodeID == "tight" {
			t.Errorf("a rank was placed on a node with 6 GB a slot for an 8 GB "+
				"working set; it would not fail slowly, it would fail (%v)",
				reasons(plan))
		}
	}
}

// TestCappedRingLeavesTheSpareDeviceIdle. Asking for fewer ranks than the
// cluster can host should idle a machine's second accelerator before its
// first, so the ring is not scattered across boxes for no reason.
func TestCappedRingLeavesTheSpareDeviceIdle(t *testing.T) {
	cands := []Candidate{
		{NodeID: "two-gpu", Host: "a", Cores: 16, MemBytes: 32 << 30, HasGPU: true, Slots: 2},
	}
	plan := Select(context.Background(), cands, Options{Max: 1, ProbeMillis: 1})
	if len(plan.Chosen) != 1 {
		t.Fatalf("Max=1 gave %d ranks", len(plan.Chosen))
	}
	if plan.Chosen[0].Slot != 0 {
		t.Errorf("the single rank pinned device %d; the spare device should be "+
			"the idle one", plan.Chosen[0].Slot)
	}
}

// TestZeroSlotsMeansOne, so a caller that has not been taught about slots
// keeps working rather than placing nothing.
func TestZeroSlotsMeansOne(t *testing.T) {
	cands := []Candidate{
		{NodeID: "a", Host: "a", Cores: 8, MemBytes: 8 << 30, Slots: 0},
		{NodeID: "b", Host: "b", Cores: 8, MemBytes: 8 << 30, Slots: 0},
	}
	plan := Select(context.Background(), cands, Options{ProbeMillis: 1})
	if len(plan.Chosen) != 2 {
		t.Fatalf("Slots=0 placed %d ranks, want 2 (one per node)", len(plan.Chosen))
	}
}

func reasons(p Plan) []string {
	var out []string
	for _, r := range p.Rejected {
		out = append(out, r.Candidate.NodeID+": "+r.Reason)
	}
	return out
}
