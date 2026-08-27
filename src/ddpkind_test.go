package main

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/ddpplace"
)

// TestPreferGPUKeepsCPUsOutOfTheRing.
//
// --gpu prefer filtered nothing before this. Every node went into admission
// ranked by a probe that measures CPU throughput, so a GPU box with a modest
// CPU was rejected as "too slow to contribute" while a CPU box with a fast one
// was admitted, and the ring came out mixed - which the shim cannot balance.
// The preference was applied afterwards as a sort of the survivors, and a sort
// cannot put back a node admission had already dropped.
func TestPreferGPUKeepsCPUsOutOfTheRing(t *testing.T) {
	cands := []ddpplace.Candidate{
		{NodeID: "gpu-modest-cpu", HasGPU: true, Cores: 4},
		{NodeID: "cpu-fast", HasGPU: false, Cores: 64},
		{NodeID: "gpu-two", HasGPU: true, Cores: 8},
	}
	got := ddpOneDeviceKind(cands, true)
	if len(got) != 2 {
		t.Fatalf("kept %d candidates, want the 2 GPU ones", len(got))
	}
	for _, c := range got {
		if !c.HasGPU {
			t.Errorf("%s has no GPU and is in a GPU ring; with equal batches "+
				"every rank waits for the slowest, and this CPU is 37x behind",
				c.NodeID)
		}
	}
}

// TestCPUsAreUsedWhenThereIsNoGPU. "prefer" is not "require": a CPU ring beats
// no ring at all.
func TestCPUsAreUsedWhenThereIsNoGPU(t *testing.T) {
	cands := []ddpplace.Candidate{
		{NodeID: "cpu-a", Cores: 16},
		{NodeID: "cpu-b", Cores: 16},
	}
	if got := ddpOneDeviceKind(cands, true); len(got) != 2 {
		t.Errorf("a cluster with no GPU produced %d candidates, want both CPUs",
			len(got))
	}
}

// TestWithoutAPreferenceNothingIsFiltered, so --gpu off keeps behaving as it
// always has.
func TestWithoutAPreferenceNothingIsFiltered(t *testing.T) {
	cands := []ddpplace.Candidate{
		{NodeID: "gpu", HasGPU: true},
		{NodeID: "cpu"},
	}
	if got := ddpOneDeviceKind(cands, false); len(got) != 2 {
		t.Errorf("--gpu off filtered the ring down to %d", len(got))
	}
}

// TestASingleGPUBeatsAMixedRing. One accelerator on its own is the right
// answer when the alternative is pairing it with something 37x slower: the
// ring would then run at the slow rank's pace, which is worse than not
// distributing at all.
func TestASingleGPUBeatsAMixedRing(t *testing.T) {
	cands := []ddpplace.Candidate{
		{NodeID: "gpu", HasGPU: true},
		{NodeID: "cpu-a"}, {NodeID: "cpu-b"}, {NodeID: "cpu-c"},
	}
	got := ddpOneDeviceKind(cands, true)
	if len(got) != 1 || got[0].NodeID != "gpu" {
		t.Errorf("got %v, want the lone GPU rather than a ring paced by a CPU", got)
	}
}
