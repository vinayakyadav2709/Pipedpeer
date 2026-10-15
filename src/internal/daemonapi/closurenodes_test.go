package daemonapi

import (
	"slices"
	"testing"
)

// B5: a map task's PIPEDPEER_NUM_SHARDS must not suppress the closure node
// count, and a submitter-set count wins.
func TestJobEnvCarriesClosureNodes(t *testing.T) {
	got := withClosureNodes([]string{"PIPEDPEER_NUM_SHARDS=8"}, 3)
	if !slices.Contains(got, "PIPEDPEER_CLOSURE_NODES=3") {
		t.Fatalf("env %v lacks PIPEDPEER_CLOSURE_NODES=3", got)
	}
	got = withClosureNodes([]string{"PIPEDPEER_CLOSURE_NODES=5"}, 3)
	if len(got) != 1 || got[0] != "PIPEDPEER_CLOSURE_NODES=5" {
		t.Fatalf("submitter value overridden: %v", got)
	}
}
