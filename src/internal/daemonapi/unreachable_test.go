package daemonapi

import "testing"

// A peer with no direct path used to be logged and then left out of
// /v1/nodes, so it looked the same as a machine that was never there.
func TestUnreachablePeersAreListedWithTheirReason(t *testing.T) {
	s := New("node-self")
	s.MarkUnreachable("peer-known", "symmetric NAT on both ends")
	s.MarkUnreachable("peer-unknown", "no candidates")

	nodes := s.withUnreachable([]nodeResp{
		{NodeID: "peer-known", State: "stale", Source: "internet"},
		{NodeID: "peer-fine", State: "healthy"},
	})
	if len(nodes) != 3 {
		t.Fatalf("got %d nodes, want 3 (two listed plus the one the store never held)", len(nodes))
	}
	byID := map[string]nodeResp{}
	for _, n := range nodes {
		byID[n.NodeID] = n
	}
	if got := byID["peer-known"]; got.State != "unreachable" || got.Capabilities[ReasonCapability] != "symmetric NAT on both ends" {
		t.Errorf("known peer = %+v, want unreachable with its reason", got)
	}
	if got := byID["peer-unknown"]; got.State != "unreachable" || got.Capabilities[ReasonCapability] != "no candidates" {
		t.Errorf("unlisted peer = %+v, want a row marked unreachable with its reason", got)
	}
	if _, ok := byID["peer-fine"].Capabilities[ReasonCapability]; ok {
		t.Error("a reachable peer was given a reason")
	}

	s.ClearUnreachable("peer-known")
	s.ClearUnreachable("peer-unknown")
	if n := s.withUnreachable(nil); len(n) != 0 {
		t.Errorf("cleared peers still listed: %+v", n)
	}
}
