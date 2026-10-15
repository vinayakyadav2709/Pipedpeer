package daemonapi

import (
	"net/http/httptest"
	"testing"
)

// An internet peer knows the submitter as 127.0.0.1:<forwarder port>, never
// by the LAN address the submitter stamps; matching by node id is what lets
// spill order sink the orchestrator there. Older submitters send no id and
// keep the address match.
func TestThePoolSubmitterIsMatchedByNodeID(t *testing.T) {
	s := &Server{peerHealths: map[string]*PeerHealth{
		"127.0.0.1:41999": {Host: "127.0.0.1", Port: 41999, NodeID: "node-a", Status: "healthy"},
		"10.0.0.9:4242":   {Host: "10.0.0.9", Port: 4242, NodeID: "node-c", Status: "healthy"},
	}}
	req := func(addr, id string) string {
		r := httptest.NewRequest("POST", "/v1/pool/map", nil)
		r.Header.Set("X-Pipedpeer-Submitter", addr)
		if id != "" {
			r.Header.Set("X-Pipedpeer-Submitter-Node", id)
		}
		return s.poolSubmitter(r)
	}
	if got := req("192.168.0.200:4242", "node-a"); got != "127.0.0.1:41999" {
		t.Errorf("internet submitter: got %q, want its forwarder 127.0.0.1:41999", got)
	}
	if got := sinkPeerLast([]string{"127.0.0.1:41999", "10.0.0.9:4242"}, req("192.168.0.200:4242", "node-a")); got[0] != "10.0.0.9:4242" {
		t.Errorf("spill order %v: the submitter must go last", got)
	}
	if got := req("192.168.0.200:4242", ""); got != "192.168.0.200:4242" {
		t.Errorf("old submitter without an id: got %q, want its address", got)
	}
	if got := req("192.168.0.200:4242", "node-unknown"); got != "192.168.0.200:4242" {
		t.Errorf("unknown id: got %q, want the address fallback", got)
	}
}
