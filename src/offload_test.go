package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// TestOffloadKeepsEveryRankOffThisMachine: under --offload no DDP rank lands
// on a node whose machine key is this machine's, including a second daemon on
// it. Every node sits at 127.0.0.1, as internet peers do, so only the key can
// tell them apart.
func TestOffloadKeepsEveryRankOffThisMachine(t *testing.T) {
	var port int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		ep := "root@127.0.0.1:22"
		node := func(id, machine string) registry.NodeRecord {
			return registry.NodeRecord{NodeID: id, SSHEndpoint: ep, DaemonPort: port, State: "healthy",
				Capabilities: map[string]string{heartbeat.MachineCapability: machine},
				Load:         registry.LoadInfo{GPUModel: "RTX 3060", TotalCPUs: 8, AvailableMemBytes: 8 << 30}}
		}
		_ = json.NewEncoder(w).Encode([]registry.NodeRecord{
			node("me", "boot-me"), node("me-2nd-daemon", "boot-me"),
			node("peer-b", "boot-b"), node("peer-c", "boot-c"),
		})
	}))
	defer srv.Close()
	_, p, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ = strconv.Atoi(p)

	ring, _, _ := ddpChooseRing(ddpRunOptions{
		DaemonPort: port, Nodes: 4, PreferGPU: true, MaxRanks: 4,
		NoSelf: true, SelfNodeID: "me", ExcludeMachine: "boot-me",
	}, registry.NodeRecord{})
	if len(ring) != 2 {
		t.Fatalf("ring has %d members, want peer-b and peer-c: %v", len(ring), ring)
	}
	for _, n := range ring {
		if n.Capabilities[heartbeat.MachineCapability] == "boot-me" {
			t.Errorf("rank placed on this machine (%s) under --offload", n.NodeID)
		}
	}
	if n := ddpEligibleCountExcept(port, false, 0, 0, "boot-me"); n != 2 {
		t.Errorf("eligible count = %d without this machine, want 2", n)
	}
	// A rank that turns up mid-run is checked by the same rule.
	o := ddpRunOptions{ExcludeMachine: "boot-me"}
	if !ddpExcluded(o, registry.NodeRecord{NodeID: "x", Capabilities: map[string]string{heartbeat.MachineCapability: "boot-me"}}) {
		t.Errorf("a joiner on this machine is not excluded")
	}
	if ddpExcluded(o, registry.NodeRecord{NodeID: "y"}) {
		t.Errorf("a node with no key is excluded; unknown is not this machine")
	}
}
