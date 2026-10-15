package main

import (
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func fastpathNode(id, host string) registry.NodeRecord {
	return registry.NodeRecord{NodeID: id, SSHEndpoint: "root@" + host + ":22", DaemonPort: 4242,
		Capabilities: map[string]string{heartbeat.MachineCapability: "m-" + id}}
}

// Ranks sharing a node other than the lead's get the node's rank list; the
// lead's own node and single-rank nodes get nothing.
func TestDDPLocalRanksOnlyOffTheLeadsNode(t *testing.T) {
	ring := []registry.NodeRecord{
		fastpathNode("lead", "10.0.0.1"), fastpathNode("lead", "10.0.0.1"),
		fastpathNode("gpu2", "10.0.0.2"), fastpathNode("gpu2", "10.0.0.2"),
		fastpathNode("cpu", "10.0.0.3"),
	}
	got := ddpLocalRanks(ring)
	if len(got) != 2 || got[2] != "2,3" || got[3] != "2,3" {
		t.Fatalf("ddpLocalRanks = %v, want ranks 2 and 3 -> \"2,3\"", got)
	}
	if got := ddpLocalRanks(ring[:3]); len(got) != 0 {
		t.Fatalf("no shared non-lead node, got %v", got)
	}
}

// The butterfly needs 3+ ranks that all reach each other: every rank gets
// every daemon's URL, and a ring holding a forwarded internet peer gets none.
func TestDDPPeerURLs(t *testing.T) {
	ring := []registry.NodeRecord{
		fastpathNode("a", "10.0.0.1"), fastpathNode("b", "10.0.0.2"), fastpathNode("c", "10.0.0.3"),
	}
	got := ddpPeerURLs(ring, 1, "m-self")
	want := "http://10.0.0.1:4242/v1/ddp/sync,http://10.0.0.2:4242/v1/ddp/sync,http://10.0.0.3:4242/v1/ddp/sync"
	if got != want {
		t.Fatalf("ddpPeerURLs = %q\nwant %q", got, want)
	}
	if got := ddpPeerURLs(ring[:2], 0, "m-self"); got != "" {
		t.Fatalf("two ranks: want the hub, got %q", got)
	}
	ring[2] = fastpathNode("fwd", "127.0.0.1") // a forwarded peer: its machine key is not ours
	if got := ddpPeerURLs(ring, 0, "m-self"); got != "" {
		t.Fatalf("forwarded member: want the hub, got %q", got)
	}
	ring[2].Capabilities[heartbeat.MachineCapability] = "m-self" // this machine
	if got := ddpPeerURLs(ring, 2, "m-self"); !strings.Contains(got, "http://127.0.0.1:4242/") {
		t.Fatalf("a rank on this machine reaches its own daemon on loopback, got %q", got)
	}
}
