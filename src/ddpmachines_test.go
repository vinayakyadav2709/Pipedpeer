package main

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func nodeOn(id, machine string) registry.NodeRecord {
	caps := map[string]string{}
	if machine != "" {
		caps[heartbeat.MachineCapability] = machine
	}
	return registry.NodeRecord{NodeID: id, Capabilities: caps}
}

// TestTheRingCountsMachinesNotRanks. The ring's own verdict on whether it was
// worth forming compares against "this machine doing every shard", scaled by
// how many machines shared the work. Scaling by the rank count instead says a
// laptop running two daemons is twice as fast as itself: measured, a two-rank
// ring on one machine reported "the ring paid for itself" on a run that took
// 191.1s against 92.8s for that machine alone.
func TestTheRingCountsMachinesNotRanks(t *testing.T) {
	cases := []struct {
		name string
		ring []registry.NodeRecord
		want int
	}{
		{
			name: "two daemons on one laptop are one machine",
			ring: []registry.NodeRecord{nodeOn("a", "boot-1"), nodeOn("b", "boot-1")},
			want: 1,
		},
		{
			name: "two machines are two",
			ring: []registry.NodeRecord{nodeOn("a", "boot-1"), nodeOn("b", "boot-2")},
			want: 2,
		},
		{
			name: "three ranks across two machines",
			ring: []registry.NodeRecord{nodeOn("a", "boot-1"), nodeOn("b", "boot-1"), nodeOn("c", "boot-2")},
			want: 2,
		},
		{
			// A node too old to report one is not evidence of sharing, and
			// assuming it shares would understate a real ring.
			name: "an unknown machine counts as its own",
			ring: []registry.NodeRecord{nodeOn("a", "boot-1"), nodeOn("b", "")},
			want: 2,
		},
		{
			name: "nothing reports a machine",
			ring: []registry.NodeRecord{nodeOn("a", ""), nodeOn("b", "")},
			want: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ddpDistinctMachines(tc.ring); got != tc.want {
				t.Errorf("ddpDistinctMachines = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestAForwardedPeerCannotLeadTheRing.
//
// A peer reached over the internet is registered at 127.0.0.1 on a local
// forwarder port, because that is what keeps the rest of the system ignorant
// of how it is reached. That address means something entirely different on
// every other machine, and every rank in a ring has to reach the lead: the
// star sync posts gradients to the lead's daemon.
//
// So a forwarded peer cannot be rank 0. Before this check the address was
// read as "rank 0 is this machine" - the two are indistinguishable by host
// alone - and every rank would have synced to the wrong daemon.
func TestAForwardedPeerCannotLeadTheRing(t *testing.T) {
	const selfMachine = "boot-self"

	cases := []struct {
		name      string
		node      registry.NodeRecord
		forwarded bool
		why       string
	}{
		{
			name: "this machine, at loopback",
			node: registry.NodeRecord{
				SSHEndpoint:  "root@127.0.0.1:22",
				Capabilities: map[string]string{heartbeat.MachineCapability: selfMachine},
			},
			forwarded: false,
			why:       "other ranks reach it by this machine's routable address",
		},
		{
			name: "an internet peer behind a local forwarder",
			node: registry.NodeRecord{
				SSHEndpoint:  "root@127.0.0.1:22",
				Capabilities: map[string]string{heartbeat.MachineCapability: "boot-far"},
			},
			forwarded: true,
			why:       "127.0.0.1 is a different machine everywhere else",
		},
		{
			name: "a LAN peer",
			node: registry.NodeRecord{
				SSHEndpoint:  "root@192.168.0.5:22",
				Capabilities: map[string]string{heartbeat.MachineCapability: "boot-lan"},
			},
			forwarded: false,
			why:       "the address is meaningful to every rank on that network",
		},
		{
			name: "localhost by name",
			node: registry.NodeRecord{
				SSHEndpoint:  "root@localhost:22",
				Capabilities: map[string]string{heartbeat.MachineCapability: "boot-far"},
			},
			forwarded: true,
			why:       "the name resolves to a different machine on each rank",
		},
		{
			// A node too old to report a machine key: assume it is not this
			// one, because assuming otherwise hands every rank an address
			// that points at itself.
			name:      "loopback with no machine key",
			node:      registry.NodeRecord{SSHEndpoint: "root@127.0.0.1:22"},
			forwarded: true,
			why:       "unknown provenance at a loopback address is not safe to lead",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ddpForwardedLead(tc.node, selfMachine); got != tc.forwarded {
				t.Errorf("ddpForwardedLead = %v, want %v — %s", got, tc.forwarded, tc.why)
			}
		})
	}
}

// TestTheRingPutsAReachableLeadFirst. When the best-scoring node cannot lead,
// the ring is reordered rather than the run being abandoned: the node is still
// a perfectly good rank, it just cannot be the one everybody syncs to.
func TestTheRingPutsAReachableLeadFirst(t *testing.T) {
	const selfMachine = "boot-self"
	forwarded := registry.NodeRecord{
		NodeID: "far", SSHEndpoint: "root@127.0.0.1:22",
		Capabilities: map[string]string{heartbeat.MachineCapability: "boot-far"},
	}
	local := registry.NodeRecord{
		NodeID: "self", SSHEndpoint: "root@127.0.0.1:22",
		Capabilities: map[string]string{heartbeat.MachineCapability: selfMachine},
	}
	lan := registry.NodeRecord{
		NodeID: "lan", SSHEndpoint: "root@192.168.0.5:22",
		Capabilities: map[string]string{heartbeat.MachineCapability: "boot-lan"},
	}

	got, ok := ddpLeadFirst([]registry.NodeRecord{forwarded, local, lan}, selfMachine)
	if !ok {
		t.Fatal("no lead was found though two candidates could lead")
	}
	if got[0].NodeID == "far" {
		t.Error("a forwarded peer was left leading the ring")
	}
	// And nobody is lost: a forwarded node is still a good rank.
	if len(got) != 3 {
		t.Errorf("ring has %d nodes, want all 3 — a node that cannot lead can still work", len(got))
	}
	seen := map[string]bool{}
	for _, n := range got {
		seen[n.NodeID] = true
	}
	for _, want := range []string{"far", "self", "lan"} {
		if !seen[want] {
			t.Errorf("%s was dropped from the ring", want)
		}
	}
}

// A ring in which nothing can lead has to say so rather than picking one and
// hanging every rank at the first barrier.
func TestARingWithNoReachableLeadIsRefused(t *testing.T) {
	far1 := registry.NodeRecord{
		NodeID: "a", SSHEndpoint: "root@127.0.0.1:22",
		Capabilities: map[string]string{heartbeat.MachineCapability: "boot-1"},
	}
	far2 := registry.NodeRecord{
		NodeID: "b", SSHEndpoint: "root@127.0.0.1:22",
		Capabilities: map[string]string{heartbeat.MachineCapability: "boot-2"},
	}
	if _, ok := ddpLeadFirst([]registry.NodeRecord{far1, far2}, "boot-self"); ok {
		t.Error("a ring of forwarded peers was accepted; every rank would sync to itself")
	}
}
