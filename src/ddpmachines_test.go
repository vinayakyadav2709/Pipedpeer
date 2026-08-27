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
