package cgroups

import "testing"

func TestPickVictimKillsLargestDescendantOnlyOverCeiling(t *testing.T) {
	procs := []procMem{
		{pid: 10, ppid: 1, rss: 100, name: "daemon"},
		{pid: 11, ppid: 10, rss: 300, name: "worker"},
		{pid: 12, ppid: 11, rss: 500, name: "python"}, // grandchild
		{pid: 20, ppid: 1, rss: 9000, name: "unrelated"},
	}
	if _, total, over := pickVictim(procs, 10, 1000); over || total != 900 {
		t.Fatalf("under ceiling: total %d over %v", total, over)
	}
	v, total, over := pickVictim(procs, 10, 800)
	if !over || total != 900 || v.pid != 12 {
		t.Fatalf("over ceiling: victim %d total %d over %v", v.pid, total, over)
	}
	// The daemon alone over the ceiling: nothing to kill, never itself.
	if _, _, over := pickVictim(procs[:1], 10, 50); over {
		t.Fatal("picked the daemon itself")
	}
}

func TestWatchdogCeiling(t *testing.T) {
	const machine = 16 << 30
	for _, c := range []struct {
		env  string
		want int64
	}{
		{"", 8 << 30},
		{"4G", 4 << 30},
		{"512M", 512 << 20},
		{"1048576", 1 << 20},
		{"max", 0},
		{"infinity", 0},
		{"50%", 8 << 30}, // not a byte count: default share, said in why
	} {
		if got, why := watchdogCeiling(c.env, machine); got != c.want || why == "" {
			t.Errorf("%q: got %d (%s), want %d", c.env, got, why, c.want)
		}
	}
	if got, _ := watchdogCeiling("", 0); got != 0 {
		t.Errorf("unknown machine size: got %d", got)
	}
}
