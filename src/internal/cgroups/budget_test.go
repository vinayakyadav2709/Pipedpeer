package cgroups

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAnUnenforcedNodeStillHasABudget.
//
// A daemon in a container has no systemd to put it in a scope, and no memory
// limit at all unless docker was given one. Measured: its memory.max read
// "max" while the host daemon beside it was correctly capped at half the
// machine, so one of the two was unbounded and the kernel went looking for a
// victim outside either cgroup. Reporting no budget would let admission there
// promise the whole machine.
func TestAnUnenforcedNodeStillHasABudget(t *testing.T) {
	b := SelfBudget()
	if b.Total <= 0 {
		if totalMemBytes() > 0 {
			t.Error("this machine reports its size but the daemon has no budget, " +
				"so admission has nothing to bound itself by")
		}
		t.Skip("NOT VERIFIED: cannot read this machine's memory size")
	}
	if b.Remaining() > b.Total {
		t.Errorf("remaining %d exceeds the total %d", b.Remaining(), b.Total)
	}
}

// TestABudgetIsNeverMoreThanTheMachine. A ceiling above the machine's size
// bounds nothing.
func TestABudgetIsNeverMoreThanTheMachine(t *testing.T) {
	total := totalMemBytes()
	if total <= 0 {
		t.Skip("NOT VERIFIED: cannot read this machine's memory size")
	}
	if b := SelfBudget(); b.Total > total {
		t.Errorf("budget %d exceeds the machine's %d bytes", b.Total, total)
	}
}

// TestSpentBudgetPromisesNothing rather than going negative, which as an
// int64 would read as an enormous amount of free memory.
func TestSpentBudgetPromisesNothing(t *testing.T) {
	b := Budget{Total: 1 << 30, Used: 4 << 30}
	if got := b.Remaining(); got != 0 {
		t.Errorf("an over-spent budget offered %d bytes, want 0", got)
	}
}

// TestUnknownBudgetOffersNothing. Callers treat a zero Total as "no opinion"
// and skip the clamp; Remaining must not invent headroom for it.
func TestUnknownBudgetOffersNothing(t *testing.T) {
	if got := (Budget{}).Remaining(); got != 0 {
		t.Errorf("an unknown budget offered %d bytes", got)
	}
}

// TestMaxIsNotALimit. cgroup files spell "no limit" as the word max; parsing
// that as a number would produce a nonsense ceiling.
func TestMaxIsNotALimit(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/memory.max"
	for _, content := range []string{"max\n", "", "  \n", "0\n", "-1\n", "banana\n"} {
		if err := writeTestFile(path, content); err != nil {
			t.Fatal(err)
		}
		if n, ok := readBytes(path); ok {
			t.Errorf("%q was read as a limit of %d", content, n)
		}
	}
	if err := writeTestFile(path, "7241236480\n"); err != nil {
		t.Fatal(err)
	}
	n, ok := readBytes(path)
	if !ok || n != 7241236480 {
		t.Errorf("a real limit read as (%d, %v)", n, ok)
	}
}

// TestTheUnenforcedShareMatchesTheEnforcedOne, so a node the kernel is not
// policing declines exactly the work a policed one would.
func TestTheUnenforcedShareMatchesTheEnforcedOne(t *testing.T) {
	total := int64(13_487_878_144)
	share := defaultShare(total)
	if share != total/2 {
		t.Errorf("unenforced share is %d, but scopeArgv asks systemd for %d",
			share, total/2)
	}
}

func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

// TestPageCacheIsNotSpentBudget.
//
// memory.current counts page cache, and reading a multi-GB closure fills it.
// Measured on a container capped at 3 GiB: 1.99 GiB "used", of which 13 MB
// was anonymous and the rest was the nix store it had just read. The daemon
// advertised 1 GiB free and placement declined a 2.3 GiB job it had room for.
func TestPageCacheIsNotSpentBudget(t *testing.T) {
	dir := t.TempDir()
	// The figures above, verbatim.
	if err := writeTestFile(dir+"/memory.current", "1994993664\n"); err != nil {
		t.Fatal(err)
	}
	if err := writeTestFile(dir+"/memory.stat", `anon 13549568
file 1874497536
inactive_file 1605939200
active_file 268562432
slab_reclaimable 103170176
`); err != nil {
		t.Fatal(err)
	}
	got := usageOf(dir)
	// 1994993664 - (1605939200 + 103170176) = 285884288
	if got != 285884288 {
		t.Errorf("usage read as %d bytes; want 285884288 with reclaimable cache "+
			"excluded. 1994993664 would mean the closure this node had just read "+
			"counted against the job it was about to run", got)
	}
}

// TestAllCacheReadsAsNothingSpent, and never as a negative — which as an
// int64 subtraction would come back as an enormous allowance.
func TestAllCacheReadsAsNothingSpent(t *testing.T) {
	dir := t.TempDir()
	if err := writeTestFile(dir+"/memory.current", "1000000\n"); err != nil {
		t.Fatal(err)
	}
	if err := writeTestFile(dir+"/memory.stat", "inactive_file 900000\nslab_reclaimable 500000\n"); err != nil {
		t.Fatal(err)
	}
	if got := usageOf(dir); got != 0 {
		t.Errorf("usage read as %d, want 0", got)
	}
}

// TestMissingStatFallsBackToTheRawFigure. An unreadable memory.stat must not
// make the budget look larger than it is; without the breakdown, the cautious
// answer is that all of it is spent.
func TestMissingStatFallsBackToTheRawFigure(t *testing.T) {
	dir := t.TempDir()
	if err := writeTestFile(dir+"/memory.current", "4096\n"); err != nil {
		t.Fatal(err)
	}
	if got := usageOf(dir); got != 4096 {
		t.Errorf("usage read as %d with no memory.stat, want the raw 4096", got)
	}
}

// TestAnAncestorsLimitBinds. Measured in the lab: a daemon in a systemd user
// scope capped at 6.7 GiB (half the host) inside a 1.17 GiB container read
// only the scope, advertised 6.7 GiB, took a torch job and the container was
// OOM-killed. Every level the kernel enforces counts, in v2 and in v1.
func TestAnAncestorsLimitBinds(t *testing.T) {
	mk := func(dir string, files map[string]string) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, content := range files {
			if err := writeTestFile(filepath.Join(dir, name), content); err != nil {
				t.Fatal(err)
			}
		}
	}
	const container = 1_256_000_000

	v2 := t.TempDir()
	scopeDir := filepath.Join(v2, "user.slice/user@1000.service/app.slice/pipedpeer-daemon-38080.scope")
	mk(v2, map[string]string{"memory.max": "1256000000\n", "memory.current": "200000000\n"})
	mk(filepath.Join(v2, "user.slice"), map[string]string{"memory.max": "max\n"})
	mk(scopeDir, map[string]string{"memory.max": "7194000000\n", "memory.current": "50000000\n"})
	mk(filepath.Join(scopeDir, "daemon"), nil)
	b, ok := limitBudget(v2, "0::/user.slice/user@1000.service/app.slice/pipedpeer-daemon-38080.scope/daemon\n")
	if !ok || b.Total != container || b.Used != 200_000_000 || !b.Enforced {
		t.Errorf("v2: budget %+v (found %v); want the container's %d with its usage", b, ok, int64(container))
	}

	// v1: the container's own subtree is mounted as the hierarchy root, so the
	// path in /proc/self/cgroup does not exist under the mount at all.
	v1 := t.TempDir()
	mk(filepath.Join(v1, "memory"), map[string]string{"memory.limit_in_bytes": "1256000000\n", "memory.usage_in_bytes": "300000000\n",
		"memory.stat": "total_inactive_file 100000000\n"})
	b, ok = limitBudget(v1, "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n1:name=systemd:/docker/abc\n")
	if !ok || b.Total != container || b.Used != 200_000_000 {
		t.Errorf("v1: budget %+v (found %v); want %d with 200000000 used", b, ok, int64(container))
	}

	// v1's "no limit" is a number near 2^63, not a limit.
	mk(filepath.Join(v1, "memory", "docker", "abc"), map[string]string{"memory.limit_in_bytes": "9223372036854771712\n"})
	if b, _ = limitBudget(v1, "4:memory:/docker/abc\n"); b.Total != container {
		t.Errorf("v1 unlimited child: budget %+v; want the root's %d", b, int64(container))
	}
}
