package cgroups

import (
	"os"
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
