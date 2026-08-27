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
