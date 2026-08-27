package cgroups

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Budget is the most memory this daemon and everything it spawns should use,
// and how much of that is already spent.
//
// It answers a question admission could not previously ask: not "how much
// memory is free on this machine" - which every daemon on the machine reads
// the same optimistic answer to - but "how much of it is mine to give".
//
// Total is the enforced ceiling where there is one. Where there is not, it is
// the ceiling this daemon would have had, and admission is expected to respect
// it anyway. That case is not hypothetical: a daemon inside a container has no
// systemd to put it in a scope and no memory limit unless docker was given
// one, so it ran completely unbounded beside a host daemon that was correctly
// capped at half the machine. The kernel then went looking for a victim
// outside either cgroup.
type Budget struct {
	// Total is the ceiling in bytes; 0 when the machine's size is unknown.
	Total int64
	// Used is current usage under that ceiling, 0 when it cannot be read.
	Used int64
	// Enforced says whether the kernel is holding this limit. When false the
	// number is advisory and admission is the only thing honouring it.
	Enforced bool
}

// Remaining is what this daemon may still promise. Never negative.
func (b Budget) Remaining() int64 {
	if b.Total <= 0 {
		return 0
	}
	if r := b.Total - b.Used; r > 0 {
		return r
	}
	return 0
}

// SelfBudget reports this daemon's memory budget.
func SelfBudget() Budget {
	own := Self()
	if own != "" {
		root := filepath.Join("/sys/fs/cgroup", own)
		// A scope's limit lives on the scope, but the daemon's processes sit
		// in a leaf beneath it so the scope can delegate controllers - so the
		// limit is on the parent of where we are.
		for _, dir := range []string{root, filepath.Dir(root)} {
			if max, ok := readBytes(filepath.Join(dir, "memory.max")); ok {
				return Budget{
					Total:    max,
					Used:     usageOf(dir),
					Enforced: true,
				}
			}
		}
	}
	// No enforced ceiling. Fall back to the share this daemon would have been
	// given, so a node the kernel is not policing still declines work it
	// cannot hold rather than discovering that from the OOM killer.
	total := totalMemBytes()
	if total <= 0 {
		return Budget{}
	}
	return Budget{Total: defaultShare(total), Used: 0, Enforced: false}
}

// defaultShare is the fraction of a machine one daemon may use. It matches
// the ceiling scopeArgv asks systemd for, so an enforced node and an
// unenforced one behave the same way.
func defaultShare(total int64) int64 { return total / 2 }

// readBytes reads a cgroup byte-count file. "max" means no limit, which is
// not a number this can work with, so it reports absent.
func readBytes(path string) (int64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// usageOf is how much of a cgroup's allowance is genuinely spent.
//
// memory.current counts page cache, and reading a multi-GB closure fills it:
// a container capped at 3 GiB reported 1.99 GiB used with 13 MB of that
// anonymous - the rest was the nix store it had just read. The daemon then
// advertised 1 GiB free, and placement declined a 2.3 GiB job it had ample
// room for.
//
// Cache is not spent memory. The kernel reclaims it before it ever kills
// anything, so a job that needs the space simply causes reclaim. Subtracting
// the inactive file pages and reclaimable slab is the same accounting
// /proc/meminfo's MemAvailable does; active file pages stay counted, which
// keeps the estimate on the cautious side.
func usageOf(dir string) int64 {
	cur, ok := readBytes(filepath.Join(dir, "memory.current"))
	if !ok {
		return 0
	}
	stat := readMemoryStat(filepath.Join(dir, "memory.stat"))
	reclaimable := stat["inactive_file"] + stat["slab_reclaimable"]
	if reclaimable >= cur {
		// Nothing but cache. Reporting zero is right, and guards against a
		// negative that would read as an enormous allowance.
		return 0
	}
	return cur - reclaimable
}

// readMemoryStat parses the "key value" lines of a cgroup memory.stat.
func readMemoryStat(path string) map[string]int64 {
	out := map[string]int64{}
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		if n, err := strconv.ParseInt(f[1], 10, 64); err == nil {
			out[f[0]] = n
		}
	}
	return out
}
