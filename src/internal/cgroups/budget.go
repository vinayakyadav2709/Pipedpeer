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
	procCgroup, err := os.ReadFile("/proc/self/cgroup")
	if err == nil {
		if b, ok := limitBudget(mountpoint, string(procCgroup)); ok {
			return b
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

// limitBudget is the tightest enforced ceiling over this process's cgroup and
// every ancestor, in the v2 hierarchy and the v1 memory hierarchy alike.
// procCgroup is /proc/self/cgroup; mount is the cgroup filesystem root.
//
// Every level counts, because the kernel enforces every level. It used to
// read only the daemon's cgroup and its parent: a daemon in a systemd user
// scope inside a 1.17 GiB container found the scope's 6.7 GiB (half the
// host) and stopped there, advertised it, took a torch job, and the container
// was OOM-killed along with its systemd. The binding level is the one with
// the least left, not the smallest limit: a large parent can be nearly full.
func limitBudget(mount, procCgroup string) (Budget, bool) {
	var best Budget
	found := false
	consider := func(b Budget) {
		if !found || b.Remaining() < best.Remaining() {
			best, found = b, true
		}
	}
	for _, line := range strings.Split(procCgroup, "\n") {
		f := strings.SplitN(line, ":", 3)
		if len(f) != 3 {
			continue
		}
		switch {
		case f[0] == "0" && f[1] == "":
			walkUp(mount, f[2], func(dir string) {
				if max, ok := readBytes(filepath.Join(dir, "memory.max")); ok {
					consider(Budget{Total: max, Used: usageOf(dir), Enforced: true})
				}
			})
		case hasController(f[1], "memory"):
			root := filepath.Join(mount, "memory")
			walkUp(root, f[2], func(dir string) {
				// v1 writes "no limit" as a page-rounded maximum near 2^63.
				if max, ok := readBytes(filepath.Join(dir, "memory.limit_in_bytes")); ok && max < 1<<60 {
					consider(Budget{Total: max, Used: usageOfV1(dir), Enforced: true})
				}
			})
		}
	}
	return best, found
}

// walkUp calls fn for root/own and each of its ancestors up to root itself.
// Levels missing from this mount (a container sees only its own subtree) are
// visited too and simply have no files.
func walkUp(root, own string, fn func(dir string)) {
	root = filepath.Clean(root)
	dir := filepath.Join(root, own)
	for strings.HasPrefix(dir, root) {
		fn(dir)
		if dir == root {
			return
		}
		dir = filepath.Dir(dir)
	}
}

func hasController(list, name string) bool {
	for _, c := range strings.Split(list, ",") {
		if c == name {
			return true
		}
	}
	return false
}

// usageOfV1 is usageOf for the v1 memory controller's file names.
func usageOfV1(dir string) int64 {
	cur, ok := readBytes(filepath.Join(dir, "memory.usage_in_bytes"))
	if !ok {
		return 0
	}
	if inactive := readMemoryStat(filepath.Join(dir, "memory.stat"))["total_inactive_file"]; inactive < cur {
		return cur - inactive
	}
	return 0
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
