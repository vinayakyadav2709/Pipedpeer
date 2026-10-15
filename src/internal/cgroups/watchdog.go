package cgroups

import (
	"context"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v4/process"
)

// StartCeilingWatchdog holds the daemon and everything it spawns to the
// ceiling a systemd scope would have given it, on machines where there is no
// scope and no cgroup limit to do it: no systemd-run, no user session bus, a
// container, cgroup v1. Without it such a daemon ran unbounded and the kernel
// OOM killer picked its victim from the whole machine.
//
// It is a user-space stand-in: once a second it sums the resident memory of
// this process tree and, above the ceiling, kills the largest descendant
// (never the daemon). It returns false, after saying why, when the kernel
// already enforces a ceiling or the ceiling was switched off.
//
// ponytail: 1 s polling can miss an allocation that outruns it; a real
// cgroup (start the daemon under systemd) is the upgrade.
func StartCeilingWatchdog(ctx context.Context, logf func(format string, args ...any)) bool {
	if b := SelfBudget(); b.Enforced {
		logf("memory ceiling enforced by cgroup: %s for the daemon and its jobs", humanize.IBytes(uint64(b.Total)))
		return false
	}
	ceiling, why := watchdogCeiling(os.Getenv(MemoryMaxEnv), totalMemBytes())
	if ceiling <= 0 {
		logf("no memory ceiling on the daemon: %s", why)
		return false
	}
	logf("no systemd scope or cgroup ceiling here; falling back to a watchdog: "+
		"the daemon and its children are held to %s (%s), the largest child is killed above it",
		humanize.IBytes(uint64(ceiling)), why)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		self := int32(os.Getpid())
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			procs, err := snapshot()
			if err != nil {
				logf("memory watchdog: cannot list processes: %v", err)
				continue
			}
			tree := treeOf(procs, self)
			for i := range tree {
				tree[i].rss = residentShare(tree[i].pid, logf)
			}
			victim, total, over := pickVictim(tree, self, ceiling)
			if !over {
				continue
			}
			if err := syscall.Kill(int(victim.pid), syscall.SIGKILL); err != nil {
				logf("memory watchdog: %s in use is over the %s ceiling, but killing pid %d (%s) failed: %v",
					humanize.IBytes(uint64(total)), humanize.IBytes(uint64(ceiling)), victim.pid, victim.name, err)
				continue
			}
			logf("memory watchdog: %s in use is over the %s ceiling; killed pid %d (%s, %s resident)",
				humanize.IBytes(uint64(total)), humanize.IBytes(uint64(ceiling)), victim.pid, victim.name, humanize.IBytes(uint64(victim.rss)))
			recordKill(WatchdogKill{PID: victim.pid, Name: victim.name, Bytes: victim.rss,
				Total: total, Ceiling: ceiling, ancestors: ancestorsOf(tree, victim.pid)})
		}
	}()
	return true
}

// watchdogCeiling resolves the ceiling from PIPEDPEER_MEMORY_MAX (as systemd
// reads it: bytes with an optional K/M/G/T suffix, base 1024, or "max") or
// half the machine. 0 means no ceiling; why says where the number came from.
func watchdogCeiling(env string, machine int64) (int64, string) {
	env = strings.TrimSpace(env)
	switch env {
	case "":
	case "max", "infinity":
		return 0, MemoryMaxEnv + "=" + env
	default:
		if n, ok := parseSystemdBytes(env); ok {
			return n, MemoryMaxEnv + "=" + env
		}
		if machine <= 0 {
			return 0, "unreadable " + MemoryMaxEnv + "=" + env + " and machine size unknown"
		}
		return defaultShare(machine), "half the machine; " + MemoryMaxEnv + "=" + env + " is not a byte count"
	}
	if machine <= 0 {
		return 0, "machine size unknown (/proc/meminfo unreadable)"
	}
	return defaultShare(machine), "half the machine"
}

func parseSystemdBytes(s string) (int64, bool) {
	mult := int64(1)
	if i := strings.IndexAny(strings.ToUpper(s), "KMGT"); i == len(s)-1 {
		mult = int64(1) << (10 * (1 + strings.IndexByte("KMGT", strings.ToUpper(s)[i])))
		s = s[:i]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n * mult, true
}

// WatchdogKill is one process the watchdog killed, kept so the job it
// belonged to can report "ran out of memory" instead of a bare exit 137.
type WatchdogKill struct {
	PID            int32
	Name           string
	Bytes          int64 // the victim's PSS
	Total, Ceiling int64
	ancestors      []int32
}

var watchdogKills struct {
	sync.Mutex
	list []WatchdogKill
}

func recordKill(k WatchdogKill) {
	watchdogKills.Lock()
	defer watchdogKills.Unlock()
	// ponytail: a kill nobody claims (its job exited 0) is only dropped by
	// this bound; 32 is far more than one job's lifetime of kills.
	if len(watchdogKills.list) >= 32 {
		watchdogKills.list = watchdogKills.list[1:]
	}
	watchdogKills.list = append(watchdogKills.list, k)
}

// TakeWatchdogKill returns, and forgets, the latest watchdog kill of root or
// any process under it.
func TakeWatchdogKill(root int32) (WatchdogKill, bool) {
	watchdogKills.Lock()
	defer watchdogKills.Unlock()
	var found WatchdogKill
	ok := false
	kept := watchdogKills.list[:0]
	for _, k := range watchdogKills.list {
		if k.PID == root || slices.Contains(k.ancestors, root) {
			found, ok = k, true
			continue
		}
		kept = append(kept, k)
	}
	watchdogKills.list = kept
	return found, ok
}

// residentShare is a process's PSS: each shared page split between the
// processes mapping it. Summing RSS instead counted torch's libraries and
// fork-shared pages once per Pool worker: 16 workers read as 8.0 GiB "in
// use" and the watchdog killed a job the kernel's own 4.5 GB cap had let
// run. Falls back to RSS, said once, when smaps_rollup is unreadable.
func residentShare(pid int32, logf func(format string, args ...any)) int64 {
	p := &process.Process{Pid: pid}
	pss, err := processPSS(p)
	if err == nil {
		return pss
	}
	pssWarnOnce.Do(func() {
		logf("memory watchdog: cannot read PSS of pid %d (err %v); counting RSS, so shared pages count more than once", pid, err)
	})
	if mi, err := p.MemoryInfo(); err == nil {
		return int64(mi.RSS)
	}
	return 0 // exited since the snapshot: it holds nothing now
}

var pssWarnOnce sync.Once

// treeOf is self and its descendants out of procs.
func treeOf(procs []procMem, self int32) []procMem {
	children := map[int32][]procMem{}
	var out []procMem
	for _, p := range procs {
		children[p.ppid] = append(children[p.ppid], p)
		if p.pid == self {
			out = append(out, p)
		}
	}
	seen := map[int32]bool{self: true}
	for queue := []int32{self}; len(queue) > 0; queue = queue[1:] {
		for _, c := range children[queue[0]] {
			if !seen[c.pid] {
				seen[c.pid] = true
				out = append(out, c)
				queue = append(queue, c.pid)
			}
		}
	}
	return out
}

// ancestorsOf walks pid's parents within tree.
func ancestorsOf(tree []procMem, pid int32) []int32 {
	parent := map[int32]int32{}
	for _, p := range tree {
		parent[p.pid] = p.ppid
	}
	var out []int32
	for seen := 0; seen < len(tree); seen++ {
		pp, ok := parent[pid]
		if !ok {
			break
		}
		out = append(out, pp)
		pid = pp
	}
	return out
}

type procMem struct {
	pid, ppid int32
	rss       int64
	name      string
}

// snapshot reads every process's parent and name; memory is read later, for
// the daemon's tree only. Processes that vanish mid-scan are skipped.
func snapshot() ([]procMem, error) {
	ps, err := process.Processes()
	if err != nil {
		return nil, err
	}
	out := make([]procMem, 0, len(ps))
	for _, p := range ps {
		ppid, err := p.Ppid()
		if err != nil {
			continue
		}
		name, _ := p.Name()
		out = append(out, procMem{pid: p.Pid, ppid: ppid, name: name})
	}
	return out, nil
}

// pickVictim sums the resident memory of self and its descendants and, when
// that exceeds ceiling, returns the largest descendant to kill.
func pickVictim(procs []procMem, self int32, ceiling int64) (victim procMem, total int64, over bool) {
	children := map[int32][]procMem{}
	for _, p := range procs {
		children[p.ppid] = append(children[p.ppid], p)
	}
	for _, p := range procs {
		if p.pid == self {
			total += p.rss
		}
	}
	queue := []int32{self}
	seen := map[int32]bool{self: true}
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		for _, c := range children[pid] {
			if seen[c.pid] {
				continue
			}
			seen[c.pid] = true
			total += c.rss
			if c.rss > victim.rss {
				victim = c
			}
			queue = append(queue, c.pid)
		}
	}
	if total <= ceiling || victim.pid == 0 {
		return procMem{}, total, false
	}
	return victim, total, true
}
