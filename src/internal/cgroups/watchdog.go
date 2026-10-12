package cgroups

import (
	"context"
	"os"
	"strconv"
	"strings"
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
			victim, total, over := pickVictim(procs, self, ceiling)
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

type procMem struct {
	pid, ppid int32
	rss       int64
	name      string
}

// snapshot reads every process's parent and resident size. Processes that
// vanish or cannot be read mid-scan are skipped: they hold no memory now.
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
		mi, err := p.MemoryInfo()
		if err != nil {
			continue
		}
		name, _ := p.Name()
		out = append(out, procMem{pid: p.Pid, ppid: ppid, rss: int64(mi.RSS), name: name})
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
