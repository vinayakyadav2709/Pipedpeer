package daemonapi

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/v4/process"
)

// peakMemTracker samples the memory footprint of a running job and remembers
// the largest value seen. The job may be a shell or crun wrapping the real
// python process, so it walks the whole descendant tree rather than trusting
// the direct child's footprint.
type peakMemTracker struct {
	mu     sync.Mutex
	peakKB int64
	stopCh chan struct{}
	warned bool   // PSS fallback already logged for this job
	cgDir  string // the job's cgroup directory; empty = walk /proc
}

// cgroupAnonKernel is anon + kernel_stack + slab from a cgroup v2 memory.stat,
// in bytes. Page cache ("file") is left out: a job reading a big CSV must not
// record the cache as its own memory, it feeds the admission estimator.
func cgroupAnonKernel(dir string) (int64, error) {
	b, err := os.ReadFile(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return 0, err
	}
	var total int64
	found := false
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) != 2 || (f[0] != "anon" && f[0] != "kernel_stack" && f[0] != "slab") {
			continue
		}
		v, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			return 0, err
		}
		total += v
		found = f[0] == "anon" || found
	}
	if !found {
		return 0, errors.New("memory.stat has no anon line")
	}
	return total, nil
}

// newPeakMemTracker starts sampling every interval for the process tree rooted
// at rootPID. Call stop() when the job finishes to halt sampling. A non-empty
// cgDir (the job's cgroup) is read instead of walking /proc, which cost ~4% of
// a core; the walk remains the fallback, logged once.
func newPeakMemTracker(rootPID int32, interval time.Duration, cgDir string) *peakMemTracker {
	t := &peakMemTracker{stopCh: make(chan struct{}), cgDir: cgDir}
	go t.poll(rootPID, interval)
	return t
}

func (t *peakMemTracker) poll(rootPID int32, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-t.stopCh:
			return
		case <-tick.C:
			kb := t.sample(rootPID)
			if kb > t.peak() {
				t.mu.Lock()
				if kb > t.peakKB {
					t.peakKB = kb
				}
				t.mu.Unlock()
			}
		}
	}
}

// stop halts sampling and returns the peak RSS in bytes.
func (t *peakMemTracker) stop() int64 {
	close(t.stopCh)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peakKB * 1024
}

// sample is one reading in KB: the cgroup when there is one, else the walk.
func (t *peakMemTracker) sample(rootPID int32) int64 {
	if t.cgDir != "" {
		b, err := cgroupAnonKernel(t.cgDir)
		if err == nil {
			return b / 1024
		}
		log.Warn().Err(err).Str("cgroup", t.cgDir).Msg("peak memory: job cgroup unreadable, walking /proc instead")
		t.cgDir = ""
	}
	return t.treeMem(rootPID)
}

func (t *peakMemTracker) peak() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peakKB
}

// treeMem sums the PSS (in KB) of a process and all its descendants. PSS
// splits each shared page between the processes mapping it, so forked Pool
// workers sharing their parent's copy-on-write pages count them once: summing
// RSS counted them once per worker (7.6 GB recorded for a job that never used
// 2), and the next run's estimate then fitted no node. A process whose
// smaps_rollup can't be read falls back to its RSS, logged once per job.
func (t *peakMemTracker) treeMem(rootPID int32) int64 {
	var total int64
	seen := map[int32]bool{}
	stack := []int32{rootPID}

	for len(stack) > 0 {
		pid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if pid <= 0 || seen[pid] {
			continue
		}
		seen[pid] = true

		p, err := process.NewProcess(pid)
		if err != nil {
			continue // exited between the walk and the read
		}
		if pss, err := processPSS(p); err == nil {
			total += pss
		} else if mi, rerr := p.MemoryInfo(); rerr == nil {
			total += int64(mi.RSS)
			if !t.warned {
				t.warned = true
				log.Warn().Err(err).Int32("pid", pid).Msg("peak memory: smaps_rollup unreadable, counting this process's RSS (shared pages may count twice)")
			}
		}
		if children, err := p.Children(); err == nil {
			for _, c := range children {
				stack = append(stack, c.Pid)
			}
		}
	}
	return total / 1024 // KB
}
