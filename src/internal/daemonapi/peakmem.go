package daemonapi

import (
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/process"
)

// peakMemTracker samples the memory footprint of a running job and remembers
// the largest value seen. The job may be a shell or crun wrapping the real
// python process, so it walks the whole descendant tree rather than trusting
// the direct child's RSS.
type peakMemTracker struct {
	mu     sync.Mutex
	peakKB int64
	stopCh chan struct{}
}

// newPeakMemTracker starts sampling every interval for the process tree rooted
// at rootPID. Call stop() when the job finishes to halt sampling.
func newPeakMemTracker(rootPID int32, interval time.Duration) *peakMemTracker {
	t := &peakMemTracker{stopCh: make(chan struct{})}
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
			kb := treeRSS(rootPID)
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

func (t *peakMemTracker) peak() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peakKB
}

// treeRSS sums the RSS (in KB) of a process and all its descendants.
func treeRSS(rootPID int32) int64 {
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

		if p, err := process.NewProcess(pid); err == nil {
			if mi, err := p.MemoryInfo(); err == nil {
				total += int64(mi.RSS) // bytes
			}
			if children, err := p.Children(); err == nil {
				for _, c := range children {
					stack = append(stack, c.Pid)
				}
			}
		}
	}
	return total / 1024 // KB
}
