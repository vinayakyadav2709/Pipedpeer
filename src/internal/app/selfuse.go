package app

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/registry"
)

// usageSampler watches this machine and the executing node while a job runs,
// for the summary's "this machine" and "on <node>" lines: the demo's point is
// that the sender stays idle while the node works. It costs one /proc read
// and one /health poll a second, and it never fails a run: a sample that
// cannot be read leaves its line out with a warning.
type usageSampler struct {
	stop chan struct{}
	cpu0 cpuTimes
	mem0 int64
	self bool // the start reads worked

	mu      sync.Mutex
	selfUse selfStats
	node    nodeStats
	nodeErr error
}

const sampleEvery = time.Second

// startUsage begins sampling; host:port is the executing daemon.
func startUsage(host string, port int) *usageSampler {
	u := &usageSampler{stop: make(chan struct{})}
	cpu, err := readCPUTimes()
	if err == nil {
		u.mem0, err = readMemUsed()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: cannot sample this machine's usage, leaving it out of the summary: %v\n", err)
	} else {
		u.cpu0, u.self = cpu, true
		go u.sampleSelf()
	}
	go u.sampleNode(fmt.Sprintf("http://%s/health", net.JoinHostPort(host, strconv.Itoa(port))))
	return u
}

func (u *usageSampler) every(f func()) {
	t := time.NewTicker(sampleEvery)
	defer t.Stop()
	for {
		f()
		select {
		case <-u.stop:
			return
		case <-t.C:
		}
	}
}

func (u *usageSampler) sampleSelf() {
	u.every(func() {
		used, err := readMemUsed()
		// The first call shells out to nvidia-smi (slow when cold); the gpu
		// package caches it for 5 s after that.
		info := gpu.Detect()
		var util float64
		if info.Vendor != gpu.VendorNone {
			util = gpu.DetectUsage().UtilizationGPU
		}
		u.mu.Lock()
		defer u.mu.Unlock()
		if err == nil && used-u.mem0 > u.selfUse.RAMPeak {
			u.selfUse.RAMPeak = used - u.mem0
		}
		u.selfUse.GPUChecked = true
		u.selfUse.HasGPU = info.Vendor != gpu.VendorNone
		if u.selfUse.HasGPU {
			u.selfUse.GPUSum += util
			u.selfUse.GPUN++
		}
	})
}

func (u *usageSampler) sampleNode(url string) {
	first := true
	u.every(func() {
		var h struct {
			Load registry.LoadInfo `json:"load"`
		}
		err := getJSON(url, &h)
		u.mu.Lock()
		defer u.mu.Unlock()
		if err != nil {
			u.node.Failed++
			u.nodeErr = err
			return
		}
		u.node.add(h.Load, first)
		first = false
	})
}

// finish stops sampling and returns the two summary values ("" to leave a
// line out). It does not wait for a sample in flight.
func (u *usageSampler) finish() (self, node string) {
	close(u.stop)
	u.mu.Lock()
	s, n, nodeErr := u.selfUse, u.node, u.nodeErr
	u.mu.Unlock()
	if u.self {
		cpu, err := readCPUTimes()
		used, merr := readMemUsed()
		if err == nil {
			err = merr
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: cannot sample this machine's usage, leaving it out of the summary: %v\n", err)
		} else {
			s.CPU = cpuBusy(u.cpu0, cpu)
			s.RAMDelta = used - u.mem0
			self = selfLine(s)
		}
	}
	if n.Failed > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d of %d usage polls of the node failed (last: %v)\n", n.Failed, n.Failed+n.Polls, nodeErr)
	}
	return self, nodeLine(n)
}

// cpuTimes is the aggregate "cpu" line of /proc/stat, in jiffies.
type cpuTimes struct{ busy, total uint64 }

func readCPUTimes() (cpuTimes, error) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuTimes{}, err
	}
	return parseCPUTimes(string(b))
}

func parseCPUTimes(stat string) (cpuTimes, error) {
	line, _, _ := strings.Cut(stat, "\n")
	f := strings.Fields(line)
	if len(f) < 5 || f[0] != "cpu" {
		return cpuTimes{}, fmt.Errorf("/proc/stat: unexpected first line %q", line)
	}
	var t cpuTimes
	// user nice system idle iowait irq softirq steal; guest time is already
	// counted in user.
	for i, v := range f[1:] {
		if i >= 8 {
			break
		}
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return cpuTimes{}, fmt.Errorf("/proc/stat: %q: %v", line, err)
		}
		t.total += n
		if i != 3 && i != 4 { // idle, iowait
			t.busy += n
		}
	}
	return t, nil
}

// cpuBusy is the share of all cores busy between two readings, in percent.
func cpuBusy(a, b cpuTimes) float64 {
	if b.total <= a.total || b.busy < a.busy {
		return 0
	}
	return float64(b.busy-a.busy) / float64(b.total-a.total) * 100
}

// readMemUsed is MemTotal - MemAvailable from /proc/meminfo, in bytes.
func readMemUsed() (int64, error) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseMemUsed(string(b))
}

func parseMemUsed(meminfo string) (int64, error) {
	var total, avail int64 = -1, -1
	for _, line := range strings.Split(meminfo, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		n, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil {
			continue
		}
		switch f[0] {
		case "MemTotal:":
			total = n * 1024
		case "MemAvailable:":
			avail = n * 1024
		}
	}
	if total < 0 || avail < 0 {
		return 0, fmt.Errorf("/proc/meminfo: no MemTotal/MemAvailable")
	}
	return total - avail, nil
}

type selfStats struct {
	CPU        float64 // % of all cores busy
	RAMDelta   int64   // used at the end minus used at the start
	RAMPeak    int64   // highest used minus used at the start
	GPUChecked bool    // GPU detection finished
	HasGPU     bool
	GPUSum     float64
	GPUN       int
}

// selfLine: "CPU 3% avg · RAM +45 MB · GPU idle".
func selfLine(s selfStats) string {
	parts := []string{fmt.Sprintf("CPU %.0f%% avg", s.CPU), "RAM " + signedBytes(s.RAMDelta)}
	if s.RAMPeak-s.RAMDelta >= 10<<20 {
		parts[1] += " (peak " + signedBytes(s.RAMPeak) + ")"
	}
	switch {
	case !s.GPUChecked:
	case !s.HasGPU:
		parts = append(parts, "no GPU")
	case s.GPUN > 0:
		parts = append(parts, gpuPart(s.GPUSum/float64(s.GPUN)))
	}
	return strings.Join(parts, " · ")
}

type nodeStats struct {
	Polls, Failed int
	CPUSum        float64
	CPUN          int
	GPU           bool
	GPUSum        float64
	GPUN          int
	VRAMPeak      int64
}

// add folds in one /health sample. The daemon's cpu_percent covers the time
// since its previous reading, so the first poll's (from before the job) is
// skipped.
func (n *nodeStats) add(l registry.LoadInfo, first bool) {
	n.Polls++
	if !first {
		n.CPUSum += l.CPUPercent
		n.CPUN++
	}
	if l.GPUModel == "" {
		return
	}
	n.GPU = true
	n.GPUSum += l.GPUUtilPercent
	n.GPUN++
	if l.GPUMemUsedBytes > n.VRAMPeak {
		n.VRAMPeak = l.GPUMemUsedBytes
	}
}

// nodeLine: "CPU 91% avg · GPU 85% avg · VRAM 1.4 GB peak"; "" with no sample.
func nodeLine(n nodeStats) string {
	var parts []string
	if n.CPUN > 0 {
		parts = append(parts, fmt.Sprintf("CPU %.0f%% avg", n.CPUSum/float64(n.CPUN)))
	}
	if n.GPU {
		parts = append(parts, gpuPart(n.GPUSum/float64(n.GPUN)))
		if n.VRAMPeak > 0 {
			parts = append(parts, "VRAM "+humanize.Bytes(uint64(n.VRAMPeak))+" peak")
		}
	}
	return strings.Join(parts, " · ")
}

func gpuPart(avg float64) string {
	if avg < 5 {
		return "GPU idle"
	}
	return fmt.Sprintf("GPU %.0f%% avg", avg)
}

func signedBytes(n int64) string {
	if n < 0 {
		return "-" + humanize.Bytes(uint64(-n))
	}
	return "+" + humanize.Bytes(uint64(n))
}
