package app

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pipedpeer/pipedpeer/internal/gpu"
	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/pipedpeer/pipedpeer/internal/say"
)

// Usage watches this machine and the executing nodes while a job runs, for
// the summary's "this machine" and "on <node>" lines: the demo's point is
// that the sender stays idle while the nodes work. It costs one /proc read
// and one /health poll per node a second, and it never fails a run: a sample
// that cannot be read leaves its line out with a warning.
type Usage struct {
	stop  chan struct{}
	cpu0  cpuTimes
	mem0  int64
	self  bool // the start reads worked
	nodes []*nodeSample

	mu      sync.Mutex // guards selfUse and every nodeSample
	selfUse selfStats
}

// UsageNode is a daemon to sample. Name labels its line; "" means the
// summary's own "ran on" name.
type UsageNode struct {
	Name string
	Host string
	Port int
}

type nodeSample struct {
	name, addr string
	stats      nodeStats
	err        error
}

// nodeUse is one node's summary value.
type nodeUse struct{ Name, Line string }

const sampleEvery = time.Second

// StartUsage begins sampling this machine and nodes. A node listed twice
// (two ranks on one machine) is sampled once.
func StartUsage(nodes []UsageNode) *Usage {
	u := &Usage{stop: make(chan struct{})}
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
	seen := map[string]bool{}
	for _, n := range nodes {
		addr := net.JoinHostPort(n.Host, strconv.Itoa(n.Port))
		if seen[addr] {
			continue
		}
		seen[addr] = true
		ns := &nodeSample{name: n.Name, addr: addr}
		u.nodes = append(u.nodes, ns)
		go u.sampleNode(ns)
	}
	return u
}

func (u *Usage) every(f func()) {
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

func (u *Usage) sampleSelf() {
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

func (u *Usage) sampleNode(ns *nodeSample) {
	url := "http://" + ns.addr + "/health"
	first := true
	u.every(func() {
		var h struct {
			Load registry.LoadInfo `json:"load"`
		}
		err := getJSON(url, &h)
		u.mu.Lock()
		defer u.mu.Unlock()
		if err != nil {
			ns.stats.Failed++
			ns.err = err
			return
		}
		ns.stats.add(h.Load, first)
		first = false
	})
}

// finish stops sampling and returns this machine's summary value ("" to
// leave the line out) and one per node that has a sample. It does not wait
// for a sample in flight.
func (u *Usage) finish() (self string, nodes []nodeUse) {
	close(u.stop)
	u.mu.Lock()
	s := u.selfUse
	for _, ns := range u.nodes {
		if ns.stats.Failed > 0 {
			fmt.Fprintf(os.Stderr, "warning: %d of %d usage polls of %s failed (last: %v)\n",
				ns.stats.Failed, ns.stats.Failed+ns.stats.Polls, ns.addr, ns.err)
		}
		if line := nodeLine(ns.stats); line != "" {
			nodes = append(nodes, nodeUse{ns.name, line})
		}
	}
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
	return self, nodes
}

// Print stops sampling and prints the usage lines in the summary's layout,
// only under -v: a plain run prints nothing but the script's output.
func (u *Usage) Print(w io.Writer) {
	self, nodes := u.finish()
	if !say.Verbose || (self == "" && len(nodes) == 0) {
		return
	}
	fmt.Fprintln(w, "\n=== Usage ===")
	if self != "" {
		fmt.Fprintf(w, "  %-12s %s\n", "this machine", self)
	}
	for _, n := range nodes {
		fmt.Fprintf(w, "  %-12s %s\n", "on "+n.Name, n.Line)
	}
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
