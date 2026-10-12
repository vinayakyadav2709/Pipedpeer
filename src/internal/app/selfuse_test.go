package app

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func TestUsageLines(t *testing.T) {
	a, err := parseCPUTimes("cpu  100 0 50 800 50 0 0 0 7 0\ncpu0 1 2 3 4 5\n")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := parseCPUTimes("cpu  110 0 60 1570 60 0 0 0 9 0\n")
	if got := cpuBusy(a, b); got < 2.49 || got > 2.51 { // 20 busy of 800 jiffies
		t.Fatalf("cpuBusy = %v, want 2.5", got)
	}
	if _, err := parseCPUTimes("intr 1 2 3"); err == nil {
		t.Fatal("parseCPUTimes accepted a non-cpu line")
	}
	used, err := parseMemUsed("MemTotal:       16000 kB\nMemFree: 1 kB\nMemAvailable:    6000 kB\n")
	if err != nil || used != 10000*1024 {
		t.Fatalf("parseMemUsed = %d, %v", used, err)
	}
	if _, err := parseMemUsed("MemTotal: 1 kB\n"); err == nil {
		t.Fatal("parseMemUsed accepted meminfo without MemAvailable")
	}

	for _, c := range []struct {
		s    selfStats
		want string
	}{
		{selfStats{CPU: 3.2, RAMDelta: 45e6, RAMPeak: 50e6, GPUChecked: true}, "CPU 3% avg · RAM +45 MB · no GPU"},
		{selfStats{CPU: 3.2, RAMDelta: 45e6, RAMPeak: 50e6, GPUChecked: true, HasGPU: true, GPUSum: 4, GPUN: 2}, "CPU 3% avg · RAM +45 MB · GPU idle"},
		{selfStats{CPU: 12, RAMDelta: -2e6, RAMPeak: 80e6, GPUChecked: true, HasGPU: true, GPUSum: 120, GPUN: 2}, "CPU 12% avg · RAM -2.0 MB (peak +80 MB) · GPU 60% avg"},
		{selfStats{CPU: 1}, "CPU 1% avg · RAM +0 B"},
	} {
		if got := selfLine(c.s); got != c.want {
			t.Errorf("selfLine = %q, want %q", got, c.want)
		}
	}

	var n nodeStats
	n.add(registry.LoadInfo{CPUPercent: 5, GPUModel: "RTX 3060", GPUUtilPercent: 0, GPUMemUsedBytes: 3e8}, true)
	n.add(registry.LoadInfo{CPUPercent: 90, GPUModel: "RTX 3060", GPUUtilPercent: 80, GPUMemUsedBytes: 14e8}, false)
	n.add(registry.LoadInfo{CPUPercent: 92, GPUModel: "RTX 3060", GPUUtilPercent: 100, GPUMemUsedBytes: 12e8}, false)
	if got, want := nodeLine(n), "CPU 91% avg · GPU 60% avg · VRAM 1.4 GB peak"; got != want {
		t.Errorf("nodeLine = %q, want %q", got, want)
	}
	if got := nodeLine(nodeStats{Polls: 1}); got != "" {
		t.Errorf("nodeLine with no CPU sample = %q, want empty", got)
	}
	cpuOnly := nodeStats{}
	cpuOnly.add(registry.LoadInfo{CPUPercent: 50}, false)
	if got, want := nodeLine(cpuOnly), "CPU 50% avg"; got != want {
		t.Errorf("nodeLine = %q, want %q", got, want)
	}
}
