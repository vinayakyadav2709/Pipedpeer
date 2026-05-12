package heartbeat

import (
	"runtime"

	"github.com/pipedpeer/pipedpeer/internal/registry"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// CollectLoad gathers current system load metrics using gopsutil.
// activeJobs is the current count of jobs being executed by the daemon.
// reservedMemBytes is the sum of all active pipedpeer job reservations.
func CollectLoad(activeJobs int, reservedMemBytes int64) registry.LoadInfo {
	totalMem := readTotalMemBytes()
	osFree := readAvailableMemBytes()
	available := osFree - reservedMemBytes
	if available < 0 {
		available = 0
	}

	return registry.LoadInfo{
		CPUPercent:        readCPUPercent(),
		MemPercent:        readMemPercent(),
		ActiveJobs:        activeJobs,
		TotalMemBytes:     totalMem,
		AvailableMemBytes: available,
		ReservedMemBytes:  reservedMemBytes,
		TotalCPUs:         runtime.NumCPU(),
	}
}

func readCPUPercent() float64 {
	// cpu.Percent with 0 interval returns the CPU usage since last call
	// (or since boot if first call). Non-blocking.
	percents, err := cpu.Percent(0, false)
	if err != nil || len(percents) == 0 {
		return 0
	}
	pct := percents[0]
	if pct > 100 {
		pct = 100
	}
	return pct
}

func readMemPercent() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return v.UsedPercent
}

// readTotalMemBytes reads total physical memory in bytes using gopsutil.
func readTotalMemBytes() int64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return int64(v.Total)
}

// readAvailableMemBytes reads available memory in bytes using gopsutil.
// This is real OS-level available memory (accounts for all non-pipedpeer usage).
func readAvailableMemBytes() int64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return int64(v.Available)
}
