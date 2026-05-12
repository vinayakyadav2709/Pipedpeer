package resourceest

import (
	"github.com/shirou/gopsutil/v4/process"
)

// ReadVmPeak reads the peak virtual memory for a process using gopsutil.
// Returns bytes. Returns 0 if unavailable (pid exited, permission denied, etc.).
func ReadVmPeak(pid int) int64 {
	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return 0
	}
	info, err := proc.MemoryInfo()
	if err != nil {
		return 0
	}
	// VMS is the total virtual memory size (closest to VmPeak available cross-platform)
	return int64(info.VMS)
}

// ReadSelfVmPeak reads VmPeak for the current process using gopsutil.
func ReadSelfVmPeak() int64 {
	pid := int32(0) // gopsutil treats 0 as the current process internally
	proc, err := process.NewProcess(pid)
	if err != nil {
		// pid 0 not supported on this platform
		return 0
	}
	info, err := proc.MemoryInfo()
	if err != nil {
		return 0
	}
	return int64(info.VMS)
}
