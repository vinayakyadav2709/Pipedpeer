package cgroups

import (
	"errors"

	"github.com/shirou/gopsutil/v4/process"
)

// processPSS reads a process's PSS (bytes) from /proc/<pid>/smaps_rollup.
func processPSS(p *process.Process) (int64, error) {
	maps, err := p.MemoryMaps(true)
	if err != nil {
		return 0, err
	}
	if maps == nil || len(*maps) == 0 {
		return 0, errors.New("smaps_rollup is empty")
	}
	return int64((*maps)[0].Pss) * 1024, nil
}
