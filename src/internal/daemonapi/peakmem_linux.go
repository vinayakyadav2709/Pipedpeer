//go:build linux

package daemonapi

import (
	"errors"

	"github.com/shirou/gopsutil/v4/process"
)

// processPSS is the process's proportional set size in bytes, from
// /proc/<pid>/smaps_rollup: forked workers' shared pages count once.
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
