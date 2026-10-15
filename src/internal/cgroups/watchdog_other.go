//go:build !linux

package cgroups

import (
	"errors"

	"github.com/shirou/gopsutil/v4/process"
)

// processPSS has no source off Linux; residentShare falls back to RSS.
func processPSS(*process.Process) (int64, error) {
	return 0, errors.New("PSS is read from /proc, which only Linux has")
}
