package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Machine is everything the local checks need, injected for tests.
type Machine struct {
	Firewall func(ctx context.Context) (tool string, open bool)
	Ports    []string // firewalld spelling, for the message
	Userns   func() (bool, string)
	Crun     func() bool
	Nix      func() bool
	// PCIRoot is /sys/bus/pci/devices; DriverDir is /proc/driver/nvidia.
	PCIRoot   string
	DriverDir string
	NvidiaSMI func() bool
	// DiskPath is where closures land; FreeBytes is statfs.
	DiskPath  string
	FreeBytes func(path string) (uint64, error)
}

const gib = 1 << 30

// Disk thresholds: a torch closure is ~6 GiB.
const (
	diskFail = 3 * gib
	diskWarn = 10 * gib
)

// Run performs the local checks.
func (m Machine) Run(ctx context.Context) []Check {
	var out []Check

	tool, open := m.Firewall(ctx)
	ports := strings.Join(m.Ports, ", ")
	switch {
	case tool == "":
		out = append(out, Check{Level: OK, What: "no host firewall running"})
	case open:
		out = append(out, Check{Level: OK, What: tool + " allows " + ports})
	default:
		out = append(out, Check{Fail, tool + " blocks " + ports + ": peers cannot reach this machine", "pipedpeer setup -y"})
	}

	if ok, why := m.Userns(); ok {
		out = append(out, Check{Level: OK, What: "user namespaces allowed"})
	} else {
		out = append(out, Check{Fail, "user namespaces refused, jobs cannot run in a sandbox here", why})
	}

	if m.Crun() {
		out = append(out, Check{Level: OK, What: "crun installed"})
	} else {
		out = append(out, Check{Fail, "crun missing (the job sandbox)", "pipedpeer setup -y"})
	}
	if m.Nix() {
		out = append(out, Check{Level: OK, What: "nix usable"})
	} else {
		out = append(out, Check{Fail, "nix missing or has no store (environments cannot be built)", "pipedpeer setup -y"})
	}

	out = append(out, m.nvidia()...)
	return append(out, m.disk())
}

// nvidia flags an NVIDIA display device whose driver is not loaded, the
// "GPU machine that runs everything on CPU" case. Read from sysfs, which
// wakes nothing.
func (m Machine) nvidia() []Check {
	devs, err := filepath.Glob(filepath.Join(m.PCIRoot, "*"))
	if err != nil {
		return []Check{{Warn, "could not list PCI devices: " + err.Error(), ""}}
	}
	found := false
	for _, d := range devs {
		vendor, _ := os.ReadFile(filepath.Join(d, "vendor"))
		class, _ := os.ReadFile(filepath.Join(d, "class"))
		if strings.TrimSpace(string(vendor)) == "0x10de" && strings.HasPrefix(strings.TrimSpace(string(class)), "0x03") {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	_, derr := os.Stat(m.DriverDir)
	switch {
	case derr != nil:
		return []Check{{Fail, "NVIDIA GPU present but its driver is not loaded (jobs will use the CPU)",
			"install the NVIDIA driver (Fedora: `sudo dnf install akmod-nvidia`; Ubuntu: `sudo ubuntu-drivers install`), then reboot"}}
	case !m.NvidiaSMI():
		return []Check{{Fail, "NVIDIA driver loaded but nvidia-smi is missing (the GPU is not detected)",
			"install the driver's utilities (Fedora: `sudo dnf install xorg-x11-drv-nvidia-cuda`; Ubuntu: `sudo apt install nvidia-utils-<version>`)"}}
	}
	return []Check{{Level: OK, What: "NVIDIA GPU and driver present"}}
}

func (m Machine) disk() Check {
	free, err := m.FreeBytes(m.DiskPath)
	if err != nil {
		return Check{Warn, "could not read free disk space at " + m.DiskPath + ": " + err.Error(), ""}
	}
	what := fmt.Sprintf("%.1f GiB free at %s", float64(free)/gib, m.DiskPath)
	fix := "free disk space; `nix-collect-garbage` removes environments no job uses"
	switch {
	case free < diskFail:
		return Check{Fail, what + " (a GPU environment needs ~6 GiB)", fix}
	case free < diskWarn:
		return Check{Warn, what + " (a GPU environment needs ~6 GiB)", fix}
	}
	return Check{Level: OK, What: what}
}

// Statfs is the real FreeBytes.
func Statfs(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}
