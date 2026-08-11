package gpu

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Vendor string

const (
	VendorNVIDIA Vendor = "nvidia"
	VendorAMD    Vendor = "amd"
	VendorIntel  Vendor = "intel"
	VendorNone   Vendor = "none"
)

type DeviceNode struct {
	Path  string `json:"path"`
	Major int64  `json:"major"`
	Minor int64  `json:"minor"`
}

type Info struct {
	Vendor      Vendor       `json:"vendor"`
	Devices     []DeviceNode `json:"devices"`
	Name        string       `json:"name,omitempty"`
	MemoryBytes int64        `json:"memory_bytes,omitempty"`
	Count       int          `json:"count,omitempty"`
	// ComputeCap is the CUDA compute capability ("8.9") for NVIDIA, or the
	// gfx target ("gfx1100") for AMD. Empty when unknown.
	ComputeCap string `json:"compute_cap,omitempty"`
}

type Usage struct {
	MemoryUsedBytes int64   `json:"memory_used_bytes"`
	MemoryFreeBytes int64   `json:"memory_free_bytes"`
	UtilizationGPU  float64 `json:"utilization_gpu_percent,omitempty"`
	UtilizationMem  float64 `json:"utilization_memory_percent,omitempty"`
}

type DeviceUsage struct {
	Index            int     `json:"index"`
	Name             string  `json:"name"`
	MemoryTotalBytes int64   `json:"memory_total_bytes"`
	MemoryUsedBytes  int64   `json:"memory_used_bytes"`
	MemoryFreeBytes  int64   `json:"memory_free_bytes"`
	UtilizationGPU   float64 `json:"utilization_gpu_percent"`
}

type cachedGPU struct {
	info  Info
	usage Usage
	devs  []DeviceUsage
	time  time.Time
}

var (
	gpuCache *cachedGPU
	cacheMu  sync.Mutex
)

const cacheTTL = 5 * time.Second

func refreshCache() {
	// Vendor must default to VendorNone, not the zero string: callers gate GPU
	// admission on `Vendor != VendorNone`, so a zero value would make a
	// CPU-only node advertise a GPU it does not have.
	gpuCache = &cachedGPU{time: time.Now(), info: Info{Vendor: VendorNone}}

	// Try each vendor in order of likelihood. First one found provides all stats.
	if info, usage, devs := refreshNVIDIA(); info.Vendor != VendorNone {
		gpuCache.info = info
		gpuCache.usage = usage
		gpuCache.devs = devs
		return
	}
	if info, usage, devs := refreshAMD(); info.Vendor != VendorNone {
		gpuCache.info = info
		gpuCache.usage = usage
		gpuCache.devs = devs
		return
	}
	if info, usage, devs := refreshIntel(); info.Vendor != VendorNone {
		gpuCache.info = info
		gpuCache.usage = usage
		gpuCache.devs = devs
		return
	}
}

func refreshNVIDIA() (Info, Usage, []DeviceUsage) {
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		return Info{Vendor: VendorNone}, Usage{}, nil
	}

	var info Info
	var usage Usage
	info.Vendor = VendorNVIDIA

	// Query GPU info
	if out, err := exec.Command("nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader").Output(); err == nil {
		parts := strings.SplitN(strings.TrimSpace(string(out)), ", ", 2)
		if len(parts) >= 2 {
			info.Name = parts[0]
			memStr := strings.Fields(parts[1])[0]
			memBytes, _ := strconv.ParseInt(memStr, 10, 64)
			info.MemoryBytes = memBytes * 1024 * 1024
		}
	}

	// GPU count
	if countOut, err := exec.Command("nvidia-smi", "--query-gpu=count", "--format=csv,noheader").Output(); err == nil {
		if c, err := strconv.Atoi(strings.TrimSpace(string(countOut))); err == nil && c > 0 {
			info.Count = c
		}
	}
	if info.Count == 0 {
		info.Count = 1
	}

	// Compute capability ("8.9"). Reported per device; the first line is enough
	// since mixed-architecture hosts are not something we schedule across.
	if ccOut, err := exec.Command("nvidia-smi", "--query-gpu=compute_cap", "--format=csv,noheader").Output(); err == nil {
		if line := strings.TrimSpace(string(ccOut)); line != "" {
			info.ComputeCap = strings.TrimSpace(strings.SplitN(line, "\n", 2)[0])
		}
	}

	// Device nodes
	nvidiaNames := []string{"nvidia0", "nvidiactl", "nvidia-modeset", "nvidia-uvm", "nvidia-uvm-tools"}
	for _, name := range nvidiaNames {
		if node := statDevice(filepath.Join("/dev", name)); node != nil {
			info.Devices = append(info.Devices, *node)
		}
	}

	// Usage and per-device stats (single nvidia-smi call)
	if out, err := exec.Command("nvidia-smi",
		"--query-gpu=index,name,memory.total,memory.used,memory.free,utilization.gpu",
		"--format=csv,noheader,nounits").Output(); err == nil {
		var devs []DeviceUsage
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			parts := strings.Split(line, ", ")
			if len(parts) < 6 {
				continue
			}
			idx, _ := strconv.Atoi(parts[0])
			total, _ := strconv.ParseInt(parts[2], 10, 64)
			used, _ := strconv.ParseInt(parts[3], 10, 64)
			free, _ := strconv.ParseInt(parts[4], 10, 64)
			utilGPU, _ := strconv.ParseFloat(parts[5], 64)
			devs = append(devs, DeviceUsage{
				Index:            idx,
				Name:             parts[1],
				MemoryTotalBytes: total * 1024 * 1024,
				MemoryUsedBytes:  used * 1024 * 1024,
				MemoryFreeBytes:  free * 1024 * 1024,
				UtilizationGPU:   utilGPU,
			})
			// Aggregate usage from first GPU
			if idx == 0 {
				usage = Usage{
					MemoryUsedBytes: used * 1024 * 1024,
					MemoryFreeBytes: free * 1024 * 1024,
					UtilizationGPU:  utilGPU,
				}
			}
		}
		return info, usage, devs
	}
	return Info{Vendor: VendorNone}, Usage{}, nil
}

// refreshAMD detects AMD GPUs via rocm-smi and reads live VRAM/utilisation.
//
// Scheduling on AMD is best-effort: rocm-smi output is not as stable across
// versions as nvidia-smi's CSV, so a parse failure degrades to detection-only
// (the node still advertises a GPU, just without live usage numbers).
func refreshAMD() (Info, Usage, []DeviceUsage) {
	info := detectAMD()
	if info.Vendor != VendorAMD {
		return Info{Vendor: VendorNone}, Usage{}, nil
	}

	devs := perDeviceAMD()
	if len(devs) > 0 {
		info.Count = len(devs)
		if info.Name == "" {
			info.Name = devs[0].Name
		}
		if info.MemoryBytes == 0 {
			info.MemoryBytes = devs[0].MemoryTotalBytes
		}
	}
	if info.Count == 0 {
		info.Count = 1
	}

	var usage Usage
	if len(devs) > 0 {
		usage = Usage{
			MemoryUsedBytes: devs[0].MemoryUsedBytes,
			MemoryFreeBytes: devs[0].MemoryFreeBytes,
			UtilizationGPU:  devs[0].UtilizationGPU,
		}
	}
	return info, usage, devs
}

// perDeviceAMD reads per-device VRAM and utilisation from rocm-smi's JSON
// output, which is the only rocm-smi format stable enough to parse.
func perDeviceAMD() []DeviceUsage {
	out, err := exec.Command("rocm-smi",
		"--showid", "--showproductname", "--showmeminfo", "vram", "--showuse", "--json").Output()
	if err != nil {
		return nil
	}

	// Keyed by card ("card0", "card1"); values vary by rocm-smi version, so
	// every field is looked up by suffix rather than an exact key.
	var raw map[string]map[string]string
	if json.Unmarshal(out, &raw) != nil {
		return nil
	}

	var devs []DeviceUsage
	for card, fields := range raw {
		idx, err := strconv.Atoi(strings.TrimPrefix(card, "card"))
		if err != nil {
			continue
		}
		d := DeviceUsage{Index: idx, Name: "AMD GPU"}
		for k, v := range fields {
			lk := strings.ToLower(k)
			switch {
			case strings.Contains(lk, "product name") || strings.Contains(lk, "card series"):
				if v != "" {
					d.Name = v
				}
			case strings.Contains(lk, "vram total memory"):
				d.MemoryTotalBytes = parseInt64(v)
			case strings.Contains(lk, "vram total used memory"):
				d.MemoryUsedBytes = parseInt64(v)
			case strings.Contains(lk, "gpu use"):
				d.UtilizationGPU = parseFloat(v)
			}
		}
		d.MemoryFreeBytes = d.MemoryTotalBytes - d.MemoryUsedBytes
		if d.MemoryFreeBytes < 0 {
			d.MemoryFreeBytes = 0
		}
		devs = append(devs, d)
	}
	sort.Slice(devs, func(i, j int) bool { return devs[i].Index < devs[j].Index })
	return devs
}

// refreshIntel detects Intel GPUs via lspci. No usage counters are reported:
// Intel exposes them through i915 perf/sysfs rather than a stable CLI, so the
// node advertises the device but is scheduled on CPU metrics alone.
func refreshIntel() (Info, Usage, []DeviceUsage) {
	info := detectIntel()
	if info.Vendor != VendorIntel {
		return Info{Vendor: VendorNone}, Usage{}, nil
	}
	if info.Count == 0 {
		info.Count = 1
	}
	return info, Usage{}, nil
}

func parseInt64(s string) int64 {
	v, _ := strconv.ParseInt(strings.TrimSpace(strings.Fields(s + " ")[0]), 10, 64)
	return v
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(strings.Fields(s + " ")[0], "%")), 64)
	return v
}

// ComputeCapability returns the compute capability of this node's GPU
// ("8.9" for NVIDIA, "gfx1100" for AMD), or "" when there is no GPU.
func ComputeCapability() string {
	return Detect().ComputeCap
}

func getCache() *cachedGPU {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	if gpuCache == nil || time.Since(gpuCache.time) > cacheTTL {
		refreshCache()
	}
	return gpuCache
}

// Detect returns GPU hardware info. Results are cached for 5 seconds.
func Detect() Info {
	return getCache().info
}

func DetectUsage() Usage {
	return getCache().usage
}

func PerDevice() []DeviceUsage {
	return getCache().devs
}

func detectAMD() Info {
	if _, err := exec.LookPath("rocm-smi"); err != nil {
		return Info{Vendor: VendorNone}
	}

	out, err := exec.Command("rocm-smi", "--showproductname", "--showmeminfo", "vram").Output()
	if err != nil {
		return Info{Vendor: VendorNone}
	}

	var name string
	var memBytes int64
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "Product Name") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				name = strings.TrimSpace(parts[1])
			}
		}
		if strings.Contains(line, "VRAM") && strings.Contains(line, "Bytes") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				memBytes, _ = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
			}
		}
	}

	devices := scanDevices("dri", []string{"card0", "card1"}, []int{128})
	// AMD also needs /dev/kfd
	if node := statDevice("/dev/kfd"); node != nil {
		devices = append(devices, *node)
	}
	return Info{
		Vendor:      VendorAMD,
		Devices:     devices,
		Name:        name,
		MemoryBytes: memBytes,
		ComputeCap:  amdGFXTarget(),
	}
}

// amdGFXTarget returns the ISA target ("gfx1100") reported by rocminfo.
func amdGFXTarget() string {
	if _, err := exec.LookPath("rocminfo"); err != nil {
		return ""
	}
	out, err := exec.Command("rocminfo").Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, "gfx") {
			continue
		}
		for _, f := range strings.Fields(line) {
			if strings.HasPrefix(f, "gfx") {
				return f
			}
		}
	}
	return ""
}

func detectIntel() Info {
	if _, err := exec.LookPath("lspci"); err != nil {
		return Info{Vendor: VendorNone}
	}
	out, err := exec.Command("lspci", "-nn").Output()
	if err != nil {
		return Info{Vendor: VendorNone}
	}
	found := false
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "VGA") && strings.Contains(line, "Intel") {
			found = true
			break
		}
		if strings.Contains(line, "3D") && strings.Contains(line, "Intel") {
			found = true
			break
		}
	}
	if !found {
		return Info{Vendor: VendorNone}
	}

	devices := scanDevices("dri", []string{"card0"}, []int{128})
	name := "Intel GPU"
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "Intel") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) == 3 {
				name = strings.TrimSpace(parts[2])
			}
		}
	}
	return Info{
		Vendor:  VendorIntel,
		Devices: devices,
		Name:    name,
	}
}

func scanDevices(subdir string, exactNames []string, renderMinors []int) []DeviceNode {
	dir := filepath.Join("/dev", subdir)
	var devices []DeviceNode

	for _, name := range exactNames {
		if node := statDevice(filepath.Join(dir, name)); node != nil {
			devices = append(devices, *node)
		}
	}

	for _, m := range renderMinors {
		name := "renderD" + strconv.Itoa(m)
		if node := statDevice(filepath.Join(dir, name)); node != nil {
			devices = append(devices, *node)
		}
	}

	return devices
}

func statDevice(path string) *DeviceNode {
	fi, err := os.Stat(path)
	if err != nil {
		return nil
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	major := int64((stat.Rdev >> 8) & 0xfff)
	minor := int64((stat.Rdev & 0xff) | ((stat.Rdev >> 12) & 0xfff00))
	return &DeviceNode{
		Path:  path,
		Major: major,
		Minor: minor,
	}
}
