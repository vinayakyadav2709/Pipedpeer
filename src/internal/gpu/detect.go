package gpu

import (
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
)

type Vendor string

const (
	VendorNVIDIA Vendor = "nvidia"
	VendorAMD    Vendor = "amd"
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
	// GPUs lists every PCI display device found in sysfs, usable or not, each
	// with the reason. Empty when sysfs showed none (the vendor-tool fallback).
	GPUs []Device `json:"gpus,omitempty"`
	// Best is the runtime index (nvidia-smi / ROCR order) of the device a job
	// should use, -1 when there is none. Name, MemoryBytes and ComputeCap
	// describe this device.
	Best int `json:"best"`
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
	gpuCache = &cachedGPU{time: time.Now(), info: Info{Vendor: VendorNone, Best: -1}}

	if pci := enumeratePCI(); len(pci) > 0 {
		gpuCache.info, gpuCache.usage, gpuCache.devs = fromSysfs(pci)
		return
	}

	// No display device in sysfs (WSL, odd containers): ask the vendor tools.
	if info, usage, devs := refreshNVIDIA(); info.Vendor != VendorNone {
		gpuCache.info, gpuCache.usage, gpuCache.devs = info, usage, devs
		return
	}
	if info, usage, devs := refreshAMD(); info.Vendor != VendorNone {
		gpuCache.info, gpuCache.usage, gpuCache.devs = info, usage, devs
	}
}

// nvidiaSMI is the tool name; tests point it at a fake.
var nvidiaSMI = "nvidia-smi"

// nvRow is one line of the single nvidia-smi query.
type nvRow struct {
	index      int
	busID      string // normalised to sysfs form, "0000:01:00.0"
	name       string
	total      int64
	used       int64
	free       int64
	util       float64
	computeCap string
}

// queryNVIDIA runs one nvidia-smi call for everything we need. compute_cap is
// unknown to drivers older than ~510, which fail the whole query; retry
// without it.
func queryNVIDIA() ([]nvRow, error) {
	fields := "index,pci.bus_id,name,memory.total,memory.used,memory.free,utilization.gpu"
	out, err := termuxexec.Command(nvidiaSMI, "--query-gpu="+fields+",compute_cap", "--format=csv,noheader,nounits").Output()
	if err != nil {
		out, err = termuxexec.Command(nvidiaSMI, "--query-gpu="+fields, "--format=csv,noheader,nounits").Output()
		if err != nil {
			return nil, err
		}
	}
	var rows []nvRow
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		p := strings.Split(line, ", ")
		if len(p) < 7 {
			continue
		}
		r := nvRow{name: p[2]}
		r.index, _ = strconv.Atoi(p[0])
		r.busID = normaliseBusID(p[1])
		r.total = parseInt64(p[3]) * 1024 * 1024
		r.used = parseInt64(p[4]) * 1024 * 1024
		r.free = parseInt64(p[5]) * 1024 * 1024
		r.util = parseFloat(p[6])
		if len(p) >= 8 {
			r.computeCap = strings.TrimSpace(p[7])
		}
		rows = append(rows, r)
	}
	return rows, nil
}

// normaliseBusID turns nvidia-smi's "00000000:01:00.0" into sysfs's
// "0000:01:00.0".
func normaliseBusID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) > 12 {
		s = s[len(s)-12:]
	}
	return s
}

func nvidiaDeviceNodes() []DeviceNode {
	var nodes []DeviceNode
	for _, name := range []string{"nvidia0", "nvidiactl", "nvidia-modeset", "nvidia-uvm", "nvidia-uvm-tools"} {
		if node := statDevice(filepath.Join(devRoot, name)); node != nil {
			nodes = append(nodes, *node)
		}
	}
	return nodes
}

// refreshNVIDIA is the fallback used only when sysfs shows no display device.
func refreshNVIDIA() (Info, Usage, []DeviceUsage) {
	if _, err := exec.LookPath(nvidiaSMI); err != nil {
		return Info{Vendor: VendorNone, Best: -1}, Usage{}, nil
	}
	rows, err := queryNVIDIA()
	if err != nil || len(rows) == 0 {
		return Info{Vendor: VendorNone, Best: -1}, Usage{}, nil
	}
	info := Info{
		Vendor: VendorNVIDIA, Devices: nvidiaDeviceNodes(), Count: len(rows), Best: rows[0].index,
		Name: rows[0].name, MemoryBytes: rows[0].total, ComputeCap: rows[0].computeCap,
	}
	usage := Usage{MemoryUsedBytes: rows[0].used, MemoryFreeBytes: rows[0].free, UtilizationGPU: rows[0].util}
	var devs []DeviceUsage
	for _, r := range rows {
		devs = append(devs, DeviceUsage{Index: r.index, Name: r.name, MemoryTotalBytes: r.total,
			MemoryUsedBytes: r.used, MemoryFreeBytes: r.free, UtilizationGPU: r.util})
	}
	return info, usage, devs
}

// refreshAMD detects AMD GPUs via rocm-smi and reads live VRAM/utilisation.
// Fallback only, as refreshNVIDIA.
//
// Scheduling on AMD is best-effort: rocm-smi output is not as stable across
// versions as nvidia-smi's CSV, so a parse failure degrades to detection-only
// (the node still advertises a GPU, just without live usage numbers).
func refreshAMD() (Info, Usage, []DeviceUsage) {
	info := detectAMD()
	if info.Vendor != VendorAMD {
		return Info{Vendor: VendorNone, Best: -1}, Usage{}, nil
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
	out, err := termuxexec.Command("rocm-smi",
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

// firstField is s's first whitespace-separated word, "" when there is none.
func firstField(s string) string {
	if f := strings.Fields(s); len(f) > 0 {
		return f[0]
	}
	return ""
}

func parseInt64(s string) int64 {
	v, _ := strconv.ParseInt(firstField(s), 10, 64)
	return v
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSuffix(firstField(s), "%"), 64)
	return v
}

// ComputeCapability returns the compute capability of this node's GPU
// ("8.9" for NVIDIA, "gfx1100" for AMD), or "" when there is no GPU.
func ComputeCapability() string {
	return Detect().ComputeCap
}

func getCache(fresh bool) *cachedGPU {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	if fresh || gpuCache == nil || time.Since(gpuCache.time) > cacheTTL {
		refreshCache()
	}
	return gpuCache
}

// Detect returns GPU hardware info. Results are cached for 5 seconds.
func Detect() Info {
	return getCache(false).info
}

// DetectFresh bypasses the 5 s cache, for job admission: a dGPU switched off
// or an eGPU unplugged since the last look must not be leased. It still never
// wakes a suspended GPU.
func DetectFresh() Info {
	return getCache(true).info
}

func DetectUsage() Usage {
	return getCache(false).usage
}

func PerDevice() []DeviceUsage {
	return getCache(false).devs
}

func detectAMD() Info {
	if _, err := exec.LookPath("rocm-smi"); err != nil {
		return Info{Vendor: VendorNone, Best: -1}
	}

	out, err := termuxexec.Command("rocm-smi", "--showproductname", "--showmeminfo", "vram").Output()
	if err != nil {
		return Info{Vendor: VendorNone, Best: -1}
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

	return Info{
		Vendor:      VendorAMD,
		Devices:     amdDeviceNodes(),
		Name:        name,
		MemoryBytes: memBytes,
		ComputeCap:  amdGFXTarget(),
		Best:        0,
	}
}

func amdDeviceNodes() []DeviceNode {
	devices := scanDevices("dri", []string{"card0", "card1"}, []int{128})
	// AMD also needs /dev/kfd
	if node := statDevice(filepath.Join(devRoot, "kfd")); node != nil {
		devices = append(devices, *node)
	}
	return devices
}

// amdGFXTarget returns the ISA target ("gfx1100") reported by rocminfo.
func amdGFXTarget() string {
	if _, err := exec.LookPath("rocminfo"); err != nil {
		return ""
	}
	out, err := termuxexec.Command("rocminfo").Output()
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

func scanDevices(subdir string, exactNames []string, renderMinors []int) []DeviceNode {
	dir := filepath.Join(devRoot, subdir)
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

// logf is the package's warning sink; detection runs in the daemon and the CLI.
func logf(format string, args ...any) { log.Printf("gpu: "+format, args...) }
