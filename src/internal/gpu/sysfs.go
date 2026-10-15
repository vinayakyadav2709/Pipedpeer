package gpu

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Roots and the cache path are variables so tests can point them at a fake tree.
var (
	sysRoot         = "/sys"
	devRoot         = "/dev"
	procRoot        = "/proc"
	staticCachePath = func() string { return filepath.Join(userdir.Data(), "gpu.json") }
)

// Device is one PCI display device (class 0x03xxxx) as sysfs shows it.
type Device struct {
	Address    string `json:"pci_address"` // "0000:01:00.0"
	VendorID   string `json:"vendor_id"`   // "0x10de"
	DeviceID   string `json:"device_id"`
	Driver     string `json:"driver,omitempty"`
	BootVGA    bool   `json:"boot_vga,omitempty"`
	Integrated bool   `json:"integrated,omitempty"`
	// Asleep: power/runtime_status is "suspended". Nothing that would wake it
	// (nvidia-smi, rocm-smi, rocminfo, amdgpu's mem_info files) runs; the
	// static fields come from gpu.json.
	Asleep bool `json:"asleep,omitempty"`
	// Stack is "cuda" or "rocm" when a closure can drive the device, "" when
	// it is not a compute GPU for us. Reason says why, either way.
	Stack  string `json:"stack,omitempty"`
	Reason string `json:"reason"`
	// Index is the runtime index (nvidia-smi / ROCR_VISIBLE_DEVICES order:
	// PCI address order within the stack), -1 when not usable.
	Index       int     `json:"index"`
	Name        string  `json:"name,omitempty"`
	MemoryBytes int64   `json:"memory_bytes,omitempty"`
	ComputeCap  string  `json:"compute_cap,omitempty"`
	Score       float64 `json:"score"`

	used, free int64
	util       float64
	live       bool // used/free/util were read this refresh
}

// static is what gpu.json keeps per device: what an awake query told us.
type static struct {
	Name        string `json:"name,omitempty"`
	MemoryBytes int64  `json:"memory_bytes,omitempty"`
	ComputeCap  string `json:"compute_cap,omitempty"`
}

func staticKey(d Device) string { return d.Address + "|" + d.Driver }

func readTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// enumeratePCI lists display devices from sysfs in PCI address order. Reading
// class, vendor, driver, boot_vga and runtime_status never wakes a device.
func enumeratePCI() []Device {
	dir := filepath.Join(sysRoot, "bus/pci/devices")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logf("cannot list %s, falling back to vendor tools: %v", dir, err)
		}
		return nil
	}
	var out []Device
	counters := map[string]int{}
	for _, e := range entries { // ReadDir sorts by name = PCI address
		p := filepath.Join(dir, e.Name())
		if !strings.HasPrefix(readTrim(filepath.Join(p, "class")), "0x03") {
			continue
		}
		d := Device{
			Address:  e.Name(),
			VendorID: readTrim(filepath.Join(p, "vendor")),
			DeviceID: readTrim(filepath.Join(p, "device")),
			BootVGA:  readTrim(filepath.Join(p, "boot_vga")) == "1",
			Asleep:   readTrim(filepath.Join(p, "power/runtime_status")) == "suspended",
			Index:    -1,
		}
		if link, err := os.Readlink(filepath.Join(p, "driver")); err == nil {
			d.Driver = filepath.Base(link)
		}
		gate(&d, p, counters["nvidia-bound"])
		if d.Driver == "nvidia" {
			counters["nvidia-bound"]++
		}
		if d.Stack != "" {
			d.Index = counters[d.Stack]
			counters[d.Stack]++
		}
		out = append(out, d)
	}
	return out
}

// gate decides whether a closure can drive the device (6fbd06e's rule
// extended: count only what an environment we build can use).
// Each device needs its own node: a container sees the host's sysfs (driver
// bound) without the host's /dev, and must not claim the GPU. /dev/nvidia<N>
// follows the driver's probe order, which is PCI order among nvidia-bound
// devices (nvSeen of them come before this one).
func gate(d *Device, sysDir string, nvSeen int) {
	switch d.VendorID {
	case "0x8086":
		d.Integrated = true
		d.Reason = "Intel graphics: no environment we build can use it"
	case "0x10de":
		switch {
		case d.Driver != "nvidia":
			d.Reason = "NVIDIA GPU without the nvidia driver (driver: " + orNone(d.Driver) + ")"
		case !exists(filepath.Join(devRoot, "nvidiactl")):
			d.Reason = "nvidia driver bound but " + filepath.Join(devRoot, "nvidiactl") + " is missing"
		case !exists(filepath.Join(devRoot, "nvidia"+strconv.Itoa(nvSeen))):
			d.Reason = "nvidia driver bound but " + filepath.Join(devRoot, "nvidia"+strconv.Itoa(nvSeen)) + " is missing (container without the GPU?)"
		default:
			d.Stack = "cuda"
			d.Reason = "nvidia driver and device nodes present"
		}
	case "0x1002":
		switch {
		case d.Driver != "amdgpu":
			d.Reason = "AMD GPU without the amdgpu driver (driver: " + orNone(d.Driver) + ")"
		case !exists(filepath.Join(devRoot, "kfd")):
			d.Reason = "amdgpu bound but " + filepath.Join(devRoot, "kfd") + " is missing (no ROCm)"
		case renderNode(sysDir) == "":
			d.Reason = "amdgpu bound but its /dev/dri/renderD* node is missing"
		default:
			d.Stack = "rocm"
			d.Reason = "amdgpu driver and /dev/kfd present"
		}
	default:
		d.Reason = "display device from vendor " + orNone(d.VendorID) + ": not a compute GPU"
	}
}

// renderNode is the device's /dev/dri/renderD* path, "" when absent. sysfs
// names it under drm/; without that dir any render node will do.
func renderNode(sysDir string) string {
	names, _ := filepath.Glob(filepath.Join(sysDir, "drm", "renderD*"))
	if len(names) == 0 {
		names, _ = filepath.Glob(filepath.Join(devRoot, "dri", "renderD*"))
		if len(names) > 0 {
			return names[0]
		}
		return ""
	}
	for _, n := range names {
		if p := filepath.Join(devRoot, "dri", filepath.Base(n)); exists(p) {
			return p
		}
	}
	return ""
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// fromSysfs fills static and live data, ranks the usable devices and builds
// the Info the rest of the code reads.
func fromSysfs(pci []Device) (Info, Usage, []DeviceUsage) {
	info := Info{Vendor: VendorNone, Best: -1, GPUs: pci}
	cache := loadStatic()
	changed := false
	for i := range pci {
		if s, ok := cache[staticKey(pci[i])]; ok {
			pci[i].Name, pci[i].MemoryBytes, pci[i].ComputeCap = s.Name, s.MemoryBytes, s.ComputeCap
		}
	}
	// Vendor tools touch every device of their stack, so they run only when
	// none of that stack's devices is asleep.
	// ponytail: one sleeping card blocks live numbers for its awake siblings;
	// query by PCI address (nvidia-smi -i) if multi-dGPU hosts matter.
	if usable, asleep := stackState(pci, "cuda"); usable && !asleep {
		changed = liveNVIDIA(pci) || changed
	}
	if usable, asleep := stackState(pci, "rocm"); usable {
		changed = liveAMD(pci, !asleep) || changed
	}
	for i := range pci {
		d := &pci[i]
		if d.Stack == "cuda" && d.Name == "" {
			d.Name = procNVIDIAModel(d.Address) // driver-held text, no wake
		}
		if d.Stack == "rocm" {
			classifyAMD(d)
		}
		// ponytail: static score (VRAM GiB) stands in for the per-device speed
		// test; replace with the in-closure matmul calibration (R11 §3 step 4).
		d.Score = float64(d.MemoryBytes) / (1 << 30)
		if d.live {
			cache[staticKey(*d)] = static{d.Name, d.MemoryBytes, d.ComputeCap}
		}
	}
	if changed {
		saveStatic(cache)
	}

	best := -1
	for i, d := range pci {
		if d.Stack != "" && (best < 0 || better(d, pci[best])) {
			best = i
		}
	}
	// An iGPU alone does not make a GPU node (6fbd06e's rule for Intel): it
	// shares the CPU's memory and bandwidth.
	if best < 0 || pci[best].Integrated {
		return info, Usage{}, nil
	}
	b := pci[best]
	info.Best, info.Name, info.MemoryBytes, info.ComputeCap = b.Index, b.Name, b.MemoryBytes, b.ComputeCap
	if b.Stack == "cuda" {
		info.Vendor, info.Devices = VendorNVIDIA, nvidiaDeviceNodes()
	} else {
		info.Vendor, info.Devices = VendorAMD, amdDeviceNodes()
	}
	var devs []DeviceUsage
	for _, d := range pci {
		if d.Stack != b.Stack {
			continue
		}
		info.Count++
		du := DeviceUsage{Index: d.Index, Name: d.Name, MemoryTotalBytes: d.MemoryBytes, MemoryFreeBytes: d.MemoryBytes}
		if d.live {
			du.MemoryUsedBytes, du.MemoryFreeBytes, du.UtilizationGPU = d.used, d.free, d.util
		}
		devs = append(devs, du)
	}
	usage := Usage{MemoryFreeBytes: b.MemoryBytes}
	if b.live {
		usage = Usage{MemoryUsedBytes: b.used, MemoryFreeBytes: b.free, UtilizationGPU: b.util}
	}
	return info, usage, devs
}

// classifyAMD marks an APU's iGPU as integrated. KFD topology is the signal:
// an APU's node has both CPU cores and SIMDs. Without a KFD node for the
// device it falls back to VRAM under 2 GiB (strict: a real 2 GB dGPU reads
// exactly 2 GiB). A suspended device with nothing cached has no VRAM and stays
// discrete: a boot display never runtime-suspends.
func classifyAMD(d *Device) {
	apu, known := kfdAPU(d.Address)
	if !known && d.MemoryBytes > 0 {
		apu = d.MemoryBytes < 2<<30
		logf("no KFD topology node for %s, deciding APU by VRAM (%d bytes): integrated=%v", d.Address, d.MemoryBytes, apu)
	}
	if apu {
		d.Integrated = true
		d.Reason = "AMD APU integrated graphics: shares system memory, not a GPU node on its own"
	}
}

// kfdAPU reports whether the KFD topology node of the PCI device at addr
// ("0000:c1:00.0") is an APU: cpu_cores_count > 0 and simd_count > 0 on the
// same node. known is false when no node matches.
func kfdAPU(addr string) (apu, known bool) {
	files, _ := filepath.Glob(filepath.Join(sysRoot, "class/kfd/kfd/topology/nodes/*/properties"))
	for _, f := range files {
		props := map[string]int64{}
		for _, line := range strings.Split(readTrim(f), "\n") {
			if k, v, ok := strings.Cut(line, " "); ok {
				props[k] = parseInt64(v)
			}
		}
		if props["simd_count"] == 0 {
			continue // CPU-only node
		}
		loc := props["location_id"] // bus<<8 | device<<3 | function
		if fmt.Sprintf("%04x:%02x:%02x.%x", props["domain"], loc>>8, (loc>>3)&0x1f, loc&7) == addr {
			return props["cpu_cores_count"] > 0, true
		}
	}
	return false, false
}

// better ranks a over b: discrete before integrated, then score, then PCI order.
func better(a, b Device) bool {
	if a.Integrated != b.Integrated {
		return !a.Integrated
	}
	return a.Score > b.Score
}

func stackState(pci []Device, stack string) (usable, asleep bool) {
	for _, d := range pci {
		if d.Stack == stack {
			usable = true
			asleep = asleep || d.Asleep
		}
	}
	return
}

// liveNVIDIA runs the single nvidia-smi query and maps rows by PCI address.
func liveNVIDIA(pci []Device) (changed bool) {
	rows, err := queryNVIDIA()
	if err != nil {
		logf("%s failed with every NVIDIA GPU awake, counting none of them: %v", nvidiaSMI, err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		for i := range pci {
			d := &pci[i]
			if d.Stack != "cuda" || d.Address != r.busID {
				continue
			}
			changed = changed || d.Name != r.name || d.MemoryBytes != r.total || d.ComputeCap != r.computeCap
			d.Name, d.MemoryBytes, d.ComputeCap, d.Index = r.name, r.total, r.computeCap, r.index
			d.used, d.free, d.util, d.live = r.used, r.free, r.util, true
			seen[d.Address] = true
		}
	}
	// Awake and node present but nvidia-smi does not show it: not usable.
	// Cached info is only for a device that is asleep.
	for i := range pci {
		d := &pci[i]
		if d.Stack == "cuda" && !seen[d.Address] {
			d.Stack, d.Index = "", -1
			if err != nil {
				d.Reason = nvidiaSMI + " failed: " + err.Error()
			} else {
				d.Reason = nvidiaSMI + " does not list this device"
			}
		}
	}
	return changed
}

// liveAMD reads amdgpu's own sysfs files of each awake device (they wake the
// device they belong to, so asleep ones are skipped) and, when every device is
// awake, rocminfo for the gfx target (it opens them all).
func liveAMD(pci []Device, allAwake bool) (changed bool) {
	gfx := ""
	if allAwake {
		gfx = amdGFXTarget()
	}
	for i := range pci {
		d := &pci[i]
		if d.Stack != "rocm" || d.Asleep {
			continue
		}
		if !allAwake {
			gfx = d.ComputeCap
		}
		p := filepath.Join(sysRoot, "bus/pci/devices", d.Address)
		name := readTrim(filepath.Join(p, "product_name"))
		if name == "" {
			name = "AMD GPU " + d.DeviceID
		}
		total := parseInt64(readTrim(filepath.Join(p, "mem_info_vram_total")))
		changed = changed || d.Name != name || d.MemoryBytes != total || d.ComputeCap != gfx
		d.Name, d.MemoryBytes, d.ComputeCap = name, total, gfx
		d.used = parseInt64(readTrim(filepath.Join(p, "mem_info_vram_used")))
		d.free = max(total-d.used, 0)
		d.util = parseFloat(readTrim(filepath.Join(p, "gpu_busy_percent")))
		d.live = true
	}
	return changed
}

// procNVIDIAModel reads the "Model:" line the nvidia driver keeps per GPU.
func procNVIDIAModel(addr string) string {
	f, err := os.Open(filepath.Join(procRoot, "driver/nvidia/gpus", addr, "information"))
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == "Model" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func loadStatic() map[string]static {
	m := map[string]static{}
	b, err := os.ReadFile(staticCachePath())
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logf("cannot read GPU cache, starting empty: %v", err)
		}
		return m
	}
	if err := json.Unmarshal(b, &m); err != nil {
		logf("GPU cache %s is damaged, starting empty: %v", staticCachePath(), err)
		return map[string]static{}
	}
	return m
}

func saveStatic(m map[string]static) {
	path := staticCachePath()
	b, _ := json.MarshalIndent(m, "", "  ")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		logf("cannot save GPU cache: %v", err)
		return
	}
	tmp := path + ".tmp." + strconv.Itoa(os.Getpid())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		logf("cannot save GPU cache: %v", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		logf("cannot save GPU cache: %v", err)
	}
}
