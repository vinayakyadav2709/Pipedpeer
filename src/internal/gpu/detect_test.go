package gpu

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDetect(t *testing.T) {
	info := Detect()
	t.Logf("Detected GPU: vendor=%s name=%s devices=%d mem=%d",
		info.Vendor, info.Name, len(info.Devices), info.MemoryBytes)

	// Test should at least not crash
	if info.Vendor == VendorNone {
		t.Log("No GPU detected — this is fine on non-GPU machines")
	} else {
		if len(info.Devices) == 0 {
			t.Errorf("GPU detected (%s) but no device nodes found", info.Vendor)
		}
		for _, d := range info.Devices {
			t.Logf("  device: %s (major=%d, minor=%d)", d.Path, d.Major, d.Minor)
			if d.Path == "" {
				t.Error("device path is empty")
			}
			if d.Major <= 0 {
				t.Errorf("invalid major number for %s: %d", d.Path, d.Major)
			}
		}
	}
}

func TestDetectNVIDIA(t *testing.T) {
	info, _, devs := refreshNVIDIA()
	if info.Vendor != VendorNVIDIA {
		t.Skip("no NVIDIA GPU on this machine")
	}
	t.Logf("NVIDIA GPU: %s, memory: %d bytes, compute_cap=%s", info.Name, info.MemoryBytes, info.ComputeCap)
	if info.MemoryBytes <= 0 {
		t.Errorf("expected positive memory, got %d", info.MemoryBytes)
	}
	for _, d := range devs {
		if d.MemoryTotalBytes <= 0 {
			t.Errorf("device %d reports no total VRAM", d.Index)
		}
		if d.MemoryFreeBytes > d.MemoryTotalBytes {
			t.Errorf("device %d free VRAM %d exceeds total %d", d.Index, d.MemoryFreeBytes, d.MemoryTotalBytes)
		}
	}
}

// refreshAMD must be safe to call on a machine without an AMD GPU — they shell out to tools that will not exist.
func TestRefreshOtherVendorsAreSafe(t *testing.T) {
	if info, _, _ := refreshAMD(); info.Vendor != VendorNone && info.Vendor != VendorAMD {
		t.Errorf("refreshAMD returned unexpected vendor %q", info.Vendor)
	}
}

func TestStatDevice(t *testing.T) {
	node := statDevice("/dev/null")
	if node == nil {
		t.Fatal("expected to stat /dev/null")
	}
	t.Logf("/dev/null: major=%d minor=%d", node.Major, node.Minor)
	if node.Major != 1 || node.Minor != 3 {
		t.Errorf("expected major=1 minor=3 for /dev/null, got major=%d minor=%d", node.Major, node.Minor)
	}
}

func TestScanDevices(t *testing.T) {
	// Test scanning /dev for existing patterns
	devices := scanDevices("", nil, nil)
	if len(devices) > 0 {
		t.Logf("Found %d devices in /dev", len(devices))
	}
}

// fakeHost points the package at a fake sysfs/dev/proc tree and a fake
// nvidia-smi that records each call in <root>/smi-called.
type fakeHost struct {
	t    *testing.T
	root string
}

func newFakeHost(t *testing.T) *fakeHost {
	root := t.TempDir()
	h := &fakeHost{t, root}
	oldSys, oldDev, oldProc, oldCache, oldSMI := sysRoot, devRoot, procRoot, staticCachePath, nvidiaSMI
	sysRoot, devRoot, procRoot = filepath.Join(root, "sys"), filepath.Join(root, "dev"), filepath.Join(root, "proc")
	staticCachePath = func() string { return filepath.Join(root, "data", "gpu.json") }
	smi := filepath.Join(root, "nvidia-smi")
	h.write("nvidia-smi", "#!/bin/sh\necho called >> "+filepath.Join(root, "smi-called")+
		"\necho '0, 00000000:01:00.0, NVIDIA GeForce RTX 3060 Laptop GPU, 6144, 100, 6044, 3, 8.6'\n")
	if err := os.Chmod(smi, 0o755); err != nil {
		t.Fatal(err)
	}
	nvidiaSMI = smi
	t.Cleanup(func() {
		sysRoot, devRoot, procRoot, staticCachePath, nvidiaSMI = oldSys, oldDev, oldProc, oldCache, oldSMI
	})
	return h
}

func (h *fakeHost) write(rel, content string) {
	p := filepath.Join(h.root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		h.t.Fatal(err)
	}
}

// pci adds a display device; files maps extra sysfs file names to contents.
func (h *fakeHost) pci(addr, vendor, driver, status string, files map[string]string) {
	d := filepath.Join("sys/bus/pci/devices", addr)
	h.write(filepath.Join(d, "class"), "0x030000")
	h.write(filepath.Join(d, "vendor"), vendor)
	h.write(filepath.Join(d, "device"), "0x1234")
	h.write(filepath.Join(d, "power/runtime_status"), status)
	for k, v := range files {
		h.write(filepath.Join(d, k), v)
	}
	if driver != "" {
		if err := os.Symlink("../../../bus/pci/drivers/"+driver, filepath.Join(h.root, d, "driver")); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *fakeHost) smiCalled() bool {
	_, err := os.Stat(filepath.Join(h.root, "smi-called"))
	return err == nil
}

func TestSuspendedDGPUIsNotPolled(t *testing.T) {
	h := newFakeHost(t)
	h.pci("0000:00:02.0", "0x8086", "i915", "active", map[string]string{"boot_vga": "1"})
	h.pci("0000:01:00.0", "0x10de", "nvidia", "suspended", nil)
	h.write("dev/nvidiactl", "")
	h.write("dev/nvidia0", "")
	h.write("proc/driver/nvidia/gpus/0000:01:00.0/information", "Model: \t\t NVIDIA GeForce RTX 3060 Laptop GPU\nIRQ: 1\n")

	info, _, devs := fromSysfs(enumeratePCI())
	if h.smiCalled() {
		t.Fatal("nvidia-smi ran while the dGPU was suspended")
	}
	if info.Vendor != VendorNVIDIA || info.Best != 0 || info.Count != 1 || len(devs) != 1 {
		t.Fatalf("asleep dGPU must still be advertised: %+v devs=%+v", info, devs)
	}
	if info.Name != "NVIDIA GeForce RTX 3060 Laptop GPU" || !info.GPUs[1].Asleep {
		t.Fatalf("name from /proc, asleep flag: %+v", info.GPUs[1])
	}

	// Awake: one call, static info cached by PCI address + driver.
	h.write("sys/bus/pci/devices/0000:01:00.0/power/runtime_status", "active")
	info, usage, _ := fromSysfs(enumeratePCI())
	if !h.smiCalled() || info.MemoryBytes != 6144<<20 || info.ComputeCap != "8.6" || usage.MemoryUsedBytes != 100<<20 {
		t.Fatalf("awake query: called=%v info=%+v usage=%+v", h.smiCalled(), info, usage)
	}
	if err := os.Remove(filepath.Join(h.root, "smi-called")); err != nil {
		t.Fatal(err)
	}

	// Asleep again: VRAM comes from gpu.json, nothing runs.
	h.write("sys/bus/pci/devices/0000:01:00.0/power/runtime_status", "suspended")
	info, _, _ = fromSysfs(enumeratePCI())
	if h.smiCalled() || info.MemoryBytes != 6144<<20 || info.ComputeCap != "8.6" {
		t.Fatalf("cached info while asleep: called=%v info=%+v", h.smiCalled(), info)
	}
}

func TestAMDAPUAndDGPUPicksDGPU(t *testing.T) {
	h := newFakeHost(t)
	h.pci("0000:03:00.0", "0x1002", "amdgpu", "active", map[string]string{
		"mem_info_vram_total": "17163091968", "mem_info_vram_used": "1000", "product_name": "Radeon RX 7800 XT"})
	h.pci("0000:c1:00.0", "0x1002", "amdgpu", "active", map[string]string{
		"boot_vga": "1", "mem_info_vram_total": "536870912", "mem_info_vram_used": "100"})
	h.write("dev/kfd", "")
	h.write("dev/dri/renderD128", "")

	info, _, devs := fromSysfs(enumeratePCI())
	if info.Vendor != VendorAMD || info.Best != 0 || info.Name != "Radeon RX 7800 XT" || info.Count != 2 || len(devs) != 2 {
		t.Fatalf("want the dGPU (index 0): %+v", info)
	}
	if !info.GPUs[1].Integrated || info.GPUs[0].Integrated {
		t.Fatalf("APU classification wrong: %+v", info.GPUs)
	}

	// Same with the dGPU later in PCI order and asleep with nothing cached:
	// a suspended device is never the boot display, so it still wins.
	h2 := newFakeHost(t)
	h2.pci("0000:05:00.0", "0x1002", "amdgpu", "active", map[string]string{"boot_vga": "1", "mem_info_vram_total": "536870912"})
	h2.pci("0000:06:00.0", "0x1002", "amdgpu", "suspended", nil)
	h2.write("dev/kfd", "")
	h2.write("dev/dri/renderD128", "")
	info, _, _ = fromSysfs(enumeratePCI())
	if info.Best != 1 {
		t.Fatalf("asleep dGPU should outrank the APU, got Best=%d %+v", info.Best, info.GPUs)
	}
}

func TestIntelNeverCounted(t *testing.T) {
	h := newFakeHost(t)
	h.pci("0000:00:02.0", "0x8086", "i915", "active", map[string]string{"boot_vga": "1"})
	h.pci("0000:00:03.0", "0x8086", "xe", "active", nil)
	info, _, devs := fromSysfs(enumeratePCI())
	if info.Vendor != VendorNone || info.Best != -1 || info.Count != 0 || devs != nil {
		t.Fatalf("Intel counted as a GPU: %+v", info)
	}
	if len(info.GPUs) != 2 || info.GPUs[0].Reason == "" || info.GPUs[0].Index != -1 {
		t.Fatalf("Intel devices should be listed with a reason: %+v", info.GPUs)
	}
}

func TestGateNeedsDriverAndNodes(t *testing.T) {
	h := newFakeHost(t)
	h.pci("0000:01:00.0", "0x10de", "nouveau", "active", nil)
	h.pci("0000:02:00.0", "0x10de", "nvidia", "active", nil) // no /dev/nvidiactl
	h.pci("0000:03:00.0", "0x1002", "amdgpu", "active", nil) // no /dev/kfd
	info, _, _ := fromSysfs(enumeratePCI())
	if info.Vendor != VendorNone || h.smiCalled() {
		t.Fatalf("unusable stacks must not count or poll: %+v", info)
	}
	for _, d := range info.GPUs {
		if d.Stack != "" || d.Reason == "" {
			t.Fatalf("device %s: stack=%q reason=%q", d.Address, d.Stack, d.Reason)
		}
	}
}

// A lab container sees the host's sysfs with the nvidia driver bound but has
// no /dev/nvidia0; and an awake device nvidia-smi cannot query is not usable.
func TestDriverBoundNoDevNode(t *testing.T) {
	h := newFakeHost(t)
	h.pci("0000:01:00.0", "0x10de", "nvidia", "active", nil)
	h.write("dev/nvidiactl", "")
	info, _, _ := fromSysfs(enumeratePCI())
	if info.Vendor != VendorNone || h.smiCalled() || info.GPUs[0].Reason == "" {
		t.Fatalf("no /dev/nvidia0 must mean no GPU: %+v", info)
	}

	h.write("dev/nvidia0", "")
	h.write("nvidia-smi", "#!/bin/sh\nexit 9\n")
	info, _, _ = fromSysfs(enumeratePCI())
	if info.Vendor != VendorNone || info.GPUs[0].Stack != "" || info.GPUs[0].Reason == "" {
		t.Fatalf("awake GPU nvidia-smi fails on must be unusable: %+v", info.GPUs)
	}
}

// kfdNode writes a KFD topology node; loc is the PCI bus<<8|dev<<3|fn.
// mem < 0 leaves local_mem_size out (older kernels).
func (h *fakeHost) kfdNode(n string, cpus, simds, loc int, mem int64) {
	p := fmt.Sprintf("cpu_cores_count %d\nsimd_count %d\nmem_banks_count 1\nlocation_id %d\ndomain 0\n", cpus, simds, loc)
	if mem >= 0 {
		p += fmt.Sprintf("local_mem_size %d\n", mem)
	}
	h.write("sys/class/kfd/kfd/topology/nodes/"+n+"/properties", p)
}

func TestAPUByKFDTopology(t *testing.T) {
	amd := func(h *fakeHost, addr string) {
		h.pci(addr, "0x1002", "amdgpu", "active", map[string]string{"mem_info_vram_total": "2147483648"})
		h.write("dev/kfd", "")
		h.write("dev/dri/renderD128", "")
	}
	// Laptop APU with a 2.0 GiB carve-out: KFD shows CPU cores and SIMDs on
	// one node, so it is integrated and the host is no GPU node.
	h := newFakeHost(t)
	amd(h, "0000:c1:00.0")
	h.kfdNode("0", 16, 8, 0xc100, -1)
	info, _, devs := fromSysfs(enumeratePCI())
	if !info.GPUs[0].Integrated || info.Vendor != VendorNone || info.Best != -1 || devs != nil {
		t.Fatalf("2 GiB APU counted as a GPU node: %+v", info)
	}

	// Split layout (Cezanne 0x1638 on kernel 7.2): CPUs on node 0, the iGPU
	// node has no CPU cores but local_mem_size 0.
	h = newFakeHost(t)
	amd(h, "0000:05:00.0")
	h.kfdNode("0", 16, 0, 0, 0)
	h.kfdNode("1", 0, 32, 0x500, 0)
	info, _, devs = fromSysfs(enumeratePCI())
	if !info.GPUs[0].Integrated || info.Vendor != VendorNone || info.Best != -1 || devs != nil {
		t.Fatalf("split-layout APU counted as a GPU node: %+v", info)
	}

	// A real 2 GiB dGPU: separate CPU and GPU nodes.
	h = newFakeHost(t)
	amd(h, "0000:03:00.0")
	h.kfdNode("0", 16, 0, 0, 0)
	h.kfdNode("1", 0, 8, 0x300, 2<<30)
	info, _, _ = fromSysfs(enumeratePCI())
	if info.GPUs[0].Integrated || info.Vendor != VendorAMD || info.Best != 0 {
		t.Fatalf("2 GiB dGPU misread as an APU: %+v", info)
	}

	// No KFD topology: VRAM fallback keeps exactly 2 GiB discrete.
	h = newFakeHost(t)
	amd(h, "0000:03:00.0")
	info, _, _ = fromSysfs(enumeratePCI())
	if info.GPUs[0].Integrated || info.Vendor != VendorAMD {
		t.Fatalf("fallback misread a 2 GiB dGPU: %+v", info)
	}
}
