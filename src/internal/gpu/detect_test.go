package gpu

import (
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

// refreshAMD and refreshIntel must be safe to call on a machine with neither
// vendor present — they shell out to tools that will not exist.
func TestRefreshOtherVendorsAreSafe(t *testing.T) {
	if info, _, _ := refreshAMD(); info.Vendor != VendorNone && info.Vendor != VendorAMD {
		t.Errorf("refreshAMD returned unexpected vendor %q", info.Vendor)
	}
	if info, _, _ := refreshIntel(); info.Vendor != VendorNone && info.Vendor != VendorIntel {
		t.Errorf("refreshIntel returned unexpected vendor %q", info.Vendor)
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
