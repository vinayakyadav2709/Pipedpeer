package daemonapi

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestAGPUSandboxSeesSysModule: libcuda reads /sys/module/nvidia*, so a GPU
// job without it gets cuInit 304 and trains on the CPU without saying so.
func TestAGPUSandboxSeesSysModule(t *testing.T) {
	if _, err := os.Stat("/sys/module"); err != nil {
		t.Skipf("host has no /sys/module: %v", err)
	}
	rootfs := t.TempDir()
	var c ociConfig
	addGPUSysModule(&c, rootfs, "job-gpu")
	i := slices.IndexFunc(c.Mounts, func(m ociMount) bool { return m.Destination == "/sys/module" })
	if i < 0 {
		t.Fatalf("no /sys/module mount in %+v", c.Mounts)
	}
	m := c.Mounts[i]
	if m.Source != "/sys/module" || m.Type != "bind" || !slices.Contains(m.Options, "ro") {
		t.Fatalf("/sys/module must be a read-only bind of the host's, got %+v", m)
	}
	if _, err := os.Stat(filepath.Join(rootfs, "sys", "module")); err != nil {
		t.Fatalf("mount point not created: %v", err)
	}
}
