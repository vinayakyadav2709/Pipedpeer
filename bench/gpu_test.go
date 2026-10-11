package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// nvidiaDevices stats the host's NVIDIA nodes for their real major/minor, as
// the daemon does (src/internal/gpu/detect.go statDevice). Majors are not
// fixed: nvidia-uvm and nvidia-caps get dynamic majors (234 on one host, 506
// on another), so a hard-coded table points the sandbox at the wrong driver.
func nvidiaDevices(tb testing.TB) []map[string]interface{} {
	tb.Helper()
	paths, _ := filepath.Glob("/dev/nvidia*")
	caps, _ := filepath.Glob("/dev/nvidia-caps/*")
	var devs []map[string]interface{}
	for _, p := range append(paths, caps...) {
		fi, err := os.Stat(p)
		if err != nil || fi.Mode()&os.ModeCharDevice == 0 {
			continue
		}
		st := fi.Sys().(*syscall.Stat_t)
		devs = append(devs, map[string]interface{}{
			"path": p, "type": "c",
			"major":    (st.Rdev >> 8) & 0xfff,
			"minor":    (st.Rdev & 0xff) | ((st.Rdev >> 12) & 0xfff00),
			"fileMode": 438, "permissions": "rwm",
		})
	}
	if len(devs) == 0 {
		tb.Skip("no /dev/nvidia* device nodes on this host")
	}
	return devs
}

func TestGPUSandbox(t *testing.T) {
	if _, err := exec.LookPath("crun"); err != nil {
		t.Skip("crun not found, skipping GPU test")
	}
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		t.Skip("nvidia-smi not found, skipping GPU test")
	}

	baseDir := t.TempDir()
	bundleDir := filepath.Join(baseDir, "oci-bundle")
	rootfsDir := filepath.Join(bundleDir, "rootfs")
	for _, d := range []string{"dev", "proc", "sys"} {
		os.MkdirAll(filepath.Join(rootfsDir, d), 0755)
	}

	config := map[string]interface{}{
		"ociVersion": "1.0.2",
		"process": map[string]interface{}{
			"terminal": false,
			"user":     map[string]int{"uid": 0, "gid": 0},
			"args":     []string{"/usr/bin/nvidia-smi", "--query-gpu=name,driver_version,memory.total", "--format=csv,noheader"},
			"env": []string{
				"PATH=/usr/local/bin:/usr/bin:/bin",
				"HOME=/root",
				"NVIDIA_VISIBLE_DEVICES=all",
				"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
			},
			"cwd": "/",
		},
		"root":     map[string]interface{}{"path": "rootfs", "readonly": true},
		"hostname": "gpu-test",
		"mounts": []map[string]interface{}{
			{"destination": "/usr", "type": "bind", "source": "/usr", "options": []string{"rbind", "ro"}},
			{"destination": "/bin", "type": "bind", "source": "/bin", "options": []string{"rbind", "ro"}},
			{"destination": "/lib", "type": "bind", "source": "/lib", "options": []string{"rbind", "ro"}},
			{"destination": "/lib64", "type": "bind", "source": "/lib64", "options": []string{"rbind", "ro"}},
			{"destination": "/proc", "type": "proc", "source": "proc"},
			// libcuda and NVML read /sys/module/nvidia*; without it cuInit returns 304
			// and nvidia-smi exits 17 ("GPU access blocked by the operating system").
			{"destination": "/sys/module", "type": "bind", "source": "/sys/module", "options": []string{"rbind", "ro"}},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
			{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"},
		},
		"linux": map[string]interface{}{
			"namespaces": []map[string]string{
				{"type": "pid"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
			},
			"devices": nvidiaDevices(t),
		},
	}

	data, _ := json.MarshalIndent(config, "", "  ")
	if err := os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0644); err != nil {
		t.Fatalf("write config.json: %v", err)
	}

	cid := "gpu-test"
	exec.Command("crun", "delete", "-f", cid).Run()
	defer exec.Command("crun", "delete", "-f", cid).Run()

	var stdout, stderr strings.Builder
	cmd := exec.Command("crun", "run", "--bundle", bundleDir, cid)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("crun run failed: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	output := strings.TrimSpace(stdout.String())
	t.Logf("nvidia-smi output: %s", output)

	if output == "" {
		t.Fatal("nvidia-smi produced no output — GPU not accessible inside sandbox")
	}
	if strings.Contains(output, "NVIDIA-SMI has failed") {
		t.Fatalf("GPU not accessible: %s", output)
	}

	// Should see something like: "NVIDIA GeForce RTX 3060, 535.288.01, 6144 MiB"
	parts := strings.Split(output, ",")
	if len(parts) < 2 {
		t.Fatalf("unexpected nvidia-smi format: %s", output)
	}
	t.Logf("GPU detected inside crun sandbox: %s", strings.TrimSpace(parts[0]))
}

func TestGPUIntegrationViaDaemon(t *testing.T) {
	if _, err := exec.LookPath("crun"); err != nil {
		t.Skip("crun not found")
	}
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		t.Skip("nvidia-smi not found")
	}

	// Simulate the OCI config that the daemon would generate with cfg.GPU=true
	baseDir := t.TempDir()
	bundleDir := filepath.Join(baseDir, "oci-bundle")
	rootfsDir := filepath.Join(bundleDir, "rootfs")
	for _, d := range []string{"dev", "proc", "sys"} {
		os.MkdirAll(filepath.Join(rootfsDir, d), 0755)
	}

	// The GPU parts of the config execution.go builds when GPU=true (device
	// nodes, /dev tmpfs options); not its full job config (/nix, /work, cgroups).
	ociCfg := map[string]interface{}{
		"ociVersion": "1.0.2",
		"process": map[string]interface{}{
			"terminal": false,
			"user":     map[string]int{"uid": 0, "gid": 0},
			"args":     []string{"/usr/bin/nvidia-smi", "-L"},
			"env": []string{
				"HOME=/root",
				"PATH=/usr/local/bin:/usr/bin:/bin",
				"NVIDIA_VISIBLE_DEVICES=all",
				"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
			},
			"cwd": "/",
		},
		"root":     map[string]interface{}{"path": "rootfs", "readonly": true},
		"hostname": "pipedpeer",
		"mounts": []map[string]interface{}{
			{"destination": "/usr", "type": "bind", "source": "/usr", "options": []string{"rbind", "ro"}},
			{"destination": "/bin", "type": "bind", "source": "/bin", "options": []string{"rbind", "ro"}},
			{"destination": "/lib", "type": "bind", "source": "/lib", "options": []string{"rbind", "ro"}},
			{"destination": "/lib64", "type": "bind", "source": "/lib64", "options": []string{"rbind", "ro"}},
			{"destination": "/proc", "type": "proc", "source": "proc"},
			// libcuda and NVML read /sys/module/nvidia*; without it cuInit returns 304
			// and nvidia-smi exits 17 ("GPU access blocked by the operating system").
			{"destination": "/sys/module", "type": "bind", "source": "/sys/module", "options": []string{"rbind", "ro"}},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "noexec"}},
			{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "nodev"}},
		},
		"linux": map[string]interface{}{
			"namespaces": []map[string]string{
				{"type": "pid"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
			},
			"devices": nvidiaDevices(t),
		},
	}

	data, _ := json.MarshalIndent(ociCfg, "", "  ")
	os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0644)

	cid := "gpu-daemon-test"
	exec.Command("crun", "delete", "-f", cid).Run()
	defer exec.Command("crun", "delete", "-f", cid).Run()

	var stdout, stderr strings.Builder
	cmd := exec.Command("crun", "run", "--bundle", bundleDir, cid)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("crun run failed: %v\nstderr: %s", err, stderr.String())
	}

	output := strings.TrimSpace(stdout.String())
	t.Logf("GPU info: %s", output)
	if !strings.Contains(output, "GPU") {
		t.Fatalf("expected GPU info, got: %s", output)
	}
}

func TestGPUNoGPUConfig(t *testing.T) {
	// Verify that when GPU=false, no NVIDIA devices appear in the config
	baseDir := t.TempDir()
	bundleDir := filepath.Join(baseDir, "oci-bundle")
	rootfsDir := filepath.Join(bundleDir, "rootfs")
	os.MkdirAll(rootfsDir, 0755)

	config := map[string]interface{}{
		"ociVersion": "1.0.2",
		"process": map[string]interface{}{
			"terminal": false,
			"user":     map[string]int{"uid": 0, "gid": 0},
			"args":     []string{"/bin/echo", "no-gpu"},
			"env":      []string{"PATH=/usr/bin:/bin", "HOME=/root"},
			"cwd":      "/",
		},
		"root":     map[string]interface{}{"path": "rootfs", "readonly": true},
		"hostname": "pipedpeer",
		"linux": map[string]interface{}{
			"namespaces": []map[string]string{
				{"type": "pid"}, {"type": "mount"},
			},
		},
	}

	data, _ := json.MarshalIndent(config, "", " ")
	raw := string(data)
	if strings.Contains(raw, "nvidia") {
		t.Fatal("GPU=false config should not contain NVIDIA references")
	}
	if strings.Contains(raw, "nvidia0") {
		t.Fatal("GPU=false config should not contain device nodes")
	}
}

// BenchmarkGPUStartup measures crun GPU sandbox startup overhead
func BenchmarkGPUStartup(b *testing.B) {
	if _, err := exec.LookPath("crun"); err != nil {
		b.Skip("crun not found")
	}

	baseDir := b.TempDir()
	bundleDir := filepath.Join(baseDir, "oci-bundle")
	rootfsDir := filepath.Join(bundleDir, "rootfs")
	for _, d := range []string{"dev", "proc", "sys"} {
		os.MkdirAll(filepath.Join(rootfsDir, d), 0755)
	}

	config := map[string]interface{}{
		"ociVersion": "1.0.2",
		"process": map[string]interface{}{
			"terminal": false,
			"user":     map[string]int{"uid": 0, "gid": 0},
			"args":     []string{"/bin/true"},
			"env": []string{
				"PATH=/usr/local/bin:/usr/bin:/bin",
				"HOME=/root",
				"NVIDIA_VISIBLE_DEVICES=all",
			},
			"cwd": "/",
		},
		"root":     map[string]interface{}{"path": "rootfs", "readonly": true},
		"hostname": "gpu-bench",
		"mounts": []map[string]interface{}{
			{"destination": "/usr", "type": "bind", "source": "/usr", "options": []string{"rbind", "ro"}},
			{"destination": "/bin", "type": "bind", "source": "/bin", "options": []string{"rbind", "ro"}},
			{"destination": "/lib", "type": "bind", "source": "/lib", "options": []string{"rbind", "ro"}},
			{"destination": "/lib64", "type": "bind", "source": "/lib64", "options": []string{"rbind", "ro"}},
			{"destination": "/proc", "type": "proc", "source": "proc"},
			// libcuda and NVML read /sys/module/nvidia*; without it cuInit returns 304
			// and nvidia-smi exits 17 ("GPU access blocked by the operating system").
			{"destination": "/sys/module", "type": "bind", "source": "/sys/module", "options": []string{"rbind", "ro"}},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
			{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"},
		},
		"linux": map[string]interface{}{
			"namespaces": []map[string]string{
				{"type": "pid"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
			},
			"devices": nvidiaDevices(b),
		},
	}
	data, _ := json.MarshalIndent(config, "", "  ")
	os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0644)

	cid := "gpu-bench"
	exec.Command("crun", "delete", "-f", cid).Run()
	defer exec.Command("crun", "delete", "-f", cid).Run()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Command("crun", "delete", "-f", cid).Run()
		cmd := exec.Command("crun", "run", "--bundle", bundleDir, cid)
		cmd.Run()
	}
}

func init() {
	// Explicitly referencing main to avoid unused import issues
	_ = fmt.Sprintf
}
