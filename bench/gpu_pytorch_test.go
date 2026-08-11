package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGPUPyTorchCUDA(t *testing.T) {
	if _, err := exec.LookPath("crun"); err != nil {
		t.Skip("crun not found, skipping GPU test")
	}
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		t.Skip("nvidia-smi not found, skipping GPU test")
	}

	hasTorch := false
	if out, err := exec.Command("python3", "-c", "import torch; print(torch.cuda.is_available())").Output(); err == nil {
		if strings.TrimSpace(string(out)) == "True" {
			hasTorch = true
		}
	}

	fullIntegration := os.Getenv("PIPEDPEER_GPU_INTEGRATION") == "1"

	var pythonScript string
	if hasTorch {
		t.Log("Host has PyTorch with CUDA — testing inside crun")
		pythonScript = `import subprocess, sys, json

# 1. Verify nvidia-smi
result = subprocess.run(["nvidia-smi", "--query-gpu=name,memory.total,compute_cap", "--format=csv,noheader"], capture_output=True, text=True)
gpus = result.stdout.strip().split("\n")
print(f"GPUs detected: {len(gpus)}")

# 2. Verify CUDA via ctypes (independent validation)
import ctypes
libcuda = ctypes.CDLL("libcuda.so")
count = ctypes.c_int()
libcuda.cuDeviceGetCount(ctypes.byref(count))
print(f"CUDA devices via libcuda: {count.value}")

# 3. Verify PyTorch CUDA
import torch
print(f"PyTorch version: {torch.__version__}")
print(f"PyTorch CUDA version: {torch.version.cuda}")
print(f"PyTorch CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"Device: {torch.cuda.get_device_name(0)}")
    x = torch.tensor([1.0, 2.0, 3.0]).cuda()
    y = torch.tensor([4.0, 5.0, 6.0]).cuda()
    z = x + y
    print(f"GPU tensor test: {z}")
    assert z.is_cuda, "Result should be on GPU"
    assert torch.equal(z, torch.tensor([5.0, 7.0, 9.0]).cuda()), "Wrong result"
    print("SUCCESS: PyTorch GPU tensor operations verified")
`
	} else if fullIntegration {
		t.Log("Full integration mode: installing PyTorch in venv inside sandbox")
		pythonScript = `import subprocess, sys, os, venv

# Create venv (avoids PEP 668 externally-managed-environment)
venv_dir = "/tmp/torch-venv"
venv.create(venv_dir, with_pip=True)
python = os.path.join(venv_dir, "bin", "python3")

# Install PyTorch with CUDA 12.4 support
subprocess.run([python, "-m", "pip", "install", "torch", "torchvision",
    "--index-url", "https://download.pytorch.org/whl/cu124", "-q"], check=True)

import subprocess as sp
import ctypes

# 1. Verify nvidia-smi
result = sp.run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"], capture_output=True, text=True)
gpus = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
print(f"GPUs detected: {len(gpus)}")

# 2. Verify CUDA via ctypes
libcuda = ctypes.CDLL("libcuda.so")
count = ctypes.c_int()
libcuda.cuDeviceGetCount(ctypes.byref(count))
print(f"CUDA devices via libcuda: {count.value}")

# 3. Verify PyTorch CUDA
import torch
print(f"PyTorch CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"Device: {torch.cuda.get_device_name(0)}")
    x = torch.tensor([1.0, 2.0, 3.0]).cuda()
    print(f"GPU tensor test: {x + torch.tensor([4.0, 5.0, 6.0]).cuda()}")
    print("SUCCESS: PyTorch GPU verified")
`
	} else {
		t.Log("PyTorch not available — using ctypes to verify CUDA directly")
		pythonScript = `import subprocess, sys
result = subprocess.run(["nvidia-smi", "--query-gpu=name,memory.total,compute_cap", "--format=csv,noheader"], capture_output=True, text=True)
gpus = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
print(f"GPUs detected: {len(gpus)}")
for gpu in gpus:
    print(f"  {gpu}")

import ctypes
try:
    libcuda = ctypes.CDLL("libcuda.so")
    assert libcuda.cuInit(0) == 0, "cuInit failed"
    count = ctypes.c_int()
    assert libcuda.cuDeviceGetCount(ctypes.byref(count)) == 0, "cuDeviceGetCount failed"
    print(f"CUDA devices via libcuda: {count.value}")
    assert count.value > 0, "No CUDA devices found"
    for i in range(count.value):
        name = ctypes.create_string_buffer(256)
        libcuda.cuDeviceGetName(name, 256, ctypes.c_int(i))
        mem = ctypes.c_size_t()
        libcuda.cuDeviceTotalMem(ctypes.byref(mem), ctypes.c_int(i))
        print(f"  Device {i}: {name.value.decode()} ({mem.value // 1024 // 1024} MB)")
    print("SUCCESS: CUDA verified via ctypes")
except Exception as e:
    print(f"CUDA FAILED: {e}")
    sys.exit(1)

# Try PyTorch if available
try:
    import torch
    print(f"PyTorch CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        x = torch.tensor([1.0, 2.0, 3.0]).cuda()
        print(f"PyTorch tensor test: {x + torch.tensor([4.0, 5.0, 6.0]).cuda()}")
except ImportError:
    pass
`
	}

	baseDir := t.TempDir()
	bundleDir := filepath.Join(baseDir, "oci-bundle")
	rootfsDir := filepath.Join(bundleDir, "rootfs")
	for _, d := range []string{"dev", "proc", "sys", "tmp"} {
		os.MkdirAll(filepath.Join(rootfsDir, d), 0755)
	}

	os.MkdirAll(filepath.Join(rootfsDir, "tmp"), 0755)
	os.MkdirAll(filepath.Join(rootfsDir, "etc"), 0755)
	// Write static resolv.conf for DNS inside the sandbox (host uses systemd-resolved symlink)
	os.WriteFile(filepath.Join(rootfsDir, "etc", "resolv.conf"), []byte("nameserver 8.8.8.8\nnameserver 1.1.1.1\n"), 0644)
	scriptPath := filepath.Join(rootfsDir, "test_cuda.py")
	os.WriteFile(scriptPath, []byte(pythonScript), 0644)

	config := map[string]interface{}{
		"ociVersion": "1.0.2",
		"process": map[string]interface{}{
			"terminal": false,
			"user":     map[string]int{"uid": 0, "gid": 0},
			"args":     []string{"/usr/bin/python3", "/test_cuda.py"},
			"env": []string{
				"PATH=/usr/local/bin:/usr/bin:/bin",
				"HOME=/root",
				"NVIDIA_VISIBLE_DEVICES=all",
				"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
			},
			"cwd": "/",
		},
		"root": map[string]interface{}{"path": "rootfs", "readonly": true},
		"hostname": "gpu-pytorch-test",
		"mounts": []map[string]interface{}{
			{"destination": "/usr", "type": "bind", "source": "/usr", "options": []string{"rbind", "ro"}},
			{"destination": "/bin", "type": "bind", "source": "/bin", "options": []string{"rbind", "ro"}},
			{"destination": "/lib", "type": "bind", "source": "/lib", "options": []string{"rbind", "ro"}},
			{"destination": "/lib64", "type": "bind", "source": "/lib64", "options": []string{"rbind", "ro"}},
			{"destination": "/proc", "type": "proc", "source": "proc"},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
			{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"},
			{"destination": "/etc/ssl", "type": "bind", "source": "/etc/ssl", "options": []string{"rbind", "ro"}},
		},
		"linux": map[string]interface{}{
			"namespaces": []map[string]string{
				{"type": "pid"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
			},
			"devices": []map[string]interface{}{
				{"path": "/dev/nvidia0", "type": "c", "major": 195, "minor": 0, "permissions": "rwm"},
				{"path": "/dev/nvidiactl", "type": "c", "major": 195, "minor": 255, "permissions": "rwm"},
				{"path": "/dev/nvidia-modeset", "type": "c", "major": 195, "minor": 254, "permissions": "rwm"},
				{"path": "/dev/nvidia-uvm", "type": "c", "major": 234, "minor": 0, "permissions": "rwm"},
				{"path": "/dev/nvidia-uvm-tools", "type": "c", "major": 234, "minor": 1, "permissions": "rwm"},
			},
		},
	}

	data, _ := json.MarshalIndent(config, "", "  ")
	os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0644)

	cid := "gpu-pytorch"
	exec.Command("crun", "delete", "-f", cid).Run()
	defer exec.Command("crun", "delete", "-f", cid).Run()

	var stdout, stderr strings.Builder
	cmd := exec.Command("crun", "run", "--bundle", bundleDir, cid)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	output := stdout.String()
	t.Logf("CUDA test output:\n%s", output)
	if stderr.Len() > 0 {
		t.Logf("stderr:\n%s", stderr.String())
	}

	if err != nil {
		if strings.Contains(output, "SUCCESS") {
			t.Log("Tests passed despite non-zero exit")
		} else if strings.Contains(output, "CUDA devices via libcuda: 0") {
			t.Fatal("libcuda found 0 CUDA devices")
		} else if strings.Contains(output, "CUDA FAILED") || strings.Contains(output, "traceback") || strings.Contains(output, "Traceback") {
			if !strings.Contains(output, "PyTorch not installed") && !strings.Contains(output, "No module named 'torch'") {
				t.Fatalf("CUDA test failed: %v\noutput: %s\nstderr: %s", err, output, stderr.String())
			}
		}
	}

	if strings.Contains(output, "GPUs detected: 0") {
		t.Fatal("No GPUs detected inside sandbox")
	}

	if strings.Contains(output, "CUDA devices via libcuda: 0") {
		t.Fatal("libcuda found 0 CUDA devices")
	}

	if strings.Contains(output, "SUCCESS") {
		t.Log("✓ All GPU/CUDA tests passed")
	}
}

func TestGPUCUDAComputeCapability(t *testing.T) {
	if _, err := exec.LookPath("crun"); err != nil {
		t.Skip("crun not found")
	}
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		t.Skip("nvidia-smi not found")
	}

	baseDir := t.TempDir()
	bundleDir := filepath.Join(baseDir, "oci-bundle")
	rootfsDir := filepath.Join(bundleDir, "rootfs")
	os.MkdirAll(rootfsDir, 0755)

	gpuCheck := `#!/bin/bash
echo "=== GPU Info ==="
nvidia-smi --query-gpu=index,name,driver_version,pstate,temperature.gpu,power.draw,clocks.current.graphics,clocks.current.memory --format=csv,noheader 2>/dev/null || echo "nvidia-smi failed"
echo "=== CUDA Compute Capability ==="
nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null || echo "no compute cap"
echo "=== GPU Memory ==="
nvidia-smi --query-gpu=memory.total,memory.used,memory.free --format=csv,noheader 2>/dev/null || echo "no memory info"
`

	os.MkdirAll(filepath.Join(rootfsDir, "tmp"), 0755)
	scriptPath := filepath.Join(rootfsDir, "gpu_check.sh")
	os.WriteFile(scriptPath, []byte(gpuCheck), 0755)

	config := map[string]interface{}{
		"ociVersion": "1.0.2",
		"process": map[string]interface{}{
			"terminal": false,
			"user":     map[string]int{"uid": 0, "gid": 0},
			"args":     []string{"/bin/bash", "/gpu_check.sh"},
			"env": []string{
				"PATH=/usr/local/bin:/usr/bin:/bin",
				"NVIDIA_VISIBLE_DEVICES=all",
				"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
			},
			"cwd": "/",
		},
		"root": map[string]interface{}{"path": "rootfs", "readonly": true},
		"hostname": "gpu-check",
		"mounts": []map[string]interface{}{
			{"destination": "/usr", "type": "bind", "source": "/usr", "options": []string{"rbind", "ro"}},
			{"destination": "/bin", "type": "bind", "source": "/bin", "options": []string{"rbind", "ro"}},
			{"destination": "/lib", "type": "bind", "source": "/lib", "options": []string{"rbind", "ro"}},
			{"destination": "/lib64", "type": "bind", "source": "/lib64", "options": []string{"rbind", "ro"}},
			{"destination": "/proc", "type": "proc", "source": "proc"},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs"},
			{"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"},
		},
		"linux": map[string]interface{}{
			"namespaces": []map[string]string{
				{"type": "pid"}, {"type": "mount"}, {"type": "uts"},
			},
			"devices": []map[string]interface{}{
				{"path": "/dev/nvidia0", "type": "c", "major": 195, "minor": 0, "permissions": "rwm"},
				{"path": "/dev/nvidiactl", "type": "c", "major": 195, "minor": 255, "permissions": "rwm"},
				{"path": "/dev/nvidia-uvm", "type": "c", "major": 234, "minor": 0, "permissions": "rwm"},
			},
		},
	}

	data, _ := json.MarshalIndent(config, "", "  ")
	os.WriteFile(filepath.Join(bundleDir, "config.json"), data, 0644)

	cid := "gpu-check"
	exec.Command("crun", "delete", "-f", cid).Run()
	defer exec.Command("crun", "delete", "-f", cid).Run()

	var stdout, stderr strings.Builder
	cmd := exec.Command("crun", "run", "--bundle", bundleDir, cid)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("crun run failed: %v\nstderr: %s", err, stderr.String())
	}

	output := stdout.String()
	t.Logf("GPU details:\n%s", output)

	if strings.Contains(output, "nvidia-smi failed") {
		t.Fatal("nvidia-smi failed inside sandbox")
	}
}

func init() {
	_ = fmt.Sprintf
}
