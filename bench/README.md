# Benchmarks & GPU Tests

## GPU Tests

Tests GPU access inside crun OCI sandboxes. Run from the `bench/` directory:

```bash
# Fast CUDA verification (ctypes) — always works
go test -v -run TestGPU

# Full PyTorch integration test (installs PyTorch inside sandbox, ~2min)
PIPEDPEER_GPU_INTEGRATION=1 go test -v -run TestGPUPyTorchCUDA
```

### Test Descriptions

| Test | What it verifies |
|---|---|
| `TestGPUSandbox` | `nvidia-smi` works inside crun (basic GPU access) |
| `TestGPUIntegrationViaDaemon` | The daemon's GPU additions (device nodes, `/dev` tmpfs options) work under crun. It does not copy the whole job config (no `/nix`, `/work`, cgroup cap) |
| `TestGPUPyTorchCUDA` | CUDA via ctypes + PyTorch GPU ops (if available) |
| `TestGPUCUDAComputeCapability` | Detailed GPU info (power, clocks, memory) |
| `TestGPUNoGPUConfig` | No NVIDIA leakage when GPU=false |

Two things every GPU sandbox needs, both learned from failures:

- **Real device numbers.** The tests stat `/dev/nvidia*` and `/dev/nvidia-caps/*`
  for their major/minor, as the daemon does (`src/internal/gpu/detect.go`).
  `nvidia-uvm` and `nvidia-caps` get dynamic majors (234 on one host, 506 on
  another). Rootless crun bind-mounts the host node by path, but rootful crun
  makes the node from these numbers.
- **`/sys/module` read-only.** libcuda and NVML read the `nvidia*` module
  entries. Without them `cuInit` returns 304 and `nvidia-smi` exits 17 with
  "GPU access blocked by the operating system", even though the device nodes
  open fine.

## Sandbox Benchmark

Compares bwrap vs crun sandbox startup overhead:

```bash
go run main.go
```

Results are saved to `bench-results.md` and `bench-results.json`.

## GPU Scheduling Tests

Run from the `src/` directory:

```bash
go test ./internal/coordinator/ -v -run "GPU"
```

Tests GPU-first partitioning, GPU scoring, and CPU fallback behavior.
