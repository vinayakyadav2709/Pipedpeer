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
| `TestGPUIntegrationViaDaemon` | Daemon's exact OCI config works with GPU |
| `TestGPUPyTorchCUDA` | CUDA via ctypes + PyTorch GPU ops (if available) |
| `TestGPUCUDAComputeCapability` | Detailed GPU info (power, clocks, memory) |
| `TestGPUNoGPUConfig` | No NVIDIA leakage when GPU=false |

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
