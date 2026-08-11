# Sandbox Benchmark Comparison — bwrap vs crun

Date: 2026-05-12T13:23:26+05:30

## Methodology
- Warmup: 5 iterations, Measured: 20 iterations per sandbox per task
- Measures total wall-clock time from sandbox start to process exit
- All tests on same machine (Linux Mint, kernel 6.8, crun 1.14.1, bwrap 0.9.0)
- bwrap: `--die-with-parent --unshare-pid --unshare-ipc --unshare-uts --ro-bind /nix ...`
- crun: OCI bundle with config.json + empty rootfs + bind mounts (same isolation level)

## Context: Why this difference doesn't matter

The sandbox startup time is **noise** in the Pipedpeer task pipeline:

| Pipeline Step | Typical Time |
|---|---|
| `nix build` (resolve deps) | 1–60 seconds |
| `nix-store --export` | 100ms–2s |
| NAR upload over HTTP | 100ms–10s |
| `nix-store --import` | 50ms–1s |
| **Sandbox startup (bwrap/crun)** | **3–16ms** ← tiny |
| Task execution | seconds to hours |

A ~7–13ms difference is **0.01–1.3%** of even the fastest pipeline (nix build of trivial script = ~1s).

## Architecture benefit summary

| Concern | bwrap | crun (OCI) |
|---|---|---|
| GPU support | Manual /dev/nvidia* bind mounts | nvidia-container-toolkit OCI hook |
| Resource limits | Manual (~systemd-run) | Built-in cgroups v2 |
| Cross-platform sandbox | Linux only | Linux only (same) |
| Packaging format | Nix paths only | OCI bundles (standard) |
| Lifecycle code | Custom shell cmd | Standard OCI lifecycle |

## Results

| Task | Metric | bwrap (ms) | crun (ms) | Diff (ms) | Diff (%) |
|---|---|---|---|---|---|
| trivial | Average | 3.657 | 14.600 | +10.943 | +299.22% |
|  | Minimum | 3.308 | 7.957 | +4.649 | |
|  | Maximum | 4.086 | 30.390 | +26.304 | |
| medium | Average | 3.457 | 16.593 | +13.136 | +380.01% |
|  | Minimum | 3.158 | 8.215 | +5.056 | |
|  | Maximum | 3.724 | 37.406 | +33.682 | |
| python | Average | 3.556 | 10.533 | +6.976 | +196.16% |
|  | Minimum | 3.203 | 8.288 | +5.086 | |
|  | Maximum | 4.007 | 27.510 | +23.503 | |

## Raw Data (all measured iterations)

| Task | Iter | bwrap (ms) | crun (ms) |
|---|---|---|---|
| trivial | 0 | 3.487 | 8.628 |
| trivial | 1 | 3.525 | 9.279 |
| trivial | 2 | 3.570 | 28.409 |
| trivial | 3 | 4.086 | 8.660 |
| trivial | 4 | 3.676 | 27.216 |
| trivial | 5 | 4.001 | 17.817 |
| trivial | 6 | 3.713 | 8.523 |
| trivial | 7 | 3.782 | 8.387 |
| trivial | 8 | 3.691 | 8.740 |
| trivial | 9 | 3.468 | 8.473 |
| trivial | 10 | 3.869 | 7.957 |
| trivial | 11 | 3.433 | 8.302 |
| trivial | 12 | 3.556 | 30.390 |
| trivial | 13 | 3.488 | 23.938 |
| trivial | 14 | 3.460 | 25.527 |
| trivial | 15 | 3.308 | 27.017 |
| trivial | 16 | 4.014 | 8.968 |
| trivial | 17 | 3.683 | 8.722 |
| trivial | 18 | 3.808 | 8.772 |
| trivial | 19 | 3.524 | 8.270 |
| medium | 0 | 3.661 | 8.431 |
| medium | 1 | 3.158 | 8.728 |
| medium | 2 | 3.218 | 8.438 |
| medium | 3 | 3.583 | 8.732 |
| medium | 4 | 3.605 | 8.433 |
| medium | 5 | 3.204 | 8.239 |
| medium | 6 | 3.430 | 8.847 |
| medium | 7 | 3.333 | 8.730 |
| medium | 8 | 3.619 | 8.215 |
| medium | 9 | 3.712 | 8.734 |
| medium | 10 | 3.611 | 33.995 |
| medium | 11 | 3.274 | 25.868 |
| medium | 12 | 3.547 | 29.036 |
| medium | 13 | 3.335 | 19.339 |
| medium | 14 | 3.670 | 8.921 |
| medium | 15 | 3.476 | 25.272 |
| medium | 16 | 3.180 | 21.679 |
| medium | 17 | 3.424 | 37.406 |
| medium | 18 | 3.724 | 20.901 |
| medium | 19 | 3.373 | 23.920 |
| python | 0 | 3.203 | 8.628 |
| python | 1 | 3.378 | 8.789 |
| python | 2 | 3.702 | 8.555 |
| python | 3 | 3.516 | 8.288 |
| python | 4 | 3.461 | 8.779 |
| python | 5 | 3.257 | 8.831 |
| python | 6 | 4.007 | 9.066 |
| python | 7 | 3.425 | 27.510 |
| python | 8 | 3.689 | 8.772 |
| python | 9 | 3.339 | 8.766 |
| python | 10 | 3.493 | 9.354 |
| python | 11 | 3.883 | 9.130 |
| python | 12 | 3.956 | 8.554 |
| python | 13 | 3.525 | 8.684 |
| python | 14 | 3.329 | 8.653 |
| python | 15 | 3.780 | 25.499 |
| python | 16 | 3.512 | 8.773 |
| python | 17 | 3.428 | 8.765 |
| python | 18 | 3.425 | 8.416 |
| python | 19 | 3.822 | 8.840 |
