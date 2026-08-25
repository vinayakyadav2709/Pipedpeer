# Measured results

Everything below was measured on this machine on 2026-08-25 against the commit
recorded in `results.json`. Nothing is estimated. Where a figure could not be
measured honestly, that is said plainly rather than filled in.

## Test bed

| | |
|---|---|
| Host | Linux 7.1.8 (Fedora 44), x86_64, 20 logical cores, 16 GiB free |
| Cluster | 3 worker daemons in podman containers, host networking, ports 38081-38083 |
| Submitting node | the same host, daemon on port 38080 |
| Python | 3.14.6, with numpy, pandas, scikit-learn, joblib and CPU torch available |

**The single most important caveat.** All three workers are containers on this
one machine. They therefore share one CPU, one memory system and the loopback
interface. This test bed can measure correctness, overhead, cache behaviour and
fault tolerance honestly. It **cannot** measure multi-machine speedup, because
moving CPU-bound work between containers on one host cannot make that host
faster. No speedup figure is claimed anywhere below. The same harness
(`bench.py`) runs unchanged against real machines.

## R1  Test suite

`cd src && go test ./... -count=1` with every optional Python library installed.

| | |
|---|---|
| Test cases passed | 246 |
| Failed | 0 |
| Skipped | 3 |

The three skips are `TestConcurrentJobsAndSharedNumpyCache` and
`TestIntegrationFullSyncAndExecute` (both need `PIPEDPEER_INTEGRATION=1`) and
`TestDetectNVIDIA` (no NVIDIA GPU on this host).

Five interception tests had been skipping silently on any machine without
pandas, torch and scikit-learn, which includes the project's own CI matrix job
that does not install them. With the libraries present they run and pass:
`TestShimHashShuffleMatchesLocal`, `TestShimOutOfCoreIntegrity`,
`TestShimCostModelDecisions`, `TestShimJoblibBackendDistributes`,
`TestShimNumPyInterception`.

Raw: `raw/B1_go_test_verbose.txt`.

## R2  Environment cache

The strongest measured result. The same trivial job submitted five times.

| Submission | Seconds, end to end |
|---|---|
| 1st (closure built and shipped) | 12.00 |
| 2nd | 0.68 |
| 3rd | 0.64 |
| 4th | 0.64 |
| 5th | 0.66 |

Median once warm: **0.66 s, against 12.00 s cold, an 18x reduction.** This is
the payoff from keying the cache on the Nix store path: the second submission
finds the closure already present on the worker and ships nothing but the
workspace. Figure `fig_5.1_closure_cache.png`.

## R3  Cost of leaving interception switched on

Same job, same cluster, interception on versus off.

| | Median seconds |
|---|---|
| `PIPEDPEER_SHIM=0` | 0.85 |
| interception on | 0.93 |

**+9%** on a whole remote job whose workload the cost model declines to
distribute. Raw: `raw/N7_*.log`.

Separately, `scripts/bench-shim-d2.sh` measures interpreter startup for a job
that never imports a heavy library:

| | Slowdown against no shim |
|---|---|
| Before the fix in this branch | 21.60x |
| After | 1.20x |

The shim used to import torch while installing itself, about 1.7 s on this
host, charged to every job whether or not it mentioned torch. The project's own
3x budget was being exceeded sevenfold and nothing in CI ran the check.
Figure `fig_5.2_shim_overhead.png`.

## R4  Work really does cross node boundaries

With `--distribute force`, all three workers logged `received pool/map`:

| Worker | pool/map requests received |
|---|---|
| lab-1 | 3 |
| lab-2 | 3 |
| lab-3 | 4 |

So interception, chunking and dispatch work end to end. Raw:
`raw/N3_pool_map_receipts.txt`. No speedup is claimed from this, for the reason
in the test bed note above.

## R5  Fault tolerance

`scripts/lab-fail.sh` fans a 20-item `Pool.map` across the cluster, kills a
worker container mid-flight, and checks the answer.

```
killing worker on :38082 mid-flight
POOL-OK sum=2470
PASS: pool.map completed correctly despite a dead worker
```

2470 is the correct sum of squares for 0..19. The run completed with correct
results despite losing a node mid-computation. Raw: `raw/B5_lab_fail.txt`.

## R6  Sandbox start cost

`bench/`, 20 measured iterations after 5 warmups, median.

| Task | bwrap | crun (in use) |
|---|---|---|
| trivial (echo) | 5.1 ms | 21.7 ms |
| medium (shell loop) | 8.0 ms | 23.7 ms |
| python (interpreter start) | 16.9 ms | 36.8 ms |

crun costs roughly 20 ms more per job than bwrap. Against a 0.66 s warm job,
let alone a 12 s cold one, that is not a number worth optimising, and crun buys
the OCI device model that GPU passthrough needs. Figure
`fig_5.3_sandbox_overhead.png`.

These are the first valid numbers this benchmark has produced: it previously
gave neither sandbox a root filesystem, so `/bin/sh` was missing and all 60
measured runs exited non-zero. The published timings were the cost of failing
to start. Fixed in this branch, along with a check that refuses to write a
report when any measured run exits non-zero.

## Not measured

Stated explicitly rather than left as a blank in a table.

| Wanted | Why it is absent | How to get it |
|---|---|---|
| Multi-machine speedup for `Pool.map` | every worker shares this one host's CPU | run `bench.py` against two or more real machines |
| Speedup for pandas hash shuffle | as above | as above |
| DDP scaling across ranks | as above; correctness is covered by `TestShimDDPSync` | as above with `--ddp N` |
| Peer discovery latency | discovery is by UDP broadcast on a LAN; containers here share the host's loopback, so the number would not mean anything | measure on a real LAN |
| GPU placement and VRAM-aware scoring | no GPU on this host | run `bench/gpu_test.go` with `PIPEDPEER_GPU_INTEGRATION=1` |
| The five demo workloads (`docker/demo-test.sh`) | pulls multi-GB CUDA closures; not attempted here | run on a machine with the disk and a GPU |
