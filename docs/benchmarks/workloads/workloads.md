# Campaign workloads

The workloads, which campaign systems run them, and how a run's output is
judged (`result_ok`, brief S8 / `docs/benchmarks/campaign-cases.md` §3). Sizes
are fixed for the whole campaign and come from `results/lab/sizes.env`
(`calibrate.sh` writes it; never change one mid-campaign). The runner
(`scripts/campaign/run.sh`) exports that file's variables before each run.

## Workload table

| Id | Script (in `docs/benchmarks/workloads/`) | Exercises | Result line |
|---|---|---|---|
| W1 | `pool_map.py` | `multiprocessing.Pool` map, CPU bound | `POOLMAP-OK items=%d seconds=%.4f checksum=%.6f` |
| W2 | `pipedpeer-demo/cpu.py --items $W2_ITEMS` | mixed: Pool, joblib, NumPy, pandas | `Pool:` / `joblib:` / `numpy:` / `pandas:` lines |
| W3 | `w3_rf.py` | joblib / scikit-learn | `accuracy: {:.4f}` |
| T1 | `t1_mlp.py` | data-parallel training on CPU | `final loss:` / `weight checksum:` |
| idle | `pool_idle.py` | a Pool map the shim's cost model must decline | `POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f` |
| trivial | `trivial.py` | pipeline overhead, not compute | `TRIVIAL-OK` |
| TS1, TS10, TS100, TS1000 | `sweep_map.py` | task granularity sweep (1, 10, 100, 1000 ms per task, no payload) | `SWEEP-OK items=%d seconds=%.4f checksum=%.6f` |
| DS10K, DS1M, DS20M | `sweep_map.py` | payload size sweep (10 KB, 1 MB, 20 MB per task) | `SWEEP-OK items=%d seconds=%.4f checksum=%.6f` |
| SB | `sweep_map.py` | CPU compute benchmark for sandbox overhead (100 ms tasks, no payload) | `SWEEP-OK items=%d seconds=%.4f checksum=%.6f` |
| EL | `sweep_map.py` | elasticity mid-job join workload (100 ms tasks, no payload) | `SWEEP-OK items=%d seconds=%.4f checksum=%.6f` |
| noop | `noop_map.py` | raw unbatched task submission throughput | `NOOP-OK items=%d seconds=%.4f tasks_per_s=%.4f` |
| lat | `lat_map.py` | round-trip task dispatch latency (p50, p90, p99) | `LAT-OK n=%d p50_ms=%.4f p90_ms=%.4f p99_ms=%.4f` |
| fileio | `fileio.py` | filesystem write, fsync, read, delete for sandbox I/O overhead | `FILEIO-OK files=%d bytes=%d seconds=%.4f` |

## Sizes

From `sizes.env` (one set for the campaign):
- Baseline sizes: `BENCH_PROCS` = pc00's `nproc` (J1 runs), `W1_ITEMS` (strong)
  and `W1_WEAK_PER_PC` (weak, items × N), `W2_ITEMS`, `W3_TREES`, `T1_EPOCHS`.
- Sweep sizes: `SWEEP_TOTAL_MS` (total compute budget across items),
  `SWEEP_ITERS_PER_MS` (inner loop iterations calibrated to 1 ms),
  `SWEEP_DATA_ITEMS` (fixed item count for payload runs),
  `SWEEP_DATA_TASK_MS` (compute duration per payload task).
- Microbenchmarks: `NOOP_ITEMS` (tasks for throughput test), `LAT_CALLS`
  (repeated sequential dispatches for latency percentiles).
- Elasticity and sandbox: `ELASTIC_TASK_MS` and `ELASTIC_ITEMS` (workload for
  dynamic joiners), `FILEIO_FILES` and `FILEIO_FILE_KB` (file count and size).

The runner exports them as the workload scripts' own environment variables
(`BENCH_ITEMS`, `BENCH_TASK_MS`, `BENCH_ITERS_PER_MS`, `BENCH_PAYLOAD_BYTES`,
`BENCH_CALLS`, `BENCH_FILES`, `BENCH_FILE_KB`, `BENCH_TREES`, `BENCH_EPOCHS`).

## Which system runs which workload

| Workload | plain | pipedpeer | ray | dask | ssh | pipedpeer-bare | pipedpeer-noshim |
|---|---|---|---|---|---|---|---|
| W1 | REF, E1 | E1, E1b, E4, E5, E6, E7, E11 | E1, E1b | E1, E1b | E1 | — | — |
| W2 | REF, E1 | E1 (smoke gate) | E1 | E1 | E1 | — | — |
| W3 | REF, E1 | E1 | E1 | E1 | E1 | — | — |
| T1 | REF | pipedpeer-exact/-fast (E8) | — | — | — | — | — |
| idle | REF | E7 | — | — | — | — | E7 |
| trivial | REF | FC, E4 (E11) | FC | FC | FC | — | — |
| TS1–TS1000 | REF | E13 | E13 | E13 | E13 | — | — |
| DS10K–DS20M | REF | E14 | E14 | E14 | E14 | — | — |
| noop | — | E15 | E15 | E15 | — | — | — |
| lat | — | E16 | E16 | E16 | — | — | — |
| SB | REF | E20 | — | — | — | E20 | — |
| fileio | — | E20 | — | — | — | E20 | — |
| EL | — | E19 | E19 | E19 | — | — | — |

`torchrun` runs T1 only (E8, `baselines/t1_torchrun.sh`).
`ray-pool` runs W1 only (E6, `baselines/w1_ray_pool.py`).
`pipedpeer-bare` runs with container isolation disabled (`--isolate=false`).
`pipedpeer` on `noop` and `lat` runs with `--distribute force` to ensure work leaves the node.

## Baseline ports

`baselines/` holds the Ray, Dask and plain-ssh ports (systems of the same
name). Each port's header says what is distributed, what stays local, and the
docs page it follows; each prints the original's result line byte-for-byte.
- Ray: `@ray.remote` tasks for parallel map (`w1_ray.py`), idle (`idle_ray.py`),
  sweeps (`sweep_ray.py`), throughput (`noop_ray.py`), and latency (`lat_ray.py`).
  Fine-grained sweep tasks batch with `batch = max(1, ceil(100 / task_ms))`.
  Joblib code uses `ray.util.joblib` (`register_ray`). E6 includes the labelled
  Pool arm `baselines/w1_ray_pool.py`.
- Dask: `dask.distributed.Client` with `client.map`/`gather` for Pool and sweep
  code (`w1_dask.py`, `sweep_dask.py`), unbatched `client.gather(client.map(...))`
  for throughput (`noop_dask.py`), sequential `client.submit(...).result()` for
  latency (`lat_dask.py`), and `joblib.parallel_backend("dask")` for joblib.
- SSH: static item partitioning across nodes (`w#_ssh_part.py`, `sweep_ssh_part.py`),
  run with GNU `parallel --sshlogin` and outputs joined on pc00 (`w#_ssh.sh`,
  `sweep_ssh.sh`). Sweep payload files are transferred with GNU parallel's
  `--transferfile` and cleaned up with `--cleanup`.
- How many lines each port changes from its original:
  `results/lab/port-lines.csv` (`scripts/campaign/port-lines.sh`).

## result_ok (how the runner judges a run)

The runner extracts a run's result line by the workload's regex and compares
the job's own reference (`$LAB/reference/<workload>-<sizekey>.txt`, the plain
N=1 output; sizes without one compare against the majority across systems,
`ref=majority`):

| Workload | Rule |
|---|---|
| W1, idle | the `checksum=` field equals the reference exactly |
| W2 | all four result lines exact |
| W3 | `accuracy:` exact; the ssh port within \|Δ\| ≤ 0.005 (disjoint tree subsets) |
| T1 | `final loss` within 1% (relative) of the one-rank run |
| trivial | `TRIVIAL-OK` present |
| TS*, DS*, SB | `checksum=` field equals the reference exactly (or majority) |
| EL | `result_ok` is null (no reference; validates checksum consistency across runs) |
| noop, lat, fileio | exactly 1 valid result line printed and command exit code 0 |

Extra log lines around a result are ignored (only lines matching the regex are
kept); a missing result line is `result_ok: false` and still recorded.
