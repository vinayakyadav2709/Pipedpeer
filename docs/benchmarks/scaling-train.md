# Scaling map, training: how sync and compute grow

**Every number here is emulated** (equal containers, links shaped with tc
netem, all on one 15-core laptop) except the rows marked "real GPU". The lab
measures how overhead grows with one variable; it says nothing about
real-hardware speedup (that is M4: two laptops over Tailscale, demo-day runs).

## Method

- Rig: `scripts/lab-emu.sh`, `LAB_NAME=m3`, 2 containers of 2 CPUs and 2 GB
  each. Profiles are one-way per container, so RTT is twice the delay: lan
  0.25 ms at 1000 mbit, wifi 4±2 ms at 80 mbit, 5g 15±5 ms at 100 mbit.
- Binary: `30eae37`, built with `CGO_ENABLED=0` from `git archive HEAD`.
- Stand-in: `train.py` from `scripts/bench-lib.sh`. It is an MLP on CPU fed by
  a DataLoader, so the shim shards it the way it shards a real script. Each
  run is 60 optimizer steps, `--ddp 2` (a fixed ring of 2 ranks), and
  `--mem 1G` per rank (512M in the ranks sweep).
- Baseline: width 512, depth 2 (0.66 M parameters), the script's batch 1024
  (the ring splits it, 512 per rank), sync every step, int8 with error
  feedback on, 1 rank per node, wifi.
- One variable at a time, everything else at the baseline:
  - model size: width 128 to 2048 (0.07 M to 8.9 M parameters), under lan,
    wifi and 5g;
  - batch: 256 to 8192;
  - sync interval: `PIPEDPEER_DDP_SYNC_EVERY` 1 to 16;
  - int8: `PIPEDPEER_DDP_INT8=0` against on, at width 512 and 2048;
  - ranks per node: 1 to 3. This stands in for GPUs per node. Each rank gets
    2 CPUs of its own (`docker update --cpus`), and the CLI is run with
    `PIPEDPEER_DDP_RANKS_PER_NODE`.
- Control (`raw`): the same `train.py` with no pipedpeer at all, run by the
  job environment's own `<store>/bin/run` inside node 1 at each rank's share of
  the batch. The **shim/step** column is the ring's compute per step minus
  this control: the time pipedpeer adds to a step outside the sync round trip.
- What each column means:
  - sync/round and bytes/round: the `ddp_sync` steps of the run record when
    one exists. DDP runs write no run record today (see the findings below),
    so every row here falls back to each rank's
    `ddp: N sync(s), Xs total (Y ms each), A MiB sent, B MiB received` line
    (`PIPEDPEER_VERBOSE=1`), averaged over the ranks. The bytes include the
    opening weight broadcast.
  - CPU/step/rank: the CPU seconds both containers used (each container's
    cgroup `cpu.stat`: daemon, ranks and CLI together). It is the median of
    the 3 full runs (60 steps) minus one calibration run of 20 steps, divided
    by the 40 extra steps and by the rank count. Start-up (python, the torch
    import, the sandbox, the opening broadcast) cancels out.
  - shim CPU/step/rank: CPU/step/rank minus the same quantity for the
    no-pipedpeer control.
  - wall columns: the script's elapsed time on the last rank to start, minus
    rounds × sync/round, divided by the step count. They are reported next to
    the host's 1-minute load when the run started.
  - Every cell is the median of 3 runs. The bench lock
    (`~/.cache/pipedpeer-bench.lock`) is held per timed run.
- **Fits use CPU time and bytes only, never wall time.** Other agents shared
  the laptop during these runs (load average 4 to 37 on 15 cores). Load
  stretches wall time, by up to 5x in one 5g cell, but it barely moves the
  CPU a run needs. Wall numbers for the paper come from the quiet rerun below.
- Fit: the least-squares slope of log(metric) against log(x), over all
  points and over the top 3 points (`slope` from `scripts/scaling/fit.py`).
  A metric is flagged superlinear when either slope beats the expected
  exponent by more than 0.15 (the M2 rule). For model size, x is the
  parameter count.

```sh
M3_KEEP=1 scripts/scaling/train.sh run      # all sweeps (or: run size batch ...)
M3_REV=<rev> scripts/scaling/train.sh raw   # the no-pipedpeer control, same lab
BL_GPU_PYTHON=~/.cache/pipedpeer-bench/gpu-venv/bin/python scripts/scaling/train.sh gpu
scripts/scaling/train.sh fit docs/benchmarks/scaling-train   # tables, flags, plots
```

Raw rows: `~/.local/state/pipedpeer/bench/scaling-train/runs.csv` and `gpu.csv`.
Each run's log sits next to them under `logs/`.

### Quiet rerun (wall times for the paper)

Every wall number below was measured while other agents loaded the host. The
paper's wall numbers come from one rerun at the end, when nothing else runs.
It goes into a fresh results directory, so its medians never mix with these:

```sh
export BL_OUT=~/.local/state/pipedpeer/bench-quiet
M3_KEEP=1 scripts/scaling/train.sh run && scripts/scaling/train.sh raw
scripts/scaling/train.sh fit docs/benchmarks/scaling-train
# a subset: ONLY="size:wifi:* size:5g:* raw-size:none:*" scripts/scaling/train.sh run size
```

All rows here are paper rows. `ONLY` takes globs over `sweep:profile:x`.
Afterwards, take the lab down with `LAB_NAME=m3 scripts/lab-emu.sh down`.

## Findings (binary 30eae37)

1. **No superlinear growth so far.** bytes/round grows as params^1.00 under
   lan, wifi and 5g. CPU/step/rank grows as params^0.85 over all points and
   params^0.97 over the top 3. Batch, interval, int8 and ranks per node: see
   the tables (pending, below).
2. **The shim's per-step work outside the sync is linear, but large.** At
   8.9 M parameters it costs 0.55 to 0.76 s of wall time per step, 2.5 to 3.6x
   the training itself (the no-pipedpeer control: 0.21 to 0.25 s). In CPU, a
   rank uses 1.24 CPU-s per step against about 0.5 for the control.
   py-spy on rank 0 (width 2048, wifi, 20 s at 100 Hz) puts forward and
   backward at about 4% of the samples. Two lines cost as much as that:
   - `ddp.py:1496` makes a float16 copy of every gradient on every step, even
     on the default int8 path (`ddp.py:1502`), which never uses it. That is
     about 0.25 s per step at 8.9 M.
   - The int8 codec, `_int8_encode` and `_int8_decode` (`ddp.py:46-51` and
     `:58-63`), builds float64 temporaries the size of the model: a
     `np.repeat` of the scales and a float64 dequantised copy.
   Proposed fix unit: build `wire` only after the int8 branch, and quantise
   per block with a float32 `reshape` instead of a float64 `repeat`. For
   before/after, use this sweep's `shim CPU/step/rank` at width 2048.
3. **The sync path is CPU-bound on a fast link.** On lan, 19 MB per round
   takes 0.35 to 0.37 s (about 50 MB/s, 400 Mbit) of a 1000 Mbit link. Wi-Fi
   reaches about 72 of 80 Mbit. Linear, so no flag.
4. **DDP runs write no run record.** `app/run.go:251` saves `steps.json` only
   when `task.single`, and DDP ranks are not single. So C24's `ddp_sync` steps
   never reach a record, and this sweep and U25's ddp-switch both fall back to
   the shim's summary line. Proposed fix unit: in `runDDP`, merge the ranks'
   receipts into one record.
5. **Lab artifact: every container is "1 machine".** The containers share the
   host's boot_id, so `PIPEDPEER_DDP_MACHINES=1`, and the shim reports "2
   ranks on 1 machine(s)". Anything that depends on the machine count, such as
   auto mode's verdict or U25's `auto` column, is skewed in the lab. With
   `--ddp N` (all rows here) the ring is fixed and the numbers stand.
6. **Cold environment: CUDA torch on CPU-only nodes.** The containers see the
   host's nvidia driver in /sys ("nvidia driver bound", no nvidia-smi), so
   the environment pulls torch-cu126: a ~4 GB download, then ~6 GB shipped to
   the peer. The warm-up took about 15 minutes.
7. A rank at 8.9 M parameters uses about 1 GB of RSS, at the `--mem 1G` cap.

## Results

The 5g size sweep, batch, interval, int8, ranks per node and the control's
CPU calibration are **still running** (`scripts/scaling/train.sh run` in the
`m3` lab). The tables below are re-generated with
`scripts/scaling/train.sh fit docs/benchmarks/scaling-train` once they finish.


#### size, 5g (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s | shim CPU/step/rank s | ring CPU/step s | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 3 | 2 | 60 | 143305 | 0.0457 | - | 0.0913 | 0.211 | 0.0094 | 0.0055 | 0.2207 | 4 |
| 0.20M | 3 | 2 | 60 | 424673 | 0.0814 | - | 0.1628 | 0.312 | 0.0156 | 0.0023 | 0.3273 | 3 |
| 0.66M | 3 | 2 | 60 | 1412082 | 0.1986 | - | 0.3972 | 0.550 | 0.0584 | 0.0386 | 0.6084 | 3 |
| 2.36M | 3 | 2 | 60 | 5071612 | 0.1859 | - | 0.3717 | 1.124 | 0.1675 | 0.1021 | 1.3021 | 3 |
| 8.92M | 1 | 2 | 60 | 19131269 | - | - | - | 2.558 | 0.6440 | 0.3963 | 3.2015 | 1 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | 1.00 | 1.00 | 1.00 | ok |

#### size, lan (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s | shim CPU/step/rank s | ring CPU/step s | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 6 | 2 | 60 | 143305 | 0.0201 | - | 0.0402 | 0.006 | 0.0077 | 0.0038 | 0.0139 | 3 |
| 0.20M | 6 | 2 | 60 | 424673 | 0.0363 | - | 0.0727 | 0.012 | 0.0169 | 0.0036 | 0.0289 | 3 |
| 0.66M | 6 | 2 | 60 | 1412082 | 0.0993 | - | 0.1987 | 0.028 | 0.0552 | 0.0355 | 0.0830 | 3 |
| 2.36M | 6 | 2 | 60 | 5073360 | 0.3184 | - | 0.6368 | 0.098 | 0.2129 | 0.1476 | 0.3109 | 3 |
| 8.92M | 6 | 2 | 60 | 19139133 | 1.2376 | - | 2.4753 | 0.345 | 0.7959 | 0.5482 | 1.1409 | 3 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | 1.00 | 1.00 | 1.00 | ok |
| cpu_step_s | 0.85 | 0.97 | 1.00 | ok |

#### size, wifi (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s | shim CPU/step/rank s | ring CPU/step s | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 3 | 2 | 60 | 143305 | - | - | - | 0.062 | 0.0090 | 0.0051 | 0.0715 | - |
| 0.20M | 3 | 2 | 60 | 424673 | - | - | - | 0.101 | 0.0187 | 0.0054 | 0.1187 | - |
| 0.66M | 3 | 2 | 60 | 1412082 | - | - | - | 0.212 | 0.0650 | 0.0452 | 0.2775 | - |
| 2.36M | 3 | 2 | 60 | 5073360 | - | - | - | 0.615 | 0.2527 | 0.1873 | 0.8677 | - |
| 8.92M | 3 | 2 | 60 | 19146997 | - | - | - | 2.131 | 0.8622 | 0.6145 | 2.9849 | - |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | 1.00 | 1.00 | 1.00 | ok |

#### raw-size, none (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s | shim CPU/step/rank s | ring CPU/step s | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 2 | 1 | 0 | 0 | - | - | - | 0.000 | 0.0039 | - | 0.0039 | 3 |
| 0.20M | 2 | 1 | 0 | 0 | - | - | - | 0.000 | 0.0133 | - | 0.0133 | 4 |
| 0.66M | 2 | 1 | 0 | 0 | - | - | - | 0.000 | 0.0198 | - | 0.0198 | 2 |
| 2.36M | 1 | 1 | 0 | 0 | - | - | - | 0.000 | 0.0653 | - | 0.0653 | 3 |
| 8.92M | 1 | 1 | 0 | 0 | - | - | - | 0.000 | 0.2477 | - | 0.2477 | 3 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|

#### raw-batch, none (emulated)

| batch | runs | world | rounds | bytes/round | CPU/step/rank s | shim CPU/step/rank s | ring CPU/step s | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 256 | 1 | 1 | 0 | 0 | - | - | - | 0.000 | 0.0070 | - | 0.0070 | 3 |
| 1024 | 1 | 1 | 0 | 0 | - | - | - | 0.000 | 0.0177 | - | 0.0177 | 3 |
| 4096 | 1 | 1 | 0 | 0 | - | - | - | 0.000 | 0.0748 | - | 0.0748 | 2 |
| 8192 | 1 | 1 | 0 | 0 | - | - | - | 0.000 | 0.1495 | - | 0.1495 | 3 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|

#### single-GPU baselines (real RTX 3060 Laptop GPU, no pipedpeer; not emulated)

| sweep | x | params | step s |
|---|---|---|---|
| size | 128 | 66049 | 0.00986 |
| size | 256 | 197633 | 0.00872 |
| size | 512 | 657409 | 0.00854 |
| size | 1024 | 2363393 | 0.00867 |
| size | 2048 | 8921089 | 0.01449 |
| batch | 256 | 657409 | 0.00671 |
| batch | 1024 | 657409 | 0.00889 |
| batch | 4096 | 657409 | 0.02764 |
| batch | 8192 | 657409 | 0.06848 |

#### superlinear flags

none
