# Scaling map, training: how sync and compute grow

**Every number here is emulated** (equal containers, links shaped with tc
netem, all on one 15-core laptop) except the rows marked "real GPU". The lab
measures how overhead grows with one variable; it says nothing about
real-hardware speedup (that is M4: two laptops over Tailscale, demo-day runs).

## Method

- Rig: `scripts/lab-emu.sh`, `LAB_NAME=m3`, 2 containers of 2 CPUs and 2 GB
  each. Profiles are one-way per container, so RTT is twice the delay: lan
  0.25 ms at 1000 mbit, wifi 4±2 ms at 80 mbit, 5g 15±5 ms at 100 mbit.
- Binary: `30eae37` (every row of runs.csv carries it in `rev`), built with
  `CGO_ENABLED=0` from `git archive HEAD`. It predates FIX-DDPCOPY (`faf2cf0`),
  which cut the per-step CPU of a DDP rank about 4-5x (0.84-1.12 s to 0.21 s
  at 8.9 M parameters), so **every CPU-per-step number below is pre-faf2cf0**;
  bytes per round and the no-pipedpeer control are unaffected.
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

## Findings (binary 30eae37, pre-faf2cf0)

1. **Growth fits.** Exponents (all points / top 3) against the expected one:

   | sweep | metric | exponent | expected |
   |---|---|---|---|
   | size, lan / wifi / 5g | bytes/round | 1.00 / 1.00 (all three) | 1.00 |
   | size, lan | CPU/step/rank | 0.85 / 0.97 | 1.00 |
   | size, lan | shim CPU/step/rank | 1.09 / 0.99 | 1.00 |
   | size, 5g | CPU/step/rank | 0.61 / 0.71 | 1.00 |
   | size, 5g | shim CPU/step/rank | 0.47 / 0.59 | 1.00 |
   | batch, wifi | bytes/round | 0.00 / 0.00 | 0.00 |
   | batch, wifi | CPU/step/rank | 0.36 / 1.01 | 1.00 |
   | interval, wifi | CPU/step/rank | -0.19 / **0.29** | 0.00 |
   | ranks, wifi | bytes/round | 0.00 | 0.00 |
   | ranks, wifi | ring CPU/step | **1.21** | 1.00 |
   | control, size | CPU/step | 0.73 / 0.94 | 1.00 |
   | control, batch | CPU/step | 0.52 / 0.78 | 1.00 |

   The size wifi cells have no CPU columns: they ran before the CPU
   calibration existed. Sync bytes grow exactly with the model and not at all
   with batch or ranks per node.
2. **Two superlinear flags, both inside this run's noise and both pre-faf2cf0.**
   The CPU-per-step method subtracts **one** 20-step calibration run per cell,
   and that one run is noisy: the baseline configuration (width 512, batch
   1024, every step, int8, 1 rank per node, wifi) appears in four sweeps and
   gives 0.044 (batch), 0.077 (ranks), 0.105 (interval) and 0.116 (int8)
   CPU-s per step per rank. The two calibration runs behind the extremes used
   18.1 and 11.9 CPU-s for the same work.
   - interval: 0.048 CPU-s/step at every 8, 0.071 at every 16. The three full
     runs at 16 spread 14.5 to 18.5 CPU-s, and 60 steps at every 16 sync only
     3 times, so one slow round moves the whole cell. Inside the band above.
   - ranks per node: ring CPU/step 0.154, 0.421, 0.563 for 1, 2, 3 ranks per
     node (world 2, 4, 6). The excess is all in the 1 to 2 step (slope 1.45;
     2 to 3 is 0.72), and per rank it is 0.077, 0.105, 0.094. One candidate
     cause, not measured: nothing caps a rank's torch threads, so k co-located
     ranks each start threads for all 2k CPUs (`src/ddp_cmd.go:754` places
     them; no `torch.set_num_threads` / `OMP_NUM_THREADS` anywhere in the
     shim). Production places more than one rank per node only on multi-GPU
     machines.
   Both get re-measured on a post-faf2cf0 binary in the quiet rerun (with more
   than one calibration run per cell) before anyone acts on them.
3. **FIXED in faf2cf0 (FIX-DDPCOPY): the shim's per-step work outside the
   sync.** On 30eae37, at 8.9 M parameters, it cost 0.55 to 0.76 s of wall
   time per step, 2.5 to 3.6x the training itself (the control: 0.21 to
   0.25 s); a rank used 1.24 CPU-s per step against 0.57 for the control.
   py-spy put forward and backward at about 4% of rank 0's samples. The cause
   was an unused float16 copy of every gradient on the int8 path and float64
   temporaries in the int8 codec. faf2cf0 measured 0.84-1.12 s to 0.21 s per
   step. The CPU columns below show the old cost.
4. **Sync every N > 1 sends full-width weights, not int8 gradients.**
   bytes/round goes from 1.41 MB at every step to 5.4 MB at every 2 (fp32,
   about 4 bytes per parameter each way). That is by design: weights average
   at full width (`ddp.py:1989-1992`, R1 #2). Fewer, larger rounds still cut
   wall time per step from 0.25 s to 0.06 s at every 16 (wall, loaded host).
   The opening broadcast is folded into the shim line's bytes, so bytes/round
   creeps up as rounds get fewer (6.99 MB at every 16).
5. **int8 halves the wire.** off/on bytes/round is 1.92 at width 512 and 1.93
   at 2048; sync/round wall drops 0.348 to 0.211 s and 3.908 to 2.067 s.
6. **The sync path is CPU-bound on a fast link.** On lan, 19 MB per round
   takes 0.35 s (about 55 MB/s, 440 Mbit) of a 1000 Mbit link. Wi-Fi reaches
   about 72 of 80 Mbit. Linear, so no flag. Re-measure after faf2cf0.
7. **FIXED in dea4824 (FIX-DDPRECORD): DDP runs wrote no run record**
   (`app/run.go:251` saved `steps.json` only when `task.single`), so every row
   here falls back to the shim's summary line.
8. **FIXED in 35b3b99 (FIX-MACHINEID): every lab container was "1 machine"**
   (shared boot_id, so `PIPEDPEER_DDP_MACHINES=1`). With `--ddp N` (all rows
   here) the ring is fixed and the numbers stand.
9. **Cold environment: CUDA torch on CPU-only nodes.** The containers see the
   host's nvidia driver in /sys, so the environment pulls torch-cu126: a ~4 GB
   download, then ~6 GB shipped to the peer; warm-up took about 15 minutes.
10. A rank at 8.9 M parameters uses about 1 GB of RSS, at the `--mem 1G` cap.

## Results

Generated by `scripts/scaling/train.sh fit docs/benchmarks/scaling-train` on
2026-10-15 from runs.csv (binary 30eae37). Wall columns were measured on a
loaded host (the `load` column) and are not for the paper. Plots (CPU per step
against x, log-log): `docs/benchmarks/scaling-train/<sweep>-<profile>.png`.

#### size, 5g (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 3 | 2 | 60 | 143305 | 0.0457 | 0.0284 | 0.0913 | 0.211 | 0.0094 | 0.0056 | 0.2207 | 4 |
| 0.20M | 3 | 2 | 60 | 424673 | 0.0814 | 0.0585 | 0.1628 | 0.312 | 0.0156 | 0.0086 | 0.3273 | 3 |
| 0.66M | 3 | 2 | 60 | 1412082 | 0.1986 | 0.1491 | 0.3972 | 0.550 | 0.0584 | 0.0400 | 0.6084 | 3 |
| 2.36M | 3 | 2 | 60 | 5071612 | 0.1859 | 0.0334 | 0.3717 | 1.124 | 0.1675 | 0.1021 | 1.3021 | 3 |
| 8.92M | 3 | 2 | 60 | 19131269 | 1.2441 | 0.6727 | 2.4882 | 2.558 | 0.6408 | 0.3809 | 3.2015 | 2 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | 1.00 | 1.00 | 1.00 | ok |
| cpu_step_s | 0.61 | 0.71 | 1.00 | ok |
| shim_cpu_step_s | 0.47 | 0.59 | 1.00 | ok |

#### size, lan (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 6 | 2 | 60 | 143305 | 0.0201 | 0.0029 | 0.0402 | 0.006 | 0.0077 | 0.0039 | 0.0139 | 3 |
| 0.20M | 6 | 2 | 60 | 424673 | 0.0363 | 0.0134 | 0.0727 | 0.012 | 0.0169 | 0.0099 | 0.0289 | 3 |
| 0.66M | 6 | 2 | 60 | 1412082 | 0.0993 | 0.0499 | 0.1987 | 0.028 | 0.0552 | 0.0368 | 0.0830 | 3 |
| 2.36M | 6 | 2 | 60 | 5073360 | 0.3184 | 0.1659 | 0.6368 | 0.098 | 0.2129 | 0.1476 | 0.3109 | 3 |
| 8.92M | 6 | 2 | 60 | 19139133 | 1.2376 | 0.6663 | 2.4753 | 0.345 | 0.7959 | 0.5359 | 1.1409 | 3 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | 1.00 | 1.00 | 1.00 | ok |
| cpu_step_s | 0.85 | 0.97 | 1.00 | ok |
| shim_cpu_step_s | 1.09 | 0.99 | 1.00 | ok |

#### size, wifi (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 3 | 2 | 60 | 143305 | - | - | - | 0.062 | 0.0090 | 0.0052 | 0.0715 | - |
| 0.20M | 3 | 2 | 60 | 424673 | - | - | - | 0.101 | 0.0187 | 0.0117 | 0.1187 | - |
| 0.66M | 3 | 2 | 60 | 1412082 | - | - | - | 0.212 | 0.0650 | 0.0466 | 0.2775 | - |
| 2.36M | 3 | 2 | 60 | 5073360 | - | - | - | 0.615 | 0.2527 | 0.1873 | 0.8677 | - |
| 8.92M | 3 | 2 | 60 | 19146997 | - | - | - | 2.131 | 0.8622 | 0.6023 | 2.9849 | - |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | 1.00 | 1.00 | 1.00 | ok |

#### batch, wifi (emulated)

| batch | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 256 | 3 | 2 | 60 | 1412082 | 0.1306 | 0.0792 | 0.2611 | 0.212 | 0.0439 | 0.0347 | 0.2549 | 4 |
| 1024 | 3 | 2 | 60 | 1412082 | 0.0439 | 0.0000 | 0.0878 | 0.207 | 0.0483 | 0.0272 | 0.2566 | 3 |
| 4096 | 3 | 2 | 60 | 1412082 | 0.2219 | 0.0817 | 0.4439 | 0.207 | 0.1047 | 0.0299 | 0.3112 | 4 |
| 8192 | 3 | 2 | 60 | 1412082 | 0.3425 | 0.0209 | 0.6849 | 0.212 | 0.1709 | 0.0152 | 0.3829 | 3 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | -0.00 | -0.00 | 0.00 | ok |
| cpu_step_s | 0.36 | 1.01 | 1.00 | ok |
| shim_cpu_step_s | -0.27 | -1.96 | 0.00 | ok |

#### interval, wifi (emulated)

| sync every | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 2 | 60 | 1412082 | 0.1055 | - | 0.2109 | 0.206 | 0.0432 | - | 0.2494 | 2 |
| 2 | 3 | 2 | 30 | 5435118 | 0.0840 | - | 0.1681 | 0.615 | 0.0225 | - | 0.3299 | 2 |
| 4 | 3 | 2 | 15 | 5606386 | 0.0478 | - | 0.0956 | 0.615 | 0.0212 | - | 0.1745 | 2 |
| 8 | 3 | 2 | 7 | 6021822 | 0.0480 | - | 0.0960 | 0.614 | 0.0258 | - | 0.0969 | 2 |
| 16 | 3 | 2 | 3 | 6990506 | 0.0714 | - | 0.1429 | 0.629 | 0.0262 | - | 0.0576 | 2 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| cpu_step_s | -0.19 | 0.29 | 0.00 | SUPERLINEAR |

#### ranks, wifi (emulated)

| ranks/node | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 3 | 2 | 60 | 1412082 | 0.0771 | - | 0.1543 | 0.208 | 0.0452 | - | 0.2507 | 1 |
| 2 | 3 | 4 | 60 | 1412082 | 0.1053 | - | 0.4212 | 0.178 | 0.0508 | - | 0.2272 | 1 |
| 3 | 3 | 6 | 60 | 1412082 | 0.0939 | - | 0.5632 | 0.180 | 0.0579 | - | 0.2381 | 1 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| bytes_round | -0.00 | - | 0.00 | ok |
| cpu_total_step_s | 1.21 | - | 1.00 | SUPERLINEAR |

#### raw-size, none (emulated)

| params | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.07M | 3 | 1 | 0 | 0 | 0.0172 | - | 0.0172 | 0.000 | 0.0038 | - | 0.0038 | 3 |
| 0.20M | 3 | 1 | 0 | 0 | 0.0229 | - | 0.0229 | 0.000 | 0.0070 | - | 0.0070 | 4 |
| 0.66M | 3 | 1 | 0 | 0 | 0.0495 | - | 0.0495 | 0.000 | 0.0184 | - | 0.0184 | 2 |
| 2.36M | 3 | 1 | 0 | 0 | 0.1525 | - | 0.1525 | 0.000 | 0.0653 | - | 0.0653 | 3 |
| 8.92M | 3 | 1 | 0 | 0 | 0.5714 | - | 0.5714 | 0.000 | 0.2600 | - | 0.2600 | 3 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| cpu_step_s | 0.73 | 0.94 | 1.00 | ok |

#### raw-batch, none (emulated)

| batch | runs | world | rounds | bytes/round | CPU/step/rank s (pre-faf2cf0) | shim CPU/step/rank s (pre-faf2cf0) | ring CPU/step s (pre-faf2cf0) | wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 256 | 3 | 1 | 0 | 0 | 0.0514 | - | 0.0514 | 0.000 | 0.0092 | - | 0.0092 | 4 |
| 1024 | 3 | 1 | 0 | 0 | 0.0598 | - | 0.0598 | 0.000 | 0.0212 | - | 0.0212 | 4 |
| 4096 | 3 | 1 | 0 | 0 | 0.1403 | - | 0.1403 | 0.000 | 0.0748 | - | 0.0748 | 4 |
| 8192 | 3 | 1 | 0 | 0 | 0.3216 | - | 0.3216 | 0.000 | 0.1558 | - | 0.1558 | 4 |

| metric | exponent (all points) | exponent (top 3) | expected | flag |
|---|---|---|---|---|
| cpu_step_s | 0.52 | 0.78 | 1.00 | ok |

#### int8 on vs off (emulated)

| profile | width | bytes/round on | off | off/on | CPU/step/rank s on (pre-faf2cf0) | off | wall: sync/round s on | off |
|---|---|---|---|---|---|---|---|---|
| wifi | 512 | 1412082 | 2717559 | 1.92 | 0.1157 | 0.0985 | 0.211 | 0.348 |
| wifi | 2048 | 19131269 | 36874922 | 1.93 | 0.9520 | 1.1643 | 2.067 | 3.908 |

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

- interval/wifi: cpu_step_s grows as x^0.29 (expected 0.00)
- ranks/wifi: cpu_total_step_s grows as x^1.21 (expected 1.00)
