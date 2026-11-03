# Lab campaign workloads

The workloads, sizes and per-system settings for every experiment in [docs/benchmarks/lab-campaign.md](../../docs/benchmarks/lab-campaign.md) (sections 1.2 and 1.3). The machine-readable copy is [`scripts/campaign/workloads.json`](../../scripts/campaign/workloads.json), which the runner reads. Change both together, and never change a size between systems.

**Sizes are provisional (laptop). Recalibrate on pc01 in session 1** with `scripts/campaign/calibrate.sh W1|W2|W3`, then put the printed sizes in `workloads.json` and in the table below.

## Workloads

| Id | Script (plain and pipedpeer run it unchanged) | Ray port | Dask port | Size variable | Result line(s) compared |
|---|---|---|---|---|---|
| W1 | `docs/benchmarks/workloads/pool_map.py` | `baselines/w1_ray.py` | `baselines/w1_dask.py` | `BENCH_ITEMS` | `items=… checksum=…` |
| W2 | `pipedpeer-demo/cpu.py` | `baselines/w2_ray.py` | `baselines/w2_dask.py` | `CPU_ITEMS` (the Pool section's items) | the `Pool:`, `joblib:`, `numpy:` and `pandas:` lines, without the timings |
| W3 | `pipedpeer-demo/01_sklearn_rf.py` | `baselines/w3_ray.py` | `baselines/w3_dask.py` | `RF_TREES` (`RF_SAMPLES` stays 200,000) | `data: …` and `accuracy: …` |
| T1 | `baselines/t1_train.py`, `BENCH_MODEL=ddp04` (MLP-C) or `tfm` (transformer) | none | none (torchrun: `baselines/t1_torchrun.py`) | `BENCH_EPOCHS` (smoke only) | `BENCH_LOSS …`, present only |

Paths under `baselines/` are in `docs/benchmarks/workloads/baselines/`. Its [README](../../docs/benchmarks/workloads/baselines/README.md) says what each port changed and why.

### T1: where MLP-C and the transformer live

`scripts/bench-train.py`, which the plan names, trains a third model: a 4-layer MLP of width 512 on 122,880 samples with a global batch of 61,440, split by `PIPEDPEER_WORLD_SIZE`. It is neither of the paper's two CPU models. MLP-C and the transformer are the `ddp04` and `tfm` branches of `diloco.py`, the heredoc in `scripts/bench-diloco.sh` that the paper's DiLoCo numbers come from. `baselines/t1_train.py` is a verbatim copy of it, and `port-lines.sh` fails if the two drift.

| Variant | Select | Model | Data, batch, epochs, optimiser |
|---|---|---|---|
| MLP-C | `BENCH_MODEL=ddp04` | MLP 64→512→512→512→1, 559,105 params | 60,000 train + 6,000 val, batch 2048, 6 epochs, SGD 0.1 momentum 0.9 |
| Transformer | `BENCH_MODEL=tfm` | 2-layer encoder, d 64, 4 heads | 40,000 + 4,000, batch 256, 2 epochs, AdamW 1e-3 |

Other knobs: `BENCH_SEED` (default 1; the data is the same for every seed), `BENCH_THREADS` (torch threads, default 2), `BENCH_EPOCHS`. `scripts/sim-diloco-ranks.py` holds the same two models (`ddp04`, `tfm`) for in-process simulation; it is not a runnable training job.

E8, per rank count R in 2, 4, 8:

- pipedpeer exact: `pipedpeer run -v --ddp R t1_train.py -e BENCH_MODEL=<m> -e BENCH_SEED=<s>`. Fast sync adds `--ddp-sync fast`. With `-v` the shim prints `… MiB sent` per rank, as `bench-diloco.sh` parses it.
- torchrun: `torchrun --nnodes=R --nproc-per-node=1 --rdzv-backend=c10d --rdzv-endpoint=pc00:29500 t1_torchrun.py` on R PCs, same env vars. It keeps the global batch (each rank takes `batch // R`), as pipedpeer's ring does. torchrun reports no bytes, so take bytes sent per rank from the host network counters (OTel `hostmetrics`).
- Reference: `t1_train.py` alone (1 rank).

T1's loss differs between 1 and R ranks by design: the sharding and shuffling differ. Its `result_compare` is therefore `present`, meaning the run printed a loss. The loss gap is what E8 measures.

## Sizes

Strong scaling: one PC alone takes 100–150 s (target 125 s). Weak scaling (W1 only): about 60 s of work per PC, so items = per-PC size × N.

| Workload | Strong size | Weak size per PC | Fit on the laptop | Strong size measured on the laptop |
|---|---|---|---|---|
| W1 | `BENCH_ITEMS=154055` | `BENCH_ITEMS=73759` × N | 0.29 s + 0.00081 s × items | 133.5 s |
| W2 | `CPU_ITEMS=1209` | none | 5.80 s + 0.0986 s × items | 113.5 s |
| W3 | `RF_TREES=1583` | none | 1.51 s + 0.078 s × trees | 128.7 s |

Provisional, laptop; recalibrate on pc01 in session 1. The laptop has 20 threads and 31 GB, and it was shared with other agents during the runs. Runs were in the `nixos/nix:2.31.3` container on the campaign env, with a 4 GiB memory limit and no CPU limit. Two calibrations 20 minutes apart gave W1 sizes of 164,307 and 154,055 (7 % apart), W2 1,118 and 1,209, and W3 1,578 and 1,583. Read the laptop's noise as about ±8 %.

`calibrate.sh` doubles the size from the smoke size until one run takes at least 8 s, then runs once more at 3× that size. Each point is the best of 2 runs. It fits wall = c0 + c1 × size and solves for 125 s and 60 s. With `CAL_VERIFY=1` it also runs the strong size once, which gives the last column above. Run it on pc01 with `CAL_VERIFY=1`, and if the check falls outside 100–150 s, run it again.

## Per-system settings (fairness)

The comparison gives every system all the cores of the PCs it may use, and runs no system's work on cores the others lack.

| | plain | pipedpeer | ray | dask |
|---|---|---|---|---|
| Machines doing work at N | pc00 alone (N=1 only) | pc00 + N−1 workers (pc00 is one of the N) | N workers; the head on pc00 has `--num-cpus=0` | N workers; the scheduler on pc00 runs no tasks |
| Start | none | the daemons of the other PCs are stopped (`pipedpeer stop`) | `ray start --head --num-cpus=0` on pc00; `ray start --address=pc00:6379 --num-cpus=$(nproc)` per worker | `dask scheduler` on pc00; `dask worker tcp://pc00:8786 --nworkers $(nproc) --nthreads 1` per worker |
| Address env | none | none | `RAY_ADDRESS=pc00:6379` | `DASK_SCHEDULER=tcp://pc00:8786` |
| W1 `BENCH_PROCS` | nproc of pc00 | nproc of pc00 (what a user writes; the shim spills beyond it) | sum of nproc over the N workers, one Ray actor per core | unused (`client.map` uses every worker process) |
| W2 Pool width | `os.cpu_count()` of pc00 | same, unchanged script | `Pool()`: all cluster CPUs | every worker process |
| joblib `n_jobs=-1` (W2, W3) | pc00's cores | the shim decides | all cluster CPUs | all worker processes |

Why these settings:

- **Dask runs one single-threaded process per core.** W1 and W2's Pool items are pure Python and hold the GIL, so threads in one worker process would serialise. The worker memory limit stays at its default, which is RAM × nthreads / cores per process, so the processes on one PC share all its RAM. Do not start fewer worker processes than cores without an explicit `--memory-limit`. The default would give each process too little memory: in the container, 4 workers on a 20-core host got 205 MiB each, were restarted mid-fit, and W3 failed with `FutureCancelledError … already forgotten`.
- **W1 `BENCH_PROCS` follows the system.** The plain and pipedpeer number is what the user's own machine has, because pipedpeer runs the user's unchanged script. Ray's `Pool(n)` creates exactly n actors cluster-wide, so with pc00's nproc it would leave most of the cluster idle.
- **The runner states the asymmetry.** pipedpeer's N counts pc00, so pipedpeer at N uses N PCs including the submitter, and Ray and Dask at N use N PCs besides pc00.
- **W2's numpy and pandas sections are serial in the original.** No port moves them, so in Ray and Dask they run on pc00. pipedpeer's shim may move them; that is part of what it does without code changes.
- **Weak-scaling references.** `result_ok` compares a run with the plain run at the same size. Weak sizes at large N are too long for plain on one PC (32 × 60 s). W1's checksum is the same on any x86-64 machine running the same env (IEEE floats, the same glibc from the closure), so compute each weak size's plain reference once, off the clock: on idle lab PCs (pc33+), or on the driver beforehand once the sizes are final.

### Known cost: Dask runs W1 one task per item

At 38,400 items on the same 20 cores in the container, W1 took 31.4 s plain, 36.8 s on Ray (one head, 20 CPUs) and 49.7 s on Dask (20 worker processes). That is about 0.5 ms of scheduler cost per item, with all three printing the same checksum. At the strong size (about 154,000 items of about 16 ms CPU each), Dask pays that cost on every item, and its scheduler, a single process, becomes the limit as N grows. This is a property of the idiom the plan fixes (`client.map`), not a misconfiguration. The write-up should say so. Dask's own chunking idioms (`dask.bag.from_sequence(..., npartitions=…)`) would avoid it at a larger port.

## Result check

Measured in the container on the campaign env, with a local Ray head (`--num-cpus=4`), a local Dask scheduler with 4 single-threaded workers, and `BENCH_PROCS=4`. Results are from `scripts/campaign/env/smoke.py 4`, run twice with identical results; walls are from the second run.

| Workload (smoke size) | plain | ray | dask | Result (identical in all three) |
|---|---|---|---|---|
| W1 (`BENCH_ITEMS=400`) | 0.8 s | 2.6 s | 1.4 s | `items=400 checksum=3937849787.468795` |
| W2 (`CPU_ITEMS=8`) | 6.7 s | 10.0 s | 9.3 s | `Pool: 8 items, checksum 239999952` / `joblib: 64 Mandelbrot tiles, 217451260 points inside` / `numpy: largest singular value 1152.7576` / `pandas: 3,000,000 rows -> 50 cities, total rain 5996596.8` |
| W3 (`RF_TREES=8`) | 2.5 s | 6.2 s | 6.6 s | `data: (200000, 20), classes: [ 30506 169494]` / `accuracy: 0.9666` |

| T1 (`BENCH_EPOCHS=1`) | plain, 1 rank | torchrun, 2 ranks (`--standalone`) |
|---|---|---|
| MLP-C | 4.7 s, `BENCH_LOSS 0.101025` | 5.9 s, `BENCH_LOSS 0.097706` |
| Transformer | 14.1 s, `BENCH_LOSS 0.278322` | 13.4 s, `BENCH_LOSS 0.282651` |

Small-size walls measure start-up, not throughput: Ray and Dask connect to their clusters and ship the function.

## Schema of `scripts/campaign/workloads.json`

```text
{
  "schema": 1,
  "doc": "results/lab/workloads.md",
  "sizes_status": "<provisional or calibrated, and where>",
  "workloads": [ <workload>, ... ]
}

<workload>:
  id              "W1" | "W2" | "W3" | "T1"
  script          the original, repo-relative; plain and pipedpeer run it unchanged
  args            extra argv after the script (all [] today)
  ports           {system: repo-relative path}: "ray", "dask" for W1-W3; "torchrun" for T1
  variants        T1 only: {name: {env}}; run each variant as its own workload
  size_var        the env var that sets the size
  strong          {env} for strong scaling (E1/E2, E4, E6, ...); null for T1
  weak_per_pc     {env} per PC for weak scaling (E1b); multiply each value by N; null if none
  smoke           {env} for a seconds-long run (rehearsal, smoke.py, calibrate.sh's start)
  system_env      {system: {env}}; values may hold {nproc_submitter} (nproc of pc00) and
                  {nproc_workers} (sum of nproc over the N worker PCs); an absent var means unset
  result_regex    Python regex; result = "\n".join(m.group(0) for m in re.finditer(rx, stdout))
  result_compare  "equal": result_ok = result == the plain run's at the same size
                  "present": result_ok = the result is non-empty (T1)
  timing_regex    Python regex whose group 1 is the script's own compute time in seconds
```

All env values are strings. The env for a run is the size env (`strong`, `weak_per_pc` × N, or `smoke`), then `system_env[system]` with the placeholders filled in, then `RAY_ADDRESS` or `DASK_SCHEDULER`. Run every system with `$HOME/.local/share/pipedpeer-lab/env/bin/python3` (pipedpeer builds its own closure from the same nixpkgs pin).

## Environment

`scripts/campaign/env/flake.nix`, pinned to nixpkgs `56c02bc00adcf003215cc4bd996d6efaf4cff188`, the pin pipedpeer itself uses (`src/internal/nixgen/flake.go`, `nixpkgsRef`). `scripts/campaign/env/install.sh` builds it to `$HOME/.local/share/pipedpeer-lab/env` and prints `LAB-ENV <store path>`; `install.sh remove` undoes it.

- Store path: `/nix/store/0zqpnkvfnkzppxba6dbykyl4b8qcrhp0-python3-3.14.7-env`; closure 2.5 GiB, 187 paths. The largest are triton (745 MiB, a dependency of nixpkgs' torch even on CPU), scikit-learn, scipy, pandas and torch's lib output.
- Versions: Python 3.14.7, numpy 2.5.1, pandas 3.0.4, scikit-learn 1.8.0, joblib 1.5.3, torch 2.12.0 (CPU, `cuda.is_available()` False), ray 2.57.0 with the `default` extras, dask and distributed 2026.7.1. All come from the pinned nixpkgs and its binary cache, with no overlay and no second pin.
- Install times in the container: a cold `install.sh` took 65 s, the first `nix build` with flake evaluation took 2 min 21 s, and a rerun takes 1.4 s and prints the same path.
- `install.sh remove` deletes the link and every store path of the closure that nothing else roots. In the container, nix refused 30 paths as still alive, which is expected for paths other roots hold. The other 2.3 GB were freed.
