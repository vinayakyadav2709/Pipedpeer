# DDP switch point: one machine or a split ring

**Every number here is emulated** (equal containers, 2 CPUs each, links shaped
with tc netem on one host) unless the row says "real GPU". Emulated numbers show
where splitting starts to pay. They are not real-hardware speedups.

## Method

- Rig: `scripts/lab-emu.sh` (equal containers, `--cpus=2`, memory `TRAIN_MEM`
  (2g by default: 2 x 2g + 4 GB spare fits this 13 GB laptop; 3g does not), profiles below). Runner: `scripts/bench-lib.sh`.
- Stand-in: `train.py` from `bench-lib.sh` (`bl_workloads`), an MLP on CPU fed
  by a DataLoader, so the shim shards it the way it shards a real script.
  - Model: small = width 256, depth 2 (~0.2 M parameters, ~0.4 MB fp16 per sync).
    Large = width 1024, depth 4 (~4.5 M, ~9 MB: the demo's size).
  - Per-step compute: light = batch 256, heavy = batch 8192.
- Variants: `solo` (1 node), `split` (2 nodes, `--ddp 2`), `auto` (2 nodes, no
  flag; the `world` column shows what the auto decision chose).
- Sync cost: the `ddp_sync` steps in the run record (steps.json) when the
  binary writes one, else the shim's `ddp: N sync(s), Xs total` line.
- Metric: the script's own training time (`BENCH_ELAPSED`), median of
  `BL_REPS` (3).
- Profiles (one-way per container, so RTT is twice the delay): lan 0.25 ms at
  1000 mbit. Wi-Fi 4±2 ms at 80 mbit. 5G 15±5 ms at 100 mbit.

```sh
scripts/bench-ddp-switch.sh before        # c7e2c7f binary
scripts/bench-ddp-switch.sh after         # the tree under test
scripts/bench-ddp-switch.sh table         # medians as markdown
BL_GPU_PYTHON=<python with CUDA torch> scripts/bench-ddp-switch.sh --gpu-baseline
```

The script exits 1 unless the matrix has a cell where split wins and a cell
where solo wins.

**The batch maths changed between the two binaries.** BEFORE gives every rank
the script's whole batch (batch x world, Finding 39). AFTER splits the script's
batch across the ranks. So a BEFORE split does more work per epoch than solo,
and the two columns cannot be compared row by row as equal work.

## BEFORE (c7e2c7f), emulated

TODO(U25): filled by `bench-ddp-switch.sh before`.

## AFTER (tree under test), emulated

TODO(verify pass): filled by `bench-ddp-switch.sh after`, one heavy run at a time.

## Single-GPU baseline (real GPU, not emulated)

TODO(verify pass): `--gpu-baseline` with a CUDA torch, run on the RTX 3060.
The host's own python has torch 2.13.0+cpu.
