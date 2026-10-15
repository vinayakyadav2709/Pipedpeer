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

QUIET-RERUN, 2026-10-16, idle host (no other lab or agent run; the load
column is the lab's own daemons). Only the four cells the paper cites: the two
by-design extremes. Medians of 3, 2-CPU 2 GB containers. Rows in
`~/.local/state/pipedpeer/bench-quiet/ddp-switch/`.

| cell | variant | profile | nodes | runs ok | wall s | script s | step s | sync s | world | loss | host load |
|---|---|---|---|---|---|---|---|---|---|---|---|
| small-heavy | solo | lan | 1 | 3/3 | 12.81 | 7.63 | 0.13 | - | 1 | 0.11 | 0.70 |
| small-heavy | split | lan | 2 | 3/3 | 10.56 | 5.73 | 0.19 | 1.50 | 2 | 0.16 | 1.78 |
| large-light | solo | lan | 1 | 3/3 | 10.36 | 4.71 | 0.08 | - | 1 | 0.26 | 0.99 |
| large-light | split | wifi | 2 | 3/3 | 74.08 | 69.41 | 2.31 | 60.60 | 2 | 0.34 | 0.43 |

The split wins small-heavy (10.6 s against 12.8 s). One node wins large-light
by 7x (10.4 s against 74.1 s): 60.6 s of the split's 69.4 s script is fp16
sync over wifi. A BEFORE split still trains on the whole batch per rank (see
above), so its loss is not the same work as solo's.

```sh
ONLY="small-heavy:solo small-heavy:split:lan large-light:solo large-light:split:wifi" \
  BL_EMU=scripts/lab-emu-warm.sh LAB_WARM_SET=bench-before scripts/bench-ddp-switch.sh before
```

## AFTER (tree under test), emulated

QUIET-RERUN, 2026-10-16, the same session and cells as BEFORE, binary
90dac09. At 6a24715 the large-light split failed on every run: the int8 codec
sent a zero scale for a block whose peak was subnormal, and the daemon refused
it with "segment 498 has a non-positive scale". 90dac09 fixes that. No AFTER log
(ddp-switch or diloco) has the one-node fallback line ("running on one node",
"running single-node"): every split formed its ring.

| cell | variant | profile | nodes | runs ok | wall s | script s | step s | world | loss | host load |
|---|---|---|---|---|---|---|---|---|---|---|
| small-heavy | solo | lan | 1 | 3/3 | 11.84 | 8.09 | 0.13 | 1 | 0.11 | 1.47 |
| small-heavy | split | lan | 2 | 3/3 | 9.11 | 5.32 | 0.09 | 2 | 0.11 | 3.00 |
| large-light | solo | lan | 1 | 3/3 | 7.98 | 4.32 | 0.07 | 1 | 0.26 | 1.64 |
| large-light | split | wifi | 2 | 3/3 | 80.65 | 77.14 | 1.29 | 2 | 0.21 | 1.05 |

The split wins small-heavy, 9.1 s against 11.8 s (1.3x). One node wins
large-light, 8.0 s against 80.7 s (10x): the 9 MB sync over wifi costs far
more than the step it shares. Both outcomes appear, so the script exits 0. The
AFTER split halves the batch per rank, so it runs twice the steps of BEFORE's
split: its step is shorter (1.29 s against 2.31 s), but the script is longer.

```sh
ONLY="small-heavy:solo small-heavy:split:lan large-light:solo large-light:split:wifi" \
  BL_EMU=scripts/lab-emu-warm.sh LAB_WARM_SET=bench-after scripts/bench-ddp-switch.sh after
```

## Single-GPU baseline (real GPU, not emulated)

TODO(verify pass): `--gpu-baseline` with a CUDA torch, run on the RTX 3060.
The host's own python has torch 2.13.0+cpu.
