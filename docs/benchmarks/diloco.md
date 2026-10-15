# DiLoCo quality: `--ddp-sync fast` against one device

**Question:** does `--ddp 2 --ddp-sync fast` (DiLoCo, U31) end within 1%
relative validation loss of training on one device? The bar is the mean over
3 seeds on every test model, reported with the spread between seeds (Decisions,
2026-10-15). U32 (promotion to auto) and U33 (Streaming DiLoCo) stay blocked
unless every model passes.

**Answer (DILOCO-TUNE, 2026-10-15): yes, with the new defaults** (plain
averaging every 64 steps after a 10-step warm-up): every model passes under
lan and wifi with 9-15x fewer bytes than exact sync. See
[Tuning](#tuning-diloco-tune-the-new-defaults). The Results and Diagnosis
sections below are the earlier runs of the paper's defaults.

**More ranks (DILOCO-MANY, 2026-10-15): the interval now depends on the
ring size** — every 32 steps for 2 ranks, 16 for 3, 4 for 4, and no fast
sync at all from 5 ranks up (nothing passed at 8). See
[Ranks](#ranks-diloco-many-the-interval-by-ring-size).

## Method

- Driver: `scripts/bench-diloco.sh` (runner `scripts/bench-lib.sh`, rig
  `scripts/lab-emu.sh`). Binary: CGO_ENABLED=0, built from a `git archive`
  of the commit named in the results.
- Models. These are CPU stand-ins with the same architecture, data,
  optimizer, batch and epochs as the demos. A held-out validation set is
  added where the demo had none.
  - `gpu`: `pipedpeer-demo/gpu.py`. MLP 64-512-512-512-1, 200k samples,
    batch 4096, SGD 0.05 with momentum 0.9, MSE, 3 epochs (147 steps).
  - `ddp04`: `pipedpeer-demo/04_torch_ddp.py` at its CPU sizing. 60k samples,
    same MLP shape, batch 2048, SGD 0.1, 6 epochs (180 steps). The demo's
    default of 3 epochs is 90 steps, mostly DiLoCo's 50-step warm-up.
  - `tfm`: a small transformer encoder (2 layers, d 64, 4 heads, sequence
    32) classifying the majority token, with 10% label noise so the loss
    stays off zero. AdamW 1e-3, batch 256, 2 epochs (314 steps). It
    exercises the Adam-moment sync.
- Seeds 1, 2 and 3 change the initialisation and shuffling. The data is
  fixed.
- Arms. All use the same batch, learning rate and epochs: the shim splits
  the script's batch across ranks and never scales it.
  - `single`: one container, no `--ddp`. This is the "one GPU" reference
    for the 1%.
  - `exact`: `--ddp 2` with per-step sync. It is a control that separates
    sharding from DiLoCo.
  - `fast`: `--ddp 2 --ddp-sync fast`, under the wifi and 5g profiles.
  - `gpu`: the single run on this machine's RTX 3060 Laptop GPU (CUDA torch
    venv, not emulated). It shows how much CPU-vs-GPU numerics alone move
    the loss.
- Containers: 2 CPUs each, 850 MB (the first run gave single 1.1 GB).
  Memory is not a loss variable; other
  labs held the rest of the host's memory cap. Every lab run passes
  `--gpu off`, because the containers see the host's NVIDIA driver and would
  otherwise fetch the CUDA wheel set.
- The fast arm's loss is rank 0's model, the one the script prints. "worst
  rank gap" is the worse of the two ranks. In the first run, with no
  OneCycleLR, the ranks could finish up to one interval apart. Since the
  end-of-epoch sync they finish on the same model.
- **Wall times are unlocked, not for the paper.** The loss runs did not hold
  the shared bench lock (loss does not depend on CPU contention), and every
  wall time and byte count is EMULATED.

## Results

Rerun 2026-10-15 on the binary built from `180edd8`: FIX-PARTIAL (`ad57683`,
the ring trains the last partial batch) and DILOCO-DIAG's end-of-epoch sync
(below) are in. `fast` is the paper defaults (outer Nesterov, lr 0.7,
momentum 0.9). `plain` is the alternative from the diagnosis: outer lr 1, no
momentum (`-e PIPEDPEER_DILOCO_OUTER_LR=1 -e PIPEDPEER_DILOCO_MOMENTUM=0`),
same schedule. Rows come from `~/.local/state/pipedpeer/bench/diloco/` (newest
row per seed). Gaps are relative to the same seed's `single` loss. Every wall
time and byte count is **emulated and unlocked, not for the paper**. The
verdict column judges only `fast`, as the 1% rule is written; the table
script marks a gap below one device as PASS too.

| model | arm | profile | seeds | val loss mean | min-max | std | gap vs single (mean) | per-seed gaps | worst rank gap | wall s | MiB sent/rank | syncs | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ddp04 | gpu (real, not emulated) | - | 3 | 0.0200 | 0.0198-0.0203 | 0.0002 | -0.16% | - | - | - | - | - | reference noise |
| ddp04 | single | - | 3 | 0.0200 | 0.0198-0.0203 | 0.0002 | - | - | - | 27 | 0.0 | 0 | - |
| ddp04 | exact | wifi | 3 | 0.0198 | 0.0190-0.0211 | 0.0009 | -0.85% | -3.85% +3.91% -2.72% | +3.91% | 52 | 98.9 | 180 | - |
| ddp04 | fast | 5g | 3 | 0.0228 | 0.0193-0.0281 | 0.0039 | +13.90% | +42.03% -5.11% +5.30% | +42.03% | 56 | 58.5 | 83 | FAIL |
| ddp04 | fast | wifi | 3 | 0.0298 | 0.0208-0.0471 | 0.0122 | +49.01% | +137.66% +2.39% +8.24% | +137.66% | 29 | 57.9 | 83 | FAIL |
| ddp04 | plain | 5g | 3 | 0.0195 | 0.0190-0.0204 | 0.0006 | -2.39% | -4.18% +0.36% -3.41% | +0.36% | 69 | 76.7 | 104 | - |
| ddp04 | plain | wifi | 3 | 0.0196 | 0.0190-0.0206 | 0.0007 | -2.08% | -4.20% +1.18% -3.30% | +1.18% | 35 | 76.7 | 104 | - |
| gpu | gpu (real, not emulated) | - | 3 | 0.0793 | 0.0785-0.0800 | 0.0006 | +0.00% | - | - | - | - | - | reference noise |
| gpu | single | - | 3 | 0.0793 | 0.0785-0.0800 | 0.0006 | - | - | - | 54 | 0.0 | 0 | - |
| gpu | exact | wifi | 3 | 0.0798 | 0.0792-0.0802 | 0.0004 | +0.63% | +0.92% +0.20% +0.78% | +0.92% | 47 | 81.1 | 147 | - |
| gpu | fast | 5g | 3 | 0.0458 | 0.0397-0.0524 | 0.0052 | -42.19% | -49.45% -34.51% -42.75% | -34.51% | 53 | 40.8 | 65 | PASS |
| gpu | fast | wifi | 3 | 0.0458 | 0.0397-0.0524 | 0.0052 | -42.23% | -49.44% -34.55% -42.86% | -34.55% | 29 | 40.8 | 65 | PASS |
| gpu | plain | 5g | 3 | 0.0798 | 0.0792-0.0801 | 0.0004 | +0.62% | +0.93% +0.15% +0.78% | +0.93% | 56 | 51.5 | 78 | - |
| gpu | plain | wifi | 3 | 0.0798 | 0.0792-0.0802 | 0.0004 | +0.61% | +0.88% +0.22% +0.74% | +0.88% | 31 | 51.5 | 78 | - |
| tfm | gpu (real, not emulated) | - | 3 | 0.2751 | 0.2726-0.2798 | 0.0033 | +0.02% | - | - | - | - | - | reference noise |
| tfm | single | - | 3 | 0.2751 | 0.2726-0.2796 | 0.0032 | - | - | - | 40 | 0.0 | 0 | - |
| tfm | exact | wifi | 3 | 0.2767 | 0.2751-0.2781 | 0.0012 | +0.60% | +2.00% -0.96% +0.81% | +2.00% | 46 | 21.7 | 314 | - |
| tfm | fast | 5g | 3 | 0.2828 | 0.2765-0.2922 | 0.0068 | +2.81% | +1.41% +4.50% +2.49% | +4.50% | 34 | 6.2 | 66 | FAIL |
| tfm | fast | wifi | 3 | 0.2803 | 0.2740-0.2888 | 0.0062 | +1.92% | +0.49% +3.29% +1.95% | +3.29% | 24 | 7.5 | 74 | FAIL |
| tfm | plain | 5g | 3 | 0.2766 | 0.2756-0.2784 | 0.0013 | +0.55% | +1.17% -0.45% +0.97% | +1.17% | 46 | 12.1 | 109 | - |
| tfm | plain | wifi | 3 | 0.3168 | 0.2748-0.4004 | 0.0591 | +15.17% | +0.94% +43.19% +0.69% | +43.19% | 24 | 6.4 | 68 | - |

Tolerance: fast's mean validation loss over the seeds within 1% (relative) of single's. Overall: FAIL | ddp04 fail | gpu pass | tfm fail

### Verdict for the paper's defaults (0.7/0.9): FAIL (superseded by the tuning below)

- `fast` (the defaults) fails ddp04 (+14% under 5g, +49% under wifi; seed gaps
  reach +138%) and tfm (+1.9% and +2.8%). Its gpu "pass" is -42%: it changes
  the training instead of matching it.
- `plain` is within 1% on every model and profile but one: gpu +0.6%,
  ddp04 -2.1% and -2.4% (exact itself spans -3.9% to +3.9% per seed here),
  tfm 5g +0.55%. tfm wifi is +15% because one seed ended at 0.400. Two lab
  replicates of that seed gave 0.285 and 0.278, so it is a rare instability
  that the timing-dependent shares and ring changes make non-deterministic,
  not a fixed offset (diagnosis table below). Changing the defaults to `plain`
  is the user's call.
- First run (`ba7b54d`, defaults, no end-of-epoch sync): ddp04 +672% / +1467%
  (one seed reached 0.77), gpu -33% / -43%, tfm +1.8% / +4.1%.

## Tuning (DILOCO-TUNE): the new defaults

**Result.** The default `--ddp-sync fast` is now plain sample-weighted model
averaging (outer lr 1, no momentum) every 64 steps and at every epoch's end,
after a 10-step exact warm-up. It is within 1% of one device on all three
models and needs 13-16 syncs per run, against 144-312 for exact sync and
62-122 for the old defaults (the gpu, ddp04 and tfm runs are 147, 180 and
314 steps).
The paper's outer Nesterov 0.7/0.9 fails at every interval these runs can
reach, so U32 may now be dispatched on this table; the Nesterov-at-large-H
half of the candidate rule is not supported (see below).

**Sweep.** Outer optimizer (plain 1/0, the paper's 0.7/0.9, the middle point
0.85/0.45) x H (fixed 4, 16, 64, 256, and the drift-adaptive rule) x warm-up
(50, the old default; then 10 and 25 for the leaders), 3 seeds each, 6 for
the finalists. The epoch-end sync caps every interval at the steps left in the
epoch (48 for gpu, 29 for ddp04, 156 for tfm), so H=64 and H=256 are the same
run on gpu and ddp04 and nothing here trains more than 156 steps locally.
Syncs count every exact warm-up step plus every DiLoCo round; bytes are
proportional to syncs (each carries the int8 model or gradient, about
0.56 MB for the MLPs), plus Adam's or SGD's state every 10th round.

**Evidence 1: in-process 2-rank simulation** (DILOCO-DIAG's harness, the
shim's own schedule functions, int8 with error feedback on both directions,
the end-of-epoch sync; no daemon, no timing). Gap = mean validation loss over
the seeds against the same seeds' one-device runs; negative is better.

| outer lr/momentum | H | warm-up | gpu | ddp04 | tfm | all pass |
|---|---|---|---|---|---|---|
| exact (control) | - | - | -0.00% (worst +0.0%; 3 seeds), 144 syncs (48.0/epoch) | -0.05% (worst -0.0%; 3 seeds), 174 syncs (29.0/epoch) | +0.07% (worst +0.2%; 3 seeds), 312 syncs (156.0/epoch) | **PASS** |
| middle 0.85/0.45 | 4 | 50 | -18.26% (worst -17.2%; 3 seeds), 74 syncs (24.7/epoch) | -7.37% (worst -3.1%; 3 seeds), 84 syncs (14.0/epoch) | -0.12% (worst +0.1%; 3 seeds), 116 syncs (58.0/epoch) | **PASS** |
| middle 0.85/0.45 | 16 | 10 | -18.95% (worst -18.1%; 3 seeds), 19 syncs (6.3/epoch) | -17.91% (worst -15.1%; 3 seeds), 22 syncs (3.7/epoch) | +0.97% (worst +1.5%; 3 seeds), 30 syncs (15.0/epoch) | **PASS** |
| middle 0.85/0.45 | 16 | 25 | -17.86% (worst -16.6%; 3 seeds), 33 syncs (11.0/epoch) | -19.29% (worst -17.7%; 3 seeds), 36 syncs (6.0/epoch) | +0.67% (worst +1.7%; 3 seeds), 44 syncs (22.0/epoch) | **PASS** |
| middle 0.85/0.45 | 16 | 50 | -14.54% (worst -14.2%; 3 seeds), 56 syncs (18.7/epoch) | -17.75% (worst -16.9%; 3 seeds), 59 syncs (9.8/epoch) | -0.01% (worst +0.4%; 3 seeds), 67 syncs (33.5/epoch) | **PASS** |
| middle 0.85/0.45 | 64 | 10 | -11.70% (worst -10.5%; 6 seeds), 13 syncs (4.3/epoch) | -16.27% (worst -10.0%; 6 seeds), 16 syncs (2.7/epoch) | -1.34% (worst +0.8%; 6 seeds), 16 syncs (8.0/epoch) | **PASS** |
| middle 0.85/0.45 | 64 | 50 | -9.33% (worst -9.2%; 3 seeds), 52 syncs (17.3/epoch) | -16.02% (worst -15.0%; 3 seeds), 55 syncs (9.2/epoch) | +1.87% (worst +2.4%; 3 seeds), 55 syncs (27.5/epoch) | fail |
| middle 0.85/0.45 | 256 | 50 | -9.33% (worst -9.2%; 3 seeds), 52 syncs (17.3/epoch) | -16.02% (worst -15.0%; 3 seeds), 55 syncs (9.2/epoch) | +2.02% (worst +2.7%; 3 seeds), 52 syncs (26.0/epoch) | fail |
| middle 0.85/0.45 | adaptive | 50 | -17.48% (worst -16.8%; 3 seeds), 77 syncs (25.7/epoch) | +3.84% (worst +26.2%; 3 seeds), 91 syncs (15.2/epoch) | +3.36% (worst +8.9%; 3 seeds), 166 syncs (83.0/epoch) | fail |
| paper 0.7/0.9 | 4 | 50 | -24.83% (worst -10.8%; 3 seeds), 74 syncs (24.7/epoch) | +57.54% (worst +72.0%; 3 seeds), 84 syncs (14.0/epoch) | +54.35% (worst +74.6%; 3 seeds), 116 syncs (58.0/epoch) | fail |
| paper 0.7/0.9 | 16 | 50 | -37.04% (worst -35.4%; 3 seeds), 56 syncs (18.7/epoch) | +168.37% (worst +548.1%; 3 seeds), 59 syncs (9.8/epoch) | +6.34% (worst +11.7%; 3 seeds), 67 syncs (33.5/epoch) | fail |
| paper 0.7/0.9 | 64 | 50 | -16.00% (worst -15.3%; 3 seeds), 52 syncs (17.3/epoch) | +6.43% (worst +32.8%; 3 seeds), 55 syncs (9.2/epoch) | +0.98% (worst +1.2%; 3 seeds), 55 syncs (27.5/epoch) | fail |
| paper 0.7/0.9 | 256 | 50 | -16.00% (worst -15.3%; 3 seeds), 52 syncs (17.3/epoch) | +6.43% (worst +32.8%; 3 seeds), 55 syncs (9.2/epoch) | +21.66% (worst +29.4%; 3 seeds), 52 syncs (26.0/epoch) | fail |
| paper 0.7/0.9 | adaptive | 50 | -44.61% (worst -42.6%; 3 seeds), 63 syncs (21.0/epoch) | +54.90% (worst +186.7%; 3 seeds), 62 syncs (10.3/epoch) | +66.80% (worst +151.9%; 3 seeds), 122 syncs (61.0/epoch) | fail |
| plain 1/0 | 4 | 50 | +0.02% (worst +0.0%; 3 seeds), 74 syncs (24.7/epoch) | +0.50% (worst +1.1%; 3 seeds), 84 syncs (14.0/epoch) | +0.53% (worst +1.2%; 3 seeds), 116 syncs (58.0/epoch) | **PASS** |
| plain 1/0 | 16 | 10 | +0.06% (worst +0.1%; 6 seeds), 19 syncs (6.3/epoch) | +0.30% (worst +1.9%; 6 seeds), 22 syncs (3.7/epoch) | -0.49% (worst +6.7%; 6 seeds), 30 syncs (15.0/epoch) | **PASS** |
| plain 1/0 | 16 | 50 | +0.06% (worst +0.1%; 3 seeds), 56 syncs (18.7/epoch) | +1.15% (worst +2.3%; 3 seeds), 59 syncs (9.8/epoch) | +1.42% (worst +3.8%; 3 seeds), 67 syncs (33.5/epoch) | fail |
| plain 1/0 | 64 | 10 | +0.08% (worst +0.2%; 6 seeds), 13 syncs (4.3/epoch) | +0.42% (worst +1.3%; 6 seeds), 16 syncs (2.7/epoch) | -0.02% (worst +4.5%; 6 seeds), 16 syncs (8.0/epoch) | **PASS** |
| plain 1/0 | 64 | 50 | +0.07% (worst +0.1%; 3 seeds), 52 syncs (17.3/epoch) | +1.13% (worst +2.5%; 3 seeds), 55 syncs (9.2/epoch) | +0.11% (worst +0.3%; 3 seeds), 55 syncs (27.5/epoch) | fail |
| plain 1/0 | 256 | 10 | +0.08% (worst +0.2%; 6 seeds), 13 syncs (4.3/epoch) | +0.42% (worst +1.3%; 6 seeds), 16 syncs (2.7/epoch) | -0.93% (worst +1.8%; 6 seeds), 12 syncs (6.0/epoch) | **PASS** |
| plain 1/0 | 256 | 50 | +0.07% (worst +0.1%; 3 seeds), 52 syncs (17.3/epoch) | +1.13% (worst +2.5%; 3 seeds), 55 syncs (9.2/epoch) | +0.56% (worst +1.0%; 3 seeds), 52 syncs (26.0/epoch) | fail |
| plain 1/0 | adaptive | 50 | +0.02% (worst +0.0%; 3 seeds), 84 syncs (28.0/epoch) | +0.49% (worst +0.8%; 3 seeds), 60 syncs (10.0/epoch) | +0.59% (worst +1.0%; 3 seeds), 177 syncs (88.5/epoch) | **PASS** |

What it shows:

- The warm-up was most of the cost: with 50 exact steps every arm needs at
  least 52 syncs. Ten warm-up steps ended no worse (plain H64: ddp04 +0.42%
  over 6 seeds against +1.13% over 3 with 50) and cut syncs 3-4x.
- Plain averaging matches one device (gpu +0.1%, ddp04 +0.4%, tfm 0.0% at
  H64 over 6 seeds; exact itself is within 0.1% in this harness). The
  drift-adaptive rule is no help: it shrinks to H=2 on tfm (177 syncs).
- The paper's 0.7/0.9 fails at every H, including one sync per epoch (tfm
  at 156 local steps: +21.7%), so "Nesterov at H >= 100" is not supported
  on these models. The middle point 0.85/0.45 passes the one-sided bar but
  by changing the training (gpu -12%, ddp04 -16%: an effective step of
  0.85/(1-0.45) = 1.55x the script's), with no fewer syncs than plain: not
  chosen, because a model nearer its stable learning rate would diverge
  the way ddp04 did under 7x.
- H=64 rather than H=256: they are the same run on gpu and ddp04, H=256
  saves 4 of 16 syncs on tfm, and no run here measured more than 156 local
  steps, so 256 would be an extrapolation. `PIPEDPEER_DILOCO_H=256` selects
  it.

**Evidence 2: lab, the new defaults** (`fast` = no `PIPEDPEER_DILOCO_*`
set), binary built from `14b3b48` plus this change, 2 containers of 2 CPUs
and 1 GB, under the lan and wifi profiles; `single` from the same session's
binary on one container. Rows from `~/.local/state/pipedpeer/bench-dtune/diloco/`.
Wall times and bytes are **emulated and unlocked, not for the paper**
(QUIET-RERUN redoes wall times); MiB sent per rank is the speed proxy.

| model | arm | profile | seeds | val loss mean | min-max | std | gap vs single (mean) | per-seed gaps | worst rank gap | wall s | MiB sent/rank | syncs (per epoch) | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ddp04 | single | - | 3 | 0.0200 | 0.0198-0.0203 | 0.0002 | - | - | - | 46 | 0.0 | 0 (0.0) | - |
| ddp04 | exact | lan | 3 | 0.0197 | 0.0191-0.0210 | 0.0009 | -1.31% | -3.55% +3.47% -3.97% | +3.47% | 14 | 98.9 | 180 (30.0) | - |
| ddp04 | fast | lan | 3 | 0.0196 | 0.0190-0.0204 | 0.0006 | -2.14% | -3.87% +0.37% -2.97% | +0.37% | 10 | 10.7 | 16 (2.7) | PASS |
| ddp04 | fast | wifi | 3 | 0.0196 | 0.0191-0.0204 | 0.0006 | -2.09% | -3.69% +0.62% -3.25% | +0.62% | 41 | 10.7 | 16 (2.7) | PASS |
| gpu | single | - | 3 | 0.0793 | 0.0785-0.0800 | 0.0006 | - | - | - | 79 | 0.0 | 0 (0.0) | - |
| gpu | exact | lan | 3 | 0.0798 | 0.0792-0.0802 | 0.0004 | +0.60% | +0.88% +0.18% +0.74% | +0.88% | 18 | 81.1 | 147 (49.0) | - |
| gpu | fast | lan | 3 | 0.0798 | 0.0792-0.0802 | 0.0005 | +0.64% | +0.87% +0.24% +0.82% | +0.87% | 14 | 9.1 | 13 (4.3) | PASS |
| gpu | fast | wifi | 3 | 0.0798 | 0.0791-0.0802 | 0.0004 | +0.61% | +0.84% +0.17% +0.82% | +0.84% | 19 | 9.1 | 13 (4.3) | PASS |
| tfm | single | - | 3 | 0.2751 | 0.2726-0.2796 | 0.0032 | - | - | - | 166 | 0.0 | 0 (0.0) | - |
| tfm | exact | lan | 3 | 0.2758 | 0.2750-0.2764 | 0.0006 | +0.27% | +1.23% -1.16% +0.78% | +1.23% | 24 | 21.7 | 314 (157.0) | - |
| tfm | fast | lan | 3 | 0.2755 | 0.2746-0.2766 | 0.0008 | +0.18% | +1.01% -1.07% +0.63% | +1.01% | 20 | 1.4 | 16 (8.0) | PASS |
| tfm | fast | wifi | 3 | 0.2764 | 0.2746-0.2793 | 0.0020 | +0.50% | +1.01% -0.12% +0.63% | +1.01% | 82 | 1.4 | 16 (8.0) | PASS |

Every model passes under both profiles: gpu +0.6%, ddp04 -2.1%, tfm +0.2% /
+0.5%, each within the `exact` control's own spread (-1.3%, +0.6%, +0.3%).
Bytes per rank drop 9x (gpu, ddp04) and 15x (tfm) against exact sync.
DILOCO-DIAG's rare tfm seed at 0.400 (plain, 50-step warm-up, adaptive H)
did not recur in these six tfm runs or the twelve simulated ones; a rare
unstable seed cannot be ruled out by 3-6 seeds.

Reproduce (one lab session; `single` and `exact` once, then the defaults):

```sh
UNLOCKED=1 BL_LAB_NAME=dtune TRAIN_MEM=1g PROFILES="lan wifi" scripts/bench-diloco.sh run
```

## Ranks (DILOCO-MANY): the interval by ring size

**Result.** Plain averaging lands further from one device as ranks are
added, and a shorter interval only partly makes up for it. The default
interval is now a table of ring sizes, the largest H that passed on every
model, held in the daemon (`dilocoRanks`, ddprebalance.go) and the shim
(`_DILOCO_RANKS`, ddp.py; a test keeps them equal):

| ranks | H | evidence (worst model) |
|---|---|---|
| 2 | 32 | tfm H=64 fails on seeds 0-2 (+2.13%) |
| 3 | 16 | ddp04 H=32 fails over 6 seeds (+1.44%) |
| 4 | 4 | ddp04 H=16 fails (+1.56%), tfm H=8 fails on seeds 0-2 (+1.74%) |
| 5+ | none | at 8, tfm fails at every H down to 2 (+1.03%) and outer lr < 1 is worse |

Auto mode (U32) never picks fast sync for a ring past the table, and its
reply carries the table's H, which every rank adopts. An explicit
`--ddp-sync fast` past the table still runs, at the last row's H=4, and
rank 0 prints one line saying the ring is beyond the validated size.
`PIPEDPEER_DILOCO_H` overrides the table. The 2-rank interval drops from
DILOCO-TUNE's 64 to 32: this harness's tfm seeds 0-2 failed at 64 even
though DILOCO-TUNE's 6 seeds and the lab passed; accuracy outranks the 4
extra syncs.

**Harness.** DILOCO-TUNE's in-process simulation extended to N equal ranks
(`scripts/sim-diloco-ranks.py`): same models, data, epochs, batch
and learning rate as one device; the global batch split evenly over the
ranks; 10-step exact warm-up; plain sample-weighted averaging (outer lr 1,
momentum 0), every epoch's end synced, optimizer state averaged every 10th
round; exact float averaging (no int8 wire). Gap = mean validation loss
against the same seeds' one-device runs. A cell passes when the mean over
seeds 0-2 **and**, where run, over seeds 0-5 are both <= +1% (bold).
Cells read `seeds 0-2 / seeds 0-5`. ddp04's epoch is 29 steps and gpu's 48,
so H=32 and H=64 are the same run on ddp04.

| model | ranks | H=2 | H=4 | H=8 | H=16 | H=32 | H=64 |
|---|---|---|---|---|---|---|---|
| gpu | 2 | - | - | **+0.02** | **+0.04** | **+0.06** | **+0.09** |
| gpu | 3 | - | - | **+0.03** | **+0.08** | **+0.11** | **+0.16** |
| gpu | 4 | - | - | **+0.07** | **+0.13** | **+0.21** | **+0.26** |
| gpu | 8 | - | - | **+0.14** | **+0.32** | **+0.46** | **+0.56** |
| gpu | 11 | - | - | **+0.24** | **+0.52** | **+0.71** | **+0.87** |
| gpu | 16 | - | - | **+0.35** | **+0.74** | +1.01 | +1.27 |
| ddp04 | 2 | - | - | **+0.79 / +0.13** | **+0.85 / +0.63** | **+0.64 / +0.35** | **+0.64 / +0.35** |
| ddp04 | 3 | - | **-0.13 / -0.25** | **-0.21 / +0.32** | **+0.39 / +0.42** | +0.87 / +1.44 | +0.87 / +1.44 |
| ddp04 | 4 | - | **-0.61 / -0.24** | **-0.24 / +0.36** | +1.56 / +1.35 | +2.28 / +2.33 | +2.28 / +2.33 |
| ddp04 | 8 | **-0.88 / -0.03** | **-0.15 / +0.57** | +2.54 / +2.53 | +2.49 / +2.07 | +1.48 / +2.37 | +1.48 / +2.37 |
| ddp04 | 11 | - | - | +1.39 | +1.17 | +3.23 | +3.23 |
| ddp04 | 16 | - | - | **+0.27** | **+0.96** | +4.96 | +4.96 |
| tfm | 2 | - | - | **+0.14 / -1.24** | **+0.35 / -1.13** | **+0.73 / -1.02** | +2.13 / -0.29 |
| tfm | 3 | - | **+0.25 / -0.27** | **+0.11 / -1.23** | **+0.69 / -0.79** | **+0.48 / -1.15** | +1.17 / -0.68 |
| tfm | 4 | **+0.91 / -0.68** | **+0.98 / -0.89** | +1.74 / -0.46 | **+0.88 / -0.95** | +1.42 / -0.63 | **+0.86 / -0.73** |
| tfm | 8 | +1.03 / -0.87 | +1.24 / -0.54 | +1.57 / -0.37 | +1.98 | +2.43 | +2.98 |
| tfm | 11 | +2.02 | +1.79 | +2.44 | +2.94 | +4.18 | +4.52 |
| tfm | 16 | +3.26 | +3.39 | +4.17 | +5.70 | +6.13 | +7.03 |

Outer lr below 1 at 8 ranks (tfm, H=4, seeds 0-2): 0.9 gives +1.67%, 0.7
+3.42%, against +1.24% at 1.0. It shrinks every round's progress and does
nothing for the drift.

What it shows:

- gpu drifts smoothly with ranks x H and passes up to 11 ranks at any H.
  ddp04 and tfm set the table. Neither is monotone cell by cell: one
  seed's loss moves +-2% (ddp04) and the one-device runs themselves spread
  that much (tfm seed 5 lands at 0.294 against 0.271-0.282), so a 3-seed
  mean is good to about +-1%, which is the bar's own size. The table takes
  the conservative reading: a cell must pass on seeds 0-2 and on all six.
- Many ranks, not a small per-rank batch, cause the tfm gap: 2 ranks of 16
  samples each (global batch 32) land -2.9% (H=8) and -2.8% (H=64) against
  one device at batch 32, while 16 ranks of 16 (global 256) land +3.3% even
  at H=2.
- 5 to 7 ranks were not run; with 8 failing they get no interval. A ring
  that grows past the table by joins keeps its starting interval.

Reproduce one cell per seed (a few seconds for gpu/ddp04 at 2 ranks, a few
minutes for tfm at 16); seeds 0-2 for every cell, 3-5 for the ddp04 and tfm
cells at 2-8 ranks, and `single` for each seed:

```sh
python3 scripts/sim-diloco-ranks.py tfm 0 eq+H16+W10 N3 src/internal/nixgen/shim
python3 scripts/sim-diloco-ranks.py tfm 0 single - src/internal/nixgen/shim
```

## Diagnosis (DILOCO-DIAG)

**Cause.** The paper's outer optimizer amplifies the step at small H. With
the averaged pseudo-gradient pointing the same way from round to round,
Nesterov's update settles at lr/(1-momentum) = 0.7/0.1 = 7 times the averaged
delta, so the model moves about 7x as far as the script's own learning rate
would take it. The paper syncs every ~500 steps, where the deltas
decorrelate; here H is 2 to 64. On top of that, the drift rule fed back.
Higher loss means more drift, which halves H, which applies the amplified step
more often. The diverged first-run seed shows exactly that: H went
32, 16, 8, 4, 2 before it ended at 0.77. gpu (SGD 0.05, undertrained in 147
steps) gains from the larger effective step; ddp04 (SGD 0.1, near its stable
limit) diverges; AdamW (tfm) loses a few percent.

**Second cause, a bug, fixed.** The run length is unknown without a
OneCycleLR, so nothing made the last steps a sync. The ranks' last interval,
up to 64 steps (a third of a 180-step run), trained each rank alone on its own
shard, and the ranks ended with different models. Now every interval is
capped so a sync lands on each epoch's last step (`_diloco_epoch_cap`). A run
ends on an epoch boundary, so the ranks end on the same global model: every
rerun row's rank losses are identical.

**Ruled out.** The pseudo-gradient sign and scale (θ_global - θ_local; the outer
step matches `torch.optim.SGD(nesterov=True)` in `test_ddp.py`). The inner
optimizer state is neither reset nor doubled: it is kept per rank and averaged
every 10th round. int8 with error feedback on the deltas: tfm in-process with
int8 +0.3% against +0.3% in fp32. Sample weighting: `plainuw` matches `plain`.

**Evidence 1: in-process 2-rank simulation** of the same algorithm (exact
fp32 averaging, equal shards, no daemon, no timing). Mean gap vs one device
over seeds 1-3, per-seed gaps after it. `ep` = the end-of-epoch sync.

| model | defaults (adaptive H) | defaults, H=2 fixed | defaults + ep | lr 1, mu 0 (adaptive) | lr 1, mu 0, H=2 | **lr 1, mu 0 + ep** | lr 0.1, mu 0.9 | lr 0.3, mu 0.7 |
|---|---|---|---|---|---|---|---|---|
| ddp04 | -7.7% | NaN on 2 seeds, +1157% on the third | +46.9% (+169% seed 3) | +7.5% | +0.5% | **+0.6%** | +28.5% | +19.5% |
| gpu | -41.4% | +177.5% | -44.5% | +0.1% | +0.0% | **+0.0%** | +15.8% | +2.8% |
| tfm | +56.3% (+162% seed 3) | +273% | +64.0% | +0.8% | +3.5% | **+0.3%** | +2.7% | +1.7% |

Fixed H on ddp04 seed 1 with the defaults: H=2 NaN, H=4 +123%, H=8 -8%, H=16
+96% (an unsynced tail). Driver: an in-process script, not kept in the repo.

**Evidence 2: lab, ddp04 and tfm, wifi**, binary `d6c8856`, run as one
session with `FAST_VARIANTS` (logs under
`~/.local/state/pipedpeer/bench-diag/`). Gaps are against the rerun's
`single`.

| model | arm | flags | val loss mean | gap | per-seed |
|---|---|---|---|---|---|
| ddp04 | fast (defaults) | - | 0.0273 | +36.3% | +24.0% +3.3% +82.5% |
| ddp04 | h8 | defaults, H=8 fixed | 0.0291 | +45.7% | +9.3% -23.2% +152.5% |
| ddp04 | mu0 | lr 0.7, momentum 0 | 0.0225 | +12.4% | +11.0% +15.4% +10.8% |
| ddp04 | plain | lr 1, momentum 0 | 0.0194 | -2.8% | -4.1% -0.3% -4.0% |
| ddp04 | plainB | same, a replicate | 0.0196 | -2.2% | -3.8% +0.8% -3.7% |
| ddp04 | plainuw | plain, unweighted | 0.0194 | -2.8% | -4.6% +0.2% -4.1% |
| tfm | fast (defaults) | - | 0.2858 | +3.9% | +6.0% +3.8% +1.9% |
| tfm | h8 | defaults, H=8 fixed | 0.2831 | +2.9% | +4.1% -2.0% +6.8% |
| tfm | mu0 | lr 0.7, momentum 0 | 0.2758 | +0.3% | +0.7% -0.7% +0.8% |
| tfm | plain | lr 1, momentum 0 | 0.2785 | +1.3% | +0.9% +2.0% +0.9% |
| tfm | plainB | same, a replicate | 0.2766 | +0.6% | +0.8% -0.7% +1.6% |
| tfm | plainuw | plain, unweighted | 0.2760 | +0.4% | +0.8% -0.8% +1.1% |

**For the user.** Outer lr 1 with no momentum is plain model averaging
(local SGD) on the same exact warm-up, drift-adaptive H and end-of-epoch sync.
It is the best alternative measured. The tuning variables
`PIPEDPEER_DILOCO_OUTER_LR`, `PIPEDPEER_DILOCO_MOMENTUM`, `PIPEDPEER_DILOCO_H`
and `PIPEDPEER_DILOCO_UNWEIGHTED` select any of these arms without a rebuild.
The defaults stay 0.7 / 0.9 until the user decides.

### Reproduce

```sh
# binary from a git archive of HEAD (or DILOCO_BIN=... DILOCO_REV=<commit>)
UNLOCKED=1 BL_LAB_NAME=u31b TRAIN_MEM=850m \
  FAST_VARIANTS="plain=-e PIPEDPEER_DILOCO_OUTER_LR=1 -e PIPEDPEER_DILOCO_MOMENTUM=0" \
  scripts/bench-diloco.sh run
BL_GPU_PYTHON=<python with CUDA torch> scripts/bench-diloco.sh gpu
scripts/bench-diloco.sh table
```
The CUDA torch used here: `uv venv ~/.cache/pipedpeer-bench/gpu-venv` plus
`uv pip install torch --index-url https://download.pytorch.org/whl/cu126`
(the host's own torch is CPU-only).
