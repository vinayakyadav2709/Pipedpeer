# DiLoCo quality: `--ddp-sync fast` against one device

**Question:** does `--ddp 2 --ddp-sync fast` (DiLoCo, U31) end within 1%
relative validation loss of training on one device? The bar is the mean over
3 seeds on every test model, reported with the spread between seeds (Decisions,
2026-10-15). U32 (promotion to auto) and U33 (Streaming DiLoCo) stay blocked
unless every model passes.

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

### Verdict: the defaults FAIL. U32 and U33 stay blocked.

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
