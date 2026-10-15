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
- Containers: 2 CPUs each, 1.1 GB for single and 850 MB for the rings.
  Memory is not a loss variable; other
  labs held the rest of the host's memory cap. Every lab run passes
  `--gpu off`, because the containers see the host's NVIDIA driver and would
  otherwise fetch the CUDA wheel set.
- The fast arm's loss is rank 0's model, the one the script prints. "worst
  rank gap" is the worse of the two ranks: without a OneCycleLR the run length
  is unknown, so DiLoCo does not tighten at the end and the ranks can finish
  up to one interval apart.
- **Wall times are unlocked, not for the paper.** The loss runs did not hold
  the shared bench lock (loss does not depend on CPU contention), and every
  wall time and byte count is EMULATED.

## Results

Run 2026-10-15 on the binary built from `ba7b54d` (U31's DiLoCo is `40dd80e`).
Rows come from `~/.local/state/pipedpeer/bench/diloco/{results,extra,gpu}.csv`
and the logs next to them. Gaps are relative to the same seed's `single`
loss; the mean gap is relative to the mean of `single`. Every wall time and
byte count is **emulated and unlocked, not for the paper**.

| model | arm | profile | seeds | val loss mean | min-max | std | gap vs single (mean) | per-seed gaps | worst rank gap | wall s | MiB sent/rank | syncs | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ddp04 | gpu (real, not emulated) | - | 3 | 0.0200 | 0.0198-0.0203 | 0.0002 | -0.16% | - | - | - | - | - | reference noise |
| ddp04 | single | - | 3 | 0.0200 | 0.0198-0.0203 | 0.0002 | - | - | - | 16 | 0.0 | 0 | - |
| ddp04 | exact | wifi | 3 | 0.0199 | 0.0192-0.0209 | 0.0007 | -0.39% | -0.79% +2.78% -3.22% | +2.78% | 48 | 95.6 | 174 | - |
| ddp04 | fast | 5g | 3 | 0.1543 | 0.1008-0.1879 | 0.0383 | +671.90% | +848.56% +757.84% +407.69% | +953.89% | 44 | 39.7 | 63 | FAIL |
| ddp04 | fast | wifi | 3 | 0.3134 | 0.0370-0.7685 | 0.3243 | +1467.26% | +86.78% +562.25% +3770.88% | +3770.88% | 23 | 39.7 | 63 | FAIL |
| gpu | gpu (real, not emulated) | - | 3 | 0.0793 | 0.0785-0.0800 | 0.0006 | +0.00% | - | - | - | - | - | reference noise |
| gpu | single | - | 3 | 0.0793 | 0.0785-0.0800 | 0.0006 | - | - | - | 26 | 0.0 | 0 | - |
| gpu | exact | wifi | 3 | 0.0805 | 0.0798-0.0810 | 0.0005 | +1.53% | +1.68% +1.20% +1.73% | +1.73% | 50 | 79.5 | 144 | - |
| gpu | fast | 5g | 3 | 0.0535 | 0.0450-0.0655 | 0.0087 | -32.56% | -42.69% -37.60% -17.45% | -17.45% | 49 | 39.2 | 62 | PASS |
| gpu | fast | wifi | 3 | 0.0454 | 0.0413-0.0502 | 0.0037 | -42.70% | -42.88% -37.25% -48.01% | -37.25% | 28 | 39.2 | 62 | PASS |
| tfm | gpu (real, not emulated) | - | 3 | 0.2751 | 0.2726-0.2798 | 0.0033 | +0.02% | - | - | - | - | - | reference noise |
| tfm | single | - | 3 | 0.2751 | 0.2726-0.2796 | 0.0032 | - | - | - | 31 | 0.0 | 0 | - |
| tfm | exact | wifi | 3 | 0.2771 | 0.2758-0.2792 | 0.0015 | +0.75% | +1.17% -0.17% +1.27% | +1.27% | 37 | 21.6 | 312 | - |
| tfm | fast | 5g | 3 | 0.2800 | 0.2747-0.2864 | 0.0048 | +1.80% | +0.77% +2.42% +2.19% | +2.42% | 36 | 7.4 | 72 | FAIL |
| tfm | fast | wifi | 3 | 0.2863 | 0.2763-0.3057 | 0.0137 | +4.10% | +1.35% +9.32% +1.51% | +9.32% | 26 | 7.3 | 71 | FAIL |

Tolerance: fast's mean validation loss over the seeds within 1% (relative) of single's. Overall: FAIL | ddp04 fail | gpu pass | tfm fail

### Verdict: FAIL. U32 and U33 stay blocked.

- `gpu` passes, but only because DiLoCo ends far *below* one device (-43%
  under wifi, -33% under 5g). It changes the training rather than matching
  it.
- `ddp04` fails badly: +672% to +1467%. One wifi seed reached 0.77 against
  0.020, so the run diverged.
- `tfm` (AdamW) fails: +1.8% under 5g and +4.1% under wifi. One wifi seed
  was +9.3%.

### What the numbers say

- **Seed noise is close to the bar.** One device's own seed spread is 2.6%
  (min-max) for `tfm` and 1.9% for `gpu`. The real GPU agrees with the CPU
  stand-in to within 0.2% on every model, so the stand-in is sound.
- **Exact sync alone misses 1% on `gpu`** (+1.5%, every seed +1.2% to
  +1.7%), and the exact arm's per-seed gaps reach +2.8% on `ddp04`. The ring
  drops the remainder below one global batch (ddp.py:403). That is 147 vs 144
  steps for `gpu`. Its shuffle is seeded by the epoch alone, so the order
  differs from one device. A 1% bar judged against `single` is therefore
  tighter than per-step DDP itself meets on this model.
- **DiLoCo moves the effective learning rate.** Under no tightening (no
  OneCycleLR, so run length unknown) the schedule was: 50 exact steps, then H
  between 2 and 32, adapted on drift. The outer Nesterov step (lr 0.7,
  momentum 0.9) runs after every 2 to 32 inner steps, not the paper's ~500.
  Its momentum then compounds over many short rounds, which acts like a much
  larger step. On `gpu` (SGD 0.05) that undertrained model gains. On `ddp04`
  (SGD 0.1, already near its stable limit) it diverges. This is a hypothesis
  from these runs, not yet isolated. The test is to rerun `ddp04` with outer
  momentum 0, or outer lr 1.0 with no momentum (plain averaging), and with
  H_FLOOR raised.
- **The profile changes the result.** For the same seed, `gpu` gave 0.041
  under wifi and 0.065 under 5g. Ring shares are sized by measured speed, so
  timing feeds the training: the arms are not seed-deterministic.
- Bytes: DiLoCo synced 39 MiB per rank against 80 to 96 MiB for exact on the
  MLPs, and 7 MiB against 22 MiB on `tfm`.

### Reproduce

```sh
# binary from a git archive of HEAD (or DILOCO_BIN=... DILOCO_REV=<commit>)
UNLOCKED=1 BL_LAB_NAME=u31b TRAIN_MEM=1.1g scripts/bench-diloco.sh run
BL_GPU_PYTHON=<python with CUDA torch> scripts/bench-diloco.sh gpu
scripts/bench-diloco.sh table
```
The CUDA torch used here: `uv venv ~/.cache/pipedpeer-bench/gpu-venv` plus
`uv pip install torch --index-url https://download.pytorch.org/whl/cu126`
(the host's own torch is CPU-only).
