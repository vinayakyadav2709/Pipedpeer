# Real hardware: two laptops and a phone

The runs behind every two-laptop and phone number in the paper and the report.
Two totals appear for each run:

- **script TOTAL**: the `TOTAL` line the demo script prints (its own wall time,
  `pipedpeer-demo/*.py`).
- **pipedpeer total**: `total_s` in the run export (`pipedpeer export`), from
  submission to results synced back; it adds environment build, upload and
  sandbox setup to the script's time.

## Setup

- Laptop A (`yeet`, RTX 3060) and laptop B (`fedora`, RTX 3050) in different
  homes, joined over a direct Tailscale path, 6-8 ms round trip. Neither home
  offered a reachable introducer, so the pipedpeer link rides Tailscale; B
  invites (a shared Tailscale node cannot open to the sharer).
- Phone: Samsung Galaxy S20 FE (SM-G781B, Android 13, kernel 4.19), Termux
  from Google Play, Debian + single-user Nix under `proot-distro` 5.3. Takes
  chunks only.

## Two laptops (2026-10-16, 01:19-01:31 IST)

Source: commit aaf5431 (body and the numbers it wrote into the paper and
report), exports in `~/pp-real-run/` on laptop A (`runs.csv` plus one
`<run id>.json` per run). Build: the nightly installed that night for the
non-DDP rows; e2cefbe on both laptops for the DDP rows.

| Run | script TOTAL | pipedpeer total | Where | Run id |
|---|---|---|---|---|
| `cpu.py`, one laptop, plain Python (3 runs) | 17.4-18.1 s | - | A | - |
| `cpu.py` (3 runs) | 13.3-14.6 s | 17.1, 15.5, 14.9 s | A 51/52/51%, B 49/48/49% of items | 1792093847339774676, 1792094034908271715, 1792094080556109969 |
| `both.py`, cold (first run of the environment) | 16.0 s | 506.0 s (487.4 s environment build, mostly CUDA PyTorch download) | A 64%, B 36% | 1792093772492169805 |
| `both.py`, warm | 15.6 s | 17.7 s | A 67%, B 33% | 1792093894991970870 |
| `gpu.py --epochs 5 --remote` (whole job on B) | 9.1 s | 11.7 s | B | 1792094125652246506 |
| `gpu.py --ddp 1` (e2cefbe) | 4.2 s | 6.4 s | A | 1792094396316110488 |
| `gpu.py --ddp 2`, exact sync (e2cefbe) | 37.1 s | 39.3 s (rank 0), 39.1 s (rank 1) | A + B | 1792094403323002174, 1792094403332683151 |
| `gpu.py --ddp 2`, fast sync (e2cefbe) | 7.4 s | 9.4 s, 9.5 s | A + B | 1792094443283333145, 1792094443291588528 |
| `gpu.py --ddp 2`, auto (e2cefbe) | 32.9 s | 35.2 s, 35.2 s | A + B | 1792094462072475892, 1792094462084208682 |

Validation loss (aaf5431): one GPU 0.0794, exact 0.0797, fast 0.0796. Exact
sync made 147 syncs of 209 ms, 81 MiB each way; fast sync made 16. The three
`--ddp 2` failures before e2cefbe (rank 1 given A's LAN address, "No route to
host") are in the same export, status `failed`/`cancelled`.

Same-build ratio: fast against exact on e2cefbe is 37.1 / 7.4 = 5.0x.

### `--ddp 2` rerun on 8df2885 (2026-10-16, ~02:20 IST)

Source: commit 85ba35a. One laptop and exact sync were not rerun (4.2 s and
37.1 s above, e2cefbe).

| Run | script TOTAL | Validation loss |
|---|---|---|
| `gpu.py --ddp 2`, fast sync | 8.5 s | 0.0797 |
| `gpu.py --ddp 2`, auto (19 exact steps, then DiLoCo; 25 syncs) | 14.6 s | 0.0796 |

## Phone (2026-10-16, ~02:00 IST)

Source: commit 950539a (body, and the numbers it wrote into the report).
Laptop side on a shared host. Build: 950539a's tree.

- `cpu.py` submitted from laptop A, spread over both laptops and the phone:
  the phone took 8 of 192 items; checksum unchanged. The phone first built the
  job's environment from the cache in 27 s.
- `phone_train.py` variant training 160 small torch models: the phone took 6 of
  160, all converged. With 32 models the laptops finished before the phone's
  worker had loaded torch (0 of 32).
- `cpu.py` on the phone itself, without its pandas step, three runs each:
  Nix's Python under `proot` 62.4, 62.8, 66.0 s; Termux's Python 3.13 73.1,
  80.8, 72.8 s. The full script took 68.4-72.4 s under `proot`.
