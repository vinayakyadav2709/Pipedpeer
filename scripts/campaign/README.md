# Campaign scripts

`scripts/campaign/` drives the lab comparison (Ray, Dask, torchrun, plain ssh vs
Pipedpeer) on the college PCs. See `docs/benchmarks/lab-campaign.md` (local-only)
for the full plan and `docs/benchmarks/campaign-cases.md` for the input cases each
script's tests take their rows from. Operator steps live in `OPERATOR.md`.

| Script | What it does | When |
|---|---|---|
| `lib.sh` | ssh/rsync/pssh fan-out, detached runs, waits, fetches | sourced by all |
| `setup-driver.sh` | Fedora driver packages, build and readiness check | before Session 1 |
| `check-driver.sh` | driver tool/key/r2.env readiness | before Session 1 |
| `lab-pc.sh` | one-line PC setup and exact undo | Session 1 (per PC) |
| `serve.sh` / `scan.sh` | stage + serve the setup line; scan the lab subnet | Session 1 |
| `inventory.sh` / `clocks.sh` / `network.sh` | machine, clock and network baseline | Session 1 |
| `install.sh` / `pins.sh` / `venv.sh` | install Pipedpeer, pin versions, build venvs | Session 1 |
| `cluster.sh` | bring up pipedpeer/Ray/Dask on N PCs | every size step |
| `run.sh` / `systems.sh` / `experiments.tsv` | resumable experiment matrix | Sessions 1-4 |
| `calibrate.sh` | size workloads to ~120 s | Session 1 |
| `metrics.sh` / `upload.sh` | host metrics and R2 upload | every run |
| `analyse.py` | medians, bootstrap CIs, figures | after each session |
| `collect.sh` | assemble results, analysis, docs and manifest into a bundle | after lab runs |
| `wipe.sh` | clean up lab artifacts, daemon, units, images and repo | after bundle upload |
| `e4.sh` / `e5.sh` / `e6.sh` / `e11.sh` | uneven machines, env shipping, faults, cluster size | Sessions 2-3 |
| `test-ubuntu-lab.sh` (lab/ubuntu) | Docker rehearsal of steps 1-8 | before Session 1 |
| `port-lines.sh` | changed-line counts per port | after ports land |
| `stamp.py` | first-output timestamp for a detached run | sourced by runs |
