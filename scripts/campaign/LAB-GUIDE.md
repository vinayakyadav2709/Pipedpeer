# Lab Campaign Guide

A step-by-step guide for setting up a driver PC, running the lab campaign across lab computers, collecting results, uploading them to Google Drive, cleaning up the driver, and updating the paper.

Target OS for driver: **Fedora** (all required tools are installed by `setup-driver.sh`).

---

## Step 1: Clone the Repository

On the driver laptop (Fedora):

```bash
git clone -b dev https://github.com/vinayakyadav2709/Pipedpeer.git
cd Pipedpeer
```

*What to do if this fails:* Ensure internet connectivity and git are available (`sudo dnf install -y git`).

---

## Step 2: Set Up the Driver

Run the setup script:

```bash
./scripts/campaign/setup-driver.sh
```

This records the initial system state, installs all required Fedora packages (via `dnf`), builds the `pipedpeer` binary, and verifies system readiness with `check-driver.sh --no-r2`. The script ends by printing `DRIVER-OK`.

*What to do if this fails:*
- If non-Fedora error appears: the driver must run Fedora.
- If package installation fails: check your internet connection and sudo credentials.
- If `DRIVER-OK` does not appear: check the `MISSING:` lines printed by `check-driver.sh` and install any missing tools as suggested.

---

## Step 3: Run Lab Sessions

Follow `scripts/campaign/OPERATOR.md` starting from **Session 1**:

1. **Serve setup files** (keep running in a dedicated terminal):
   ```bash
   scripts/campaign/serve.sh <driver-ip> <lab-cidr>
   ```
2. **Setup each lab PC**: Take the baseline snapshot on each lab PC, then run the setup line printed by `serve.sh`.
3. **Discover PCs**:
   ```bash
   scripts/campaign/scan.sh <lab-cidr>
   ```
4. **Baseline and installation**: Run `inventory.sh`, `clocks.sh`, `network.sh`, and `install.sh` as directed in `OPERATOR.md`.
5. **Execute experiments**: Run the experiment matrix (`calibrate.sh`, `run.sh`, `e4.sh` through `e19.sh`).

*What to do if this fails:* Consult the "What to do when" table at the bottom of `scripts/campaign/OPERATOR.md`.

---

## Step 4: Clean Lab PCs (Before Driver Wipe)

> [!IMPORTANT]
> You **MUST** clean the lab PCs before wiping the driver. The driver's `wipe.sh` script deletes `~/.ssh/pipedpeer-lab`, which is needed to contact the lab PCs over SSH.

On the driver:

```bash
source scripts/campaign/lib.sh
f=$LAB/hosts-leaving.txt   # every PC in hosts.tsv, also the excluded ones
awk -F'\t' '!/^#/ && NF >= 2 { print $1 }' "$LAB/hosts.tsv" >"$f"
lab_fanout uninstall "$f" 'pipedpeer uninstall -y'
lab_fanout undo "$f" 'sudo systemd-run --unit=lab-undo bash /tmp/lab-pc.sh undo'
```

On each lab PC:
- Check that `journalctl -u lab-undo` ends with `undo complete`.
- Verify the system matches its initial snapshot. The undo also removes `~/pipedpeer-lab`, `~/.ssh/pipedpeer-lab-pc00*` and, where setup found none, Nix.

*What to do if this fails:* If a lab PC is unreachable or fails undo, log in locally and run `sudo bash /tmp/lab-pc.sh undo` directly on that machine.

---

## Step 5: Collect the Results Bundle

On the driver:

```bash
./scripts/campaign/collect.sh
```

This creates the folder `~/Documents/pipedpeer/lab-bundle`. It holds the results, the tables (`analysis.txt`) and figures (`figures/`) from `analyse.py`, the paper and report LaTeX files, the cleanup script, and `MANIFEST.sha256`, a list of checksums for every file.

*What to do if this fails:*
- If it complains `runs.jsonl` is missing: ensure experiments finished and wrote to `results/lab/runs.jsonl`.
- If `ANALYSIS-FAILED` is printed: read the error printed after it and fix the cause. The folder has no `MANIFEST.sha256` then, so `wipe.sh` refuses it. Your results are still in `results/lab`; delete `~/Documents/pipedpeer/lab-bundle` and run `collect.sh` again.
- If it says the target folder is not empty: an earlier bundle is there. Move it elsewhere or delete it if you no longer need it, then run `collect.sh` again.

---

## Step 6: Upload to Google Drive

Open your web browser and upload the entire folder `~/Documents/pipedpeer/lab-bundle` to your Google Drive folder for safe-keeping.

*What to do if this fails:* Ensure upload completes 100% and all files (especially `analysis.txt`, `figures/`, `results/lab/runs.jsonl`) are present in Drive.

---

## Step 7: Wipe the Driver PC

Once the bundle is safely uploaded and verified:

```bash
./scripts/campaign/wipe.sh
```

The script checks every file in `~/Documents/pipedpeer/lab-bundle` against `MANIFEST.sha256` and refuses to run if one changed or the bundle is missing. It lists everything it will remove, then asks you to confirm. To see the list without deleting anything, add `--dry-run` first. To confirm, type:
```text
DELETE EVERYTHING
```

This removes:
- The `pipedpeer` daemon and binary
- User systemd units (`pipedpeer.service`, `pipedpeer-agents.slice`)
- Lab state, cache, data directories, and SSH lab keys (`~/.ssh/pipedpeer-lab*`)
- Lab containers and images (in podman/docker)
- Nix, only if it was not on this PC before `setup-driver.sh` ran and has been installed since
- The cloned Pipedpeer repository

Nothing in `~/Documents/pipedpeer` is removed, so the bundle and anything else you put there stay. Delete that folder yourself once you no longer need it.

*What to do if this fails:* Any failing step prints `PROBLEM: ...`. The script continues through all remaining steps and exits non-zero at the end with a summary count.

---

## Step 8: Update Paper and Manuscript

On any computer (using only the uploaded bundle or the local `~/Documents/pipedpeer/lab-bundle`):
Follow `PAPER-UPDATE.md` (inside the bundle). You can either manually update the LaTeX files or copy the included prompt into an AI assistant (such as DeepSeek or Claude) along with `analysis.txt` to generate publication-ready LaTeX diffs.
