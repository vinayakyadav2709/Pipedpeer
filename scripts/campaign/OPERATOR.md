# Operator sheet

For a complete walkthrough from a fresh clone, see `LAB-GUIDE.md`.

Run everything from the repo root on the driver (the laptop) unless a line says
otherwise. Paths below are relative to `scripts/campaign/`; results land in
`results/lab/` (`$LAB_RESULTS` moves it). Each command prints its own
verdict; the case spec (`docs/benchmarks/campaign-cases.md`) lists what each
script does when something breaks. Containers (`test-ubuntu-lab.sh`) prove the
scripts' logic and ordering, never speed; they have no GNOME session, proxy,
captive portal or WiFi isolation.

## Before the lab

Driver tools (Fedora; `check-driver.sh` prints the same line per missing tool):

    sudo dnf install -y pssh parallel rsync nmap iperf3 openssh-clients python3-matplotlib python3-pandas python3-scipy python3-pytest rclone ShellCheck uv golang podman make

Cloudflare R2 is not used. Results are saved locally in `results/lab/` and copied
to Google Drive after each lab session and before any PC cleanup.

The lab key the PCs will trust (the check below needs it; `serve.sh` reuses it):

    [ -f ~/.ssh/pipedpeer-lab ] || ssh-keygen -t ed25519 -N '' -f ~/.ssh/pipedpeer-lab

Or let `scripts/campaign/setup-driver.sh` do it: it records whether Nix was
already on the driver (so `wipe.sh` removes only what the campaign added),
installs the packages above, builds pipedpeer and runs `check-driver.sh`.

Check the driver (everything must print `OK`, last line `DRIVER-OK`):

    scripts/campaign/check-driver.sh --no-r2

Rehearse once on Ubuntu containers (3 GB of RAM free; minutes to an hour).
The rehearsal installs the local build, so build it first:

    make build

    scripts/campaign/test-ubuntu-lab.sh up
    scripts/campaign/test-ubuntu-lab.sh test
    scripts/campaign/test-ubuntu-lab.sh down

## Session 1

1. Serve the setup files (second terminal; leave it running). `<driver ip>` is
   the driver's address on the lab network, `<lab cidr>` the lab subnet:

       scripts/campaign/serve.sh <driver ip> <lab cidr>

2. On each PC, before anything is typed, take the baseline snapshot (kept off
   the PC): `dpkg -l`, `ls /etc/sudoers.d`, `~/.ssh/authorized_keys`,
   `ufw status verbose`, `systemctl is-enabled sleep.target suspend.target
   hibernate.target hybrid-sleep.target`. "Leaving" compares against it.
   Then the user types the setup line `serve.sh` printed (it is also in
   `results/lab/serve/SETUP-LINE.txt`). It prints one `LAB-PC host=...` line.
   Before the full install, check two PCs end to end (real conditions): the
   setup line typed by the user, `scan.sh`, `install.sh`, `run.sh SMOKE
   --sizes 1,2`, an ssh drop (laptop WiFi off 60 s during a detached run; it
   must finish and be fetched once), then the leaving steps below on one PC.
   Only then set up the rest.
3. Find the PCs; `<pc00 ip>` is the PC nearest the driver:

       scripts/campaign/scan.sh <lab cidr> --pc00 <pc00 ip>

4. Machine and clock baseline:

       scripts/campaign/inventory.sh
       scripts/campaign/clocks.sh

5. Install and connect (`<version>` is the pinned release; for a local build
   prefix `PP_BINARY=bin/pipedpeer`):

       scripts/campaign/install.sh <version>
       scripts/campaign/network.sh

6. Workload files, pinned versions, one venv per PC, workload sizes (re-calibrate
   first since `sizes.env` contains new keys for sweeps, noop, lat, elastic and fileio):

       scripts/campaign/stage.sh
       scripts/campaign/pins.sh
       scripts/campaign/venv.sh
       scripts/campaign/calibrate.sh

7. The first runs:

       scripts/campaign/run.sh REF
       scripts/campaign/run.sh SMOKE
       scripts/campaign/run.sh FC

8. Write `results/lab/predictions.md`, one line per prediction, before any
   E1 run: `PRED <system> <workload> min_n=<N> speedup=<x>`.
Before each long run, check that no PC is idle-suspending or locked:
`systemctl is-enabled sleep.target suspend.target` prints `masked` on every PC.
Setup masks them; a PC that rebooted or a user who logged in on the console can
undo that, and a PC that sleeps mid-run is a failed run.

## Sessions 2 and 3

Experiments can be executed across sessions; each run logs its progress and
rerunning any `run.sh` line automatically skips completed runs in `runs.jsonl`.
Operator attendance per experiment:
- **E4** (uneven machines), **E5** (environment distribution), **E6** (node
  faults, killed by the script), **E13-E16**, **E20**, **E17-E19**: unattended.
- **E11** (cluster size): the setup line must first be run on every available
  PC, by hand; the measurement then runs unattended.
- **E3** (optional manual setup timing): the operator follows each tool's quick
  start by hand.

Commands:

    scripts/campaign/run.sh E1
    python3 scripts/campaign/analyse.py --spread
    scripts/campaign/run.sh E1b
    scripts/campaign/run.sh E7
    scripts/campaign/run.sh E8
    scripts/campaign/run.sh E13
    scripts/campaign/run.sh E14
    scripts/campaign/run.sh E15
    scripts/campaign/run.sh E16
    scripts/campaign/run.sh E20
    scripts/campaign/e4.sh
    scripts/campaign/e5.sh --n 32
    scripts/campaign/e6.sh --n 16
    scripts/campaign/e11.sh
    scripts/campaign/e17.sh
    scripts/campaign/e18.sh
    scripts/campaign/e19.sh
    python3 scripts/campaign/analyse.py

Run `scripts/campaign/collect.sh` to package results for Google Drive after each session and before any PC cleanup.

All tables use one clock, the driver's. Only a duration measured across two
PCs (for example a join time taken on the joiner after t0 on the driver) is
corrected by the offsets from `clocks.sh`; nothing else is shifted.

`analyse.py --spread` prints `REPEATS-OK` or `RAISE-REPEATS <k>`; for the
latter see "What to do when". Rerunning any `run.sh` line skips runs already in
`runs.jsonl`.

## Leaving

On the driver, in this order (the repository is only there; the PCs get the
commands over ssh):

    source scripts/campaign/lib.sh   # from the repo root; defines lab_fanout, $LAB
    f=$LAB/hosts-leaving.txt         # every PC in hosts.tsv, also the excluded ones
    awk -F'\t' '!/^#/ && NF >= 2 { print $1 }' "$LAB/hosts.tsv" >"$f"
    lab_fanout uninstall "$f" 'pipedpeer uninstall -y'
    lab_fanout undo "$f" 'sudo systemd-run --unit=lab-undo bash /tmp/lab-pc.sh undo'

The undo stops sshd, so the fan-out cannot read its answer. Check on each PC
(or its screen): `journalctl -u lab-undo` ends with `undo complete`. If
`/tmp/lab-pc.sh` is gone (reboot), type the setup line's download part again
first. The undo also removes `~/pipedpeer-lab` and `~/.ssh/pipedpeer-lab-pc00*`
on the PC, and Nix where setup found none (a Nix that was there before is left).
Then compare each PC with the snapshot taken in Session 1, step 2, before
setup (same five commands, plus `ls ~/pipedpeer-lab ~/.ssh`).

## What to do when

| You see | Do |
|---|---|
| A PC is off or dropped | wait for it; `scripts/campaign/scan.sh <lab cidr>` finds it again (ids stay). If it stays gone, add its id to `results/lab/excluded.txt` with a `#` reason |
| `FANOUT step=<s> ok=<a> failed=<b>` with `failed>0` | read `results/lab/logs/<s>/FAILED`; fix the PC, then run the same script again |
| `TIMEOUT <run id>` | the run unit is left running; wait, then rerun the same `run.sh` line (it fetches the finished run) |
| `NOT-READY <system> want=<n> have=<k>` | `scripts/campaign/cluster.sh down`, then `scripts/campaign/cluster.sh up <n>`; if it repeats, exclude the PC as in the first row |
| `RAISE-REPEATS <k>` | `scripts/campaign/run.sh <experiment> --repeats <k>` for the experiment named in the `SPREAD` lines, then `analyse.py --spread` again |
| `UPLOAD-FAILED <why>` | expected since Cloudflare R2 is not used; copy `results/lab/` to Google Drive manually after each session and before PC cleanup |
| A line in `results/lab/BLOCKERS.md` | stop that experiment, read the line, tell the project owner; later experiments may go on |
