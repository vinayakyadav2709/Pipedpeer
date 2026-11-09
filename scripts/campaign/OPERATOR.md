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

**When anything in this sheet fails, do not try to fix it.** Run

    scripts/campaign/collect.sh --bug

It prints `BUG-BUNDLE <folder>`: logs, versions and memory evidence from the
driver and every PC, with keys, `r2.env`, `cluster.json` and invite codes left
out. Send that whole folder to the project owner and stop.

Start with the three-PC check. It is the first thing run on real PCs, before any
other test: Docker cannot show CPU-busy thresholds, real network transfer, the
nix store mode or a real join. It runs the riskiest pieces first (E17 losing
its introducer, a Ray cluster) so the worst surprises come in the first hour.
It runs on 3 PCs (E17 needs three) with its own results folder, so
nothing from it can reach `results/lab`. In every terminal used for the check
(also the `serve.sh` one), first:

    export LAB_RESULTS="$HOME/pipedpeer-rc-check"
    mkdir -p "$LAB_RESULTS" && echo "rc-check-$(date +%F)" >"$LAB_RESULTS/campaign_id"
    bash -c 'source scripts/campaign/lib.sh; echo "$LAB"'

Pass: the last line is `$HOME/pipedpeer-rc-check` spelled out (an absolute path
without spaces). Anything else (empty, relative, `results/lab`): stop and report.
Close these terminals, or `unset LAB_RESULTS`, before "The full session" below.

Why this is enough: every file the scripts keep on the driver (`hosts.tsv`,
`ssh_config`, `known_hosts`, `sizes.env`, `runs.jsonl`, `campaign_id`, `serve/`,
logs) is built from `$LAB`, and no script names `results/lab` outside comments
and usage text. Shared by both folders on purpose: the lab key
`~/.ssh/pipedpeer-lab` and the ssh control sockets `~/.ssh/cm-*`, which hold no
results. The PCs are shared too: what the check installs stays on them until
C10 and C11.

C1. Serve and set up three PCs (second terminal, leave it running; `<driver ip>`
    and `<lab cidr>` as in step 1 of the full session). pc00 should be the PC
    with the most memory (8 GiB or more: it carries the Ray head and the Dask
    scheduler). On each of the three PCs
    take the baseline snapshot (step 2 of the full session), then the user types
    the setup line printed here:

        scripts/campaign/serve.sh <driver ip> <lab cidr>

    Pass: each PC prints one `LAB-PC host=...` line. Otherwise stop and report.

C2. Find them, then inventory, install, stage, venv:

        scripts/campaign/scan.sh <lab cidr> --pc00 <pc00 ip>
        scripts/campaign/inventory.sh
        scripts/campaign/install.sh <version>
        scripts/campaign/network.sh
        scripts/campaign/stage.sh
        scripts/campaign/pins.sh
        scripts/campaign/venv.sh
        scripts/campaign/clocks.sh
        scripts/campaign/preflight.sh

    Pass: `scan.sh` lists exactly three PCs, no command prints `FAIL`,
    `FAILED` or exits non-zero, and `preflight.sh` ends with `fail=0`. Each
    `PREFLIGHT ... FAIL` line says what to do on that PC; do it and run
    `preflight.sh` again. `WARN` lines: write them down and go on. A command
    that fails after a fix may be run again; `install.sh` and `venv.sh` are
    safe to rerun.

C3. Short sizes (target 5 s, only in `$LAB_RESULTS`), then the smoke run:

        scripts/campaign/calibrate.sh --target 5
        scripts/campaign/run.sh SMOKE --sizes 1,2

    Pass: `calibrate.sh` ends by printing `sizes.env`; `run.sh` ends without
    `NOT-READY`, `TIMEOUT` or `FAILED`. Otherwise stop and report.

C3a. The reference checksums E17 compares against, then a small E17 (about
     5 minutes; it kills pc00's daemon on purpose and starts it again):

        scripts/campaign/run.sh REF --sizes 1
        scripts/campaign/e17.sh --n 2 --joiners 1 --repeats 2

     Pass: it ends with `E17-DONE`, and every row of
     `$LAB_RESULTS/analysis-input/e17.csv` after the header has `kill_at_s`
     filled (pc00 really died mid-job), `job_result_ok` 1 (the job survived
     with the right answer), `new_join_ok` 1 with a `new_join_s` of a few
     seconds (the third PC joined through pc01's new invite), and
     `new_remote_items` above 0 (work still left it). `old_invite_joined` is 0
     here by construction (it needs a second joiner). On the emulated lab a
     row looked like `6,1.617,2.589,1,1.233,1,20,0`. An empty field means a
     step timed out: the reason is in `$LAB_RESULTS/campaign.log` (grep E17).
     "partly connected" from connect is expected: pc01's invite still names
     the dead pc00. Anything else: `collect.sh --bug` and stop.

C3b. A small Ray job, the second worst surprise (pc00 hosts the Ray head):

        scripts/campaign/e19.sh --n 1 --add 1 --repeats 1 --systems ray

     Pass: exits 0 with no `NOT-READY` or `TIMEOUT`, and
     `$LAB_RESULTS/analysis-input/e19.csv` has a `ray,baseline` and a
     `ray,join` row with `result_ok` 1 and `join_status` ok. On the emulated
     lab both took about 30 s. Memory: `cluster.sh` refuses a Ray head under
     2 GiB and a worker under 1 GiB (`RAY-MEMORY ... refused` in the log,
     `NOT-READY ray pc=<pc>`) and warns under 4 / 2 GiB. The emulated pc00
     (Ray head, dashboard, Dask scheduler and workers, the daemon) peaked at
     1.24 GB resident and 3.0-3.3 GB with page cache, so a real pc00 needs
     4 GiB free beyond its desktop; `preflight.sh` asks for 8 GiB total. If
     the run hangs (no new `wait ... not finished yet` progress for 10
     minutes, pc00 slow to answer ssh), that is the stall seen on the laptop:
     on pc00 run `free -m; cat /proc/pressure/memory` and note it, then
     Ctrl-C, `collect.sh --bug`, and stop.

C4. The four runs, one command at a time:

        scripts/campaign/e18.sh --n 2 --repeats 1
        scripts/campaign/e19.sh --n 1 --add 1 --repeats 1
        scripts/campaign/run.sh E20 --repeats 1
        scripts/campaign/run.sh E14 --sizes 2 --workloads DS20M --repeats 1

    Pass: each exits 0 and prints no `NOT-READY` or `TIMEOUT`, and `$LAB_RESULTS/BLOCKERS.md` does not exist.
    `UPLOAD-FAILED` is expected (no R2) and is not a failure. Otherwise stop and
    report which command and its last 20 lines.

C5. The sampler against `top`. Run `e19.sh --n 1 --add 1 --repeats 1` once more
    and, while it runs, on the joining PC (the one that is not pc00):

        top -b -n 3 -d 2 | grep '^%Cpu'

    Pass: `top` shows the cores busy (`us`+`sy` above 50 %) during the same
    seconds that `$LAB_RESULTS/analysis-input/e19.csv` gives a `first_work_s`
    for that run, and no `first_work_s` is reported while `top` shows idle. If
    they disagree, stop and report both numbers.

C6. `pipedpeer-bare` really ran (the store is not private):

        grep '"system":"pipedpeer-bare"' "$LAB_RESULTS/runs.jsonl" | grep -c '"result_ok":true'

    Pass: 1 or more. Otherwise
    stop and report.

C7. E14's 20 MB payloads pass the pool body limit:

        grep '"workload":"DS20M"' "$LAB_RESULTS/runs.jsonl" | grep -c '"result_ok":true'

    Pass: 1 or more for `pipedpeer`. If it is 0, or `err` files under
    `$LAB_RESULTS/hosts/*/runs/*` mention a body limit or `413`: stop and report.

C8. torchrun at N=2 (E8 needs it):

        scripts/campaign/run.sh E8 --systems torchrun --sizes 2 --repeats 1

    Pass: exits 0 without `NOT-READY`. Otherwise stop and report.

C9. The ssh drop: during a detached `run.sh SMOKE --sizes 2 --repeats 1` (new
    `--repeats` value so it is not skipped), switch the laptop WiFi off for 60 s,
    then on. Pass: the command finishes by itself, and
    `grep -c '"workload":"W1"' "$LAB_RESULTS/runs.jsonl"` does not grow by more
    than the runs it started (no run twice). Otherwise stop and report.

C10. Leave one PC (the Leaving section, with `LAB_RESULTS` still set, applied to
     the PC you choose by editing `$f` to one id). Pass: `journalctl -u lab-undo`
     on it ends with `undo complete`, and the snapshot comparison is clean.

C11. Write down, per item above, "what differed from Docker, on 3 real PCs, one
     LAN" in `$LAB_RESULTS/rc-report.md`, and give the folder to the project owner.
     Then clean up so nothing from the check can be mistaken for a result:

         unset LAB_RESULTS
         ls results/lab/runs.jsonl 2>&1   # pass: "No such file" (or the real runs only)

     Keep `$HOME/pipedpeer-rc-check` until the owner has read it. The other PC
     keeps the install; "The full session" repeats `install.sh`, which is safe
     to repeat.

## The full session

1. Serve the setup files (second terminal; leave it running). `<driver ip>` is
   the driver's address on the lab network, `<lab cidr>` the lab subnet:

       scripts/campaign/serve.sh <driver ip> <lab cidr>

2. On each PC, before anything is typed, take the baseline snapshot (kept off
   the PC): `dpkg -l`, `ls /etc/sudoers.d`, `~/.ssh/authorized_keys`,
   `ufw status verbose`, `systemctl is-enabled sleep.target suspend.target
   hibernate.target hybrid-sleep.target`. "Leaving" compares against it.
   Then the user types the setup line `serve.sh` printed (it is also in
   `results/lab/serve/SETUP-LINE.txt`). It prints one `LAB-PC host=...` line.
   Before the full install, do the three-PC check above. Only then set up the rest.
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
    lab_fanout undo "$f" 'sudo systemd-run --unit=lab-undo bash /var/lib/pipedpeer-lab/lab-pc.sh undo'

The undo stops sshd, so the fan-out cannot read its answer. Check on each PC
(or its screen): `journalctl -u lab-undo` ends with `undo complete`. If
`/var/lib/pipedpeer-lab/lab-pc.sh` is gone (it is removed only by a finished undo), type the setup line's download part again
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
| Anything failing that this table does not cover | `scripts/campaign/collect.sh --bug`; send the folder it names (`BUG-BUNDLE <folder>`) to the project owner. Do not try fixes on the PCs or in the scripts |
