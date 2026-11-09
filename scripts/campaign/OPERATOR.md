# Operator sheet

Run everything from the repo root on the driver (the laptop) unless a line says
otherwise. Paths below are relative to `scripts/campaign/`; results land in
`results/lab/` (`$LAB_RESULTS` moves it). Each command prints its own
verdict; the case spec (`docs/benchmarks/campaign-cases.md`) lists what each
script does when something breaks. Containers (`test-ubuntu-lab.sh`) prove the
scripts' logic and ordering, never speed; they have no GNOME session, proxy,
captive portal or WiFi isolation.

## Before the lab

Driver tools (Fedora; `check-driver.sh` prints the same line per missing tool):

    sudo dnf install -y pssh parallel rsync nmap iperf3 openssh-clients python3-matplotlib python3-pandas python3-scipy python3-pytest rclone ShellCheck uv

R2 credentials, one file, mode 600 (replace the four placeholders):

    mkdir -p ~/.config/pipedpeer-lab && install -m 600 /dev/null ~/.config/pipedpeer-lab/r2.env
    cat >~/.config/pipedpeer-lab/r2.env <<'EOF'
    RCLONE_CONFIG_R2_TYPE=s3
    RCLONE_CONFIG_R2_PROVIDER=Cloudflare
    RCLONE_CONFIG_R2_ACCESS_KEY_ID=<access key id>
    RCLONE_CONFIG_R2_SECRET_ACCESS_KEY=<secret access key>
    RCLONE_CONFIG_R2_ENDPOINT=https://<account id>.r2.cloudflarestorage.com
    EOF

The lab key the PCs will trust (the check below needs it; `serve.sh` reuses it):

    [ -f ~/.ssh/pipedpeer-lab ] || ssh-keygen -t ed25519 -N '' -f ~/.ssh/pipedpeer-lab

Check the driver (everything must print `OK`, last line `DRIVER-OK`):

    scripts/campaign/check-driver.sh

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
3. Find the PCs; `<pc00 ip>` is the PC nearest the driver:

       scripts/campaign/scan.sh <lab cidr> --pc00 <pc00 ip>

4. Machine and clock baseline:

       scripts/campaign/inventory.sh
       scripts/campaign/clocks.sh

5. Install and connect (`<version>` is the pinned release; for a local build
   prefix `PP_BINARY=bin/pipedpeer`):

       scripts/campaign/install.sh <version>
       scripts/campaign/network.sh

6. Workload files, pinned versions, one venv per PC, workload sizes:

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
9. Real-conditions check on 2 PCs before the long runs: the setup line typed by
   the user, `scan.sh`, `install.sh`, `run.sh SMOKE --sizes 1,2`, an ssh drop
   (laptop WiFi off 60 s during a detached run; it must finish and be fetched
   once), then the leaving steps below on one PC.

## Sessions 2 and 3

    scripts/campaign/run.sh E1
    python3 scripts/campaign/analyse.py --spread
    scripts/campaign/upload.sh
    scripts/campaign/run.sh E1b
    scripts/campaign/upload.sh
    scripts/campaign/run.sh E7
    scripts/campaign/run.sh E8
    scripts/campaign/upload.sh
    scripts/campaign/e4.sh
    scripts/campaign/e5.sh --n 2
    scripts/campaign/e6.sh --n 2
    scripts/campaign/e11.sh
    scripts/campaign/upload.sh
    python3 scripts/campaign/analyse.py

`analyse.py --spread` prints `REPEATS-OK` or `RAISE-REPEATS <k>`; for the
latter see "What to do when". Rerunning any `run.sh` line skips runs already in
`runs.jsonl`.

## Leaving

On the driver, in this order (the repository is only there; the PCs get the
commands over ssh):

    source scripts/campaign/lib.sh   # from the repo root; defines lab_hosts, lab_fanout
    f=$(lab_hosts <number of PCs>)
    lab_fanout uninstall "$f" 'pipedpeer uninstall -y'
    lab_fanout undo "$f" 'sudo systemd-run --unit=lab-undo bash /tmp/lab-pc.sh undo'

The undo stops sshd, so the fan-out cannot read its answer. Check on each PC
(or its screen): `journalctl -u lab-undo` ends with `undo complete`. If
`/tmp/lab-pc.sh` is gone (reboot), type the setup line's download part again
first. Then compare each PC with the snapshot taken in Session 1, step 2,
before setup (same five commands).

## What to do when

| You see | Do |
|---|---|
| A PC is off or dropped | wait for it; `scripts/campaign/scan.sh <lab cidr>` finds it again (ids stay). If it stays gone, add its id to `results/lab/excluded.txt` with a `#` reason |
| `FANOUT step=<s> ok=<a> failed=<b>` with `failed>0` | read `results/lab/logs/<s>/FAILED`; fix the PC, then run the same script again |
| `TIMEOUT <run id>` | the run unit is left running; wait, then rerun the same `run.sh` line (it fetches the finished run) |
| `NOT-READY <system> want=<n> have=<k>` | `scripts/campaign/cluster.sh down`, then `scripts/campaign/cluster.sh up <n>`; if it repeats, exclude the PC as in the first row |
| `RAISE-REPEATS <k>` | `scripts/campaign/run.sh <experiment> --repeats <k>` for the experiment named in the `SPREAD` lines, then `analyse.py --spread` again |
| `UPLOAD-FAILED <why>` | fix the cause (network, `r2.env`), run `scripts/campaign/upload.sh` again |
| A line in `results/lab/BLOCKERS.md` | stop that experiment, read the line, tell the project owner; later experiments may go on |
