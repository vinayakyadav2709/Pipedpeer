# The headless lab day

The lab campaign ([docs/benchmarks/lab-campaign.md](../../docs/benchmarks/lab-campaign.md)) without a driver laptop. One lab PC, pc00, does everything the laptop did: it is the Ansible controller, the OTel driver collector, the Ray head, the Dask scheduler and the pipedpeer submitter, and it counts as one of the N for pipedpeer.

| File | What |
|---|---|
| `lab.sh` | The one script typed on every lab PC, published as `landing/public/lab.sh`. Worker setup, `head`, `status`, `undo`. |
| `publish.sh` | Run at home: campaign key, passphrase, encrypted bundle, filled `landing/public/lab.sh`. `selftest` checks the live site. |
| `lab-fill.sh` | Fills the four published lines of `lab.sh`, and checks (in CI) that the published copy is the source. |
| `lab-day.sh` | The whole day on pc00, run by `lab.sh head` as the systemd unit `pipedpeer-lab-day`. |
| `day/` | The day's steps: `lib.sh` (ledger, progress, Ansible), `prepare.sh`, `finish.sh`, `cleanup-host.sh`, `sizes.py`, `tables.py`. |
| `profile-minimal.tsv` | The experiments the day runs, with `run.sh` arguments and time estimates. |
| `test/headless-test.sh` | The rehearsal on four systemd containers. |

## Before the lab (the project lead, at home)

1. Put the ingest Worker's URL and token in `~/.config/pipedpeer-lab/ingest.env` (mode 600): `INGEST_URL=…`, `INGEST_TOKEN=…` ([ingest/README.md](ingest/README.md)). `publish.sh` puts this file in the bundle as `r2.env`; `sync-r2.sh` finds the ingest credentials there. The Worker holds the only write access to the bucket and refuses uploads past the free tier, so the token is all the passphrase exposes. (R2 S3 keys in `r2.env` still work, without the cap.)
2. Merge the campaign branches, commit, and push the commit to GitHub. The lab PCs download the code at exactly that commit from codeload.
3. Run `scripts/campaign/publish.sh` (optionally `--version <pipedpeer nightly version>`; without it the day pins the newest nightly when it starts). Press Enter to get a generated passphrase, and write it down outside the repo.
4. Commit `landing/public/lab.sh` and `landing/public/lab-secrets.enc` to `dev` and push. The Pages workflow deploys `landing/` from `dev`.
5. When the deploy is done, run `scripts/campaign/publish.sh selftest`. It downloads both files from the live site, checks that they are the ones you committed and that codeload serves the commit, and decrypts the bundle with the passphrase you type.

Until step 3 has run, `landing/public/lab.sh` holds the placeholders, and on a lab PC it stops with `ERROR: this copy of lab.sh is not published yet`.

## In the lab (the operator)

On every lab PC except one, log in as the lab user, open a terminal, and type:

```sh
curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh | sudo bash
```

It asks for the user's sudo password (if sudo asks), installs the SSH server if needed, and ends with:

```text
LAB-PC host=lab-114 ip=10.20.1.114 user=student cores=8 ram_gib=16 machine_id=…

==================================================================
  DONE: this PC is ready, you can leave it.   (ip 10.20.1.114)
==================================================================
```

On the last PC, which becomes pc00, type the same line with `head`:

```sh
curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh | sudo bash -s -- head
```

It asks `Campaign passphrase:` (three tries). Then it sets this PC up like the others, downloads the code, starts the day and shows its log. The day waits until 17 PCs (pc00 included) answer, printing `discover: 12 of 17 PCs answered` as they appear. Write `head 9` to wait for 9 instead. If the expected count does not appear within 30 minutes, the day goes on with the PCs it found, and records that.

The operator may close the terminal, log out, or let the screen lock: the day is a system unit and does not depend on the session. To look again, type in any terminal on pc00:

```sh
curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh | sudo bash -s -- status
```

That prints the phase, the current step, the steps done and an ETA. `sudo journalctl -fu pipedpeer-lab-day` follows the full log.

The day ends with a summary on pc00's screen (if the terminal is still open) and in `status`:

```text
======================================================================
  PipedPeer lab day: summary (2026-11-20 17:42)

  PCs: 17 found of 17 expected (pc00 included)

  Experiments:
    e1    ok        108 runs, 0 failed
    …
  Results in Cloudflare R2:
    r2:pipedpeer-lab/raw/    results/lab: runs.jsonl, CSVs, figures, logs
    r2:pipedpeer-lab/otel/   telemetry (host metrics, daemon logs)
    local:  412 objects in …
    remote: 412 objects in r2:pipedpeer-lab/raw

  ALL DONE, the lab PCs are back as they were. You can leave.
======================================================================
```

If something could not be cleaned, the last line is `DONE, but these PCs are not fully back as they were:` followed by one line per PC saying what is left.

If the day stops early (pc00 rebooted, power cut, the unit was killed), type the `head` line again on pc00. It asks for the passphrase again and continues from the last finished step.

## What the day does

| Phase | Steps |
|---|---|
| setup | Ansible in a venv on pc00 (`virtualenv.pyz` when Ubuntu lacks `python3-venv`), plus `ansible.posix`, which provides the json callback run.sh reads |
| discover | `scan.sh` on pc00's own subnet every 15 s, until `LAB_PCS` PCs log in with the campaign key; pc00 is pinned to `pc00` and reached with `ansible_connection=local` |
| prepare | `machines.csv`; `network.csv` (iperf3 both ways to pc00, one PC at a time; ping RTT); `clocks.txt` (chrony only on PCs over 10 ms); the code to every PC; `install-pipedpeer.sh --version <v> -y` and `pipedpeer doctor`; `pipedpeer invite` / `connect`; the Nix env; tcpdump (`lab.sh pkg`, so undo removes it), otelcol and every PC's collector, poller and capture (`node.sh start <pc> <pc00> --session s1 --pcap`, the session also in `~/.local/share/pipedpeer-lab/session`), the driver collector on pc00 (file only with ingest credentials: `sync-r2.sh` uploads its rotated files) |
| calibrate | `calibrate.sh W1/W2/W3` on pc01 with `CAL_VERIFY=1` at a 60 s strong target (`LAB_CAL_STRONG_S`), again if the verify run lands more than 25 % off it; writes `workloads.json` and `results/lab/workloads.md`. Then the plain W1 result at each e1b weak size, one per idle worker, in parallel, into run.sh's `result-cache.json` |
| run | Each row of the profile: a health check, then `PL_SYNC=0 run.sh <id> --inventory results/lab/inventory.ini <args>`, a line in `attempts.jsonl`, then `sync-r2.sh` |
| analyse | `analyse.py`, then `sync-r2.sh` |
| cleanup | Every PC: `node.sh pcap-stop`, and its pcap files pulled into `results/lab/pcap/<pc>/` over ssh. Every worker: `cleanup-host.sh`, then `lab.sh undo`. Driver collector down, final sync, `sync-r2.sh count` must match. Then pc00: `cleanup-host.sh`, summary, `lab.sh undo` |

### The minimal profile

`profile-minimal.tsv`: 17 PCs, N = 1, 4, 16, 3 repeats after 1 warm-up. It runs e1 (W1, W2, W3 × plain, pipedpeer, ray, dask), e1b, e4, e5 and e6 at 16, e7, and e8 at ranks 2, 4, 8. It skips e3, e9, e10 and e11. Pass another profile with `LAB_PROFILE`.

The estimates in the profile add up to about 5.5 hours of experiments plus about 1 hour of setup and cleanup. That is more than the three hours the operator expects. Most of the time is e1 at N=1, where each workload's 125 s strong size runs 16 times (4 systems × 4 runs). To fit three hours, either lower the strong target (`calibrate.sh W1 60` gives a 60 s size), or drop W3 from e1 (`--workloads W1,W2`), or reduce e8.

### Distributing the Nix env

The campaign env is a 2.5 GiB closure. If 17 PCs each fetched it from cache.nixos.org, the lab's internet link would carry about 42 GiB. Instead:

1. pc00 builds it (`env/install.sh`), the only download from cache.nixos.org.
2. pc00 writes the closure, and the flake's inputs (`nix flake archive`), into a binary cache at `~/pipedpeer-lab/serve/nix` (`nix copy --to file://…?compression=zstd`). Its file server (`python3 -m http.server` on port 8137) serves that cache.
3. Each worker runs `sudo nix copy --no-check-sigs --from http://pc00:8137/nix <paths>`, then `env/install.sh`, which finds every path already present and downloads nothing.

The alternatives were worse:

- **`nix copy --to ssh-ng://worker` from pc00** needs the lab user to be a trusted Nix user on the worker. pc00 builds the env's top path locally, so that path carries no signature. Making the lab user trusted means editing `/etc/nix/nix.conf` on every PC.
- **A signed cache with pc00's key** also needs `nix.conf` changes on each PC, for `trusted-public-keys` and `substituters`.

Checked with real Nix (nixos/nix 2.31.3, two containers): the first built a small flake on the campaign's nixpkgs pin, then ran `nix copy --to file://…?compression=zstd` and `nix flake archive --json --to …`. The second had **no network**. It ran `nix copy --no-check-sigs --from file://…` with the output path and the two archived source paths, then `nix build --option substituters "" path:<flake>`. Both the evaluation of nixpkgs and the build found everything locally, and the program ran. The top path was built locally and carries no signature.

The binary-cache pull above runs as root through the passwordless sudo that `lab.sh` already grants, so the worker's Nix accepts the unsigned paths with no configuration change. If the pull fails, `install.sh` falls back to cache.nixos.org on its own. The file server also hands out the code tarball and the otelcol release, so the code comes from GitHub once and otelcol once.

## How the secrets are protected

- The SSH private key and `r2.env` are never in the repo. On Pages they exist only as `lab-secrets.enc`: a tar encrypted with `openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -md sha256 -salt`. The passphrase is the only protection, so `publish.sh` refuses one shorter than 24 characters or 6 words. The passphrase it generates is 6 groups of 5 characters from a 32-letter alphabet, about 150 bits.
- On pc00, `lab.sh head` reads the passphrase from `/dev/tty`, never from argv or the environment. openssl gets it on a file descriptor. The decrypted key must match the public key embedded in `lab.sh`, which catches a wrong passphrase that happens to decrypt.
- The decrypted files live in `/run/pipedpeer-lab`, on tmpfs, so a reboot wipes them. The directory belongs to root with mode 0711: nobody can list it. The two files in it belong to the lab user with mode 0600, because the day runs as that user and ssh refuses group-readable keys. The brief asked for a root-owned 0700 directory readable by the lab user; 0711 with lab-owned 0600 files is the nearest arrangement that ssh accepts. `lab.sh undo` on pc00 deletes the directory, and that undo runs only after the final R2 count matches.
- The keys reach the collector and rclone as environment variables only (`otel/README.md`).
- The SSH key opens only lab PCs that ran `lab.sh`, and `undo` removes it from each one. **After the lab**, revoke the R2 token, delete `~/.config/pipedpeer-lab/campaign_ed25519{,.pub}`, and remove `lab-secrets.enc` from `dev`. Git history keeps the old bundle, which then holds only a dead key and a revoked token.

## Resume and robustness

- `~/pipedpeer-lab/done` on pc00 is the ledger: one line per finished step, per experiment, and per PC cleaned or undone. On a rerun, `lab.sh head` skips the setup it already did (`lab.sh` records each change once). It does not unpack the code again, because the day's results live inside it. Then it restarts the unit, and `lab-day.sh` skips every step the ledger lists.
- An experiment cut off midway runs again from its start. The lines it had already written stay in `runs.jsonl`, and `results/lab/attempts.jsonl` marks them: `{"experiment": "e4", "status": "interrupted", "runs_from_line": 120, "runs_to_line": 126}`. Each later attempt has its own line with status `ok` or `failed`.
- A step that only pc00 can do (bootstrap, pipedpeer install, the Nix env on pc00) aborts the day if it fails. The day then goes straight to cleanup, so the PCs are still put back, and the summary says `STOPPED EARLY`.
- A worker that fails a prepare step, or does not answer the health check before each experiment, is dropped: removed from `results/lab/inventory.ini` and listed in `results/lab/dropped.csv` and `BLOCKERS.md`. It stays in `~/pipedpeer-lab/inventory-all.ini`, so cleanup tries it again. A failed experiment is recorded and the day continues.
- Cleanup never repeats: each worker's cleanup and undo has its own ledger line. If the final R2 count does not match after three syncs, pc00 keeps its results and secrets, the unit exits with an error, and rerunning `head` retries the sync. pc00's own undo runs last. Before it, the summary and a `day-finished` mark go to `/var/lib/pipedpeer-lab`, so a cut-off undo finishes on the next `head`.
- Nix is removed (`/nix/nix-installer uninstall`) only from PCs that had no `/nix` before the pipedpeer install. A PC with no record keeps its Nix. pipedpeer is uninstalled only where it was not there before.
- `lab.sh undo` also removes what the campaign leaves in the lab user's home, from a fixed list (`~/pipedpeer-lab`, `~/.ansible`, `~/.nix-profile`, `~/.cache/nix`, `~/.local/share/pipedpeer-lab`, `~/.config/pipedpeer`, `~/.ssh/known_hosts`, and so on). It removes only entries that did not exist at setup, plus parent directories that setup did not find and that are now empty. The user's own files stay.

## Rehearsal

`scripts/campaign/test/headless-test.sh` runs the whole day on four systemd containers (pc00 and w1, w3 on Ubuntu with ufw, w2 on Fedora with firewalld; 1 GB and 3 × 768 MB). A local http server stands in for Pages and codeload, `rclone serve s3` for R2. The test types the worker line on w1-w3 and the head line on pc00 through a pty. It kills the unit during e4, types the head line again, and then checks the resume, the R2 contents, the summary, and that every container's snapshot after the day equals the one before. `lab-pc-test.sh` covers lab.sh's worker mode, `pkg`, `enable` and `undo` in more detail.

The rehearsal uses stand-ins (in `test/headless-stubs/`) for:

- **pipedpeer and Nix:** a real install needs crun and Nix inside the container, plus the 2.5 GiB closure on four containers. The fake `pipedpeer` keeps state where the real one does, and the fake `nix` writes, serves and fetches a binary cache, so the distribution path through pc00's file server runs. Real `nix copy` between two stores is not exercised.
- **`env/install.sh`:** links the system python3 as the env.
- **`calibrate.sh`:** prints small sizes in the real `CALIBRATE` format, so `workloads.json` and `workloads.md` are rewritten for real.
- **`run.sh` and `analyse.py`:** lab/runner's real ones need pipedpeer, Ray and Dask. The stand-in reaches every PC through the day's inventory with Ansible, and writes one runs.jsonl line per (N, system, repeat).

Everything else is real: lab.sh, the passphrase on `/dev/tty`, the decryption, the systemd unit, scan.sh, Ansible over SSH, iperf3 installed and removed through `lab.sh pkg`, clocks.sh, the weak references, otelcol on every container with the driver uploading to the S3 stand-in, sync-r2.sh and its count, cleanup, and undo.

## Integration (lab/integrate)

What the merge of lab/runner, lab/traces and lab/headless needed, and where each item went. The rehearsal below runs all of it.

| # | Item | Resolution |
|---|---|---|
| 1 | json callback | `lib/ansible.sh install` installs `ansible.posix` beside `community.docker`, into the venv's collections dir the day also uses. |
| 2 | venv on Ubuntu | `pl_install` falls back to `virtualenv.pyz` when `python3 -m venv` fails, as the day's bootstrap does. |
| 3 | pc00 is local | Nothing in `systems.py` assumes ssh: every command is the raw module, and `Host.ip` is `ansible_host`. The rehearsal runs pc00 with `ansible_connection=local` throughout. |
| 4 | SSH options | `pl_ansible_env` keeps the exported `ANSIBLE_SSH_ARGS`; the day's `~/pipedpeer-lab/known_hosts` is the only one written. |
| 5 | weak references | `day/sizes.py` uses `record.ResultCache.key`; the rehearsal checks that every e1b run's `result_ref` is `plain-precomputed:<pc>`. |
| 6 | cut-off attempts | `analyse.py` drops the `[runs_from_line, runs_to_line)` lines of every `interrupted` entry in `attempts.jsonl` (test: `test_interrupted_attempt_lines_are_dropped`); the summary counts runs the same way. |
| 7 | exit status | `run.sh` exits 0 when the experiment finished, failed runs included; non-zero only on a crash or Ctrl-C. |
| 8 | profile arguments | `e5 --n 16` and `e6 --n 16` take N from the last `--n` value; `e4 --n 4` caps 8/4/2/1; `e7` always runs at N=2; `e8 --n 2,4,8 --repeats 3` is ranks 2, 4, 8 with seeds 1-3 after a seed-1 warm-up; `e1`/`e1b --n 1,4,16` applies to every workload. |
| 9 | fewer workers than N | Each run at that N is recorded as failed (`inventory has 15 workers, N=16 asked`) and the experiment goes on; E5's pip method now does the same instead of running on fewer PCs. The rehearsal's e1 at N=4 on 3 workers exercises it. |
| 10 | what run.sh leaves | Everything is under `~/.local/share/pipedpeer-lab` (work dirs, E5's venv, logs, `current-run`, `session`, pcap), `/tmp/ray`, `/tmp/dask-*`; E5's pip uses `--no-cache-dir`. `cleanup-host.sh` now stops the poller and tcpdump (`node.sh stop`) and checks they are gone; `install-otelcol.sh remove` does the same. The rehearsal's snapshot diff found the home leftovers listed in `lab.sh`'s `HOME_PATHS`. |
| 11 | analyse.py | Runs from the repo root with no arguments, in the Ansible venv (matplotlib). |
| 12 | double sync | The day sets `PL_SYNC=0`, so `run.sh` leaves the upload to the day's own `sync-r2.sh`, after `attempts.jsonl` has the attempt. |
| 13-15 | otel hooks | Unchanged names. The day now starts every PC with `--session s1 --pcap`, installs tcpdump with `lab.sh pkg`, and pulls the pcaps (step `pull.pcap`) before cleanup. |
| 16 | docs | Part 1 of `lab-campaign.md` is the no-laptop flow; `results/lab/OPERATOR.md` is the operator's page. |

The runner contract of [otel/README.md](otel/README.md) ("Runner integration"):

- `current-run`: `runner.Context.mark` writes the run id to `~/.local/share/pipedpeer-lab/current-run` on every PC before each run and `idle` after it, in one background Ansible fan-out, waited for before the timed command starts.
- Logs: `pipedpeer run` output goes to `logs/pipedpeer-run/<run_id>.log` on pc00, Ray's and Dask's driver output to `logs/ray/run-<run_id>.log` and `logs/dask/run-<run_id>.log`, `ray start` to `logs/ray/start-<experiment>-n<N>-<run_id>.log`, the Dask scheduler and workers append to `logs/dask/scheduler.log` and `worker.log`.
- Ray starts with `--metrics-export-port=8090` and ports 6380-6384, 10002-10999; Dask workers with `--worker-port 9000:9099 --nanny-port 9100:9199` and their dashboards on (pp-poll.py finds them).
- The process globs of E6 and the kills match whitespace, not any character: `?` matched the `-` of `logs/pipedpeer-run/` in the wrapping shell's command line, which would have signalled the shell.

Also fixed on the way: `bootstrap-ssh.sh` still fetched the laptop-era `lab-pc.sh` from `serve.sh`; it now runs the published worker line (`LAB_SITE` moves it), so it works from pc00. `calibrate.sh` counts the CPUs it may use (`PL_NPROC`, else affinity), as `nproc` and the runner do. The day's calibration window follows the target (±25 %) instead of the plan's 100-150 s.
