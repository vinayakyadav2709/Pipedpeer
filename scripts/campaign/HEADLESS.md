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

1. Put the R2 credentials in `~/.config/pipedpeer-lab/r2.env` (mode 600): `R2_ACCOUNT_ID=…`, `R2_ACCESS_KEY_ID=…`, `R2_SECRET_ACCESS_KEY=…`. Make the token an R2 API token with **Object Read & Write on the `pipedpeer-lab` bucket only**: anyone who learns the passphrase gets it.
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
| prepare | `machines.csv`; `network.csv` (iperf3 both ways to pc00, one PC at a time; ping RTT); `clocks.txt` (chrony only on PCs over 10 ms); the code to every PC; `install-pipedpeer.sh --version <v> -y` and `pipedpeer doctor`; `pipedpeer invite` / `connect`; the Nix env; otelcol and the node collectors, the driver collector on pc00 |
| calibrate | `calibrate.sh W1/W2/W3` on pc01 with `CAL_VERIFY=1`, again if the strong size runs outside 100-150 s; writes `workloads.json` and `results/lab/workloads.md`. Then the plain W1 result at each e1b weak size, one per idle worker, in parallel, into run.sh's `result-cache.json` |
| run | Each row of the profile: `run.sh <id> --inventory results/lab/inventory.ini <args>`, then `sync-r2.sh` |
| analyse | `analyse.py`, then `sync-r2.sh` |
| cleanup | Every worker: `cleanup-host.sh`, then `lab.sh undo`. Driver collector down, final sync, `sync-r2.sh count` must match. Then pc00: `cleanup-host.sh`, summary, `lab.sh undo` |

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

## Integration needed

lab/runner (`run.sh`, `lib/`, `experiments/`, `analyse.py`):

1. **The json callback.** `lib/ansible.sh` `_pl_adhoc` sets `ANSIBLE_STDOUT_CALLBACK=json`, but ansible-core 2.22 has no json callback; it lives in the `ansible.posix` collection. `pl_install` installs only `community.docker`. The day's bootstrap installs `ansible.posix` into `$VENV/collections` and then calls `lib/ansible.sh install`. Add `ansible.posix` to `pl_install` too, so run.sh works outside the day.
2. **The venv on Ubuntu.** `pl_install` runs `python3 -m venv`, which fails on stock Ubuntu (no `python3-venv`). The day creates the venv first (with the `virtualenv.pyz` fallback), so `pl_install` skips that step. For run.sh outside the day, use the same fallback.
3. **pc00 is local.** The inventory has `ansible_connection=local` on the `pc00` line. `inventory.py` keeps unknown host vars, and `lab_ip` defaults to `ansible_host` (pc00's lab address). Check that nothing in `systems.py` assumes pc00 is reached over SSH.
4. **SSH options.** The day exports `ANSIBLE_SSH_ARGS` (the campaign key, `UserKnownHostsFile=~/pipedpeer-lab/known_hosts`, no user ssh config). `pl_ansible_env` keeps an exported value. Keep that, so run.sh never writes to the user's `~/.ssh`.
5. **Weak references.** Before e1b, the day writes `results/lab/result-cache.json` entries `W1|BENCH_ITEMS=<per-PC size × N>` with `source: "plain-precomputed:<pc>"`, using `record.ResultCache.key` and `workloads.extract` when importable. `runner.check()` reads the cache first, so e1b compares against them.
6. **Cut-off attempts.** `analyse.py` should drop the `runs.jsonl` lines in `[runs_from_line, runs_to_line)` (0-based line numbers) of every `attempts.jsonl` entry with `status: "interrupted"`. Without that, a resumed experiment counts its first repeats twice.
7. **Exit status.** The day treats a non-zero exit of `run.sh` as "experiment failed" (BLOCKERS.md, summary). Failed runs inside a finished experiment should still exit 0.
8. **Profile arguments.** Check that `e5 --n 16`, `e6 --n 16`, `e4 --n 4`, `e7 --repeats 3` and `e8 --n 2,4,8 --repeats 3` mean what the profile says. e5 and e6 default to 32 and 16.
9. **Fewer workers than N.** A dropped worker can leave fewer than 16 workers. run.sh should record the N=16 runs as failed (or run at the largest N it can, and say so), not stop.
10. **What run.sh leaves on a PC.** `cleanup-host.sh` removes `~/.local/share/pipedpeer-lab` and `~/pipedpeer-lab`, and in `/tmp` only `ray`, `dask-*`, `dask-scratch-space`, `torchelastic_*` and `pipedpeer-lab*` owned by the lab user. It also stops `ray`, `dask` and `torchrun` processes. If run.sh writes anywhere else (E5's pip venvs, a pip cache in `~/.cache/pip`), add the path to `cleanup-host.sh` or to `HOME_PATHS` in `lab.sh`.
11. **`analyse.py`** runs as `$VENV/bin/python scripts/campaign/analyse.py` from the repo root, with no arguments. The venv has matplotlib.
12. **sync after each experiment.** run.sh calls `sync-r2.sh` at the end of each experiment, and the day calls it again. Both are harmless; either one may be dropped.

lab/traces (`otel/`):

13. The day calls `telemetry.sh up` (with `OTELCOL_TARBALL=<pc00's copy>`) and `telemetry.sh down`; `otel/install-otelcol.sh` (with `OTELCOL_TARBALL`) and `install-otelcol.sh remove`; and `otel/node.sh start <pc_id> <pc00 ip>` and `node.sh status`. It also reads `lab_fetch`, `lab_arch` and `OTELCOL_VERSION`/`OTELCOL_SHA256_*` from `otel/lib.sh`. If these change, update `day/prepare.sh` (`prep_otel`, `ensure_services`) and `day/cleanup-host.sh`.
14. `final_sync` parses `sync-r2.sh count` lines starting `local:` and `remote:` followed by the number. Keep that format.
15. Anything new that otel/ installs on a PC must be removed by `install-otelcol.sh remove`, or listed in `cleanup-host.sh`.

docs:

16. Part 1 of `docs/benchmarks/lab-campaign.md` still describes the laptop flow. Replace it with the operator steps above when the campaign branches merge. `serve.sh` stays as a laptop fallback for worker setup only.
