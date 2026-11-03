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
3. Run `scripts/campaign/publish.sh`. It builds pipedpeer at HEAD and uploads it as the GitHub prerelease `lab-<commit12>` (needs `go` and `gh`); the day installs exactly that build, served from pc00. `--version <nightly>` pins a published nightly instead, but no nightly yet has E4's headcount split or the per-step items E12 and E13 read. Press Enter to get a generated passphrase, and write it down outside the repo.
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
| prepare | `machines.csv`; `network.csv` (iperf3 both ways to pc00, one PC at a time; ping RTT); `clocks.txt` (chrony only on PCs over 10 ms); the code to every PC; the campaign's own pipedpeer (release `lab-<commit12>`, downloaded once to pc00's file server) through `install-pipedpeer.sh --version lab-<commit12> -y`, and `pipedpeer doctor`; `netem.csv` (which PCs have `sch_netem` for E15); `node-ids.csv`; `pipedpeer invite` / `connect`; the Nix env; tcpdump (`lab.sh pkg`, so undo removes it), otelcol and every PC's collector, poller and capture (`node.sh start <pc> <pc00> --session s1 --pcap`, the session also in `~/.local/share/pipedpeer-lab/session`), the driver collector on pc00 (file only with ingest credentials: `sync-r2.sh` uploads its rotated files) |
| calibrate | `calibrate.sh W1/W2/W3` on pc01 with `CAL_VERIFY=1` at a 60 s strong target (`LAB_CAL_STRONG_S`), again if the verify run lands more than 25 % off it; writes `workloads.json` and `results/lab/workloads.md`. Then the plain W1 result at each e1b weak size, one per idle worker, in parallel, into run.sh's `result-cache.json` |
| run | Each row of the profile: a health check, then `PL_SYNC=0 run.sh <id> --inventory results/lab/inventory.ini <args>`, a line in `attempts.jsonl`, then `sync-r2.sh` |
| analyse | `analyse.py`, then `sync-r2.sh` |
| cleanup | Every PC: `node.sh pcap-stop`, and its pcap files pulled into `results/lab/pcap/<pc>/` over ssh. Every worker: `cleanup-host.sh`, then `lab.sh undo`. Driver collector down, final sync, `sync-r2.sh count` must match. Then pc00: `cleanup-host.sh`, summary, `lab.sh undo` |

### The minimal profile

`profile-minimal.tsv`: 17 PCs, N = 1, 4, 16, 3 repeats after 1 warm-up. It runs e1 (W1, W2, W3, W4 × plain, pipedpeer, ray, dask), e1b, e4, e5 and e6 at 16, the headline experiments e12 (elastic join 4 → 16, about 8 min), e13 (churn at 16, about 10 min) and e15 (netem at N=4, about 12 min), e7, and e8 at ranks 2, 4, 8. It skips e3, e9, e10 and e11. E14 (curl to cluster) adds no row: the step `prep.firstjob` runs the day's first distributed job right after the join (about 3 min, most of it the job's Nix closure, which E1 would otherwise build), and `day/timeline.sh` records each PC's setup, SSH, install and join times in `results/lab/timeline.jsonl`. [FIGURES.md](FIGURES.md) describes the headline figures. Pass another profile with `LAB_PROFILE`.

### Time

The ETA in `lab.sh status` adds the step estimates in `lab-day.sh` (setup and cleanup) and `profile-minimal.tsv` (`est_min`). They come from the full rehearsal (`test/headless-test.sh`, 4 PCs, 2 CPUs each, strong target 10 s), scaled to the lab. The rehearsal's whole day took 57 minutes of steps; its per-step minutes are in `results/lab/rehearsal-day/timings.txt`.

**Per-run overhead.** Most of a rehearsal run is not work: in e1, 80 runs took 18.2 min, of which 10.3 min was inside the timed commands, so 7.9 min over the 64 runs that started is **7.4 s per run** (activation, the run id on every PC, the receipt and record fetch, Ansible). The other experiments measured 3-16 s (e12 and e13: 14-16 s, joins and kills). The lab has 17 PCs, so every fan-out reaches more hosts; the estimate takes **11 s per run** (×1.5).

**Work per run.** A strong size is 60 s on one PC (W4 30 s), weak 60 s per PC. Speed-up is assumed at 70 % efficiency at N=4 and 50 % at N=16, on the slow side, so the estimate errs long: 60/(4×0.7) = 21.4 s, 60/(16×0.5) = 7.5 s.

| Step | Runs | Arithmetic | Minutes |
|---|---|---|---|
| e1 | 160 | W1-W3 each: 16 × 60 + 12 × 21.4 + 12 × 7.5 = 1307 s; W4 half, 654 s; 3 × 1307 + 654 = 4575 s = 76.3 min; overhead 160 × 11 s = 29.3 min | 105 |
| e1b | 40 | 16 × 60 s at N=1, 24 × 75 s at N=4 and 16 (weak, 80 %): 2760 s = 46 min; 40 × 11 s = 7.3 min | 53 |
| e4 | 16 | caps 8/4/2/1 = 15 of an 8-core PC's cores at 70 %: 60 × 8/(15 × 0.7) = 46 s; 16 × 46 s = 12.3 min; 16 × 11 s = 2.9 min | 15 |
| e5 | 12 | cold: pip 300 s, Ray 300 s, pipedpeer 120 s; updates 9 × 60 s; 1260 s = 21 min; 12 × 11 s = 2.2 min | 23 |
| e6 | 18 | 18 × 15 s (W1 at 16, plus recovery) = 4.5 min; 18 × 11 s = 3.3 min; 6 Ray/Dask rebuilds after kills × 30 s = 3 min | 12 |
| e12 | 3 | 3 × 80 s while PCs join; 3 × 21 s overhead (rehearsal 14 s × 1.5) | 6 |
| e13 | 3 | 3 × 90 s with churn; 3 × 24 s overhead | 7 |
| e15 | 18 | 3 delays × 2 repeats × 3 systems × 35 s (21 s at N=4, up to twice that with 80 ms) = 10.5 min; 18 × 11 s = 3.3 min | 14 |
| e7 | 8 | 8 × (5 + 11) s | 2 |
| e8 | 78 | lab/runner's 72 min for 72 runs (the rehearsal ran one epoch, no lab-size data); plus the new one-rank reference, 6 × 100 s = 10 min | 82 |
| **experiments** | | | **319** |

| Setup and cleanup | Rehearsal (3 workers) | Lab estimate, and why | Minutes |
|---|---|---|---|
| bootstrap | 1.6 | the same pip installs | 2 |
| discover | 0 | the operator's walk round; overlaps typing | 5 |
| prep.network | 0.7 | iperf3 one worker at a time, about 14 s each: 16 × 14 s | 4 |
| prep.pipedpeer | 0.7 | install in parallel; the Determinate installer downloads on each PC | 3 |
| prep.firstjob (E14) | 5.5 | the workers import the job's environment first; containers were slow at it | 5 |
| prep.nixenv | 0.7 (11 cold) | 16 × 719 MB from pc00 at 1 Gbit/s is 92 s on the wire; ×3 for disk and import, plus evaluation | 8 |
| calibrate | 3.3 at 10 s | at 60 s: about 140 s per workload (doubling points, the 3× point, the verify run), W4 90 s, 510 s; one retry | 12 |
| weakrefs | 0.3 | N=16's reference is 16 × 60 s on one PC | 17 |
| other prepare steps | 1.2 | | 6 |
| clean.workers | 3.5 one by one | about 70 s per PC (the Nix uninstall), now all PCs at once | 2 |
| analyse, clocks.end, pull.pcap, undo.workers, final.sync, clean.pc00 | 0.5 | final upload of about 1 GB of telemetry and pcap at the lab's uplink | 9 |
| **setup and cleanup** | | | **73** |

**The day: 319 + 73 = 392 minutes, about 6.5 hours**, plus the time to type the line on 17 PCs before pc00. That is more than a 3-hour visit. The large items are e1 (105), e8 (82), e1b (53) and the N=16 weak reference (17). Ways to fit 3.5 hours, each a profile edit: e8 at ranks 2 and 8 only (-27), e1b at N = 1 and 16 (-18, and weakrefs -4), `--repeats 2` everywhere (-75), or e1 without W3 (-25). The estimate errs long: the efficiency assumptions are pessimistic, and e8's 72 minutes is lab/runner's own figure, not measured.

### Distributing the Nix env

The campaign env is a 2.5 GiB closure. If 17 PCs each fetched it from cache.nixos.org, the lab's internet link would carry about 42 GiB. Instead:

1. pc00 builds it (`env/install.sh`), the only download from cache.nixos.org.
2. pc00 writes the closure, and the flake's inputs (`nix flake archive`), into a binary cache at `~/pipedpeer-lab/serve/nix` (`nix copy --to file://…?compression=zstd`). Its file server (`python3 -m http.server` on port 8137) serves that cache.
3. Each worker runs `sudo nix copy --no-check-sigs --from http://pc00:8137/nix <paths>`, then `env/install.sh`, which finds every path already present and downloads nothing.

The alternatives were worse:

- **`nix copy --to ssh-ng://worker` from pc00** needs the lab user to be a trusted Nix user on the worker. pc00 builds the env's top path locally, so that path carries no signature. Making the lab user trusted means editing `/etc/nix/nix.conf` on every PC.
- **A signed cache with pc00's key** also needs `nix.conf` changes on each PC, for `trusted-public-keys` and `substituters`.

Checked with real Nix (nixos/nix 2.31.3, two containers): the first built a small flake on the campaign's nixpkgs pin, then ran `nix copy --to file://…?compression=zstd` and `nix flake archive --json --to …`. The second had **no network**. It ran `nix copy --no-check-sigs --from file://…` with the output path and the two archived source paths, then `nix build --option substituters "" path:<flake>`. Both the evaluation of nixpkgs and the build found everything locally, and the program ran. The top path was built locally and carries no signature.

The full rehearsal exercises this path for real: pc00 writes the cache (719 MB, zstd) and three workers with fresh Determinate stores pull it (11 min in the first full run, 45 s in the last; all four containers share one host disk, so the lab’s numbers will differ). The binary-cache pull above runs as root through the passwordless sudo that `lab.sh` already grants, so the worker's Nix accepts the unsigned paths with no configuration change. If the pull fails, `install.sh` falls back to cache.nixos.org on its own. The file server also hands out the code tarball and the otelcol release, so the code comes from GitHub once and otelcol once.

## How the secrets are protected

- The SSH private key and `r2.env` are never in the repo. On Pages they exist only as `lab-secrets.enc`: a tar encrypted with `openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -md sha256 -salt`. The passphrase is the only protection, so `publish.sh` refuses one shorter than 24 characters or 6 words. The passphrase it generates is 6 groups of 5 characters from a 32-letter alphabet, about 150 bits.
- On pc00, `lab.sh head` reads the passphrase from `/dev/tty`, never from argv or the environment. openssl gets it on a file descriptor. The decrypted key must match the public key embedded in `lab.sh`, which catches a wrong passphrase that happens to decrypt.
- The decrypted files live in `/run/pipedpeer-lab`, on tmpfs, so a reboot wipes them. The directory belongs to root with mode 0711: nobody can list it. The two files in it belong to the lab user with mode 0600, because the day runs as that user and ssh refuses group-readable keys. The brief asked for a root-owned 0700 directory readable by the lab user; 0711 with lab-owned 0600 files is the nearest arrangement that ssh accepts. `lab.sh undo` on pc00 deletes the directory, and that undo runs only after the final R2 count matches.
- The keys reach the collector and rclone as environment variables only (`otel/README.md`).
- The SSH key opens only lab PCs that ran `lab.sh`, and `undo` removes it from each one. **After the lab**, rotate the ingest token (`wrangler secret put INGEST_TOKEN`), delete `~/.config/pipedpeer-lab/campaign_ed25519{,.pub}`, delete the `lab-<commit12>` release, and remove `lab-secrets.enc` from `dev` (OPERATOR.md, "After the lab"). Git history keeps the old bundle, which then holds only a deleted key and a dead token.

## Resume and robustness

- `~/pipedpeer-lab/done` on pc00 is the ledger: one line per finished step, per experiment, and per PC cleaned or undone. On a rerun, `lab.sh head` skips the setup it already did (`lab.sh` records each change once). It does not unpack the code again, because the day's results live inside it. Then it restarts the unit, and `lab-day.sh` skips every step the ledger lists.
- An experiment cut off midway runs again from its start. The lines it had already written stay in `runs.jsonl`, and `results/lab/attempts.jsonl` marks them: `{"experiment": "e4", "status": "interrupted", "runs_from_line": 120, "runs_to_line": 126}`. Each later attempt has its own line with status `ok` or `failed`.
- A step that only pc00 can do (bootstrap, pipedpeer install, the Nix env on pc00) aborts the day if it fails. The day then goes straight to cleanup, so the PCs are still put back, and the summary says `STOPPED EARLY`.
- A worker that fails a prepare step, or does not answer the health check before each experiment, is dropped: removed from `results/lab/inventory.ini` and listed in `results/lab/dropped.csv` and `BLOCKERS.md`. It stays in `~/pipedpeer-lab/inventory-all.ini`, so cleanup tries it again. A failed experiment is recorded and the day continues.
- Cleanup never repeats: each worker's cleanup and undo has its own ledger line. If the final R2 count does not match after three syncs, pc00 keeps its results and secrets, the unit exits with an error, and rerunning `head` retries the sync. pc00's own undo runs last. Before it, the summary and a `day-finished` mark go to `/var/lib/pipedpeer-lab`, so a cut-off undo finishes on the next `head`.
- Nix is removed (`/nix/nix-installer uninstall`) only from PCs that had no `/nix` before the pipedpeer install. A PC with no record keeps its Nix. pipedpeer is uninstalled only where it was not there before.
- `lab.sh undo` also removes what the campaign leaves in the lab user's home, from a fixed list (`~/pipedpeer-lab`, `~/.ansible`, `~/.nix-profile`, `~/.cache/nix`, `~/.local/share/pipedpeer-lab`, `~/.config/pipedpeer`, `~/.ssh/known_hosts`, and so on). It removes only entries that did not exist at setup, plus parent directories that setup did not find and that are now empty. The user's own files stay.

## Rehearsal

`scripts/campaign/test/headless-test.sh` runs the whole day, as the lab will, on four systemd containers: pc00 and w1, w3 on Ubuntu 24.04 with ufw, w2 on Fedora 43 with firewalld; pc00 2 GB, workers 1.25 GB, 2 CPUs each. It types the worker line on w1-w3 and the head line on pc00 through a pty, kills the day's unit during e4, types the head line again, and checks:

- the resume (finished steps skipped, e4 marked interrupted and run again, the summary's ALL DONE);
- every experiment of `test/profile-rehearsal.tsv` finished, with e1's failures limited to the expected ones;
- the uploads: runs.jsonl, attempts, CSVs, figures, every PC's pcap, the driver collector's files, and the final count;
- the traces: 12 sources from all four PCs, plus `pipedpeer-run` from pc00, with `lab.run_id`;
- that pipedpeer W1 runs at N ≥ 2 sent items to peers, that runs.jsonl lines stay small, and that no Ray Pool actor or task ran on pc00 (`test/ray-placement.py`);
- that every PC's snapshot after the day equals the one before: packages, users, units, the lab user's home (every file hashed), sudoers, `/etc/ssh`, `/etc/nix`, `/etc/profile.d`, `/usr/local/bin`, the lab user's files in `/tmp`, `/nix`, the lab user's processes and the firewall.

**Real:** lab.sh, the passphrase on `/dev/tty`, the decryption, the systemd unit, scan.sh, Ansible over SSH, pipedpeer built from the checkout and installed by `install-pipedpeer.sh` from a release served by pc00 (checksum checked), `pipedpeer setup -y` with the Determinate Nix installer and crun on each worker, the join, the campaign env built on pc00 and pulled by each worker from pc00's binary cache, calibrate.sh, run.sh with real Ray, Dask and torchrun, analyse.py, otelcol, pp-poll.py and tcpdump on every PC, the driver collector, ingest.sh and sync-r2.sh with the count, cleanup with the Nix uninstall, and lab.sh undo.

**Stand-ins, and why:**

- **The internet:** a local http server serves Pages (lab.sh and the bundle), codeload (the working tree as the tarball) and the GitHub release (`lab-rehearsal`). `test/ingest-standin.py` has the ingest Worker's routes, token check and caps, with a directory for the bucket. No rehearsal data reaches the real bucket.
- **pc00's Nix:** a copy of the `pipedpeer-lab-nix` volume, where the 2.5 GiB campaign env is already built. pc00 is a PC that "had Nix", so the day keeps it, and pc00 does not download the env from cache.nixos.org. The workers start without Nix.
- **CPUs:** the containers see all 20 host CPUs. `--cpus 2`, `PL_NPROC=2` and `PYTHON_CPU_COUNT=2` make them 2-core PCs.
- **Sizes:** `test/profile-rehearsal.tsv`: N up to what four PCs hold, one repeat, a 10 s calibration target, and the smoke sizes of rehearse.sh for e4, e5, e8, e12, e13 and e15.
- **netem:** the containers share the host's kernel, which has `sch_netem` loaded, so `netem.csv` says yes everywhere; the "no" path is not exercised.

**Result** (2026-11-03, the run committed in `results/lab/rehearsal-day/`): all checks passed, 65 minutes of steps. e1: 80 runs, 18 failed: the 16 runs at N=4 for Ray and Dask (3 workers; recorded, e1 went on) and Ray W3 at N=2 twice (Ray's memory monitor in 1.25 GB containers). e6 and e13: Ray failed when a worker was killed, which is the fault's measured outcome, and Dask under churn hit the runner's 360 s stop. E14 (the first job, right after the join) ran wholly on pc00: the workers were still importing its environment after a minute.

Earlier runs of the rehearsal found the product and harness bugs listed below; the test now guards against each one.

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

### The lab/sample gaps

[results/lab/sample/REPORT.md](../../results/lab/sample/REPORT.md) section 4 lists what the sample run found. Status here:

| Gap | Status |
|---|---|
| 17, rehearsal uploads to the real bucket | Fixed. `runner.Context.sync` uploads only for a campaign run (no `--rehearsal`, results in `results/lab`); `PL_SYNC=1` forces it for a harness with a stand-in, and passes `LAB_RESULTS`. headless-test.sh's uploads go to `test/ingest-standin.py` through the bundle's credentials. |
| 1, receipt size | Fixed in the runner. runs.jsonl keeps `receipts.summary()`: the receipt's small fields and its steps aggregated per (step, node), a few kB. The whole receipt is `logs/<run_id>/receipt.json.gz` (`receipt_path`). `sample_figures.py` reads lane steps from it (checked: identical lanes for all 26 sample runs). Aggregating per-item steps in the shim itself is still open (shim owner). |
| 2, node id to pc id | Fixed. `prep.join` writes `results/lab/node-ids.csv` (`pc_id,node_id`, from `~/.local/state/pipedpeer/daemon.node`); `sample_figures.py` prefers it to the daemon-log scrape. |
| 6, E8 one-rank loss | Fixed. E8 runs the T1 script plain on pc00 once per variant and seed first; `analyse.py`'s E8 table has a "loss vs 1 rank" column. |
| 7, daemon log time | Fixed for the lines that carry one: the collector parses JSON lines' `time` and Go log lines' date. zerolog's console lines (`13:14:51 INF`) have no date and keep the read time; logging RFC 3339 from the daemon is the product fix. |
| 9, uncapped awss3 path | Already gzip (`driver.yaml.tmpl`); the day uses the ingest path. |
| 10, `system.network.connections` | Off. |
| 11, telemetry.sh and PL_RESULTS | `telemetry.sh` and `sync-r2.sh` use `LAB_RESULTS`, else `PL_RESULTS`, else `results/lab`. |
| 12, `--size` and weak scaling | An override is per PC in weak mode, multiplied by N (test). |
| 13, failed runs lose stderr | Timed commands merge stderr into the output (test); with `ansible_connection=local` there is no pty to do it. |
| 14, clocks measured once | Step `clocks.end` writes `results/lab/clocks-end.txt` before the pcap pull. |
| 15, E6 warm-up group | The warm-up line has `fault: none`. |
| 3, container CPU is the host's | Not fixed: the real lab is unaffected. headless-test.sh gives each container `--cpus 2`. |
| 4, Ray/Dask task timeline | Not fixed: needs changes to the baseline ports. |
| 5, no container CPU limit | headless-test.sh: `--cpus 2`, `PL_NPROC=2`, `PYTHON_CPU_COUNT=2`. `lab/docker-compose.yml` (rehearse.sh) unchanged. |
| 8, process, Ray and Dask metrics | Present since lab/traces (`host.process`, `prom.ray`, `prom.dask`); headless-test.sh checks them per PC. |
| 16, harness restarts | Not applicable to the day (a systemd unit). |

Found by the rehearsal, in the product:

- **The daemon died with the ssh session that started it.** `pipedpeer start` (and `setup`) left the daemon in the caller's session; over `ssh -t`, which is how Ansible's raw module runs every command, the pty's hangup killed it seconds after `start` reported it running. Every worker's daemon was down after the install, and after each of the runner's restarts. `daemonctl` now starts it with `Setsid`.
- **Jobs failed where the daemon had no `XDG_RUNTIME_DIR`** (pc00's daemon runs inside the day's system unit): crun fell back to `/run/crun`. The daemon now gives crun `$XDG_STATE_HOME/pipedpeer/run`.

And in the baselines: Ray's Pool and joblib actors asked for no CPU, so pc00, the zero-CPU Ray head, ran a share of every Ray job (16 Pool actors, 86 CPU seconds in one rehearsal). The ports now ask for one CPU per actor, and `test/ray-placement.py` checks it from the process metrics.

Also fixed on the way: `bootstrap-ssh.sh` still fetched the laptop-era `lab-pc.sh` from `serve.sh`; it now runs the published worker line (`LAB_SITE` moves it), so it works from pc00. `calibrate.sh` counts the CPUs it may use (`PL_NPROC`, else affinity), as `nproc` and the runner do. The day's calibration window follows the target (±25 %) instead of the plan's 100-150 s.
