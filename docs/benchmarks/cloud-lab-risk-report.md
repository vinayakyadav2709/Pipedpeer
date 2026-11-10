# Cloud lab risk run: what one VM could find before the real PCs

Every result here is **emulated, one VM**: 4 CPUs, 16 GB RAM, about 30 GB of disk,
Linux 6.18, Docker 29.9 with the **cgroup v1** (cgroupfs) driver. Real Ubuntu
22.04/24.04 PCs use cgroup v2. A result that depends on memory limits, CPU caps or
systemd units ran on v1, and is marked "(v1)". The lab PCs were privileged
containers with systemd as PID 1, all on one bridge network. Nothing here measures
speed, separate networks or real hardware.

Branch `cloud-lab-risk`, on `dev` at 983b190. Evidence is in
`docs/benchmarks/cloud-lab/`.

## VM accommodations (not in the repo)

- **TLS interception CA.** This VM's network intercepts HTTPS, so containers need
  the interception CA. A `docker` shim in the scratchpad added the CA to every lab
  image at build time; the repo's Dockerfiles are unchanged.
- **CA in the Nix store.** The Nix builds pin `SSL_CERT_FILE` to nixpkgs' `cacert`
  (`src/internal/nixgen/flake.go:226`). The CA was appended to that store path on
  each PC; a store path that gets replaced loses the patch, which happened once
  (see task 5).
- **Host packages.** rsync, pssh, parallel, shellcheck and the analysis Python
  packages were installed for the fake-lab tests.
- **Unrelated container.** A `pg` (postgres) container from the environment ran
  throughout and was left alone.

## Task 0: can this VM boot the lab? Yes (v1)

`docker run --privileged --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw
--tmpfs /run --tmpfs /run/lock --memory 2048m pp-ubu:24.04` (systemd 255.4,
Ubuntu 24.04.5):

- `systemctl is-system-running --wait` printed `running`.
- `systemd-run --user --unit=t0probe sleep 30` as `lab` (after `loginctl
  enable-linger`) gave `active`, in
  `/docker/<id>/user.slice/user-1001.slice/user@1001.service/app.slice/t0probe.service`.
  It first failed with `Failed to connect to bus`: the bare image has no D-Bus.
  The `openssh-server`/`curl`/`rsync` that `lab-pc.sh` installs bring in `dbus`
  and `libpam-systemd`, after which it worked. The harness relies on that
  install, as it does on a real desktop.
- The container's `memory.limit_in_bytes` was 2147483648. Python allocating in
  100 MB steps was killed (137) at 2000 MB, and `OOMKilled=true`.

Output: [cloud-lab/task0-boot-check.txt](cloud-lab/task0-boot-check.txt).

Does not prove: v2 behaviour (`memory.events`, `memory.pressure`, `memory.max`
reclaim). The VM has no v2 memory controller in use.

## Task 1: `UBU_LAB_CPUSET`: done

f4842db. `--cpuset-cpus "$UBU_LAB_CPUSET"` goes to every lab container (PCs and
driver), and unset means no pinning; documented in the header. With
`UBU_LAB_CPUSET=1-3`, all four containers showed `Cpus_allowed_list: 1-3` and
`nproc=3`. The log was not kept; this rests on the session's own record.

## Task 2: lab up, minimal E17: done; E17 works end to end, except the old-invite arm

- **Lab steps 1–4.** The test steps PASS, after two fixes. Step 4 failed first
  because pip in the Nix build could not verify TLS: a VM artifact, fixed by the
  CA patch.
- **Harness bug 1: a stale warm-up log** (0ead01b). Re-running `install.sh` after
  that fix reported pc00 FAILED within a second. Its `warmup.log` was from the
  earlier install: `pipedpeer setup` will not restart a build that failed minutes
  ago, and pc00 is never retried. install.sh now restarts a failed log that setup
  left unchanged, once, with `PIPEDPEER_WARMUP_RETRY=1`. The fake-lab row proves
  this; live, pc00's backoff had expired by the next run, so this path did not
  fire.
- **Harness bug 2: venv.sh could not be re-run** (9afe0ae). The second run failed
  on every PC: uv 0.13 refuses an existing venv. Fixed with `UV_VENV_CLEAR=1`.
- **stage, pins, venv:** pass. `calibrate.sh --target 5` and `run.sh REF --sizes 1`
  ran too; E17 needs `sizes.env`, and needs REF for `job_result_ok`.
- **`e17.sh --dry-run --n 2 --joiners 1 --repeats 1`:** printed the plan.

Real runs, `analysis-input/e17.csv`
(`i,kill_at_s,job_wall_s,job_result_ok,new_join_s,new_join_ok,new_remote_items,old_invite_joined`):

| row | what it showed | harness fault behind it, fixed in |
|---|---|---|
| `1,3.04,8.744,0,,0,,0` | Job survived the kill. But 366 items went to the *joiner* (10.136.162.18), which was still a cluster member. `result_ok` null was written as 0. The join was counted as failed: connect exited 2. | Joiners isolated before the job; exit 2 confirmed by status; null result left empty (10bd5de) |
| `2,,2.937,,1.378,1,22,0` | The job ended before the kill: a fixed 10 s "median" put the kill at 3 s, but the calibrated job took 2.9 s | Kill point from the W1 calibration target when there is no E1 run (34dd99c) |
| `3,1.589,2.536,,60.516,1,23,0` | Join counted, but `new_join_s` was 60.5 s | `connect` waits its full 60 s for the dead pc00 named in the invite; e17 timed connect, not the join (b3905a5) |
| `4,1.544,2.682,1,10.433,1,10,0` | **Not valid.** My own REF run was still going and its `cluster.sh up 1` restarted pc00 at 20:50:58, mid-join | Operator error (two campaign jobs at once) |
| `5,1.609,2.765,1,60.579,1,14,0` | As row 3, with REF done | (fixed by b3905a5) |
| `6,1.617,2.589,1,1.233,1,20,0` | **Clean.** Killed 1.6 s in, job right (`result_ok` 1), fresh PC joined via pc01's invite in 1.2 s, its forced job sent 20 items away | — |

The 366 items in row 1, and the 20:50:58 restart in row 4, come from the session
and its logs were not kept; they rest on the session's own record.

**E17 works end to end** (row 6): the job survives losing pc00, a fresh PC joins
through the survivor's new invite, and work still leaves it.

**Not tested: the old invite does not join.** That arm needs a second joiner, so
4 PCs, and the VM had room for 3; it records 0 by construction.

Also fixed: `e17_small_job` logged every failure as "exit 0" (f52d59e).

**wget crash: did not recur.** It is used in the setup line, step 2 and the image
builds, and no segfault was seen on CPUs 1–3. That is weak evidence: the
containers were pinned away from CPU 0 and this is different hardware, so it
neither confirms nor rules out the laptop's CPU 0. The log was not kept; this
rests on the session's own record.

## Task 3: Ray: done (v1)

`e19.sh --n 1 --add 1 --repeats 1 --systems ray`, sampled every 10 s.
`raymon.sh` read the head container's v1 `memory.usage_in_bytes`,
`max_usage_in_bytes`, `failcnt`, `oom_control` and `memory.stat`, host PSI, and the
RSS of every Ray process. The v2 `memory.events` and `memory.pressure` do not exist
on this VM. `--num-cpus` was forced on the head through a copy of cluster.sh. Full
table: `cloud-lab/ray-variants.csv`.

| head `--num-cpus` / pc00 limit | baseline s | join_wall_s | `result_ok` | peak usage MB (with cache) | peak RSS MB | failcnt | task workers (max per sample) |
|---|---|---|---|---|---|---|---|
| 3 (probed) / 4 GB | 27.1 | 30.1 | 1 | 3261 | 1236 | 0 | 3 |
| Ray's own / 4 GB | 27.5 | 30.7 | 1 | 3273 | 1238 | 0 | 3 |
| 2 / 4 GB | 27.5 | 31.6 | 1 | 3308 | 1267 | 0 | 4 |
| 4 / 4 GB | 28.2 | 30.5 | 1 | 3284 | 1238 | 0 | 3 |
| 16 / 3 GB | 26.4 | 30.7 | 1 | **3071 of 3072** | 1234 | 0 | 3 |
| 3 / 3 GB | 26.2 | 29.5 | 1 | 2999 | 1238 | 0 | 3 |

- **Does a Ray workload finish?** Yes, every one of the six, with the same checksum.
- **With how much memory?** The whole pc00 container peaked at about 1.24 GB
  resident (Ray head, dashboard, Dask scheduler and 3 workers, the daemon, and the
  job) and 3.0–3.3 GB with page cache. Ten Ray dashboard processes alone hold about
  100 MB each. The per-variant raymon samples were not kept (only the summary CSV
  is); the process count and the per-process figures rest on the session's own
  record.
- **Is the stall caused by how many workers Ray starts?** Not supported here.
  `--num-cpus` 2 to 16 left the task workers at 3–4 and the RSS the same. With 16
  CPUs and a 3 GB head, usage sat at 3071/3072 MB, the laptop's exact figure, with
  `failcnt` 0, and the job finished. That figure is page cache filling the limit,
  not a stall in itself.
- **Ray's own CPU count was right here** (3.0) because this lab pins with a cpuset,
  which Ray reads; the laptop used a CFS quota (`docker update --cpus 3`), which it
  does not.
- **Ray's memory figure is wrong in a container:** `ray status` showed 10.21 GiB of
  memory inside the 4 GiB container.

Does not prove: anything about cgroup v2. The laptop stall (memory.events max
≈2.4 M, no OOM) looks like v2 `memory.max` reclaim on mapped files, which v1 here
cannot show. Sampling every 10 s can miss short worker bursts. Only the EL
workload ran.

Also found: an e19 `--systems ray` run still starts every system on pc00 (Ray,
Dask and pipedpeer): `cluster.sh up N` is called without `--systems`. That is how
the head container carries Dask too. Left as is, and recorded.

## Task 4: a true CPU count for Ray, and a memory check: done

- **CPU count** (b3009ab). `lib.sh` `LAB_CPUS_PROBE` / `lab_usable_cpus` take
  `nproc` (affinity: cpuset, AllowedCPUs) lowered to the CPU quota of the PC's
  cgroup or any ancestor (v2 `cpu.max`, v1 cfs). Every Ray start passes
  `--num-cpus`; a `--cap` still wins; a failed probe passes no flag and logs it.
  e19's joiner uses the same count for Ray, Dask and the sampler. The case table
  is in `test/cluster_cpus_test.sh`. Live, `cluster.sh` logged `ray on pc00:
  --num-cpus=3`.
- **Memory** (e95f9a5). Nothing checked memory before. Ray is now refused on a
  head under 2048 MiB or a worker under 1024 MiB (`NOT-READY ray pc=<pc>`,
  `RAY-MEMORY ... refused`), with a warning under 4096 / 2048 MiB. The available
  memory is MemTotal or a smaller cgroup limit (`LAB_MEM_PROBE`). The thresholds
  come from the table above, with headroom (v1).

## Task 5: steps 5–8: done; all four pass, after one harness fix

`UBU_LAB_FROM=5 scripts/campaign/test-ubuntu-lab.sh test`, with `UBU_LAB_SLEEP`
and `UBU_LAB_DROP` at their defaults (600 s job, 90 s drop).
`CAMPAIGN_TIMEOUT_CAP_S` was left **unset**: it only lowers a run's timeout
(`run.sh:546`), so unset gives the full `experiments.tsv` timeouts.

The test script's PASS/FAIL lines are in
[cloud-lab/ubuntu-lab-steps.log](cloud-lab/ubuntu-lab-steps.log). The driver's
detailed `campaign.log` was not kept (step 8 removes the lab's results volume),
so the per-run detail and the "about 50 minutes" for step 7 rest on the
session's own record.

- **Step 5** (pins, venv): PASS. The first try failed on the VM, not the harness:
  the E19 purge had replaced the `nss-cacert` store path, which dropped the
  interception-CA patch. Re-patched, then PASS.
- **Step 6** (driver off the network for 90 s during a 600 s detached job): PASS.
  The job was not started twice and its result was fetched. Retries on a dropped
  SSH work (v1; one bridge network).
- **Step 7** (SMOKE, REF, FC, E7, E8, e4 dry-run, e5, e6, e11, upload to the local
  `rclone serve s3`, analyse): PASS. The first try stopped in REF only because this
  session's machine restarted, which killed dockerd. After the containers were
  restarted it passed whole, in about 50 minutes.
- **Step 8** (uninstall, undo, compare with the step-1 snapshot):
  - First FAIL on all three PCs: `/usr/bin/bash: /tmp/lab-pc.sh: No such file or
    directory`. The restart was a reboot, and systemd cleans `/tmp`.
  - **Harness bug** (ef613de): every post-setup command
    pointed at `/tmp/lab-pc.sh`, though setup keeps its own copy in
    `/var/lib/pipedpeer-lab/`. Ports and undo now use that copy, in the test,
    OPERATOR.md, LAB-GUIDE.md and preflight. Step 8 also tolerates a rerun.
  - Then PASS: every PC was identical to its step-1 snapshot, even after the
    reboot.

Does not prove: undo against a real GNOME session (gsettings) or a real ufw on
the host. Steps 6 and 7 ran on one bridge network.

## Task 6: E19's purge and warm-up retry: done, the purge was broken

`e19.sh --n 1 --add 1 --repeats N --systems pipedpeer`, rows in
`cloud-lab/e19-pipedpeer.csv`:

- **Repeat 1: the purge did not happen.** 223824 of 219985368 bytes removed,
  `joiner_warm=1`. `nix-store --delete` refused python, openssl, glibc and the
  rest: they are also referenced by the common environment the warm-up built (tqdm,
  narwhals, wheel, build hooks), none of which is GC-rooted. On every campaign PC
  the warm-up runs, so the joiner could never start cold.
- **Repeats 2 and 3: the purge read 0/0 and `joiner_warm=0` with python still on
  disk.** The previous purge had removed the top path, so `-qR` on the joiner
  listed nothing. Repeat 3's job took 50.8 s (row `pipedpeer,join,3` in
  `cloud-lab/e19-pipedpeer.csv`), about 20 s more than its repeats; the run
  recorded no cause for it.
- **Fix** (586ab1f). Outside referrers are purged too, in passes; a rooted
  referrer is logged. A joiner without the top path asks pc00 for the list.
- **Repeat 4 after the fix:** `219761544` of `219761544` bytes, 142 outside paths
  deleted, `joiner_warm=0`, join ok (`join_s` 1.68), `result_ok` 1. The logs say
  "top path ... already gone here; requisites taken from pc00's closure" and
  "deleted 142 path(s) outside the closure". The log was not kept; this rests on
  the session's own record.
- **Warm-up retry:** seen live in install.sh, not e19. Step 4's second run logged
  `warmup pc01 (ubu2): failed (exit 143) ...; restarting it (1/2, the connect token
  swap can kill a running build)`, and both PCs then reached `done`. No retry was
  triggered inside e19. The log was not kept; this rests on the session's own
  record.
- **Not checked:** `seed_s` is 8.42 in every repeat, so it looks constant rather
  than measured per repeat.

## Task 7: pre-run checks on real PCs: done

What a real PC can get wrong, and where it is checked (2297f35 adds
`preflight.sh`, run after scan/clocks; live it read every value below
correctly):

| item | before | now |
|---|---|---|
| memory | recorded only (`lab-pc.sh:173`, `inventory.sh:117`) | `preflight.sh` FAIL under 4 GiB (8 for pc00), counting a cgroup limit; `cluster.sh` refuses Ray below 2/1 GiB |
| usable CPUs | `nproc` recorded (`lab-pc.sh:173`) | WARN when the quota or cpuset is under nproc; Ray gets the usable count |
| sleep | masked at setup (`lab-pc.sh:78-92`); OPERATOR said to check by hand | FAIL when any of the four targets is not masked |
| firewall | ufw rules added if ufw is active (`lab-pc.sh:50-60`) | FAIL when ufw is active without 4242/tcp, 4242-4244/udp; WARN for an nftables drop policy with ufw off |
| clock | offsets measured (`clocks.sh`), never judged | WARN on NTP not synchronised, and on an offset over 100 ms |
| disk | not checked | FAIL under 15 GiB free for home or /nix (closure plus venv took about 5.5 GB per PC) |
| Ubuntu version | recorded (`inventory.sh:110`) | FAIL when not Ubuntu, WARN when not 22.04/24.04 |
| UDP ports between PCs | not checked | still not checked (needs the daemons; their health after install shows 4242/tcp only) |

## Task 8: riskiest checks first: done

7c37f92. Session 1's check now runs on three PCs and adds `clocks.sh` and
`preflight.sh`. Right after calibration come C3a (REF, then `e17.sh --n 2
--joiners 1 --repeats 2`) and C3b (`e19.sh --n 1 --add 1 --repeats 1 --systems
ray`), with the passing rows from this run, the memory a Ray head needs, and what
the laptop's stall looks like.

## Task 9: bug reports in one step: done

`collect.sh` could not help with a failed run: it refused a lab with no
`runs.jsonl`, stopped when analyse.py failed, took nothing from the PCs, and had no
secret filter.

`collect.sh --bug` (59f518e, f185369) builds one folder: the lab dir without
`r2.env`, `cluster.json` or keys, the driver's versions, and from every PC its
versions, status, journals (daemon, Ray, Dask), `warmup.log`, Ray logs, memory
(v1 and v2 files, PSI, kernel OOM lines) and disk. Invite codes and tokens are
redacted, and `SUMMARY.txt` names what was left out or not reached.

The first live run found two bugs that the fake lab hid: ssh ate the hosts loop
(1 of 3 PCs collected), and `sizes.env` was dropped. After the fix: 3 of 3 PCs, no
key or code in the bundle (`cloud-lab/bug-bundle-sample.txt`). That sample is a
20 KB excerpt (the first 120 lines of each PC's report); the whole bundle was
about 6.4 MB, and its size rests on the session's own record. OPERATOR.md tells
the operator to send that folder and not try fixes.

## Answers

- **Does E17 work end to end?** Yes on the emulated lab (row 6), after five harness
  fixes. The old-invite arm is untested (it needs 4 PCs).
- **Does a Ray workload finish, and with what memory?** Yes, in all six variants.
  The pc00 container used about 1.24 GB resident and 3.0–3.3 GB with cache, at a 3
  or 4 GB limit (v1). The laptop stall did not reproduce, and the worker count did
  not follow `--num-cpus`.
- **Did the wget crash recur?** No. That does not clear CPU 0: the containers were
  pinned to CPUs 1–3, on different hardware.

## Product bugs found, not fixed

1. **`pipedpeer connect` waits the full 60 s for a dead member named in the
   invite.**
   - Then it exits 2 with "fix: the machines cannot reach each other on 4242/tcp",
     which misleads when the machine is simply off.
   - The join itself is visible in about 1 s.
   - Reproduce: E17 row 3/5; or kill one member's daemon, take `pipedpeer invite`
     from another, run `pipedpeer connect <code>` on a fresh PC, and time it
     (`connectWait`, `src/connect_cmd.go:29`).
2. **The common environment the warm-up builds is not GC-rooted.**
   - `nix-store --gc` (or the E19 purge) removes it.
   - `warmup.log` still says `done`, so the next job rebuilds without warning.
   - Reproduce: after install, `nix-store --gc`, then `pipedpeer run` any job.
3. **Ray reports the host's memory inside a memory-limited cgroup** (10.21 GiB in a
   4 GiB container), so its memory monitor cannot protect the head. This is Ray,
   not Pipedpeer; on bare PCs it reads the real total.

## What only real PCs can answer

- cgroup v2 behaviour: whether the laptop's Ray stall (memory.events max ≈2.4 M,
  no OOM, at 3 GB) recurs on a 3–4 GB pc00 with `memory.max` reclaim.
- `AllowedCPUs` caps with `--cap` (v2 cpuset): the probe reads them through
  affinity; not exercised here.
- Real joins across networks, UDP 4243/4244 through real firewalls, WiFi, and
  suspend or lock screens.
- The wget segfault and CPU 0 on the laptop.
- Speed of anything.
- The old-invite arm of E17, which needs 4 PCs.

## Tried/failed:

- A `docker build` of the driver image stalled at about 30 KB/s in pip
  (transient, VM network); a retry worked.
- pc00's venv pip install stalled (about 25 KB/s) while pc01 and pc02 finished;
  killed, and venv.sh rerun (which exposed the venv rerun bug).
- Host `pip install -r requirements-dev.txt` (Ray, torch) was too big and slow for
  the 30 GB disk; installed only the analysis packages with uv.
- The first Ray variant chain reused the earlier run's results ("ALREADY-STARTED"):
  the PCs' run dirs were still there. Cleared them per variant and reran.
- E17 row 4 is invalid: I ran REF and E17 at once and REF restarted pc00.
- `CAMPAIGN_TIMEOUT_CAP_S` cannot raise a timeout (`run.sh:546`, it only lowers);
  left unset for task 5, so the `experiments.tsv` timeouts applied.
- `e19_test.sh` row H3 fails on this VM (`chmod 555` does not stop root); it fails
  the same way without these changes.
- `make campaign-test` (whole suite, once at the end; failures rerun alone, none
  reported LOAD-ONLY): **26 of 30 files pass**. The 4 failures are this machine's:
  - `e19_test.sh` H3 and `test_sweep.py::test_fileio_read_only_dir_names_the_path`
    need a non-root user (read-only dirs do not stop root).
  - `upload_test.sh` needs `rclone`, which I could not install on this host (the
    download was refused).
  - `ports_test.sh`: the 2 rows that failed on this VM ("ray stopped, nothing
    left") pass on an ordinary non-root PC (12 of 12 on unchanged code); here
    root's view of the stopped units was the difference.

  One earlier commit message says 29; the right count is 26.
- Earlier in the session, single files failed under load (the `cpu_sampler` row
  in `e19_test`, the E17 Ctrl-C row) and passed when rerun alone.
