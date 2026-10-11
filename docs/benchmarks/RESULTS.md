# Results

Every number here traces to a file in `raw/`. There are three sources, and each
result says which one it comes from.

| Tag | What | Where |
|---|---|---|
| **[Oct]** | Run for the report on 2026-10-02 at commit `4c91ef3` | `raw/T*`, `raw/O*`, `raw/N*_run*.log`, `results.json` |
| **[Aug]** | The previous pass, 2026-08-25, on a different host | `raw/B*`, `raw/N3_pool_map_receipts.txt`, `results-2026-08-25.json` |
| **[Oct-12]** | N7 re-run on 2026-10-12 at commit `08ce1d4`, on a second host (Fedora 44, 16 threads, Python 3.14.8, docker lab) | `raw/N7_*`, the `n7_*` entries of `results.json` (each carries its own `host`) |
| **[commit]** | Multi-machine measurements recorded in commit messages, 2026-08-26 to 08-29 | `raw/C_commit_measurements.md` (verbatim quotes) |

## Test bed [Oct]

- **Host:** Fedora 43, Linux 6.19.13, Intel i5-13420H with 12 threads, 15 GiB
  of RAM, and an NVIDIA GPU that is unused by the lab.
- **Toolchain:** Go 1.25.9 and Python 3.14.4. torch 2.14.1+cpu is installed in
  a virtualenv, matching what CI installs.
- **Lab:** three podman containers with host networking, as daemons on
  38081–38083, built by `scripts/lab-up.sh`. The submitting daemon is on 38080.
  The binary was built from `4c91ef3`.
- **Workaround:** on this host `$HOME` is a git repository, which breaks the
  default build directory (`../known-issues.md` item 1). Every run therefore
  used `XDG_CACHE_HOME=/var/tmp/pipedpeer-docs-cache`.
- **Limit:** all workers share one CPU, so the lab **cannot** show speedup. The
  only speedup figures in this file are [commit] measurements between real
  machines.

## R1 Tests [Oct]

| | Passed | Failed | Skipped | Raw |
|---|---|---|---|---|
| `go test -v -count=1 ./...` with torch | 615 | 0 | 2 | `raw/T1_go_test_verbose.txt` |
| same, without torch | 607 | 3 | 7 | `raw/T1b_go_test_without_torch.txt` |

- **The two skips:** `TestConcurrentJobsAndSharedNumpyCache` and
  `TestIntegrationFullSyncAndExecute`. Both need `PIPEDPEER_INTEGRATION=1`.
- **Skip check:** `scripts/check-skips.sh` passed: "all 15 distribution tests
  ran". Raw: `raw/T2_check_skips.txt`.
- **Mutation audit:** `scripts/audit-tests.sh` caught all 95 cases, with 0 not
  failing. Raw: `raw/T3_audit_tests.txt`.
- **CI** (`.github/workflows/ci.yml`) runs on pushes to `main` and on pull
  requests into `main` and `dev`. Steps:
  1. installs torch (CPU), pandas, joblib and scikit-learn;
  2. runs `gofmt`, `go vet` and `go test -race`;
  3. runs `bench-shim-d2.sh`;
  4. runs `check-skips.sh`;
  5. runs `bench-pool.sh` with `BENCH_ITEMS=32 BENCH_SPIN=200000`.

## R2 Environment cache [Oct]

`python3 docs/benchmarks/bench.py --repeat 5`, N1, a trivial job with `--remote`:

| Run | s | Node | Sent |
|---|---|---|---|
| 0 | 23.41 | aaaac1c7 | whole closure |
| 1 | 8.08 | ea8a95c6 | whole closure (this worker did not hold it yet) |
| 2 | 0.86 | aaaac1c7 | nothing |
| 3 | 0.83 | aaaac1c7 | nothing |
| 4 | 0.82 | aaaac1c7 | nothing |

- **Warm cost:** the median of the runs that sent nothing is **0.83 s**,
  against 23.41 s for the first run.
- **Sources:** per-run details are in `raw/O1_bench_py.txt` and
  `raw/N1_trivial_run*.log`.
- **August, on the other host:** 12.00 s cold, 0.65 s warm.

## R3 Interception cost [Oct]

**Whole job** [Oct-12] (N7, `workloads/pool_idle.py`: a 16-item `Pool.map`
of ~0.1 ms items, which the cost model must decline; `--remote`, five runs
each, `python3 docs/benchmarks/bench.py --only n7`):

| | Median | Range | Notes |
|---|---|---|---|
| `PIPEDPEER_SHIM=0` | 0.678 s | 0.671–0.852 s | |
| shim on | 0.796 s | 0.747–0.797 s, plus one cold 7.03 s first run | every receipt: `dispatched_items 0`; every log: `tail is 0.01s of work, below the 0.50s dispatch floor; staying local` |

This measures idle interception: about **0.12 s** per job, the measurement
head plus the decision. `bench.py` now runs N7 with `PIPEDPEER_RECEIPT` and
fails any run whose receipt shows a dispatched item. The earlier [Oct] N7 used
`pool_map.py`, which the shim distributes (18 of 48 items in every run), so it
measured distribution, not idle cost. The same workload with
`--distribute force` did dispatch (4 items), so the cluster was able to take
work and the decline is the cost model's. The first shim-on run includes a
cold start on its worker.

Lab note: fresh lab workers refuse the submitter's closure ("lacks a signature
by a trusted key", `../known-issues.md` Observed #3), so before these runs each
worker's `/etc/nix/nix.conf` got `extra-trusted-public-keys` from its own
`trusted-public-keys` file, which is what `pipedpeer setup` writes once a
daemon knows its peers.

**Start-up** (`scripts/bench-shim-d2.sh`, raw `raw/O2_shim_startup.txt`):
local-only 0.0944 s against shim-on 0.1168 s, a ratio of **1.24×**. That passes
the 3× budget. August, before patching was deferred, measured 20.0×
(`raw/B3_shim_overhead.txt`).

## R4 Work crosses nodes [Oct]

- **Forced `Pool.map`** (N3, `--distribute force`). All five receipts read
  "remote ok: 18 items … on 192.168.0.107:38080, :38081, :38082", with 12
  items local and the same checksum each time.
- **numpy** (N5). The log shows `svd: offloading 34 MB matrix to one worker`.
  The workload's `a @ b` is not intercepted.
- **`BENCH_ITEMS=48 scripts/bench-pool.sh`** against the lab
  (`raw/O3_bench_pool_lab.txt`):

  | | s |
  |---|---|
  | plain | 0.17 |
  | tuned | 0.17 |
  | pipedpeer | 0.24 |

  The receipt shows 12 items on the cluster and 12 local. It passed the 3×
  never-slower budget. One host cannot be faster.

## R5 Fault tolerance [Oct]: FAILED

`scripts/lab-fail.sh` (`raw/O4_lab_fail.txt`) printed:

```
FAIL: no worker ever received a pool chunk, so there was nothing to lose.
```

What happened:
- The job ran on the submitting host, and its log says "Closure already on the
  node; nothing to send".
- The receipt reads `remote_items 0, local_items 80, dispatched_items 0`.
- The workers were freshly created and never received the closure, so no peer
  was eligible for spilled work. That reading is inferred, not verified
  (`../known-issues.md` item 3).

The August `lab-fail.sh` output (`raw/B5_lab_fail.txt`, `POOL-OK sum=2470`) came
from the earlier harness, which commit `fca0107` showed had never distributed
anything. It is therefore not evidence of recovery either.

## R6 Sandbox start cost [Aug]

Not repeated. Median of 20, with the first 5 iterations as warm-up
(`raw/B2_sandbox_bench.*`):

| Task | bwrap | crun |
|---|---|---|
| trivial | 5.1 ms | 21.7 ms |
| medium | 8.0 ms | 23.7 ms |
| python | 16.9 ms | 36.8 ms |

## R7 Between real machines [commit]

These are verbatim in `raw/C_commit_measurements.md`, with the hash, date and
machine as each message describes them. None was repeated for the report. The
commits that matter most:
- **Pool map:**
  - fca0107: 20-core + 16-core peer over Tailscale, 4.9–5.4 s against
    5.8–6.4 s plain.
  - 8c9284c: 8:2:1 lab, 2.87 s by measured throughput against 3.61 s by
    headcount.
- **Training:**
  - 364b3d9: 54.9 / 69.4 / 56.1 s for one machine / two ranks fp16 / two ranks
    int8.
  - 2fc3914: 23.6 s against 25.4 s, measured against equal batches.
  - 2da0857: GPU 1.5 s against CPU 55.0 s.
- **Closure transfer:**
  - 193e1bf: 7 of 43 paths, 206.0 MB down to 28.6 MB.
  - 837f9c2: 3 of 39 paths, 0.43 MiB.
- **Network:**
  - 42c2426: 0.03 s with the introducer suspended; introducer traffic 0.2 MB.
  - fe43933 / e06c534: symmetric NAT on a phone tether.
- **Security:** cf2e3ae, signed closure under `require-sigs = true`.

## Not measured

| Wanted | How to get it |
|---|---|
| Repeated multi-machine benchmark | `scripts/bench-pool.sh`, `bench-split.sh` and `bench-ddp.sh` on real machines |
| Fault injection that distributes | `scripts/lab-fail.sh` once the seeding issue is resolved, or on a lab whose workers already hold the closure |
| Cross-network acceptance | `scripts/net-verify.sh` on three networks (`pipedpeer-demo/DEMO.md`) |
| Two GPUs on one node | `scripts/bench-multigpu.sh` (exits 3, "NOT VERIFIED", without the hardware) |
| Demo workloads end to end | `docker/demo-test.sh` |
