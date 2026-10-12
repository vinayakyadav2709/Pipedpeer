# Next steps: coming back to Pipedpeer

The project was frozen on 2026-10-12, after the demo round. This file is the
whole handoff: it assumes a fresh clone and nothing else (no local notes, no
rehearsal evidence, no access to the second GPU laptop). File:line references
are from the freeze; re-check them with `grep -n` before you edit.

If `TODO-BEFORE-DEMO.md` still exists at the repo root, the demo-round
checklist (the Docker walkthrough, the second GPU laptop restore, the venue
prep; the two-laptop GPU run is deferred) is not finished yet; do it, or delete the file if the demos are over.

## 1. Where it stands

**Code.** The freeze is the head of `dev` at the freeze (the demo-round branch `nightly-fixes` was merged into `dev` and deleted on 2026-10-12). The latest
commit seen while writing this was `aa9f169` ("Describe this round's last
features in the report, paper and system report"); the guide patches from the
last rehearsal (V5) land on top of it. `dev` and `main` were not updated.

**Checks at the freeze.**

| Check | Result |
|---|---|
| `go test -count=1 -p 1 ./...` (src/) | all 38 packages ok: 829 tests, 822 pass, 7 skip |
| The 4 narpack signed-closure tests | pass only with nix on PATH, skip otherwise (part of the 7) |
| `scripts/check-skips.sh` | "all 15 distribution tests ran" |
| `go vet`, `gofmt -l`, staticcheck | clean |

The distribution tests are the shim tests that prove Pool, joblib, pandas,
numpy and torch DDP work actually leaves the machine; they skip without
pandas, scikit-learn, joblib and torch, which is why `check-skips.sh` exists.

**Rehearsals.** Five container rehearsals (V1 to V5) ran the whole demo on one
laptop. The outputs quoted in the guides are verbatim from these runs; the
raw evidence files were not kept in the repo.

| Run | Commit | What it proved |
|---|---|---|
| V1 | 8e14b9b | the venue lab end to end (lab-lan): join, headline spread, failover, events |
| V2 | 31b5364, cc56753 | GPU lab: GPU step, `--ddp 2` ring, a rank dying (DDP-DOWN) and rejoining (DDP-UP); signed closure import on multi-user nix; Ctrl+C mid-upload frees the lease; `--gpu force` from a GPU-less PC; `/ui` in a laptop-sized window |
| V3 | 9949ad0 | GPU run from a GPU-less PC 7.0 s (was 212 s); `07_everything` cold 664.6 s, cached 26.7 s; finale 8/8 ok; ring grows from 1 to 2 |
| V4 | c567369 | DDP start barrier at 1 Mbit; a late rank dropped with a plain line; attach and reconnect replay; `auth set` restarts the daemon |
| V5 | ef68f02 | usage lines pass; node-kill only half passes; Ctrl+C during a cold pack unverified; `--offload` blocked by a lab artifact (section 2) |

**Demo guides** (all in `pipedpeer-demo/`):

| File | What it is |
|---|---|
| [DEMO-SCRIPT.md](../pipedpeer-demo/DEMO-SCRIPT.md) | the run sheet: live steps 0 to 9 (only repeatable steps), backup steps B1 to B7, "watch out for" per step |
| [VERIFY.md](../pipedpeer-demo/VERIFY.md) | a user-run guide: each command with the exact output the rehearsal saw |
| [SETUP.md](../pipedpeer-demo/SETUP.md) | setting up each machine, the lab-visit steps and demo-day steps |
| `DEMO.md` | the three-network walkthrough |

**Venue** (user, 2026-10-12). Two layouts, both in the guides:

| Layout | Machines | Notes |
|---|---|---|
| 1 (likely) | no college lab: LAPTOP-A and LAPTOP-B (Fedora, NVIDIA GPU) and LAPTOP-C (no GPU), all on Wi-Fi | LAPTOP-C is the live join, the GPU sender and the machine stopped in failover; everyone joins the rendezvous by IP (Wi-Fi client isolation); smaller speed-ups, the 1.2× rule decides what stays live |
| 2 | a college lab: 1–3 Ubuntu PCs on ethernet, plus our laptops on Wi-Fi | mixed subnets, so everyone joins the rendezvous by IP; with fewer PCs the PC roles fold together |

DEMO-SCRIPT.md, "Machines and screens", maps each role to a machine per
layout; SETUP.md has a "Layout 1" prep; VERIFY.md runs both layouts in Docker
(pc1 plays LAPTOP-C, pc2 plays LAPTOP-B without a GPU, pc3/pc4 are the
college PCs). Before demo day the friend runs VERIFY.md in Docker
(TODO-BEFORE-DEMO.md, section 1); the real two-laptop GPU run is later.

Background: [known-issues.md](known-issues.md),
[fixes-and-roadmap.md](fixes-and-roadmap.md),
[capabilities.md](capabilities.md), [system-report.md](system-report.md).

## 2. Not verified on real hardware

Start with VERIFY.md, section "Not re-checked on real machines yet". Those
features are unit-tested only: the slow-helper race, skipping a node that
refuses the closure, `--remote` waiting for room, the 3-attempt cap, DDP rejoin
with `print(loss)` after the loop, `--max-concurrent` surviving `auth set`,
`--ddp 2` usage lines and the start barrier lines.

V5's partial results:

| Item | Result |
|---|---|
| #13 kill the job's node, two machines | half pass. The node loss was seen in 16 s (gateway probe), but the reschedule then retried every 30 s with no limit and only resumed when the node came back. This is FIX-REQUEUE |
| #7 Ctrl+C during a cold pack | unverified: the pack finished too fast to interrupt. Ctrl+C mid-upload left no nix processes. Found FIX-BUILDINT instead |
| `--offload` (ef68f02) | the daemon half works (every part went to peers). The spread line was wrong only because lab containers share the host's boot_id, so the script counted itself as a peer. Needs two physical machines |
| GPU step from a GPU-less PC | not captured (CUDA download stalled). On the host GPU: training 13.4 s, total 15.1 s, nvidia-smi at 100% for 13 s |

**The two-laptop GPU ring was never completed.** The `--ddp 2` ring across two
real GPU laptops (RTX 3060 + RTX 3050, a third machine submitting) placed on
both, then the second GPU laptop dropped off its network after ~1 GB of
closure over a ~2 MB/s link and the run finished on one rank. The ring logic
is proven only in the GPU lab (two containers sharing one GPU). Not to be
done now; later, when both laptops are together (user, 2026-10-12).

## 3. Known gaps, left by choice

Both were found in V5. The user chose to document them, not fix them, because
the project was frozen and the demo flow never hits them. They are also listed
in VERIFY.md under "Known gaps found in V5".

| Gap | Where | Size | Fix |
|---|---|---|---|
| FIX-REQUEUE: a rescheduled job with no reachable node retries every 30 s forever | `src/internal/coordinator/coordinator.go:503-599` `PlaceWithRetry`; interval at :504-518 | S | bound it by the placement window (`--remote` waits up to 10 min) and fail with a plain message |
| FIX-BUILDINT: Ctrl+C during `[4/7] Building locally...` of a `--remote` run is ignored until the build ends | `src/internal/app/env.go:66` `BuildEnvironment` takes no ctx; the `nix build` at :146 uses `nixstore.Cmd`; caller `src/internal/app/run.go:117` | S | pass the run's ctx and use `nixstore.CmdContext` (`src/internal/nixstore/nixstore.go:228`) |

## 4. Future work, in priority order

| # | Unit | What | Entry points | Size | Why this rank |
|---|---|---|---|---|---|
| 1 | FIX-REQUEUE | section 3 | coordinator.go:503-599 | S | real users hit it whenever a node dies with no spare |
| 2 | FIX-BUILDINT | section 3 | app/env.go:66, :146 | S | Ctrl+C must always work; small |
| 3 | AUDIT | whole-repo audit beyond the demo round: security, "errors are never silent", dead code, docs drift. Findings become fix units | everything; start with `CONTEXT-MAP.md` | M-L | the demo round reviewed only its own diff |
| 4 | SKIPPED-CHECKS | run the VERIFY.md "Not re-checked" list on the labs or real machines, fix what fails | VERIFY.md; `scripts/lab-lan.sh`, `scripts/lab-gpu.sh` | M (mostly running) | features shipped without one real run |
| 5 | FRIEND-RING | the real two-laptop GPU ring with a third machine submitting. **Not to be done now; later, when both laptops are together** (user, 2026-10-12) | DEMO-SCRIPT GPU backup step; SETUP.md | needs two people | the only proof of DDP across physical GPUs |
| 6 | DDP-LEAD | two-GPU training survives the loss of the lead (rank 0) machine. Today the lead's daemon runs the rendezvous and gradient sync, so its death ends the run | `src/main.go:1091` (`rank0 := ring[0]`), `src/internal/daemonapi/ddpsync.go`, `ddpup.go`, `src/ddpdown.go:161-170` (the "can't continue" message) | ~300-400 lines | the largest robustness gap; needs a design choice (who takes over the sync), so ask first |
| 7 | PRUNE-EDGE | `PruneCompletedJobDirs` checks the job dir's mtime; appending to `output.jsonl` doesn't touch it, so a job running longer than `JobRetention` (7 d) can lose its dir mid-run | `src/internal/daemonapi/server.go:445-494`, mtime check :480, retention :431 | S | data loss, but only for week-long jobs |
| 8 | DAEMON-IMPORT-CTX | the daemon runs nix imports/exports with `nixstore.Cmd` and no ctx; a cancelled upload's import runs to the end | `src/internal/daemonapi/narcache.go:210,235,278,316`; `execution.go:1194-1214` (`importNAR`) | S | wasted work after a cancel, no wrong results |
| 9 | UNTRUST | a `pipedpeer untrust` command; `nixtrust.Remove` exists but nothing calls it | `src/internal/nixtrust/nixtrust.go:298`; the `trust` command at `src/main.go:401` | S | trust can be granted but not taken back |
| 10 | DDP-BARRIER-BACKENDS | the DDP start barrier covers only the daemon sync channel, not gloo/nccl | shim start wait `src/internal/nixgen/shim.go:3593`; backend choice :4281; `ddpsync.go:344` | S-M (not estimated) | gloo/nccl are the fallback transport, rarely used |
| 11 | NODES-PRUNE | test daemons left ~20 stale rows in a dev machine's `nodes.db` (cleared by hand at the freeze). Prune long-unreachable identities or document the cleanup | `src/internal/nodestore/store.go:317` `PruneStale` (skips manual rows, drops its SQL error without a log); called at `server.go:1873` | S | cosmetic, but stale rows could be picked by `--remote` |
| 12 | Docs drift | known-issues.md is dated 2026-10-02 (4c91ef3) with later sections; file:line references here and in CONTEXT-MAP drift with every change. Fold into AUDIT | `docs/`, `CONTEXT-MAP.md`, this file | S | stale docs mislead the next agent |

The report, paper and system report were brought up to date at the freeze
(aa9f169: report 80 pages, paper 5 pages).

## 5. Decisions already made (do not reopen)

All by the user, 2026-10-12, unless marked.

**Freeze**
- The project is frozen after the demo round. All deferred work (section 4)
  was skipped on purpose; only the report/paper sync was done.
- FIX-REQUEUE and FIX-BUILDINT are documented as known gaps, not fixed.
- `s` and `s2` were merged into `nightly-fixes`, which was then merged into
  `dev` (2026-10-12) so the nightly channel serves the demo build. `main` and
  the stable releases were not touched.
- The live demo shows only sensible, repeatable steps; Ctrl+C, network loss
  and the like are backup steps, shown only if asked.
- DDP lead (rank 0) survival: after the demo. The demo drill kills a non-lead.

**Behaviour**
- `run --offload`: the script runs here; every distributed piece goes to other
  machines; if no peer can take a piece it runs locally with a warning (ef68f02).
- `--remote` waits up to 10 min for room, with a progress line every 30 s (1e07d56).
- DDP keeps going when a rank dies: drop it, say which and where, re-spread its
  share (DDP-DOWN). A machine that comes back rejoins (DDP-UP), using a
  consistent-hash ring for shards and a model + optimizer snapshot at an epoch
  boundary.
- Ctrl+C or closing the terminal stops the job everywhere within a couple of
  seconds and frees leases. A submitter that vanishes with no signal leaves a
  `--remote` job running and attachable.
- Background mode: `--detach` and Ctrl+Z detach; `attach <id>` replays and
  follows; the watcher downloads results into the script's folder when the job
  ends. Detach/attach is only output visibility; every other step is identical
  to a foreground run. Unattached job dirs are kept 24 h.
- `--cold` re-sends the environment as if the peer had none; it does not rebuild.
- `events` is per machine. `attach` only from the machine that started the job.
- `\r` progress updates are forwarded so tqdm bars animate.
- No result cache. Do cache nix packages: a common-packages pre-build on every
  machine, plus auto packages for unknown imports and `requirements.txt`.
- Memory: a job with no estimate and a daemon without systemd both get a
  default cap.
- `--gpu prefer` picks one device kind and never blends GPU and CPU ranks.
- The run summary has a "this machine" usage line (CPU, RAM, GPU); the GPU demo
  step trains long enough (~10-20 s) for nvidia-smi to move.
- tqdm, seaborn, sympy, networkx and statsmodels are in the import-to-nix table.

**Demo and venue**
- No projector: every screen is read on a laptop held up. No VPS: the
  rendezvous runs on a laptop and everyone joins its LAN IP.
- The GPU step is a highlight: a GPU-less PC submits, a laptop GPU runs it.
  The GPU environment is built by the sender and pre-seeded on every machine.
- Wow order: unchanged script across the lab, then failover, then the
  teacher's own script. Numbers shown live only if good; one cold run, the
  rest cached; the finale runs jobs in parallel.
- Laptop sleep is not tested or shown. The rehearsal adds netem delay/loss and
  keeps one lab PC with ufw on.
- No big new builds without asking; small demo additions are fine.

**Assumption audit (accepted: "agreed with all, make them all")**
Changed, and built in fix pass 8: #4 a DDP joiner's skipped epochs yield one
no-op batch so loop variables exist (82596fe); #6 a node that refuses the
closure is skipped, the job goes to the next (1e07d56); #7 nix export/pack
children die on Ctrl+C (99cf0ad); #9 a slow pool peer is raced locally, first
result wins (8541355); #10 the CLI reads its own daemon's memory ceiling; #11
`--remote` waits 10 min; #12 `--max-concurrent` survives `auth set`/`join`
(76a0c3b); #13 "offline" also probes the default gateway and introducer
(879b86c); #29 at most 3 attempts on distinct nodes (1e07d56). #5 (lead
survival) deferred. Every other audited assumption was kept as built. All
merge-round assumptions were confirmed the same way.

## 6. Open questions for the user

Eight agent assumptions from the last two batches were never confirmed.

| # | Assumption | Recommend |
|---|---|---|
| 1 | POOLRACE: the remote copy of a raced part has no HTTP timeout; a dead peer is caught by TCP keepalive or the cancel. If both copies fail, the part fails with both errors | keep: a fixed timeout would kill long, healthy parts |
| 2 | POOLRACE: a late-started local win is not counted in speed measurements | keep: it would skew the peer's speed estimate |
| 3 | PLACE: "untried node first" applies only to the smart strategy; round-robin already rotates | keep |
| 4 | OFFLOAD: `--offload` ships every interceptable op like `--distribute force` (no cost model). DDP fails with a hint if placement drops a rank between counting and placing | keep: the flag is an explicit opt-in |
| 5 | SELFUSE: attach/collect summaries have no usage lines (no samples saved while detached); DDP usage lines print under "=== Usage ===" | keep |
| 6 | JOINEPOCH: BatchNorm running stats on a joining rank see the catch-up batches, then the lead's buffers overwrite them | keep: the lead's buffers are authoritative |
| 7 | DDPREADY: the start wait defaults to 5 min (`PIPEDPEER_DDP_START_WAIT`); on a ~2 MB/s link a slow machine misses it and joins later | keep: the late rank joins via DDP-UP; pre-seeding the environment avoids it |
| 8 | CLI2: the restart after `auth set`/`join` keeps `--max-concurrent` (76a0c3b); nothing else the daemon was started with is confirmed kept | change: have AUDIT check every `start` flag survives the restart |

## 7. Resuming on a new machine

### Tools

| Tool | Notes |
|---|---|
| Go | the version in `src/go.mod` (1.25.0 at the freeze) |
| Nix | multi-user install. On some installs nix is not on PATH for non-login shells: put `/nix/var/nix/profiles/default/bin` first, or the 4 signed-closure tests skip |
| docker | both labs use docker (lab-lan needs the host on the bridge, which rootless podman can't do). The GPU lab needs the NVIDIA container toolkit with a CDI spec (`docker run --device nvidia.com/gpu=all ... nvidia-smi` must work) |
| Python deps | pandas, scikit-learn, joblib, torch for the shim tests; otherwise they skip |
| tectonic | to rebuild the PDFs. A local TeX Live may lack `geometry.sty`. Under XeTeX the paper keeps a Times look through TeX Gyre Termes |

### Commands

```sh
cd src && go build -o ../bin/pipedpeer .          # never a bare `go build` (stray binary)
cd src && gofmt -l . ; go vet ./...
cd src && PATH=/nix/var/nix/profiles/default/bin:$PATH go test -count=1 -p 1 ./...
scripts/check-skips.sh                            # must say "all 15 distribution tests ran"
cd docs/report && tectonic -X compile --outdir <out> main.tex
cd docs/paper  && tectonic -X compile --outdir <out> paper.tex
```

Labs, one at a time:

```sh
scripts/lab-lan.sh up | join all | x <pc> <cmd> | logs | down   # venue: 4 PCs on one bridge, ~6 GB
scripts/lab-gpu.sh up | join all | x <node> <cmd> | logs | down # gpu1+gpu2 share one GPU, sub1 submits, ~7 GB
```

### Lab facts and traps

- Containers share the host kernel's boot_id, so they count as one physical
  machine (`heartbeat.Machine()`): `--offload` can't be shown in a lab without
  per-container boot_id bind-mounts.
- The GPU lab's nix store is lost on down/up; rebuilding the CUDA environment
  takes ~20 min. Update the binary in place instead of recycling the lab.
- Move this machine's `nodes.db` aside during a lab, or stale rows show up.
- `lab-gpu.sh` writes logs and its token under a hard-coded folder in your home
  (`logroot`, near the top of the script); create it or change it.
- Lab PCs have no python3, so `7-gpu.sh` skips there. Lab PCs (~1.2 GB each)
  can't run `12-everything`. netem runs as `docker exec <pc> tc ...`.
- The lab proves logic, never speedup (shared CPU); speed numbers come from
  real machines.

### Safety

Heavy work (labs, full tests, big builds) can crash a 13-16 GB laptop. Run it
in a memory-capped scope and check `free -g` shows at least 3 GB first:

```sh
cd src && systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=1G -p CPUQuota=400% \
  --setenv=PATH=/nix/var/nix/profiles/default/bin:$PATH \
  sh -c 'gofmt -l . ; go vet ./... && go test -count=1 -p 1 ./...'
```

Never run two labs, or a lab and a full test, at once.

## 8. How to work on it

1. Cut a new branch from `dev` (for example `deferred-fixes`) and open a PR
   back into `dev` when it is verified; `main` is for tagged releases
   ([CONTRIBUTING.md](../CONTRIBUTING.md)).
2. Re-check the file:line references above, then pick from section 4 in order.
   Ask section 6's questions once, up front.
3. Before merging into any branch a demo uses, re-run the demo flow:
   VERIFY.md end to end on lab-lan, the GPU lab for DDP, and the full test
   suite with `check-skips.sh`.
4. Update this file when an item lands or a decision changes.
