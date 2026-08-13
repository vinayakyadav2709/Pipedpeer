# Pipedpeer — Agent Handoff

Last updated: 2026-08-14. Working branch `oss-setup` (pushes to `poc`/`dev`).

---

## 1. Project goal

**Pipedpeer runs your Python workload on other machines without changing your code.**

```bash
pipedpeer python train.py     # runs on the best node in your swarm, feels local
```

Every machine runs a small daemon. Daemons find each other on the LAN, publish their
capacity (cores, RAM, GPUs, load), and accept jobs from peers. There is no head node,
no cluster manager, no YAML.

The differentiator versus Ray/Dask: **we require zero code changes.** Ray makes you add
`import ray` + `@ray.remote`; we build the Python environment ourselves, so we can plant
a shim inside it and intercept the parallel primitives the script already uses.

Repo: `github.com/vinayakyadav2709/Pipedpeer` · Go module `github.com/pipedpeer/pipedpeer`
· Go 1.25 · all Go code under `src/`.

---

## 2. Current state — exactly where we are

### Very last thing I did (IMPORTANT — uncommitted, untested)

I was mid-refactor on **Part 2b** (`pipedpeer map`). Two files are in the working tree:

| File | State |
|---|---|
| `src/internal/app/env.go` | **NEW, untracked.** Extracts `Environment` + `BuildEnvironment()` — the expensive, node-independent half of a run (dep resolution → flake → `nix build` → NAR export → workspace tar). |
| `src/internal/app/run.go` | **MODIFIED, rewritten.** Now = `Run()` (builds env, delegates) + `RunTask()` (the per-node half: upload → execute → download results). |

**Why:** today `app.Run` does all 7 stages per script. A fan-out must build the
environment **once** and then run N tasks against it. This split is the prerequisite for
`pipedpeer map` and for the interception shim later.

**Verification status:** `go build ./...` ✅ and `go vet ./...` ✅ both pass.
**`go test` has NOT been run against this refactor.** See §5 for what to do.

Both files are complete — no placeholders, no `// ... rest of code`.

### Git state

```
branch oss-setup → pushed to poc (and dev, up to commit 2befd26)
2befd26 Return only files a job changed          ← last commit (Part 2a)
4d1c6fc Fix nightly artifact upload and integration setup
71bdde5 Add open-source project documentation
24bada5 Add stable and nightly install channels
3e3d91b Add CI and nightly pipelines
97574de Stop lease expiry test racing its own setup
71d2ced Make job admission atomic
db0685d Apply gofmt across source tree
18a4c4f Remove committed pycache files, extend gitignore
```

### Branch model (set up this session)

| Branch | Stream | Notes |
|---|---|---|
| `main` | stable | Deliberately an **orphan placeholder README** (`82f2b17`). No stable release cut yet. Protected. |
| `dev` | nightly | Nightly prereleases build from here. Protected, requires CI. **Default branch.** |
| `poc` | internal | Playground, no guarantees, no CI gate. |

- Tag `archive/python-poc` preserves the old Python prototype history (it was deleted from
  `main`; the tag is what keeps those commits from being GC'd).
- Deleted branches: `background_worker`, `poc-local-gpu`, `test-goreleaser-setup`.
- Default branch was `poc` (internal playground was the public landing page) → changed to `dev`.

---

## 3. Architecture — how the system actually works

This is the expensive-to-rediscover knowledge. Trust it over comments in the code, which
are stale in a few places (noted below).

### 3.1 Transport

- **HTTP/JSON + WebSocket on port 38080 is the only load-bearing transport.** Every node
  runs a chi server (`src/internal/daemonapi/server.go`, `buildRouter`). Routes:
  `/health`, `/v1/accept`, `/v1/commit`, `/v1/renew`, `/v1/complete`, `/v1/cancel`,
  `/v1/jobs/upload`, `/v1/jobs/{id}/exec` (WS), `/v1/jobs/{id}/results`, `/v1/roundrobin`,
  `/v1/jobs`, `/v1/nodes`.
- **NATS is wired but effectively dead.** Each daemon starts an *embedded* nats-server bound
  to `127.0.0.1` on a random port with no cluster routes — so it can only talk to itself.
  `main.go` builds `coordinator.Config` **without** `Bus`, so every path falls through to
  HTTP. The NATS code paths have tests (`*_nats_test.go`) but are unused in production.
  Don't assume NATS does anything today.
- **Discovery is UDP broadcast on :38099, not mDNS** (despite comments and
  `nodestore.SourceDisplay` saying mDNS). `Advertiser` answers the literal probe
  `PIPEDPEER_DISC_V1`; `Discover()` sends to 255.255.255.255 + per-interface broadcasts.
- **No auth, no TLS, no NAT traversal.** Anyone who can reach :38080 can upload and execute
  arbitrary code. This is a documented, deliberate trade-off (see `SECURITY.md`).
- Actual membership in practice: the daemon's `StartPeerPoller(10s)` does UDP discover →
  `upsertSelf()` → `GET /health` every known node → `PruneStale(5m)`.

### 3.2 Execution path (one job, today)

`coordinator.ExecuteWithRetry` (`src/internal/coordinator/coordinator.go`):
1. `FindNode()` → merge candidates (local daemon `/v1/nodes`, registry, self) → filter
   (arch, GPU, memory, cores, VRAM) → score → `POST /v1/accept` = reserve a lease.
2. `POST /v1/commit`.
3. Renew every 30s while the executor runs; `CompleteLease` at the end; on failure,
   re-place with backoff.

The executor is `app.Run` — the 7-stage pipeline:
1. import scan (or `uv.lock`) → 2. resolve nixpkgs → 3. generate `flake.nix` →
4. `nix build` **on the submitter** → 5. `nix-store --export` → NAR →
6. workspace tar + multipart upload → 7. WS execute, stream stdout/stderr, download results.

Node side (`src/internal/daemonapi/execution.go`): untar into `<jobDir>/<id>/work`,
`nix-store --import` the NAR, then run either inside a hand-rolled **crun OCI bundle**
(`--isolate`, default true) or as a plain host process (`--isolate=false`).

**Dispatch is strictly one task → one node.** No fan-out exists yet. The only cluster-wide
fan-out code in the repo is read-only: `src/clustertasks.go` (`fetchClusterTasks`) — a good
template for concurrent multi-node calls.

### 3.3 Scoring (`coordinator.go`)

Hard filters: arch mismatch, `requireGPU && !hasGPU`, insufficient free mem / cores / VRAM.
Then `scoreNode` = `1 − CPU%/200 − Mem%/200 − 0.05·ActiveJobs` plus centred capability
terms (cores, MHz, free RAM, free-core ratio), × HealthScore. `scoreGPUNode` adds ≤0.8 for
VRAM/device count/free VRAM/compute capability, minus GPU utilisation.
Strategies: `smart` (GPU group then CPU group) and `round-robin` (sorted by NodeID, index
from the daemon's atomic counter via `/v1/roundrobin`).

### 3.4 Environment provisioning

- **Nix is the only implemented mechanism.** `nixgen.GenerateFlake` pins
  `github:NixOS/nixpkgs/nixos-24.11` and produces `writeShellScriptBin "run"`. The whole
  downstream contract is that `<storePath>/bin/run` exists.
- `pythondeps.nixpkgsMapping` has only ~35 entries; **unmapped imports are silently dropped.**
- Because the closure is built on the submitter and shipped as a NAR, **arch must match**.
- **crun/OCI is already half-there:** `daemonapi/execution.go` hand-writes an OCI
  `config.json` (types `ociConfig`, `ociProcess`, …) and runs `crun run --bundle`. But the
  rootfs is **empty** with `/nix` bind-mounted — there is **no OCI image support** (no
  registry pull, no layers). That gap is Part 3.
- `bwrap` is dead code (`scripts/bootstrap/modules/bwrap.sh` only); nothing in Go uses it.

### 3.5 State / persistence

- **sqlite is used for exactly one thing:** the node store
  (`~/.local/share/pipedpeer/nodes.db`, pure-Go `modernc.org/sqlite`, WAL, 1 conn).
- **Job history is JSON files**, one dir per job under `~/.local/share/pipedpeer/jobs/<id>/`
  (`metadata.json`, `script.py`, `flake.nix`, logs, and now `results-manifest.json`).
- **Volatile, lost on daemon restart:** the uploaded-job map, all leases, peer-health cache,
  round-robin counter. Job work dirs accumulate with no GC.
- Identity = a plain UUIDv4 in `node_identity.json`. **No keypair, nothing signable.**

---

## 4. Design decisions and the reasoning behind them

These came from direct user decisions. Violating them will get the work rejected.

### D1. NO SDK. Interception only. ⚠️ The user was emphatic about this.

> "thats the whole point we cant create sdk like this we need to do it with cli only like
> whole objective is just run pipedpeer python main.py and it will like its local"

So: **never propose `import pipedpeer`.** The mechanism is:

`pipedpeer python main.py` already controls the interpreter launch and the environment, so
we put a `sitecustomize.py` on `PYTHONPATH` — Python auto-imports it before the user's first
line — which swaps the parallel primitives for cluster-backed ones:

- `multiprocessing.Pool` / `concurrent.futures.ProcessPoolExecutor` → cluster executor.
  **Why these are safe:** Python already forces process-pool work to be picklable and
  share-nothing. Code that works with a local process pool has *already* promised the
  properties remote execution needs. (Thread pools are left alone — shared memory.)
- `joblib` via its **official pluggable-backend API** (not monkey-patching). This is the
  single highest-leverage component: sklearn's entire `n_jobs=` surface runs on joblib.
- `numpy.matmul`/`dot` above a high size threshold → block partitioning (the `np_d` idea
  from the old Python prototype). **Why matmul works:** O(n³) compute on O(n²) data, so past
  a threshold compute dominates transfer. Elementwise ops are deliberately excluded —
  memory-bound, shipping always loses.

**Why not static analysis / "we divide the code ourselves":** proving loop-iteration
independence in Python is undecidable in practice, and a wrong guess *corrupts results*
rather than merely missing a speedup. No production system does it (not Ray, Dask, or Spark).
Runtime interception is precise because the program *tells* us what's parallel by calling
the primitive. Analysis is still useful **advisory**: at submit time we already import-scan,
so we can say "this script uses multiprocessing → will distribute across N nodes" or
"this script is serial → single node".

### D2. The never-slower invariant. ⚠️ User pushed hard on this.

> "but if we send all parallel code to differnet nodes it will fucking slow everything down"

Correct, and the design must make that impossible:

- `ClusterPool` **starts as a real local pool.** The first chunks run on local cores — that
  *is* the measurement (real per-item compute cost, real payload size).
- Spill to remote only when `measured_per_item_cost × remaining_items` clearly exceeds
  dispatch + transfer cost (per-peer network cost measured via an extended ping/bandwidth probe).
- **Local cores never stop pulling from the queue.** A remote node is an additional consumer;
  every chunk it takes is one local didn't have to do. It can add less than hoped; it cannot subtract.
- Straggler tail: an idle local core speculatively re-runs the final in-flight chunks,
  first result wins.
- **Worst case ≡ a plain local `multiprocessing.Pool`.** This must be enforced by a CI
  benchmark gate (shim-on vs local-only on a small workload, within noise).

### D3. No task repetition

One task = one pickled payload = one lease = one node. Completion is recorded per task.
A task re-runs **only** when its node dies (lease expiry). A task that fails in *user code*
is reported failed, never silently re-run. The single exception is D2's straggler tail.

### D4. Heterogeneous nodes: over-decompose + pull-as-you-finish

Never split into exactly-N-nodes pieces (equal pieces on unequal machines = everyone waits
for the slowest). Create many more tasks than nodes (`--split parts:auto` ≈ 2–3× eligible
node count) and hand the next task to **whichever node frees up first**. Speed is never
predicted; it's revealed. Hard constraints (arch/GPU/mem) are filters; scoring picks among
the eligible.

### D5. Security posture: trusted networks only

> "for now we are not setting up anything for them they need to manage there network so we
> can go with no worries so its simple"

Documented plainly in `README.md` + `SECURITY.md`. Real auth arrives with Part 4 (WebRTC),
where node identity becomes an ed25519 keypair.

### D6. Misc locked decisions

- **License: Apache-2.0** (patent grant; standard for infra tooling).
- **`main` stays an empty placeholder** until the first stable cut — user's explicit call.
- **Windows dropped from releases**: running jobs needs crun + Linux namespaces + Linux GPU
  device nodes, so a Windows binary could never execute work. darwin stays as a submit-side client.
- Python prototype removed from `main` (recoverable via `archive/python-poc`).

---

## 5. Roadblocks / open items

### 🔴 Immediate: the uncommitted refactor is untested

Run this first:

```bash
cd src && go test -count=1 ./...
```

- **If it passes:** also run `go test -race -count=1 ./...`, then `gofmt -l .` (must be
  empty), then commit the refactor and continue with Part 2b (§ plan.md).
- **If it fails:** the likely culprits, in order of probability:
  1. **`main_cli_test.go` / `main_integration_test.go`** may reference `app.Run`'s old shape
     or the old `daemonctl.DownloadResults` signature. `DownloadResults` changed from
     `error` to `(*ResultManifest, error)` in commit `2befd26` — grep for callers.
  2. The stage-numbering output changed slightly (`RunTask` prints only when
     `task.StageFmt != ""`); any test asserting on stdout text may need updating.
  3. `findProjectRoot` moved from `run.go` to `env.go` — same package, so this is only a
     problem if something declared it twice.
  - Fix forward; do **not** revert to the pre-refactor `run.go`, since Part 2b depends on the split.

### 🟡 Known latent issues (found during exploration, not yet fixed)

- `Record.PeakMemBytes` is **never written**, so the "historical" tier in `resourceest`
  never fires. Worth fixing early in Part 2 — placement quality matters much more once
  you're placing N tasks at once.
- Daemon job map + leases are **in-memory only**; a daemon restart loses all of it.
- `pythondeps` silently drops unmapped imports (~35-entry mapping) — a real UX cliff.
- Nightly integration job is `continue-on-error: true`. It passed on its first green run;
  flip it to blocking after ~a week of green.
- GoReleaser `--snapshot` derives version from the latest tag (`v0.1.3` exists), so nightly
  versions look like `0.1.4-nightly.<date>-<sha>`.

### 🟢 Fixed this session (don't re-investigate)

- **Admission control TOCTOU** (`71d2ced`) — the concurrency cap read the lease count and
  inserted the lease in *separate* critical sections, so concurrent submitters all saw the
  same free slot (cap of 5 admitted 7). GPU reservation had the identical gap. Now one lock
  spans cap check → memory check → GPU reserve → insert, with host metrics gathered first.
  **This matters for Part 2**: fan-out is exactly the concurrent-submitter pattern that broke it.
- **Flaky lease-expiry test** (`97574de`) — it raced an 80 ms lease against a `gpu.Detect()`
  call that shells out to `nvidia-smi`. Failed in isolation (cold cache) and under load.

---

## 6. Implicit knowledge — the unspoken rules

1. **NEVER attribute work to Claude/Anthropic/AI** in anything that reaches GitHub: commit
   messages, PR titles/bodies, code comments, READMEs, docs. No `Co-Authored-By` trailers,
   no "Generated with" footers. Commits are authored as `falcon <vinayakyadav2709@gmail.com>`.
   (This handoff bundle is an explicit, user-requested exception.)
2. **Go commands must run from `src/`** — the module root is `src/`, not the repo root.
   `cd src && go test ./...`. `bench/` is a separate module.
3. **This repo has codedb hooks that block `find` and `grep`.** Either use the `codedb_*`
   MCP tools, or prefix with `CODEDB_NO_HOOKS=1`. `export CODEDB_NO_HOOKS=1` at the start of
   a compound command works.
4. **Don't write wall-clock-race tests.** Two of the three flakes found this session came
   from tests assuming an operation completes inside a short window. `gpu.Detect()` and
   gopsutil calls shell out to vendor tools and are slow on a cold cache. If a test must
   exercise expiry, give it a wide window and anchor waits to a recorded timestamp.
5. **The user prefers direct execution over deliberation.** Plan when asked, then build.
   They interrupt with "plan first" / "tell me how" when they want discussion.
6. **Don't use subagents unless asked.** If subagents are used, the user specified:
   **opus or sonnet only, never fable.**
7. **Comments in this codebase are stale in places** — "mDNS" is UDP broadcast, bootstrap
   docs referenced a `pipedpeer doctor` command that never existed (fixed to `setup`),
   `tests/Taskfile.yml` used the removed `pipedpeer peers` (fixed to `nodes`).
8. **CI runners are 2-core and contended.** Anything timing-sensitive that passes locally
   may still fail there. The full `-race` suite takes ~2m10s on CI.
9. The user's `~/Projects/Pipedpeer` checkout may still be on the old local `poc-unified`
   branch — they should `git fetch --prune && git switch dev` (or `poc`).

---

## 7. Key files

| Path | What |
|---|---|
| `src/main.go` | All CLI commands (cobra). `run` is the big one; `python` is a shorthand that rewrites to `run --strategy round-robin --no-self --isolate=false`. Hidden `__daemon__` first-arg mode spawns the daemon. |
| `src/internal/app/env.go` | **NEW** `Environment` + `BuildEnvironment` — shared, node-independent build. |
| `src/internal/app/run.go` | `Run` (single job) + `RunTask` (per-node half). |
| `src/internal/coordinator/coordinator.go` | Placement, scoring, lease lifecycle, retries. |
| `src/internal/daemonapi/server.go` | Node HTTP API, leases, **admission control** (`processAccept`). |
| `src/internal/daemonapi/execution.go` | Upload/untar, OCI bundle + crun, WS exec, results tar. |
| `src/internal/daemonctl/execution.go` | Client side: `UploadJob`, `StreamExecute`, `DownloadResults`, `ExportNAR`, `CreateWorkspaceTar`. |
| `src/clustertasks.go` | The only existing multi-node fan-out (read-only) — template for concurrency. |
| `lab/docker-compose.yml` | 3-worker cluster (`privileged`, `network_mode: host` — that's what makes UDP discovery and crun work). `make lab-up`. |
| `.github/workflows/{ci,nightly,release}.yml` | The pipelines built this session. |

---

## 8. CI/CD as built (all verified green on real runners)

- **`ci.yml`** — PRs + pushes to `main`/`dev`: gofmt gate → `go vet` → `go test -race` →
  cross-compile matrix (linux/darwin × amd64/arm64).
- **`nightly.yml`** — cron 02:00 UTC on `dev` + `workflow_dispatch`. Skips if `dev` hasn't
  moved since the last nightly (compares HEAD to the release's `targetCommitish`). Then
  tests → integration (installs Nix + crun; non-blocking) → GoReleaser snapshot → rolling
  `nightly` prerelease (delete + recreate the release/tag each run).
  **Gotcha already solved:** GoReleaser's `formats: [binary]` does *not* rename files on
  disk — binaries stay at `dist/pipedpeer_<os>_<arch>*/pipedpeer` and the friendly name is
  only metadata. The workflow stages uploads by reading `dist/artifacts.json` with `jq`.
  Also: the integration job must build `lab/pipedpeer` first, because the lab compose file
  bind-mounts it.
- **`release.yml`** — `v*` tags only.
- **Install channels** — `scripts/install-pipedpeer.sh --channel stable|nightly`
  (or `PIPEDPEER_CHANNEL`). Verified end-to-end against the published nightly.
