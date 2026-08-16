# Pipedpeer — Roadmap

Read `handoff.md` first for architecture and the reasoning behind these decisions.

---

## ✅ Completed

### Part 0 — Repo consolidation
- [x] Push the previously local-only unified Go codebase (was at risk on one machine)
- [x] 3-branch model: `main` (stable placeholder), `dev` (nightly), `poc` (internal)
- [x] Archive the Python prototype at tag `archive/python-poc`, remove from `main`
- [x] Delete stale branches (`background_worker`, `poc-local-gpu`, `test-goreleaser-setup`)
- [x] Default branch `poc` → `dev`; branch protection on `main` + `dev`
- [x] Hygiene: untrack `__pycache__`, extend `.gitignore`

### Part 1 — CI/CD + open-source setup (all verified green on real runners)
- [x] `gofmt` the tree (prerequisite for a formatting gate)
- [x] `ci.yml` — gofmt gate, `go vet`, `go test -race`, cross-compile matrix
- [x] `nightly.yml` — skip-if-stale → tests → integration → rolling `nightly` prerelease
- [x] `release.yml` scoped to `v*` tags; Windows targets dropped; changelog config
- [x] `install-pipedpeer.sh --channel stable|nightly` (verified end-to-end)
- [x] LICENSE (Apache-2.0), README, CONTRIBUTING, SECURITY
- [x] Fix stale references (`pipedpeer doctor` → `setup`, `pipedpeer peers` → `nodes`)

### Bugs fixed en route
- [x] **Admission control TOCTOU** — concurrency cap over-admitted under concurrent
      submitters (cap 5 → 7 admitted); GPU reservation had the same race
- [x] **Flaky lease-expiry test** — raced an 80 ms lease against a `nvidia-smi` shell-out

### Part 2a — Results manifest (commit `2befd26`)
- [x] Daemon stamps uploaded files and returns **only** what the job created/modified
- [x] Client extracts in Go (not shelling to `tar`), refuses path traversal, returns a manifest
- [x] Manifest recorded in job history; `ResultsDir` is now a parameter (fan-out prerequisite)

---

## ✅ Part 2c — Transparent interception: multi-node dispatch proven live

All of Part 2c is shipped and verified end-to-end in the 3-node lab (`lab/`), including:

- [x] Shim ships worker functions **by source** (`func_src`/`func_name`) plus pickled globals
      (`extra_b64`, a dict — the fixed right-hand operand) so workers need no shim on path
- [x] **One-hop fan-out** (`no_fanout`): the origin splits exactly once; forwarded chunks are
      terminal and results funnel back to the origin (tree model). Each part is assigned a
      **distinct** peer (part i prefers peers[i], falls back to the next best on failure), so
      every healthy peer gets work. Multi-hop (Spark-style combiner nodes) is a later flag-flip.
- [x] **Memory offload**: warm workers run from files (`{in_file, out_file}` messages) — payloads
      and results never live in daemon RAM or pipes (fixes OOM kills / websocket 1006)
- [x] **Memory admission**: the shim sends `required_mem`; a node refuses with 503 when it
      cannot fit the working set and the shim falls back locally
- [x] Live proof: 5000×5000 float32 matmul, `MATMUL_DONE` correct, zero fallbacks, all 3 labs
      participate (`0a8623b`)

### Remaining Part 2c trimmings
- [x] Torch dispatch live test — 4000×4000 matmul on the 3-node lab, torch 2.12.0
      closure, all 3 labs participated, zero fallbacks (PIPEDPEER_TORCH=1)
- [x] A unit test for the 503 admission path (`TestPoolMapAdmissionRefusesOverMemory`)
- [x] JobSet grouping — records already carry `job_set` (set by mapset.go) and
      `pipedpeer jobs` already prints the column; real grouping stays cosmetic
- [x] Nightly integration is already blocking (`continue-on-error: false`, landed in
      `84d3ff9`) and has been green nightly since Aug 13

---

## 🔜 Immediate next steps (start here)

### 1. Cut the first stable release
- [ ] One more live multi-node matmul + torch run on a clean lab for the record
- [ ] Add the 503-admission unit test, then `go test -race ./...`
- [ ] Promote `dev` → `main`, tag `v0.1.0` (release workflow fires on `v*`, stable channel)
- [ ] Flip nightly integration off `continue-on-error` after ~a week of green

### 2. Part 2b — `pipedpeer map` (scatter/gather) — shipped

The refactor gives you `BuildEnvironment()` (once) + `RunTask()` (per task). Build on it.

- [x] `src/internal/app/mapset.go` — task resolution (`--inputs` glob, `--args-file`,
      `--input` + `--split rows:N|parts:N|parts:auto`) and the fan-out runner `RunMap`
      (semaphore, per-task results dir + `PIPEDPEER_SHARD_ID/NUM_SHARDS`, `--reduce`)
- [x] `src/main.go` `newMapCmd` — builds env once, fans out via one shared coordinator,
      per-task `ExecuteWithRetry` placement, per-task `RunTask` (quiet `StageFmt`)
- [x] `mapset_test.go` — CSV header-preserving shards, line shards, args-file, inputs
      glob, split parsing, unknown-format refusal
- [ ] JobSet grouping in jobhistory / `pipedpeer tasks` (each task is already its own
      `jobhistory` record with `JobName="map"`; real grouping is cosmetic)

**New command** (`src/main.go`, modelled on `newRunCmd`):

```bash
pipedpeer map --script task.py --inputs "shards/*.csv"       # one task per file
pipedpeer map --script sim.py  --args-file params.txt        # one task per line
pipedpeer map --script proc.py --input big.csv --split rows:500000
pipedpeer map ... --reduce merge.py --concurrency 8
```

**Implementation sketch** (new file `src/internal/app/mapset.go`):
1. Resolve the task list from `--inputs` / `--args-file` / `--split`. For `--split`, chunk
   record-oriented files (CSV/JSONL/line-delimited) into shard files; **refuse formats you
   can't split safely** rather than guessing. `parts:auto` ≈ 2–3× the eligible node count
   (ask the coordinator for the candidate count).
2. `BuildEnvironment()` **once**.
3. Run tasks concurrently under a semaphore (`--concurrency`, default ≈ 2× node count).
   Each task: its own `coordinator` placement (reuse `ExecuteWithRetry` — per-task retry and
   re-placement already exist), then `RunTask()` with:
   - `ScriptArgs` = the shard/arg for that task
   - `ResultsDir` = `results/<jobset>/task-<N>/`
   - `Envs` += `PIPEDPEER_SHARD_ID`, `PIPEDPEER_NUM_SHARDS`
4. Persist a JobSet record in `jobhistory`; make `pipedpeer tasks`/dashboard group by JobSet.
5. `--reduce script.py` runs **locally** over the gathered result dirs.

**Watch out:** every task gets its own `jobhistory` record (good), but `RunTask` prints
progress — pass `StageFmt: ""` for fan-out tasks so N tasks don't interleave garbage on the
terminal, and print a per-task summary line instead.

### 3. Part 2d — Supporting fixes (cheap, do alongside 2b)
- [x] Write `Record.PeakMemBytes` after runs so the "historical" estimation tier actually fires
- [x] Persist the daemon's job map + leases (currently lost on restart)
- [x] Content-address NAR/workspace uploads (skip upload if the node already has that hash) —
      this is what makes N-node fan-out fast instead of N× upload

---

## 🗺️ Future roadmap

### Part 2c — Transparent interception (**the headline feature**)

**Completed** — see the ✅ section above for the live multi-node proof. Historical notes:

`pipedpeer python main.py` distributes an **unmodified** script. No SDK — see `handoff.md`
§4/D1 for why, and do not deviate from it. Original feature list (all shipped):

- [x] Ship a `sitecustomize.py` inside the Nix closure, on `PYTHONPATH`, so Python
      auto-imports it before the user's first line
- [x] Patch `multiprocessing.Pool` and `concurrent.futures.ProcessPoolExecutor` →
      cluster executor talking to the local daemon
- [x] Register a `joblib` backend via its official plugin API (covers all of sklearn `n_jobs`)
- [x] Intercept `numpy.matmul`/`dot` above a high size threshold (block partitioning)
- [x] Intercept `torch.matmul`/`mm`/`Tensor.matmul`/`Tensor.mm` above a size threshold
      (block-row partitioning; worker computes on its GPU when available) — opt-in via
      `PIPEDPEER_TORCH=1`, the ML/GPU demo path
- [x] **Never-slower invariant** (`handoff.md` §4/D2): start local, measure, spill only when
      it clearly wins, local cores never stop pulling, speculative re-run for the straggler tail
- [x] **CI benchmark gate**: shim-on vs local-only on a small workload must be within noise
- [x] Adaptive batching: chunk size from measured per-item cost; faster nodes get bigger chunks

Note (shipped): the shim spills chunks to the local daemon's `/v1/pool/map`, which executes the
pickled function via `bin/run`. Warm workers (`497a330`) keep one persistent closure process per
store path, turning dispatch into a pipe write; multi-node spill (`71515b5`, `cb682a4`) splits
chunks across healthy peers that share the closure, local always participating and dead peers
falling back to local; the straggler-tail re-run and adaptive batching landed in `d2dee3f`;
numpy matmul/dot block-row interception via the items_b64 pool path in `e13a593` (opt-in via
PIPEDPEER_NUMPY=1). The CI gate is a `scripts/bench-shim-d2.sh` report job in nightly
(`continue-on-error` semantics): it asserts the shim stays within 3x of a plain local Pool when
no daemon is reachable — a gross-regression tripwire, not a tight wall-clock gate (shared CI is
too noisy for that, and the never-slower invariant itself is enforced structurally by the Go
tests).

Explicitly **out of scope** (say so in the README): Ray-style actors, a distributed object
store (intermediates route through the driver), dynamic/nested task graphs, and synchronised
multi-node training (DDP/allreduce) — nobody, including Ray, does that transparently.

### Part 3 — OCI image support alongside Nix

crun is already the sandbox; what's missing is **images**, not a runtime.

- [ ] `EnvProvider` interface with `NixProvider` (today) and `OCIProvider` (`--image python:3.12-slim`)
- [ ] Pull daemon-less with `go-containerregistry`: node pulls + flattens layers into a rootfs
      cached at `~/.local/share/pipedpeer/images/<digest>` (keeps the zero-daemon design —
      crun stays the only runtime dep, no docker/podman required)
- [ ] Existing OCI bundle switches from "empty rootfs + `/nix` bind" to "image rootfs + `/work` bind";
      GPU device injection and vendor env vars carry over unchanged
- [ ] Upload protocol gains an env descriptor `{type: nix|oci, store_path | image_ref+digest}`
- [ ] Nodes advertise supported backends in heartbeat capabilities; coordinator filters on it
- [ ] Bonus: OCI relaxes the strict arch-match that NAR shipping forces (multi-arch images)
- [ ] Delete the dead `bwrap` module and its doc references

### Part 4 — WebRTC / internet-wide operation

Current LAN couplings: UDP subnet broadcast, nodes advertising private IPv4, direct plain
HTTP to :38080, no NAT traversal, no auth.

- [ ] **Transport abstraction first** — run the existing HTTP+WS protocol over an injectable
      listener/dialer, so LAN (TCP) and internet (pion WebRTC data channel, detached for
      stream semantics) share one protocol
- [ ] **Signaling** — extend the registry service (:38090) into a rendezvous; nodes hold an
      outbound WebSocket and exchange SDP through it (the NATS-KV registry backend already
      exists and is tested, just unwired)
- [ ] **ICE** — public STUN by default; document optional TURN for symmetric NAT
- [ ] **Identity becomes real** — ed25519 keypair replaces the bare UUID; pin the DTLS
      fingerprint to the node key; network ID + join token (`pipedpeer network create|join`).
      **This is where authentication lands**, retroactively fixing the LAN posture too.
- [ ] LAN behaviour stays default; internet mode is opt-in (`--rendezvous <url> --network <id>`).
      The nodestore's source-priority merge already handles registry + discovery coexisting.

### Ongoing / operational
- [ ] Flip nightly integration off `continue-on-error` after ~a week of green
