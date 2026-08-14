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

## 🔜 Immediate next steps (start here)

### 1. Verify and commit the in-flight refactor ⚠️ FIRST THING

`src/internal/app/env.go` (new) and `src/internal/app/run.go` (rewritten) are in the working
tree, **build + vet clean but untested**.

```bash
cd src
go test -count=1 ./...          # if green:
go test -race -count=1 ./...
gofmt -l .                      # must print nothing
```

- **Pass** → commit (suggested message: `Split environment build from task execution`) and go to step 2.
- **Fail** → see `handoff.md` §5 for the three likely causes (most likely: a test calling
  the old `daemonctl.DownloadResults` signature, which now returns `(*ResultManifest, error)`).
  Fix forward; do not revert the split — Part 2b depends on it.

### 2. Part 2b — `pipedpeer map` (scatter/gather)

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

`pipedpeer python main.py` distributes an **unmodified** script. No SDK — see `handoff.md`
§4/D1 for why, and do not deviate from it.

- [x] Ship a `sitecustomize.py` inside the Nix closure, on `PYTHONPATH`, so Python
      auto-imports it before the user's first line
- [x] Patch `multiprocessing.Pool` and `concurrent.futures.ProcessPoolExecutor` →
      cluster executor talking to the local daemon
- [x] Register a `joblib` backend via its official plugin API (covers all of sklearn `n_jobs`)
- [ ] Intercept `numpy.matmul`/`dot` above a high size threshold (block partitioning)
- [ ] **Warm workers:** one persistent worker process per node per JobSet (one lease per node,
      not per task); tasks stream as pickled messages over the existing WS channel. This turns
      dispatch from seconds (job provisioning) into milliseconds.
- [ ] **Never-slower invariant** (`handoff.md` §4/D2): start local, measure, spill only when
      it clearly wins, local cores never stop pulling, speculative re-run for the straggler tail
- [ ] **CI benchmark gate**: shim-on vs local-only on a small workload must be within noise
- [ ] Adaptive batching: chunk size from measured per-item cost; faster nodes get bigger chunks

Note (slice shipped `6e34c08`): the shim currently spills each chunk to the local daemon's
`/v1/pool/map`, which executes the pickled function in the closure via `bin/run`. Local-first
measure-then-spill is in; warm workers, multi-node spill, numpy blocking, the straggler-tail
re-run, adaptive batching and the CI gate are still open.

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
- [ ] Cut the first stable release: promote `dev` → `main`, tag `v0.1.0` (release workflow
      fires on `v*` and publishes to the stable channel)
