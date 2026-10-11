# Fixes and roadmap

This records what was fixed after the report was written (branch
`nightly-fixes-s`, October 2026), how to check each fix, and what comes next.
`known-issues.md` holds the original defect list and marks each entry resolved.

The work is in phases:

| Phase | What | Status |
|---|---|---|
| 0 | Fix the known issues, demo-breaking ones first | Done: `ad75b4b`, `fad3ce4`, `2c1a49a` |
| 1 | Per-stage timeline for every job: where the time goes | In progress |
| 2 | Placement explanation, shim receipt and cluster stats in the CLI | Planned |
| 3 | A web page served by the daemon (`/ui`) showing the same data live | Planned |
| 4 | Benchmarks (local, cold, warm; 1–3 workers), a combined capstone script, and an updated demo flow | Planned |

## Phase 0: what was fixed

### Fixes that could break a demo (`ad75b4b`)

| Problem | Fix |
|---|---|
| The build hung forever when the home directory is a git repository: Nix refused the generated `flake.nix` as "not tracked by Git", and the run was retried without end | The flake is built as `path:.`, so Nix never treats the scratch directory as part of an enclosing repository |
| A script that crashed, or an environment that failed to build, was rescheduled forever, rebuilding each time | Both fail the same way on any node, so the run stops and reports the error. A job killed by a signal (exit code above 128, such as a node shutting down or running out of memory) is still retried elsewhere |
| Daemon start-up deleted job history: the history and the daemon's job directories share `jobs/`, and each pruner removed the other's entries | The daemon removes only UUID-named job directories. The history removes only directories holding a record |
| The cluster's signing keys sometimes could not be recorded: two writers renamed each other's temporary file away | Each write uses its own uniquely named temporary file |
| `--remote` looked for a node once and could report "no eligible node found" spuriously | It waits up to 15 s for a node (`PIPEDPEER_REMOTE_WAIT`, e.g. `0s` for one look), and lists why each node was turned down |
| A peer with no direct path was only logged, and never shown | `pipedpeer nodes` lists it under "No direct path", with the reason |

### Pool, shim and resolver fixes (`fad3ce4`)

| Problem | Fix |
|---|---|
| A pool request big enough to be split into memory-sized micro-chunks was answered in JSON, which the shim could not parse | It is answered in the format it was asked in, with its receipt |
| Work a weak node forwarded to a peer came back labelled `local`, so receipts credited the origin | The forwarding node relabels it `peer:<address>` |
| One internet peer leaving removed every peer behind a `127.0.0.1` forwarder | Removal matches host and port |
| The pool sized itself by `os.cpu_count()`, so a pinned or CPU-limited job started more workers than it had cores | It uses the CPU affinity and the cgroup `cpu.max` quota |
| `import PIL` mapped to a package nixpkgs does not have (`Pillow`), so the build failed | Mapped to `pillow`. Import names that differ from package names (`cv2`, `bs4`, `skimage`) and common packages (tqdm, seaborn, statsmodels, xgboost, lightgbm, polars, pyarrow, numba, sympy, networkx, plotly, h5py, tabulate, rich) were added |
| An import with no known package was silently dropped, and the script failed later on the node | The build prints a warning naming it, and suggests `--pkg` |
| Three tests failed, rather than skipped, without torch | They skip |
| Data races in the `nattype` and `portmap` tests | Fixed; `go test -race ./...` is clean |

### Security and setup fixes (`2c1a49a`)

| Problem | Fix |
|---|---|
| LAN discovery sent the cluster secret to every address on the subnet | Probes send an HMAC proof bound to the probed address and the time. The proof opens `GET /health` only, for two minutes |
| Firewall changes during setup ran without `sudo` and failed for a normal user | Changes go through `sudo`; checks stay unprivileged |
| Setup aborted on a fresh machine because no peer keys were known yet | Setup trusts this node's own key and records it. Peers' keys are added on a later run |
| The punch nonce was identical every time (zero-seeded generator) | Drawn from `crypto/rand` |
| Partial closures were written to `$TMPDIR`, often a RAM-backed tmpfs | Written to the on-disk scratch directory |
| With a private Nix store, the daemon checked the host's `/nix/store` and concluded closures were missing | `/nix` paths are looked up inside the private store |
| `SECURITY.md` still said there was no authentication | Rewritten for the current design and its limits |

### Investigated, not changed

- **`imap_unordered` returns results in order.** "Unordered" allows any order,
  input order included. The cost is that a slow item holds back its batch.
- **numpy's `@` is not intercepted.** Python refuses to patch operators on
  `numpy.ndarray` (an immutable C type). Use `np.matmul(a, b)` or `a.dot(b)` for
  a product that should be distributed.
- **A forwarded pool request can fan out again.** This is intended: the origin
  forwarded because it lacked memory, and the peer spreading the work is how
  it gets done.
- **`TestLeaseExpiryRedirectsTask` can fail under `go test -race ./...`.** It
  passes alone and as a package, before and after these changes. Under the
  load of the whole race suite its heartbeats are starved.
- **Closures not reaching peers when a job runs on the submitting host** (known
  issue 3). This still needs checking on a live cluster. The original log shows
  the shim's cost model keeping all 80 items local, which is a different cause
  from a missing closure.

## How to check the fixes

Build first. Every command below uses the new binary.

```bash
make build
alias pp="$PWD/bin/pipedpeer"
pp stop; pp start
```

**Automated: every fix has a regression test.**

```bash
cd src && go test -race ./... ; cd ..
```

**By hand:**

```bash
mkdir -p /tmp/pptest && cd /tmp/pptest

# A crashing script stops instead of looping: one traceback, no "Rescheduling".
printf 'print("start")\n1/0\n' > bad.py
pp run bad.py --remote

# Unknown imports are named; PIL builds.
printf 'import foobarbaz\n' > unknown.py
pp run unknown.py 2>&1 | grep -A1 Warning
printf 'from PIL import Image\nprint("PIL OK")\n' > pil.py
pp run pil.py

# Building with the cache inside a git repository.
rm -rf /tmp/gitcache && mkdir /tmp/gitcache && git -C /tmp/gitcache init -q
printf 'print("built fine")\n' > ok.py
XDG_CACHE_HOME=/tmp/gitcache/cache pp run ok.py

# History survives a daemon restart.
ID=$(ls ~/.local/share/pipedpeer/jobs | grep -E '^[0-9]+$' | head -1)
touch -d '10 days ago' ~/.local/share/pipedpeer/jobs/$ID
pp stop && pp start && sleep 5
ls -d ~/.local/share/pipedpeer/jobs/$ID && echo "history kept"

# --remote waits for peers, or says why each node was turned down.
pp stop && pp start && pp run ok.py --remote --check-only

# Discovery with a secret set: /health refuses a request without it, peers
# are still found. Needs the new binary on every machine.
pp auth set
curl -s -o /dev/null -w '%{http_code}\n' localhost:38080/health   # 401
pp nodes
```

The pool sizing fix shows on the docker bench rig, where each worker has three
cores. A pool job there logs `using 3 workers`, not one per host core.

## Next phases

### Phase 1: per-stage timeline

- Every job records when each stage started and how long it took:
  1. detecting imports
  2. resolving packages
  3. generating the flake
  4. the Nix build
  5. exporting the closure
  6. uploading
  7. execution
  8. syncing results back
- `pipedpeer job --id <id> --timeline` prints them as a waterfall.
- This shows where the 10–14 s of fixed overhead per run goes, and gives the
  cold versus warm comparison for the demo.

### Phase 2: explain and receipt views

- `pipedpeer job --id <id> --explain` shows every candidate node, its score or
  the reason it was rejected, and why the chosen one won.
- `pipedpeer job --id <id> --receipt` shows the shim's receipt: items run
  remotely and locally, parts per node and their time, and the cost model's
  decisions to ship or keep.
- `pipedpeer stats` summarises the cluster: nodes, load and pool counters.

### Phase 3: web page

- `http://<node>:38080/ui` is a page embedded in the daemon. It needs no extra
  service.
- It shows nodes, running tasks, each job's timeline and its receipt, and
  refreshes every couple of seconds.
- It reads the same data as the CLI, from one `/v1/stats` endpoint.

### Phase 4: benchmarks and the demo

- A committed benchmark driver produces one CSV and the charts. It runs every
  demo script locally, cold and warm, on one, two and three workers.
- A capstone script chains the earlier ones: pandas out-of-core → numpy features
  → scikit-learn → a pool of lambdas → result files synced back.
- The run sheet gets real per-step timings, a step for the capstone and the web
  page, and the timeline and receipt in the proof step.
