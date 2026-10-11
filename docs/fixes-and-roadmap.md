# Fixes and roadmap

This records what was fixed after the report was written (branch
`nightly-fixes-s`, October 2026), how to check each fix, and what comes next.
`known-issues.md` holds the original defect list and marks each entry resolved.

The work is in phases:

| Phase | What | Status |
|---|---|---|
| 0 | Fix the known issues, demo-breaking ones first | Done: `ad75b4b`, `fad3ce4`, `2c1a49a` |
| 1 | Per-stage timeline for every job: where the time goes | Done |
| 2 | Placement explanation, shim receipt and cluster stats in the CLI | Done |
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

### Phase 1: per-stage timeline (done)

Every run now records a timeline in its history record (`stages` in
`metadata.json`). The stages are:

| Stage | Where it runs | What it covers |
|---|---|---|
| placement | submitter | How long the coordinator spent finding a node. Only when the coordinator picked one |
| detect imports, resolve packages, generate flake, nix build, export closure | submitter | Building the environment |
| upload | submitter → node | Sending the workspace, and the closure if the node lacked it |
| seed peers | node | The node giving the closure to its own peers before it answers the upload, so pool work can spill to them |
| closure import | node | Importing the closure into the node's store. Only when it was not there yet |
| sandbox setup | node | Everything between the request and the script starting |
| script | node | The script itself |
| stream + wrap-up | node | What is left of the execution: output streaming and the round trip. Only when it is 50 ms or more |
| sync results | submitter | Bringing changed files back |

Nodes report their share of the run themselves, in the final message of the
execution stream. A node running an older version gets a single `execute`
stage instead.

**Seeing it:**

```bash
pipedpeer run job.py                        # the run ends with a one-line "Timeline: ..."
pipedpeer job --last --timeline             # waterfall for the most recent job
pipedpeer job --id <id> --timeline          # waterfall for any job
```

Example (a cold run on this machine, with 5 peers discovered):

```
STAGE             WHERE        START      TIME
nix build         local         +0ms      3.5s  █████████████
export closure    local        +3.5s       6ms               █
upload            e5b24777     +3.5s       1ms               █
seed peers        e5b24777     +3.5s      6.5s               █████████████████████████
sandbox setup     e5b24777    +10.0s       1ms                                         █
script            e5b24777    +10.0s      97ms                                         █
sync results      local       +10.1s       7ms                                         █
total                                    10.1s
```

The same script run again warm took 0.8 s in total (upload 2 ms, no seeding).

**What it found.** The "10–14 s of fixed overhead" in the run sheet is the Nix
build plus seeding peers, and both are cold-run costs. Seeding is synchronous,
and it happens even for a script that never uses a pool. Skipping it when the
script cannot spill would remove most of a cold run's wait. That changes when
pool work can spill, so it is left as the next optimisation rather than folded
into this phase.

### Phase 2: explain, receipt and stats (done)

**Why a node was chosen.** A run placed by the coordinator (`--remote`,
`pipedpeer python`) keeps the whole decision: every node considered, its
score or the reason it was rejected, and the load figures the score is made
of.

```
$ pipedpeer job --last --explain
Strategy: smart. 5 node(s) considered, 5 eligible.
The job was estimated to need 512Mi (import estimate).

   NODE       HOST           SOURCE      SCORE  CPU%  MEM%  JOBS  FREE CPU   FREE MEM
 → 94bd5f98   fedora         discovery   0.753    26    50     0    8.9/12      7.5Gi
   f631e17c   worker3        discovery   0.712     6    50     0     2.8/3      3.4Gi
   ...
Chosen: score=0.753, source=discovery
```

A run on the submitting machine says no placement was made.

**What the shim distributed, and why.** Every intercepted run's receipt now
comes back with the run, and is stored in its history:
- The daemon points the shim at `<workdir>/.pipedpeer/receipt.json`.
- It returns the receipt in the execution's final message.
- `.pipedpeer/` is never synced into the project.

The receipt counts each item once, by where its kept result came from. Local
cores race slow remote chunks, so some items are computed in both places;
those are reported separately. It lists items per peer and each dispatch. It
also records every cost-model decision with its reason, for `pool.map`,
`numpy.matmul`, `numpy.linalg.*`, `pandas.groupby` and `pandas.merge`.

```
$ pipedpeer job --last --receipt
Items: 200 total
  on other machines       15  ██
  on the running node    185  ███████████████████████████
  (75 computed on both: idle local cores raced the slowest remote chunks)

By peer:
  192.168.0.107:38080        31  ████
  ...
Cost model decisions:
  shipped     pool.map             188 items x 0.038s = 7.17s of work, over the 0.50s worth a round trip
```

A run ends with one `Shim:` line summarising this.

**The cluster at a glance.** `pipedpeer stats` (`--json` for the raw
snapshot) shows:
- every node's cores, load and free memory
- the tasks running on it
- its pool counters: chunks received, chunks from peers, items run, parts sent on, failures
- this machine's recent runs, with total time, slowest stage and how much was distributed

The data comes from `internal/clusterstats`, which the web page in Phase 3
will reuse.

The three views combine: `pipedpeer job --id <id> --timeline --explain --receipt`.

**Fixed along the way.**
- The receipt left out items the local cores computed while racing the
  cluster, so its total fell short of the map: 103 of 200 in a live run.
- The shim's `.pipedpeer/` files (for example its `__pycache__`) could be
  synced back into the user's project.

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
