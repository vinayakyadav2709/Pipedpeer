# Fixes and roadmap

This records the work done after the report was written, on branch
`nightly-fixes-s` in October 2026. It covers what changed, how to test each
part, and what comes next. `known-issues.md` holds the original defect list,
with each entry marked when resolved.

| Phase | What | Status | Commits |
|---|---|---|---|
| 0 | Fix the known issues, demo-breaking ones first | Done | `ad75b4b`, `fad3ce4`, `2c1a49a` |
| 1 | A per-stage timeline for every run: where the time goes | Done | `b30092d` |
| 2 | Why a node was chosen, what the shim distributed, and the cluster at a glance | Done | `6d04e97` |
| 3 | A live web page served by the daemon at `/ui` | Done | see `git log` |
| 4 | Benchmarks, a combined capstone script, and the updated demo flow | Planned | |

Every phase below has the same parts:
- **What changed**
- **Test it automatically**
- **Try it by hand**: commands, and what you should see
- **Limits**

---

## Before testing anything

Build the new binary, and restart the local daemon on it:

```bash
cd ~/Projects/Pipedpeer
make build                       # writes bin/pipedpeer
alias pp="$PWD/bin/pipedpeer"
pp stop; pp start
```

`make install` replaces the installed `pipedpeer` with it.

Every daemon in a cluster needs the new binary for the parts that involve a
node:
- the node's own timings (Phase 1)
- returning the receipt (Phase 2)
- the discovery proof (Phase 0)

A node on an older binary still works. It reports a single `execute` stage and
no receipt.

**The whole test suite:**

```bash
cd src && go test -race ./... ; cd ..
```

Every package passes. The exception is `TestLeaseExpiryRedirectsTask`, which
can fail under the full race suite because of CPU starvation. It passes on its
own, and failed the same way before any of this work (see Phase 0, "Limits").

A scratch folder for the by-hand checks:

```bash
mkdir -p /tmp/pptest && cd /tmp/pptest && touch .pipedpeerignore
```

`.pipedpeerignore` marks the project root, so only this folder is uploaded.

---

## Phase 0: fix the known issues

### What changed

**Fixes that could break a demo** (`ad75b4b`)

| Problem | Fix |
|---|---|
| The build hung forever when the home directory is a git repository. Nix refused the generated `flake.nix` as "not tracked by Git", and the run was retried without end | The flake is built as `path:.`, so Nix never treats the scratch directory as part of an enclosing repository |
| A script that crashed, or an environment that failed to build, was rescheduled forever, rebuilding each time | Both would fail the same way on any node, so the run stops and reports the error. A job killed by a signal (exit code above 128, such as a node shutting down or running out of memory) is still retried elsewhere |
| Daemon start-up deleted job history. The history and the daemon's job directories share `jobs/`, and each pruner removed the other's entries | The daemon removes only UUID-named job directories. The history removes only directories holding a record |
| The cluster's signing keys sometimes could not be recorded: two writers renamed each other's temporary file away | Each write uses its own uniquely named temporary file |
| `--remote` looked for a node once, and could report "no eligible node found" spuriously | It waits up to 15 s for a node, and lists why each one was turned down. `PIPEDPEER_REMOTE_WAIT` changes the wait (`0s` takes one look) |
| A peer with no direct path was only logged, never shown | `pipedpeer nodes` lists it under "No direct path", with the reason |

**Pool, shim and resolver fixes** (`fad3ce4`)

| Problem | Fix |
|---|---|
| A pool request big enough to be split into memory-sized micro-chunks was answered in JSON, which the shim could not parse | It is answered in the format it was asked in, with its receipt |
| Work a weak node forwarded to a peer came back labelled `local`, so receipts credited the origin | The forwarding node relabels it `peer:<address>` |
| One internet peer leaving removed every peer behind a `127.0.0.1` forwarder | Removal matches host and port |
| The pool sized itself by `os.cpu_count()`, so a pinned or CPU-limited job started more workers than it had cores | It uses the CPU affinity and the cgroup `cpu.max` quota |
| `import PIL` mapped to a package nixpkgs does not have (`Pillow`), so the build failed | Mapped to `pillow`. Added import names that differ from package names (`cv2`, `bs4`, `skimage`), and common packages: tqdm, seaborn, statsmodels, xgboost, lightgbm, polars, pyarrow, numba, sympy, networkx, plotly, h5py, tabulate, rich |
| An import with no known package was silently dropped, and the script failed later on the node | The build prints a warning naming it, and suggests `--pkg` |
| Three tests failed, rather than skipped, without torch | They skip |
| Data races in the `nattype` and `portmap` tests | Fixed |

**Security and setup fixes** (`2c1a49a`)

| Problem | Fix |
|---|---|
| LAN discovery sent the cluster secret to every address on the subnet | Probes send an HMAC proof bound to the probed address and the time. It opens `GET /health` only, for two minutes |
| Firewall changes during setup ran without `sudo`, and failed for a normal user | Changes go through `sudo`; checks stay unprivileged |
| Setup aborted on a fresh machine because no peer keys were known yet | Setup trusts this node's own key and records it. Peers' keys are added on a later run |
| The punch nonce was identical every time (a zero-seeded generator) | Drawn from `crypto/rand` |
| Partial closures were written to `$TMPDIR`, often a RAM-backed tmpfs | Written to the on-disk scratch directory |
| With a private Nix store, the daemon checked the host's `/nix/store` and concluded closures were missing | `/nix` paths are looked up inside the private store |
| `SECURITY.md` still said there was no authentication | Rewritten for the current design and its limits |

### Test it automatically

```bash
cd src && go test -count=1 -run 'TestPrune|TestIsPermanent|TestExecuteWithRetryStopsOnPermanent|TestFindNodeWithin|TestUnreachable|TestTrustedKeysSurvive|TestPoolMapMicroChunksAnswerInFrames|TestRelabelForwarded|TestRemoveManualAt|TestPoolWidthFollowsCPUPinning|TestUnresolvedImports|TestMappedNamesAreLowercase|TestProbeProves|TestProofIsBound|TestFirewallChangesGoThroughSudo|TestSetupWithNoKeysYet|TestHostPath|TestRemoteRunFails' ./... ; cd ..
```

### Try it by hand

Run these in `/tmp/pptest`.

**1. A crashing script stops instead of retrying forever.**

```bash
printf 'print("start")\n1/0\n' > bad.py
pp run bad.py --remote
```

Expect one `ZeroDivisionError`, then the command exits. There is no
`Rescheduling (attempt 2)`.

**2. Unknown imports are named, and PIL builds.**

```bash
printf 'import foobarbaz\n' > unknown.py
pp run unknown.py 2>&1 | grep -A1 Warning
printf 'from PIL import Image\nprint("PIL OK")\n' > pil.py
pp run pil.py
```

Expect `Warning: no Nix package known for: foobarbaz`, then `PIL OK`.

**3. Building with the cache inside a git repository.**

```bash
rm -rf /tmp/gitcache && mkdir /tmp/gitcache && git -C /tmp/gitcache init -q
printf 'print("built fine")\n' > ok.py
XDG_CACHE_HOME=/tmp/gitcache/cache pp run ok.py
```

Expect `built fine`. Before the fix: "flake.nix … is not tracked by Git",
retried forever.

**4. Job history survives a daemon restart.**

```bash
ID=$(ls ~/.local/share/pipedpeer/jobs | grep -E '^[0-9]+$' | head -1)
touch -d '10 days ago' ~/.local/share/pipedpeer/jobs/$ID
pp stop && pp start && sleep 5
ls -d ~/.local/share/pipedpeer/jobs/$ID && echo "history kept"
```

**5. `--remote` waits for peers.**

```bash
pp stop && pp start && pp run ok.py --remote --check-only
```

Expect either "waiting up to 15s…" followed by `Would place on node …`, or a
list of why each node was turned down.

**6. Discovery doesn't send the secret.** Every machine needs the new binary
for this one.

```bash
pp auth set
curl -s -o /dev/null -w '%{http_code}\n' localhost:38080/health   # 401
pp nodes                                                          # peers still found
```

**7. Pool sizing follows pinning.** On the docker bench rig (3 cores per
worker), a pool job's log reads `using 3 workers`, not one per host core.

### Limits

- **`imap_unordered` returns results in order.** "Unordered" allows any order,
  input order included. The cost is that a slow item holds back its batch.
- **numpy's `@` is not intercepted.** Python refuses to patch operators on
  `numpy.ndarray`, which is an immutable C type. Write `np.matmul(a, b)` or
  `a.dot(b)` for a product that should be distributed.
- **A forwarded pool request can fan out again.** This is intended: the origin
  forwarded the work because it lacked memory, and the peer spreading it is how
  it gets done.
- **`TestLeaseExpiryRedirectsTask` can fail under `go test -race ./...`.** It
  passes alone and as a package, before and after this work. Under the load of
  the whole race suite its heartbeats are starved.
- **Known issue 3 (closures not reaching peers when a job runs on the
  submitting host) still needs a live check.** The original log shows the
  cost model keeping all 80 items local, which is a different cause. Since
  Phase 2, `--receipt` on such a run says which it is.

---

## Phase 1: where a run's time goes

### What changed

Every run now records a timeline in its history (`stages` in `metadata.json`):

| Stage | Where it runs | What it covers |
|---|---|---|
| placement | this machine | The coordinator finding a node. Only when the coordinator picked one |
| detect imports, resolve packages, generate flake, nix build, export closure | this machine | Building the environment |
| upload | this machine → node | Sending the workspace, and the closure if the node lacked it |
| seed peers | node | The node giving the closure to its own peers before it answers, so pool work can spill to them |
| closure import | node | Importing the closure into the node's store. Only when it was not there yet |
| sandbox setup | node | From the request to the script starting |
| script | node | The script itself |
| stream + wrap-up | node | The rest of the execution: output streaming and the round trip. Only when it is 50 ms or more |
| sync results | this machine | Bringing changed files back |

Nodes measure their own stages. They send the measurements in the
execution's final message, and the seeding time in the upload's answer.

Where it shows:
- Every run now ends with a one-line `Timeline:` summary.
- `pipedpeer job --timeline` draws the timeline as bars on a shared time axis.
- `pipedpeer job --last` picks the most recent run, so no ID needs copying.

### Test it automatically

```bash
cd src && go test -count=1 -run 'TestTimelineSummaryTotalAndWaterfall|TestWaterfallWithNoStages|TestBeginEnd|TestExecStagesSplitTheNodesAccount|TestExecutionReportsTiming' ./... ; cd ..
```

`TestExecutionReportsTiming` runs a real daemon in the test. It uploads a
script that sleeps 300 ms, executes it, and checks that the node reported at
least 300 ms of script time.

### Try it by hand

```bash
cd /tmp/pptest
printf 'import time\nt=time.time()\nsum(i*i for i in range(3_000_000))\nprint("done in %%.2fs" %% (time.time()-t))\n' > job.py
pp run job.py
pp job --last --timeline
```

The run ends with a line such as:

```
Timeline: detect imports 0ms · … · nix build 520ms · export closure 5ms · upload 5.3s · sandbox setup 0ms · script 254ms · sync results 8ms (total 6.1s)
```

and `job --last --timeline` draws it:

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

**To see cold against warm, run the same script twice.** The second run skips
seeding, and its build is a cache hit:

| | Nix build | Seed peers | Script | Total |
|---|---|---|---|---|
| Cold (new closure, 5 peers) | 3.5 s | 6.5 s | 0.1 s | 10.1 s |
| Warm (same script again) | 0.45 s | – | 0.3 s | 0.8 s |

A cold run needs a closure the peers have never seen. Import a package not
used before, such as `from tabulate import tabulate`.

### Limits

- **Seeding is the largest cold-run cost.** It is synchronous, and it happens
  even for a script that never uses a pool. Skipping it when the script cannot
  spill would remove most of a cold run's wait. That changes when pool work can
  spill, so it is left as the next optimisation.
- **A node on an older binary shows one `execute` stage** instead of setup,
  import and script.
- **`placement` is recorded only when the coordinator chose the node** (with
  `--remote`, or `pipedpeer python`).

---

## Phase 2: why that node, what was distributed, and the cluster

### What changed

**`pipedpeer job --explain`: why a node was chosen.** A run placed by the
coordinator keeps the whole decision:
- every node considered
- its score, or the reason it was rejected
- the load figures the score is made of: CPU, memory, running jobs, free cores and free memory

**`pipedpeer job --receipt`: what the shim distributed, and why.** For every
intercepted run, the daemon points the shim's receipt at
`<workdir>/.pipedpeer/receipt.json`. It returns the receipt with the run, and
the run's history keeps it. The receipt shows:
- how many items' results came from other machines, and how many from the
  running node, counting each item once
- how many items were computed on both sides, because local cores raced slow
  remote chunks
- items per peer, and each dispatch with its time
- every cost-model decision with its reason, for `pool.map`, `numpy.matmul`,
  `numpy.linalg.*`, `pandas.groupby` and `pandas.merge`

A run ends with a one-line `Shim:` summary of it.

**`pipedpeer stats`: the cluster at a glance.** It shows:
- every node's cores, CPU use, free memory and running tasks
- each node's pool counters: chunks received, chunks from peers, items run,
  parts sent on to peers, failures
- this machine's recent runs, with total time, slowest stage and how much was
  distributed

`--json` prints the same snapshot as data. It is built by
`internal/clusterstats`, which the Phase 3 web page reuses.

**Fixed along the way:**
- The receipt left out items the local cores computed while racing the
  cluster. A live 200-item map showed 103.
- It then counted items computed on both sides twice, and showed 230.
- The shim's own `.pipedpeer/` files (its `__pycache__`, the receipt) could be
  synced back into the user's project. That directory is now excluded.

### Test it automatically

```bash
cd src && go test -count=1 -run 'TestExplanation|TestExplainCandidatesMarksTheWinner|TestReceipt|TestEmptyReceipt|TestCollectAsksEachNode' ./... ; cd ..
```

`TestReceiptReturnsWithTheRunNotTheResults` runs a real daemon. It checks that
the receipt comes back with the run, and that nothing under `.pipedpeer/` is
synced into the project.

`TestReceiptRecordsWhyWorkShippedOrStayed` runs the real shim against a fake
cluster. It checks three things:
- A worthwhile map records a "shipped" decision with its reason.
- A tiny map records "kept local".
- Every item is accounted for.

### Try it by hand

A pool script whose work is worth spreading:

```bash
cd /tmp/pptest
cat > pool.py <<'EOF'
import multiprocessing, time
def work(x):
    s = 0
    for i in range(200_000):
        s += i * x
    return s
if __name__ == "__main__":
    t = time.time()
    with multiprocessing.Pool(4) as p:
        r = p.map(work, range(200))
    print("POOL", len(r), "in %.2fs" % (time.time() - t))
EOF
pp run pool.py
pp job --last --receipt
```

The run ends with a line such as
`Shim: 15 of 200 items ran on other machines · 5 peer(s) · 1 shipped / 0 kept local`.
The receipt then shows:

```
Items: 200 total
  on other machines       15  ██
  on the running node    185  ███████████████████████████
  (75 computed on both: idle local cores raced the slowest remote chunks)

By peer:
  192.168.0.107:38080        31  ████
  172.21.0.3:38080           18  ██
  ...
Cost model decisions:
  shipped     pool.map             188 items x 0.038s = 7.17s of work, over the 0.50s worth a round trip
```

A run placed by the coordinator, explained:

```bash
pp run job.py --remote
pp job --last --explain
```

```
Strategy: smart. 5 node(s) considered, 5 eligible.
The job was estimated to need 512Mi (import estimate).

   NODE       HOST           SOURCE      SCORE  CPU%  MEM%  JOBS  FREE CPU   FREE MEM
 → 94bd5f98   fedora         discovery   0.753    26    50     0    8.9/12      7.5Gi
   f631e17c   worker3        discovery   0.712     6    50     0     2.8/3      3.4Gi
   ...
Chosen: score=0.753, source=discovery
```

The cluster:

```bash
pp stats
pp stats --json | head -40
```

```
Cluster: 6 node(s), 6 healthy · 36 cores · 28 GiB free memory · 2 GPU node(s) · 0 task(s) running

NODE       HOST           STATE       CORES  CPU%   MEM FREE JOBS     CHUNKS FROM PEER ITEMS RUN TO PEERS FAILED
e5b24777   fedora         healthy*       12     6    7.5 GiB    0          6        0         0       29      0
4ea2071a   worker1        healthy         3     6    3.4 GiB    0         61       61        94        0      0
...
Recent runs from this machine:
ID                   SCRIPT             STATUS     NODE          TOTAL  SLOWEST STAGE          DISTRIBUTED
1791725631738480222  pool.py            succeeded  e5b24777       2.3s  seed peers 1s          15/200 items remote
```

All three views at once: `pp job --last --timeline --explain --receipt`.

### Limits

- **`--explain` covers runs the coordinator placed.** A plain `pipedpeer run`
  executes on this machine, and says that no placement was made. Runs from
  `pipedpeer map` are not recorded yet.
- **On a fast orchestrator, local cores win most races.** In the run above,
  only 15 of 200 results came from elsewhere, though 90 items were run
  remotely. The receipt shows this honestly. Spreading pays more with a weaker
  orchestrator, or with heavier items.
- **Pool counters in `stats` count since each daemon started**, not per run.

---

## Phase 3: the live web page

### What changed

Every daemon now serves a page at **`http://<machine>:38080/ui`**. It needs
no extra service, no build step and no internet access: the page is one file
compiled into the binary.

It refreshes every two seconds and shows:
- **Summary tiles:** healthy nodes, cores, free memory, GPU nodes, tasks running.
- **Nodes:** each node's state (a coloured dot plus a word), cores, a CPU meter, free memory, running tasks and pool counters. Hovering a meter shows CPU and memory. An unreachable node shows its reason.
- **Recent runs from this machine:** when it started, status, node, total time, slowest stage, and a small bar of how much ran elsewhere.
- **The selected run:**
  - **Where the time went:** the Phase 1 timeline as bars on a time axis. Blue is this machine, orange is the node that ran it. Hover any bar for its start and duration.
  - **Where the work ran:** the Phase 2 receipt. The headline is "N of M items ran on other machines", with a split bar, items per peer, and the cost model's decisions with their reasons.
  - **Why this node:** the Phase 2 placement table, or a plain sentence when no choice was made.

Clicking a run selects it, and the address becomes `/ui#run=<id>`, so a run
can be linked to directly. The page has a pause button and a light/dark toggle
(it follows the system theme otherwise), and it works at phone width.

**The data.** The page reads one endpoint, `GET /v1/stats`. It returns the
same snapshot as `pipedpeer stats --json`, from the same `clusterstats`
package, and is cached for one second so several open tabs don't multiply the
calls to every node.

**With a shared secret set:**
- The page itself loads without the secret, because a browser cannot attach a header to the address-bar request. The page holds no data.
- `/v1/stats` and every other route still require the secret.
- The page asks for the secret once, keeps it in that browser only, and sends it with each request.
- A Content-Security-Policy confines the page to its own daemon.

**`pipedpeer ui`** prints the page's address, both for this machine and from
elsewhere on the network, and opens it in a browser. `--print` only prints.

Screenshots: `docs/screenshots/ui-light.png`, `ui-dark.png` and `ui-phone.png`.

### Test it automatically

```bash
cd src && go test -count=1 -run 'TestUIPage' -v ./internal/daemonapi/ ; cd ..
```

These tests check four things:
- `/ui` loads without the secret when one is set.
- `/v1/stats`, `/v1/nodes` and a path-traversal attempt (`/ui/../v1/stats`) are refused without it.
- `/v1/stats` with the secret returns a snapshot.
- The page references no outside resource.

### Try it by hand

```bash
pp stop; pp start            # the daemon must be running the new binary
pp run pool.py               # something to show (see Phase 2)
pp ui                        # opens http://127.0.0.1:38080/ui
```

What to check on the page:
1. The tiles and the nodes table match `pp stats`.
2. The newest run is selected. Its timeline matches `pp job --last --timeline`,
   and "Where the work ran" matches `pp job --last --receipt`.
3. Start another run (`pp run pool.py`) while the page is open. It appears at
   the top within two seconds, as `running · executing`, then `succeeded`.
4. Click an older run. The address changes to `#run=<id>`, and reloading keeps
   it selected.
5. Open the page from another machine, using the "From elsewhere" address
   printed by `pp ui`.
6. Set a secret (`pp auth set`, then restart the daemon) and reload. The page
   asks for the secret; paste the one `pp auth show` prints. Check that
   `curl localhost:38080/v1/stats` returns 401.
7. Narrow the window to phone width. The tables scroll sideways, and the
   timeline still draws.

Without a browser:

```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:38080/ui      # 200
curl -s localhost:38080/v1/stats | python3 -m json.tool | head -30
```

### Limits

- **"Recent runs" are the runs submitted from the machine whose page you
  opened.** Each machine keeps its own history. Open the page on the
  submitting machine to see its runs.
- **The page polls; it does not stream.** A stage shorter than two seconds
  may pass between refreshes, but the finished run's timeline shows every
  stage.
- **The page is read-only.** It cannot start or stop runs.

## Phase 4: benchmarks and the demo (planned)

- **A committed benchmark driver.** It produces one CSV and the charts, running
  every demo script locally, cold and warm, on one, two and three workers.
- **A capstone script** that chains the earlier ones: pandas out-of-core →
  numpy features → scikit-learn → a pool of lambdas → result files synced back.
- **An updated run sheet** with real per-step timings, a step for the capstone
  and one for the web page, and the timeline and receipt in the proof step.
