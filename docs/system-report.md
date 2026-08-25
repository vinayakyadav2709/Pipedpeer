# PipedPeer: system report

Source material for the major project report and the conference paper. Written
against commit `b774de8` on `dev`, 2026-08-25. Every non-obvious claim carries a
file reference; every number in Chapter 5 traces to a file in
`docs/benchmarks/raw/`.

**Read this first.** The previous report (`Major_Project-4-1.pdf`, May 2026)
describes a system that no longer exists. Carrying its text forward would put
false claims in front of an examiner. The differences are set out in Appendix B.

---

## Front matter

| | |
|---|---|
| Title | PipedPeer: Zero-Configuration Distributed Computing via Recursive, Decentralised Coordination |
| Authors | Yateen Vaviya (2023300251), Vinayak Yadav (2023300264), Sahil Gupta (2023300072) |
| Guide | Prof. Rupali Sawant |
| Department | Computer Engineering |
| Institute | Bharatiya Vidya Bhavan's Sardar Patel Institute of Technology, Munshi Nagar, Andheri-West, Mumbai 400058 |
| University | University of Mumbai |
| Year | 2026-2027 |

**Keywords.** Distributed computing, peer-to-peer, transparent interception,
Nix, content-addressed caching, OCI sandboxing, cost-based scheduling, Python.

---

## Chapter 1  Introduction

### 1.1 Problem statement

A developer with a laptop, a lab workstation and a spare desktop has plenty of
idle compute and no practical way to use it. The frameworks that exist for this
(Spark, Ray, Dask) all assume a cluster that somebody has already configured:
a head node, matching software on every worker, a scheduler process, and a
trusted network. Setting that up costs more than most jobs are worth. The
alternative, `ssh` plus `rsync`, means reproducing the environment by hand on
every machine and getting no scheduling, no isolation and no fault tolerance.

Two problems sit underneath this, and they are usually solved separately:

1. **The environment problem.** A script needs an interpreter and a specific set
   of libraries. Making the remote machine match the local one is the bulk of
   the work, and it is why "just ssh into it" does not scale past one machine.
2. **The code problem.** Even with a matching environment, using more than one
   machine normally means rewriting the program against a cluster API.

PipedPeer addresses both without asking the user to do anything. The
environment is reproduced exactly by shipping a Nix closure, and the program is
distributed without modification by intercepting the parallel primitives it
already uses.

### 1.2 Objectives

1. Run an unmodified Python script on the best available machine on the network,
   with results appearing in the user's working directory as if it had run
   locally.
2. Reproduce the script's environment exactly on that machine, and pay for the
   transfer at most once per environment per cluster.
3. Choose the machine from live capacity: architecture and GPU as hard
   requirements, then load, memory and hardware as a ranking.
4. Distribute the parallel work inside the script across the cluster with no
   code changes, and only when doing so is actually faster.
5. Isolate the job, and survive the loss of any node without losing the run.
6. Require nothing on a new machine beyond one binary.

### 1.3 Scope

In scope: the daemon, the CLI, peer discovery, placement and leasing, closure
build and transfer, sandboxed execution, result sync, the interception shim
(`multiprocessing`, pandas, numpy, joblib and scikit-learn, torch data
parallelism), GPU-aware placement, and a live TUI.

Out of scope in the current implementation: authentication and encryption (see
6.3), runtimes other than Python, discovery beyond the LAN without a registry,
and any incentive mechanism.

### 1.4 Technologies

| Technology | Role |
|---|---|
| Go 1.25 | Daemon, CLI, coordinator, all in one static binary |
| `go-chi/chi` | HTTP routing for the daemon API |
| `gorilla/websocket` | Streaming a job's stdout and stderr back live |
| Nix (pinned nixpkgs) | Reproducible environments, content-addressed |
| `crun` | OCI sandbox for each job |
| `modernc.org/sqlite` | Node store; pure Go, so the binary stays static |
| `gopsutil` | CPU, memory and process metrics |
| Cobra and Viper | CLI structure and configuration |
| Bubble Tea, Lip Gloss | Dashboard TUI |
| NATS (embedded) | Optional alternate transport for lease messages |
| `zerolog` | Structured logging |
| Python (embedded shim) | The interception layer, 1,900 lines shipped inside the binary |

### 1.5 Assumptions and constraints

- Every participating machine runs the same binary. Nothing else is installed.
- Workers must run Linux; `crun` and the Nix store are Linux-specific. The
  submitting side builds on macOS as a client.
- The network is trusted. There is no authentication, authorisation or transport
  encryption; this is a deliberate scope decision, documented in `SECURITY.md`
  and restated in 6.3.
- Nix must be present on a worker to receive a closure. `pipedpeer setup`
  installs it.
- The submitting process must stay alive for the duration of a job: it holds the
  lease and receives the stream.

---

## Chapter 2  Literature survey

The survey covers six areas: sandboxing and environment portability at the edge,
messaging and actor-style fault tolerance, failure-transparent programming
models, Python portability, scheduling, and collaborative offloading. Sixteen
papers are reviewed in the report proper.

**What must change from the previous report.** Most of its "Relevance to
PipedPeer" paragraphs justify design decisions the system has since reversed:
they argue for bubblewrap (replaced by crun), for NATS push dispatch (replaced
by HTTP and WebSocket), and for a durable JetStream task queue (the package was
deleted). Paper 13 in particular is cited as motivating partial offloading "as a
direct future extension"; partial offloading is now the system's headline
feature. Every relevance paragraph is rewritten in the report.

**Systems the comparison actually needs.** Spark, Ray, Dask, BOINC, MapReduce
and Nix are cited directly in Chapter 5's comparison and were absent from the
survey's framing.

---

## Chapter 3  Analysis

### 3.1 Architecture

Every machine runs one long-lived **daemon**. The CLI is short-lived and holds
no peer-to-peer logic of its own; notably the **coordinator is a library linked
into the CLI**, not a service, so there is no scheduler process and no head
node anywhere in the system. A machine is a submitter and a worker at the same
time, and the same binary does both.

See `fig_3.1_system_design.png`.

The subsystems, all under `src/internal/`:

| Package | Responsibility |
|---|---|
| `coordinator` | Filter, score and choose a node; take a lease; retry |
| `daemonapi` | HTTP and WebSocket surface, leases, sandbox, pool spill, DDP sync |
| `discovery` | UDP multicast and broadcast, TCP `/health` sweep fallback |
| `nodestore` | SQLite table of known peers and their last observed capacity |
| `nixgen` | Flake generation, and the interception shim as an embedded asset |
| `pythondeps` | Import scanning and import-to-package mapping |
| `resourceest` | Four-tier memory estimate |
| `jobhistory` | Per-job record, stage, result manifest |
| `heartbeat` | Capability and load reporting |
| `gpu` | Vendor detection and per-device telemetry |
| `registry` | Optional standalone directory for cross-subnet discovery |
| `identity`, `daemonctl`, `natsbus`, `setup`, `logging`, `ping`, `app` | Supporting |

### 3.2 Data flow

Level 0 (`fig_3.2_dfd_level0.png`) has three external entities: the user, peer
nodes, and an optional registry.

Level 1 (`fig_3.3_dfd_level1.png`) decomposes the daemon into seven processes
and three stores: resolve environment, discover peers, place task, transfer,
execute, distribute parallel work, return results; over the node store, the
closure cache and job history.

### 3.3 Use cases

`fig_3.4_use_case.png`. A user runs a script, asks for a GPU or a memory floor,
trains with data parallelism, and inspects jobs and nodes. An operator installs
prerequisites and manages the daemon. A peer daemon accepts and leases work,
serves closures to other peers, and executes parallel chunks.

### 3.4 Sequence

`fig_3.5_sequence.png`. The full path for one job:

1. The CLI scans the script's imports and builds a Nix closure locally.
2. It estimates the job's memory requirement.
3. It asks its own daemon for the healthy peer list.
4. The coordinator filters and scores, then sends `accept` to the best node.
5. On acceptance it sends `commit`, which reserves capacity atomically.
6. It uploads the workspace, and the closure only if the worker lacks it.
7. The worker broadcasts the closure to peers that lack it, so they become
   eligible to receive spilled work.
8. Execution runs in a `crun` sandbox; stdout and stderr stream back per line
   over a WebSocket.
9. The shim inside the job distributes parallel work if the cost model agrees.
10. On exit, changed files and a deletion manifest come back as a tar.

---

## Chapter 4  Design and methodology

### 4.1 Node discovery

Three sources feed one table, and none of them blocks the others.

**UDP, not mDNS.** `discovery/discovery.go` opens two sockets on port **38099**,
one joined to multicast group **239.255.0.99** and one bound for broadcast, both
with `SO_REUSEADDR` and `SO_REUSEPORT`. The probe is the literal string
`PIPEDPEER_DISC_V1` and the reply is that header followed by a JSON descriptor.
The multicast socket exists for a specific reason recorded in the source: a UDP
broadcast to a shared port is delivered by the kernel to only one socket, so two
daemons on the same host would never see each other. Multicast with `REUSEPORT`
reaches all of them.

The dashboard still labels this source "mDNS", and the previous report called it
mDNS throughout. It is not mDNS: there is no DNS-SD, no service record and no
conflict resolution.

**TCP fallback.** Access points with client isolation drop broadcast without
error, so `discovery/tcp.go` sweeps each local subnet, 64 probes at a time, with
a 700 ms `GET /health` per address. It runs only when the UDP probe returns
nothing.

**Node store.** `nodestore/store.go` keeps peers in SQLite. Its upsert preserves
fields rather than overwriting them: a discovery packet knows only an address, so
it must not erase the capabilities the health poller wrote. A peer poller runs
every 10 s. Two thresholds matter, and they are different on purpose:

- `staleAfter = 45 * time.Second` (`daemonapi/server.go:834`), four missed
  rounds, after which `/v1/nodes` stops reporting a node as healthy. This is the
  death filter that everything else reads.
- `staleNodeTTL = 5 * time.Minute` (`daemonapi/server.go:1061`), after which an
  auto-discovered row is deleted outright. Manually added peers are never
  deleted by this path.

### 4.2 Placement and scoring

Candidates are merged from the node store, live discovery, the optional registry
and self, deduplicated by node ID with a source priority. Then five hard filters
run in order (`fig_4.1_placement_flow.png`): architecture must match the closure;
a GPU must exist if one was required; and free memory, free cores and free VRAM
must each meet the request. Each rejection records a reason, which surfaces in
`pipedpeer job`.

Survivors are scored. This is the report's central equation, quoted verbatim
from `coordinator.scoreNode`:

```go
score := 1.0
score -= n.Load.CPUPercent / 200.0          // up to -0.50
score -= n.Load.MemPercent / 200.0          // up to -0.50
score -= float64(n.Load.ActiveJobs) * 0.05  // per running job

cores   := normalized(float64(totalCores(n)), 32.0)
mhz     := normalized(capFloat(n, "cpu_mhz"), 4000.0)
freeRAM := normalized(float64(n.Load.AvailableMemBytes)/(1<<30), 32.0)

score += (cores         - 0.5) * 0.10
score += (mhz           - 0.5) * 0.06
score += (freeRAM       - 0.5) * 0.10
score += (freeCoreRatio - 0.5) * 0.10

score *= n.HealthScore
```

Two properties are worth drawing out. First, load dominates: the two load terms
can remove 1.0 between them while the four hardware terms move the score by at
most 0.36 in total, so a fast but busy machine loses to a slower idle one.
Second, `normalized` returns **0.5 for a missing value**, the neutral midpoint,
so a node that reports no clock speed is neither rewarded nor punished for it.
That is what makes the scorer safe on heterogeneous hardware where telemetry is
uneven. See `fig_4.2_scoring_weights.png`.

When a GPU is wanted, a second function adds up to +0.8 more from total VRAM,
device count, free VRAM on the best device and compute capability, minus a
utilisation penalty.

### 4.3 Leases

Placement is a three-phase handshake, and its state machine
(`fig_4.3_lease_fsm.png`) is where crash recovery lives.

```
accept  -> Reserved   (capacity held, expires after 30 s + 5 s grace)
commit  -> Running    (renewed every 30 s, reaped after 5 min without renewal)
complete/cancel -> released
```

Constants from `daemonapi/server.go:35-42`: `DefaultLeaseDuration = 30s`,
`DefaultGracePeriod = 5s`, `DefaultSweepInterval = 2s`,
`DefaultRunningLeaseTTL = 5m`.

The admission check and the lease insertion happen **under one lock**, so two
submitters racing for the last slot cannot both win. The concurrency limit
counts reserved and running leases together, deliberately, so a burst of
submitters cannot slip through the window between accept and commit.

Recovery is a consequence of the design rather than a separate mechanism. A
submitter that dies stops renewing, and the sweeper reclaims its slot after five
minutes. A worker that dies stops answering `/health`, is marked unreachable,
drops out of `/v1/nodes` after 45 s, and the in-flight WebSocket read fails,
which the coordinator treats as a failed attempt and reschedules.

### 4.4 Reproducing the environment

**Import scanning is a regex, not an AST.** `pythondeps/deps.go` applies
`^\s*(?:import|from)\s+([a-zA-Z0-9_]+)` line by line. This is worth stating
plainly because the previous report claimed an AST parse. The consequence is
that only the top-level module token is seen, and `import a, b` captures only
`a`.

Each name is then classified as a local file, a standard library module (a
hand-maintained list of about 130), or an external dependency. External names
are mapped to nixpkgs attributes through a 37-entry table that handles cases
like `sklearn` to `scikit-learn` and `PIL` to `Pillow`.

**The honest limitation.** A third-party import that is not in that table
resolves to the empty string and is silently dropped from the closure. There is
no PyPI or nixpkgs index lookup. `uv.lock`, which replaces detection entirely
when present, and the explicit `--pkg` flag are the escape hatches.

**The keystone: the store path is the cache key.** `nixgen/flake.go:34` pins an
exact revision:

```go
const nixpkgsRef = "github:NixOS/nixpkgs/56c02bc00adcf003215cc4bd996d6efaf4cff188"
```

The comment above it states the reasoning, and the whole caching layer follows
from it. A branch would resolve at build time, so two nodes building the same
script on different days would produce different store paths; since the cache is
keyed on the store path, the system would re-ship a multi-hundred-megabyte
archive for every job. Pinning an exact revision makes the store path a stable,
content-derived name for an environment, cluster-wide and across time. That is
what makes R2 in Chapter 5 possible.

Two deliberate exceptions, both x86_64-linux only and both documented in place:
scikit-learn is pinned to a specific wheel because nixpkgs lags the version the
project targets, and torch is fetched as a CUDA wheel set inside a **fixed-output
derivation** with a recorded hash, because building torch from source takes
hours.

### 4.5 Closure transfer

See `fig_4.4_closure_cache.png`. Before uploading, the submitter asks the target
`GET /v1/store?path=...&runnable=1`. Only on a negative answer does it export
and compress the closure.

There are **two different questions** a node can be asked, and both are needed:

- Is the archive in this node's cache?
- Is the closure materialised, that is, does `<storePath>/bin/run` exist?

The second is the `runnable=1` form. It exists because a rig with a shared
`/nix/store` volume has the closure on disk without ever having cached an
archive, and re-broadcasting to it would be wasted work; while on a per-node
store, `bin/run` appears only after a real import. One predicate would be wrong
for one of the two layouts.

After an upload the worker pushes the closure to peers that lack it. This is not
merely helpful: `EnablePoolSpill` only offers spill work to peers that already
hold the closure, so the broadcast is what makes the cluster usable for the
distribution in 4.7.

### 4.6 Execution and isolation

Jobs run under `crun` in an OCI bundle (`fig_4.5_sandbox.png`). Mounts: `/nix`
read-only, the workspace read-write at `/work`, tmpfs for `/tmp` and `/dev/shm`.
`/dev/shm` is mode 1777 specifically because Python's `multiprocessing` maps
POSIX shared memory for its semaphores and fails without it.

GPU support mounts the device nodes and then binds the driver libraries
(`libcuda.so*`, `libnvidia-*`) **file by file** into `/run/opengl-driver/lib`,
rather than binding the host library directory. Binding the directory would put
the host C library ahead of the closure's on the search path. Without this,
torch silently falls back to the CPU, which looks like a working run.

**What the sandbox does not do**, stated plainly because a security section that
overclaims is worse than none: there are no cgroup limits, no seccomp filter, no
capability set, no user namespace and no network namespace. The job runs as root
with the host's network. Namespaces in use are pid, ipc, uts and mount. If
`crun` is missing the daemon prints a warning and runs the job unisolated: the
sandbox is a hardening layer, not a correctness requirement.

Results are diffed rather than copied wholesale. Only files created or modified
during the run return, with deletions carried in a manifest, so a job cannot
overwrite the submitter's own sources. Both the untar on the worker and the
extract on the submitter refuse any path that escapes the target directory, and
both refusals are covered by tests.

### 4.7 Transparent interception

This is the system's distinguishing feature and the previous report does not
mention it.

**Delivery.** The shim is a Python file embedded in the Go binary as a string.
It is appended to the workspace archive at `.pipedpeer/shim/sitecustomize.py`
using a tar transform, so the user's project on disk is never touched, and the
worker points `PYTHONPATH` at that directory. CPython imports `sitecustomize`
automatically before the user's first line. Nothing is installed on any node.
See `fig_4.6_shim_injection.png`.

**The shadowing hazard.** Nix ships its own `sitecustomize.py`, and only the
first one on `PYTHONPATH` is imported. PipedPeer's would therefore suppress the
one that adds the environment's package paths, and every import in the closure
would fail. The shim replays that work explicitly before doing anything else.

**Patching is deferred.** Each library is patched on its first import, through a
`sys.meta_path` finder that wraps the loader. This branch changed that: the shim
previously imported torch, pandas and numpy while installing itself, so every
job paid about 1.7 s for torch whether or not it used it. See R3.

**What is intercepted, and how.**

| Primitive | Strategy |
|---|---|
| `multiprocessing.Pool`, `ProcessPoolExecutor` | Measure a few items locally, then race local and remote halves |
| `pandas` `groupby().agg()`, `merge`, `join` | Hash shuffle on the key |
| `pandas.read_csv`, `read_parquet` | Chunked out-of-core reads at record boundaries |
| `numpy.matmul`, `dot`, `tensordot` | Split into row blocks, concatenate |
| `numpy.linalg.svd`, `eig` | Whole matrix to one worker |
| `joblib.Parallel`, scikit-learn `n_jobs` | One batch per request |
| torch training | Gradients averaged across ranks |

**Why the shuffle is exact.** Rows are bucketed by a hash of the group or join
key, so equal keys always land in the same bucket and therefore on the same
node.

**The caveat this buys.** Exactness here means exactness of *values*, not of row
order. A concatenation of shuffle buckets does not reproduce pandas' documented
join ordering, which is why the join tests sort both sides before comparing. A
script that depends on the row order of a `merge` result, which is not unusual,
can therefore see a different order under interception. Grouped aggregation is
unaffected: those tests assert frame equality directly. Every key is complete on exactly one node, so each node's aggregation is a
complete aggregation and the origin combines partials with a plain
concatenation. There are no combiners, and consequently **any** aggregation
specification works, including ones with no combiner form. Bucket 0 stays local.
See `fig_4.8_hash_shuffle.png`. `TestShimHashShuffleMatchesLocal` asserts frame
equality against local pandas for four join types.

**Data parallel training without a socket mesh.** Rather than let
`torch.distributed` build its rank-to-rank mesh, which needs direct TCP on its
own ports between every pair of ranks, each rank posts its gradient blob to the
lead node's ordinary daemon port and gets the full set back once every rank has
arrived (`daemonapi/ddpsync.go`). The daemon is a blackboard: it never
interprets the payload, and the averaging happens in the shim. This trades a
little bandwidth for working on any network where the daemon port already works,
which is the only assumption the rest of the system makes. See
`fig_4.10_ddp.png`.

### 4.8 The cost model

Interception is only worthwhile when the cluster actually wins, so every
intercepted call is gated. From the shim, verbatim:

```python
K = int(_NUM_SHARDS)
if K < 2 or nbytes < 32 * 1024 * 1024:
    return False
if flops_per_byte < 8 and nbytes <= 512 * 1024 * 1024:
    return False
bw = _measure_bandwidth()
if not bw:
    return False
est_transfer = nbytes / bw
est_local    = nbytes * flops_per_byte / 1e9
est_remote   = est_local / K * 1.3        # parallel speedup, 30% overhead
return est_local > est_transfer + est_remote
```

Three guards before any arithmetic: at least two nodes, at least 32 MB of
payload, and a carve-out that refuses low-intensity work below 512 MB. Then the
comparison is simply whether computing locally costs more than moving the data
plus computing remotely. `fig_4.9_cost_model.png` plots the resulting decision
regions.

Bandwidth is measured, not assumed: 64 MB of random data through the real
dispatch path, cached for 300 s. If the probe fails the answer is "stay local".

The numpy path uses a second model calibrated against real BLAS throughput
(matmul about 200 GFLOP/s, SVD about 1.5 GFLOP/s). The consequence is worth
stating because it is counter-intuitive and the project treats it as a
correctness property: **dense matmul never ships**, because local BLAS beats the
transport at any practical size. `docker/demo-test.sh` fails the build if the
string `matmul: sending` ever appears. SVD, at two orders of magnitude lower
throughput, does ship.

`--distribute force` overrides the whole model for demonstrations.

### 4.9 Fault tolerance

The invariant is that using the cluster is never worse than not using it. Five
independent layers hold it up:

1. **The race.** `Pool.map` chunks are dealt alternately to the local side and
   the cluster, and results fill a slot table first-wins. Idle local cores re-run
   any chunk the cluster has not returned. The call returns only once the local
   side has covered every item, so a completely dead cluster degrades to a plain
   local `Pool`. See `fig_4.7_race_timeline.png`.
2. **Per-primitive fallback.** Every intercepted call is wrapped; any exception
   logs and falls back to the original implementation.
3. **Peer fallback.** A failed peer is skipped for the rest of that chunk and
   the next candidate is tried; if all fail, the work runs locally.
4. **Worker respawn.** A dead warm worker is restarted, falling back to a
   one-shot process if that fails.
5. **Rescheduling.** A failed job is placed again, with backoff doubling from
   2 s to a 2 minute ceiling. Tasks are never dropped; only the user interrupting
   ends the loop.

Memory is bounded at the same time. A request that needs more than the node has
is forwarded once to a healthier peer; one that needs more than 40% of available
memory is split into sequential micro-chunks rather than refused.

### 4.10 Resource estimation

Four tiers, first hit wins: an explicit `--mem`; the largest peak RSS from the
last five runs of the same script plus 20%; the sum of input file sizes times
three plus a per-library baseline; or the per-library baseline alone. The
default when nothing is known is 512 MB.

The historical tier closes a feedback loop: the worker samples the job's process
tree while it runs, records the peak, and that number sizes the next run's
admission request.

### 4.11 Test workloads

Ten dispatch scripts in `test_project/`, five demo workloads in
`pipedpeer-demo/` covering scikit-learn, numpy, out-of-core pandas, torch DDP
and file sync, a three-node podman lab, and a four-container docker rig with a
shared Nix store.

---

## Chapter 5  Results

Full detail and raw output: `docs/benchmarks/RESULTS.md` and
`docs/benchmarks/raw/`. Measured on 2026-08-25, Fedora 44, 20 logical cores,
with three worker daemons in podman containers.

**The caveat that governs this chapter.** All three workers are containers on
one machine, sharing one CPU and one memory system. This bed measures
correctness, overhead, cache behaviour and fault tolerance honestly. It cannot
measure multi-machine speedup, because moving CPU-bound work between containers
on one host cannot make that host faster. **No speedup figure is claimed
anywhere.** The same harness runs unchanged against real machines.

### 5.1 Functional verification

`go test ./... -count=1` with every optional Python library present:
**246 passed, 0 failed, 3 skipped.** The skips need `PIPEDPEER_INTEGRATION=1`
or an NVIDIA GPU.

Five interception tests had been skipping on any machine without pandas, torch
and scikit-learn, which includes the project's own CI job, so the paths they
cover were effectively unverified in automation. With the libraries installed
they run and pass. What each one establishes:

| Test | Property |
|---|---|
| `TestShimHashShuffleMatchesLocal` | grouped aggregation is frame-equal to local pandas; the four join types are equal **up to row order and index** (the test sorts both sides before comparing) |
| `TestShimNumPyInterception` | matmul is split into row blocks; SVD ships whole; results match |
| `TestShimCostModelDecisions` | the decision boundaries hold at pinned bandwidths |
| `TestShimJoblibBackendDistributes` | an unmodified `RandomForestClassifier(n_jobs=-1)` reaches the cluster |
| `TestShimOutOfCoreIntegrity` | chunk boundaries fall on record boundaries; memory stays bounded |
| `TestShimDDPSync` | two real ranks; each rank's gradient is exactly the mean, by `torch.equal` |
| `TestShimRaceCorrectWithDeadRemote` | every remote chunk failing still yields correct results |

### 5.2 Environment cache

The headline measurement. The same trivial job, submitted five times:

| Submission | Seconds |
|---|---|
| 1st, closure shipped | 12.00 |
| 2nd to 5th, median | 0.66 |

**An 18x reduction**, and the direct payoff of pinning nixpkgs so the store path
is a stable cluster-wide cache key (4.4). The second submission ships only the
workspace. See `fig_5.1_closure_cache.png`.

### 5.3 Cost of interception

| | Median seconds |
|---|---|
| Interception off | 0.85 |
| Interception on | 0.93 |

**+9%** on a whole remote job whose workload the cost model declines to
distribute. That is the price of leaving the feature on by default.

Interpreter startup, for a job that never imports a heavy library:

| | Slowdown |
|---|---|
| Before the fix in this branch | 21.60x |
| After | 1.20x |

The shim imported torch while installing itself, about 1.7 s, charged to every
job. The project's own 3x budget was exceeded sevenfold, and nothing in CI ran
the check that would have caught it. See `fig_5.2_shim_overhead.png` and 6.2.

### 5.4 Distribution actually happens

With `--distribute force`, all three workers logged `received pool/map`
(3, 3 and 4 requests). Interception, chunking and dispatch work end to end
across node boundaries. Raw: `raw/N3_pool_map_receipts.txt`.

### 5.5 Fault tolerance

`scripts/lab-fail.sh` fans a 20-item `Pool.map` across the cluster and kills a
worker container mid-flight:

```
killing worker on :38082 mid-flight
POOL-OK sum=2470
PASS: pool.map completed correctly despite a dead worker
```

2470 is the correct sum of squares for 0 to 19. Correct results despite losing a
node mid-computation, which is the invariant of 4.9 demonstrated end to end.

### 5.6 Sandbox cost

Median of 20 iterations: crun costs 15 to 20 ms more per job than bwrap
(21.7 vs 5.1 ms trivial, 36.8 vs 16.9 ms for an interpreter start). Against a
0.65 s warm job that is not worth optimising, and crun provides the OCI device
model that GPU passthrough needs. See `fig_5.3_sandbox_overhead.png`.

These are the first valid numbers this benchmark has produced; see 6.2.

### 5.7 Scale of the implementation

| | |
|---|---|
| Go | 24,679 lines across 72 files |
| Internal packages | 18 |
| HTTP endpoints | 18 |
| Embedded Python shim | about 1,900 lines |
| Test files / cases | 33 / 246 passing |

### 5.8 Comparison

See `fig_5.4_comparison.png`.

**What PipedPeer does differently**

| | PipedPeer | Spark | Ray | Dask | BOINC | ssh |
|---|---|---|---|---|---|---|
| Runs unmodified Python | yes | no | partial | partial | no | yes |
| No cluster to configure | yes | no | no | no | no | partial |
| No head or master node | yes | no | no | no | no | yes |
| Reproduces the environment | yes | partial | partial | partial | partial | no |
| Sandboxes each job | yes | no | no | no | partial | no |
| Survives losing a node | yes | yes | yes | yes | yes | no |

**Where PipedPeer is behind**

| | PipedPeer | Spark | Ray | Dask | BOINC | ssh |
|---|---|---|---|---|---|---|
| Authenticated and encrypted | **no** | yes | partial | partial | yes | yes |
| Works beyond one LAN | partial | yes | yes | yes | yes | yes |
| Runtimes other than Python | **no** | yes | partial | no | yes | yes |
| Proven in production use | **no** | yes | yes | yes | yes | yes |

The first table is the weaker half of the argument: those criteria are the ones
the system was built to change, so it wins them by construction, and a reviewer
should discount them accordingly. The second table is the one worth reading.
PipedPeer is the only system here with **no authentication and no transport
encryption at all**, which confines it to a network the user already trusts and
is the single largest barrier to deploying it anywhere else. It is also the only
one tied to one language and one subnet, and the only one with no production
history: the others have years of deployment, this has a three-node lab.

Spark, Ray and Dask are more capable at scale, with mature schedulers, richer
fault tolerance and far more operational tooling. The honest summary is that
PipedPeer sits at a different point on the curve rather than a better one,
trading reach, security and maturity for the removal of setup cost and code
change. BOINC is closest in spirit but is a platform for specific projects, each
with its own server infrastructure, rather than a general executor.

### 5.9 Not measured

Recorded rather than left blank:

| Wanted | Why absent |
|---|---|
| Multi-machine speedup, `Pool.map` and pandas shuffle | one host; needs real machines |
| DDP scaling across ranks | same; correctness covered by `TestShimDDPSync` |
| Discovery latency | containers share loopback, so the number would be meaningless |
| GPU placement and VRAM scoring | no GPU on this host |
| The five demo workloads | multi-GB CUDA closures; not attempted here |

---

## Chapter 6  Conclusion, limitations, future work

### 6.1 Conclusion

PipedPeer runs an unmodified Python script on another machine with no cluster
configuration, reproduces its environment exactly, and distributes the parallel
work inside it without touching the source. The two measurements that carry the
argument are the environment cache, 12.00 s cold against 0.65 s warm, and the
fault injection, correct results after losing a node mid-run. The design
decision underneath the first is pinning nixpkgs so a store path names an
environment identically on every node; the decision underneath the second is
that every remote path is a strict superset of a working local path.

The cost model is the part most worth defending in a paper. A system that
distributes everything it can intercept would be slower than plain Python for
most real workloads. Measuring bandwidth and declining to distribute dense
matmul, and treating that refusal as a property to be tested rather than a
missing feature, is what makes always-on interception defensible.

### 6.2 Limitations

**Of the system.**

- LAN scope. Discovery is UDP on a local subnet; anything wider needs the
  registry or manual peers.
- Python only.
- Workers must run Linux.
- The submitting process must survive the job; it holds the lease.
- Unmapped third-party imports are dropped from the closure silently (4.4).
- The sandbox has no cgroup limits, no seccomp and no user namespace (4.6).

**Of the evaluation.** Single host, so no speedup number is claimed (5.9).

**Found while writing this report.** Three defects, all fixed in this branch:

1. The shim imported torch at startup, taxing every job about 1.7 s and blowing
   the project's own 3x never-slower budget by sevenfold. The tripwire that
   detects it, `scripts/bench-shim-d2.sh`, is not run by CI.
2. `bench/` gave neither sandbox a root filesystem, so `/bin/sh` was absent and
   all 60 measured runs exited non-zero. Its published timings were the cost of
   failing to start. It now refuses to write a report if any measured run fails.
3. `scripts/lab-up.sh` was not idempotent and `lab-fail.sh` used bare `/tmp` as
   a job workspace, so the fault-tolerance demo could not be run twice.

**Drift worth cleaning up.** `PIPEDPEER_PANDAS` and `PIPEDPEER_TORCH` are set in
tests but never read; the `jobhistory` state machine is implemented and tested
but the runtime never calls it; `EventBus` is never assigned, so job events are
dead code; `QueueDepth` and `RecentFailures` are declared but never populated;
the dashboard labels UDP discovery as mDNS; job history and daemon job
directories resolve to the same path on a machine that is both submitter and
worker.

### 6.3 Security

`SECURITY.md` states the position: PipedPeer assumes every peer on the network
is trusted, and there is no authentication, authorisation or transport
encryption. Anyone who can reach the daemon port can execute code as root on
that machine, read job output and workspace files, and deregister nodes. This is
a scope decision for a LAN tool, not an oversight, and it is the single largest
barrier to running the system anywhere else.

### 6.4 Future work

Authentication and an encrypted transport, which the security posture makes a
prerequisite for anything beyond a trusted LAN. Discovery beyond the subnet.
Runtimes other than Python. Sender fault tolerance, so a submitter can
disconnect and collect results later. Placement that accounts for where data
already is, rather than only where capacity is.

---

## Appendix A  Traceability

| Claim | Evidence |
|---|---|
| Store path is the cluster-wide cache key | `nixgen/flake.go:34`; measured in 5.2 |
| Placement scoring | `coordinator.scoreNode`; `scoring_test.go` |
| Lease constants | `daemonapi/server.go:35-42` |
| Staleness thresholds | `daemonapi/server.go:834,1061` |
| Discovery is UDP, not mDNS | `discovery/discovery.go:39-47` |
| Import scan is a regex | `pythondeps/deps.go` |
| Hash shuffle is exact in value | `TestShimHashShuffleMatchesLocal`; joins compared after sorting, so row order is not preserved |
| DDP gradients are exactly the mean | `TestShimDDPSync` |
| Remote failure degrades to local | `TestShimRaceCorrectWithDeadRemote`; 5.5 |
| Dense matmul never ships | `docker/demo-test.sh` negative assertion; 5.3 |
| Sandbox has no cgroups or seccomp | `daemonapi/execution.go` OCI config |
| No authentication anywhere | `SECURITY.md`; router in `daemonapi/server.go` |

## Appendix B  What changed since the previous report

| Previous report | Current implementation |
|---|---|
| bubblewrap sandbox | `crun` OCI bundle |
| Push dispatch over NATS | HTTP and WebSocket; NATS is embedded on loopback at a random port with no clustering, so by default no two daemons share a bus |
| `internal/taskqueue/`, JetStream, dead-letter | Package does not exist; retry is a loop in the submitting CLI |
| `internal/peers/` | Does not exist; manual peers live in the node store |
| Result delivery as a zip of the job directory | Tar of changed files plus a deletion manifest |
| mDNS discovery | UDP multicast and broadcast, with a TCP sweep fallback |
| Python AST import scanner | A single line regex |
| HealthScore with three terms | Eight terms, plus a separate five-term GPU score |
| "Parallel job splitting is not implemented", listed as a limitation | The interception shim, now the system's headline feature |
| No GPU support | Detection, VRAM-aware placement, per-device reservation, driver mounting |
| Results chapter of `[PASS/FAIL]` and `[X] ms` placeholders | Chapter 5, measured |

Absent from the previous report entirely: the content-addressed closure cache
and broadcast, memory-bounded pool spill with peer forwarding, and the cost
model.
