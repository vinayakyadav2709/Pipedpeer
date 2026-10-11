# Interception

How a running job's parallel work reaches other nodes without code changes: the Python shim on one side, the daemon's pool and DDP handlers on the other. Covers `nixgen/shim.go`, `daemonapi/pool*.go`, `daemonapi/ddp*.go`, `schedule` and `ddpplace`. (`nixgen/flake.go` belongs to [Environments and closures](../nixstore/CONTEXT.md).) Design detail: [system report 4.9](../../../docs/system-report.md#49-transparent-interception) and [4.10](../../../docs/system-report.md#410-the-cost-model).

## Language

### Pool work

**Shim**:
The `sitecustomize.py` injected into every job's environment that patches parallel primitives; lives as a Go raw string in `shim.go`.
_Avoid_: SDK, agent, plugin

**Interception**:
Replacing a library call (Pool, joblib, numpy, pandas, torch DDP) with a cluster-aware one when the cost model says the cluster wins.

**Kernel**:
The user function a Pool maps over; ships as source, or by value with cloudpickle when it has no source ([ADR-0017](../../../docs/adr/0017-kernels-ship-as-source-then-cloudpickle.md)).
_Avoid_: task function, job

**Spill**:
Sending part of an intercepted call from the job to its local daemon's `/v1/pool/map` for other nodes to run.
_Avoid_: offload (used only for numpy whole-matrix work), dispatch

**Chunk**:
One batch of items in a spill request.

**Origin**:
The daemon that receives a spill from its own job; it splits the chunk exactly once.

**One-hop fan-out**:
The rule that a forwarded part is terminal (`no_fanout`): peers never re-split, and results return to the origin.

**Share**:
The number of items a device gets, sized by measured throughput so all finish together (makespan); a device that cannot shorten the makespan gets none.
_Avoid_: split evenly, round-robin

**Cost model**:
The shim's decision whether a call is worth spilling, from measured per-item cost, payload size and link bandwidth.

**Force mode**:
`--distribute force`: every spill gate says yes, bypassing the cost model.

**Pool width**:
How many local worker processes an intercepted Pool runs: `min(cpu_count, MemAvailable*0.4 // 256MB)`, where available memory is capped by the cgroup's headroom less reclaimable `inactive_file` (`7128212`). The count the user passed is ignored unless `PIPEDPEER_RESPECT_POOL_SIZE=1` (`_pool_width`, `_avail_bytes`, `_cgroup_headroom` in `shim.go`).
_Avoid_: processes, concurrency

**Warm worker**:
A persistent Python process per closure on a node that runs chunks, so dispatch is a pipe write rather than an interpreter start.

**Micro-chunks**:
Sequential slices of a request too big for this node's free memory at once; forwarded to a peer if even one slice does not fit.

**Frames**:
The pool wire format: JSON header line plus length-prefixed raw pickles ([ADR-0009](../../../docs/adr/0009-pool-frames-wire-format.md)).

**Receipt**:
The record of where each item actually ran (local, which peer, forwarded), written by the shim and the daemon.
_Avoid_: stats, log

### Data frames

**Hash-shuffle**:
Distributing a pandas groupby or merge by key hash so each node reduces its keys and the origin combines exactly.

**Out-of-core read**:
A large CSV or parquet read in byte-range chunks across nodes, returned as a partitioned frame.

### Training

**Rank**:
One training process in a DDP ring; a node with two accelerators can hold two.

**Ring**:
The set of ranks for one `--ddp K` run, chosen by measured throughput (`ddpplace`).

**Lead**:
The rank whose daemon receives syncs and averages them; must be told apart from a forwarded remote peer.

**Sync**:
One round where every rank posts its gradients (or weights) to the lead's daemon and gets back the average ([ADR-0010](../../../docs/adr/0010-gradient-sync-through-the-daemon.md)).

**Sync interval**:
Steps between syncs; 1 by default, `auto` only when the user asks.

## Invariants

- Never slower than local ([ADR-0002](../../../docs/adr/0002-never-slower-than-local.md)); local cores keep pulling while peers work.
- Spill goes only to peers that already hold the closure (`daemonapi/server.go` `peerHasStore`).
- A reply uses the format the request asked for (`0cffbbb`).
- Every `PIPEDPEER_*` variable the shim names must be read somewhere; a test fails otherwise (`8492458`).
- `shim.go` is a Go raw string: the Python inside cannot contain a backtick.

## Entry points

- Shim source and install order: `nixgen/shim.go` `ShimSitecustomize`, `_install`, `WriteShim`.
- Pool: `shim.go` `_ClusterPool`, `_race`, `_pool_width`, `_func_payload`, `_func_pickle`, `_pool_send`.
- Cost gates: `shim.go` `_should_spill`, `_numpy_should_offload`.
- Libraries: `shim.go` `_install_numpy`, `_install_pandas`, `_install_io` (out-of-core), `_install_ddp`, `_install_joblib`.
- Injection into a job: `daemonapi/execution.go` `handleJobExec`.
- Daemon pool: `daemonapi/pool.go` `handlePoolMap`, `runChunk`, `forwardPoolMap`, `runMicroChunks`, warm worker `spawn`.
- Shares: `daemonapi/poolrates.go` `planShares`, `schedule/schedule.go` `Compute`.
- DDP: `daemonapi/ddpsync.go` `handleDDPSync`, `daemonapi/ddpreduce.go` `ddpAccumulator`, `src/main.go` `runDDP`.
- Tests need pandas, scikit-learn, joblib and torch or they skip: `scripts/check-skips.sh`, `scripts/test-on-host.sh`.
