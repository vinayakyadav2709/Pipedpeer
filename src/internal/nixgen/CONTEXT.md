# Interception

How a running job's parallel work reaches other nodes without code changes: the Python shim on one side, the daemon's pool and DDP handlers on the other. Covers `nixgen/shim/` (`sitecustomize.py` + package `pipedpeer_shim/`) (embedded by `nixgen/embed.go`), `daemonapi/pool*.go`, `daemonapi/ddp*.go`, `schedule` and `ddpplace`. (`nixgen/flake.go` belongs to [Environments and closures](../nixstore/CONTEXT.md).) Design detail: [system report 4.9](../../../docs/system-report.md#49-transparent-interception) and [4.10](../../../docs/system-report.md#410-the-cost-model).

## Language

### Pool work

**Shim**:
The Python package injected into every job's environment (`.pipedpeer/shim/` on PYTHONPATH) that patches parallel primitives: `sitecustomize.py` (install order) imports, from package `pipedpeer_shim`, `core` (logging, env, shared state), `stats`, `record`, `pool`, `transfer`, `numpy_ops`, `pandas_ops`, `ddp`. Silent unless `PIPEDPEER_VERBOSE=1` (`run -v`); `_warn` lines (wrong result or the user must act) always print.
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
How many local worker processes an intercepted Pool runs: `min(cpu_count, MemAvailable*0.4 // 256MB)`, where available memory is capped by the cgroup's headroom less reclaimable `inactive_file` (`7128212`). The count the user passed is ignored unless `PIPEDPEER_RESPECT_POOL_SIZE=1` (`_pool_width`, `_avail_bytes`, `_cgroup_headroom` in `shim/pipedpeer_shim/pool.py`).
_Avoid_: processes, concurrency

**Warm worker**:
A persistent Python process per closure on a node that runs chunks, so dispatch is a pipe write rather than an interpreter start.

**Micro-chunks**:
Sequential slices of a request too big for this node's free memory at once; forwarded to a peer if even one slice does not fit.

**Frames**:
The pool wire format: JSON header line plus length-prefixed raw pickles ([ADR-0009](../../../docs/adr/0009-pool-frames-wire-format.md)).

**Receipt**:
The record of where each item actually ran (local, which peer, forwarded), written by the shim and the daemon; its additive `steps` key carries the run record's steps.
_Avoid_: stats, log

**Run record**:
Timed steps (pickle, compress, send, queue, compute, receive, unpickle, env_build, env_send, ddp_sync, script) per machine and chunk; `shim/pipedpeer_shim/record.py` `step()` on the job side, `runrec` on the Go side. Shim work is never folded into `script`.

**In-flight depth**:
Copies a node may hold at once, from its measured round trip (at most 3); losing race copies are cancelled, and a peer that lost its last 3 races stops being raced.

### Data frames

**Hash-shuffle**:
Distributing a pandas groupby or merge by key hash so each node reduces its keys and the origin combines exactly.

**Out-of-core read**:
A large CSV or parquet read in byte-range chunks across nodes, returned as a partitioned frame.

### Training

**Rank**:
One training process in a DDP ring; a node with two accelerators can hold two.

**Ring**:
The set of ranks for one training run, chosen by measured throughput (`ddpplace`); one device kind, lead on a GPU node whenever one exists. Auto mode may shrink it to the fastest rank (exit 77) after warm-up; `--ddp N` fixes it (`PIPEDPEER_DDP_FIXED=1`).

**Lead**:
The rank whose daemon receives syncs and averages them; must be told apart from a forwarded remote peer.

**Sync**:
One round where every rank posts its gradients (or weights) to the lead's daemon and gets back the average ([ADR-0010](../../../docs/adr/0010-gradient-sync-through-the-daemon.md)).

**Batch split**:
Per-rank slices of the script's batch, by speed, summing to the script's batch; the learning rate is never changed.

**Sync interval**:
Steps between syncs; 1 by default. `--ddp-sync fast` (`PIPEDPEER_DDP_SYNC_MODE=fast`) is DiLoCo: a 10-step exact warm-up, then plain sample-weighted model averaging every 64 steps and at every epoch's end, tightening near the end when the run length is known (tuned in docs/benchmarks/diloco.md; `PIPEDPEER_DILOCO_*` select the outer Nesterov step and the drift-driven interval).

**Training exit codes**:
75 missed start barrier, 76 ring aborted or lead lost (after one `pipedpeer: training stopped: <reason>` line), 77 rank released because the ring shrank (not a failure).

## Invariants

- Never slower than local ([ADR-0002](../../../docs/adr/0002-never-slower-than-local.md)); local cores keep pulling while peers work.
- Spill goes only to peers that already hold the closure (`daemonapi/server.go` `peerHasStore`).
- A reply uses the format the request asked for (`0cffbbb`).
- Every `PIPEDPEER_*` variable the shim names must be read somewhere; a test fails otherwise (`8492458`).
- The shim modules stay importable without torch/numpy/pandas (they import lazily); `scripts/shim-check.sh` runs ruff and pytest.
- A global another module rebinds is read through its module (`core._NUM_SHARDS`), never copied with `from … import`.

## Entry points

- Shim source and install order: `nixgen/embed.go` `ShimFS`, `WriteShimDir`, `ShimSource`; `shim/sitecustomize.py`.
- Pool: `shim/pipedpeer_shim/pool.py` `_ClusterPool`, `_race`, `_pool_width`, `_func_payload`; transfer `shim/pipedpeer_shim/transfer.py` `_pool_send`; daemon wire `daemonapi/poolwire.go`.
- Cost gates: `_should_spill` (`shim/pipedpeer_shim/transfer.py`), `_numpy_should_offload` (`shim/pipedpeer_shim/numpy_ops.py`); per-machine rates across runs `daemonapi/poolstats.go`.
- Libraries: `shim/pipedpeer_shim/numpy_ops.py`, `shim/pipedpeer_shim/pandas_ops.py` (incl. out-of-core), `shim/pipedpeer_shim/ddp.py` `_install_ddp`, `shim/pipedpeer_shim/pool.py` (joblib).
- Injection into a job: `daemonapi/execution.go` `handleJobExec`.
- Daemon pool: `daemonapi/pool.go` `handlePoolMap`, `runChunk`, `forwardPoolMap`, `runMicroChunks`, warm worker `spawn`.
- Shares: `daemonapi/poolrates.go` `planShares`, `schedule/schedule.go` `Compute`.
- DDP: `daemonapi/ddpsync.go` `handleDDPSync`, `daemonapi/ddpreduce.go` `ddpAccumulator`, `src/ddp_cmd.go` `runDDP`; the one-GPU-or-split decision `daemonapi/ddprebalance.go`.
- Tests need pandas, scikit-learn, joblib and torch or they skip: `scripts/check-skips.sh`, `scripts/test-on-host.sh`.
