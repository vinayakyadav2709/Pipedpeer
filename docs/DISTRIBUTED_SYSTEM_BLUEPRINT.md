# Pipedpeer Distributed System Blueprint

## 1. Goals

- Run jobs across multiple nodes with fault tolerance.
- Keep Nix as the dependency/runtime packaging strategy.
- Support reproducible execution via host Nix builds with SSH deployment.
- Allow optional GPU execution via containerized Nix (future).
- Allow each node to act as both submitter and executor.
- Support three execution modes:
  - **Script mode**: Sync entire workspace (`pwd`) to remote, execute normally, and sync output files back.
  - **SDK mode**: Intercept specific Python operations, send them via RPC to the Pipedpeer daemon, which distributes tasks across the cluster. (No workspace files sent).
  - **Script+SDK mode**: Sync workspace like Script mode, but remote execution acts as an SDK master to distribute sub-operations across the cluster.
- Terminal modes:
  - **Attached**: Real-time terminal output streaming with graceful `Ctrl+C` handling.
  - **Detached**: Background async execution and artifact polling.
- Enable GPU-aware scheduling.
- Preserve strong cache reuse for shared dependencies and data artifacts.

## 2. Core High-Level Architecture

### Layers

- Registry and control plane
- Scheduler and optimizer
- Executor runtime
- Artifact and data plane
- CLI and SDK interface

### Node behavior

Each node runs one process that can:

- Register itself and heartbeat with registry.
- Accept incoming jobs.
- Submit jobs to other nodes.
- Execute jobs locally.
- Track task lifecycle and report status.

This means every node is dual-role by design.

## 3. Runtime Packaging and Isolation Strategy

### Current Model: Host Nix + SSH

**Primary execution path:**
- Host machine runs Nix with flakes support
- `pipedpeer` CLI compiles Nix expression locally
- Copies build outputs to remote node via SSH
- Remote executes in bubblewrap-isolated namespace
- Advantages: simple, fast iteration, strong cache reuse

**Installation:**
- Bootstrap installs: Nix, SSH, bubblewrap
- See `scripts/bootstrap/all.sh` for automated setup

### Future Model: Containerized Nix for GPU

**Optional GPU execution path (planned):**
- Host still builds with Nix, but dispatches GPU jobs to OCI containers
- OCI image carries Nix + GPU drivers inside
- Mounts host `/nix` volume for build cache persistence
- Remote GPU worker spawns container with device pass-through
- Benefits: GPU isolation, reproducible GPU environments, optional complexity
- Will be implemented after core API is stable; current bootstrap keeps nodes CPU-only

### Execution isolation

Per job:
- Use bubblewrap namespaces for CPU jobs to isolate network/filesystem/process spaces
- Keep read-only access to /nix store
- For GPU jobs: use container-level isolation + bwrap double-wrapping (optional)

## 4. Registry Design

### Registry responsibilities

- Node registration with capabilities/specs.
- Heartbeat liveness tracking.
- Dead detection via lease expiry.
- Capability query endpoints for scheduler.

### Improvements recommended

- Use lease-based liveness, not just alive/dead flags.
- Add record versions to prevent stale overwrite.
- Add anti-entropy full reconcile between replicas.
- Keep node health score (recent failure rate, latency, queue depth).
- Use persistent storage backend for consistency.

### Replication

- Multiple registry replicas are good.
- Gossip can be used for propagation.
- Also include periodic full-state sync for correctness.

### Fallback behavior

When registry is unreachable:

- Use last known registry snapshot.
- Use LAN discovery for reachable peers.
- Always include self as candidate node implicitly.

## 5. Node Identity

Do not use IP address as node identity.

Use:

- Stable node ID (UUID or key fingerprint).
- Endpoint addresses stored as mutable metadata.

Why:

- IP changes (DHCP, NAT, restart) cause collisions/stale records.
- Stable IDs prevent identity conflict and misrouting.

## 6. Scheduling and Reservation Flow

### Suggested task flow

1. Client submits task.
2. Scheduler fetches candidate nodes.
3. Scheduler filters by hard requirements.
4. Scheduler scores candidates by optimizer.
5. Scheduler sends parallel reservation requests to top candidates.
6. First accepted lease is committed.
7. Remaining reservations are cancelled or left to expire.

### Lease-based reservation

Reservation response should include:

- lease_id
- expiry_time
- reserved resources

Semantics:

- Commit before expiry to start task.
- No commit means automatic release.
- Renewal allowed if needed.

Benefits:

- Prevents resource lock leaks.
- Handles race conditions from parallel reservation.
- Makes cancellations safer.

Current implementation status:
- The CLI supports both explicit remote (`--remote`) and automatic coordinator placement.
- Lease-based reservations are active: accept → commit → complete lifecycle.
- Admission control enforces memory-based rejection at both coordinator and daemon level.
- Detached/background jobs are supported with async artifact sync.
- Task lifecycle state machine is implemented: queued → reserved → running → succeeded/failed/cancelled/expired.
- Queue/retry: if no node can accept, tasks queue and retry at configurable intervals.

## 7. Task Lifecycle and Fault Tolerance

Use explicit task states:

- queued
- reserved
- running
- succeeded
- failed
- cancelled
- expired

Fault handling:

- Heartbeat/poll running tasks.
- Detect executor failure.
- Reschedule idempotent tasks.
- Keep retry policy with backoff and max attempts.

## 8. Idempotency and Task Key

Use idempotency keys to avoid duplicate execution on retries.

Recommended model:

- SDK/CLI auto-generates key by default.
- Advanced users can override key manually.
- Server enforces key semantics.

If same key arrives again:

- Return existing task reference/status instead of re-running.

## 9. Script Mode

Script mode behavior:

- User submits entrypoint script.
- Runtime builds environment and executes job.
- Return stdout/stderr and optional artifacts.

Near-term scope:

- Output-only response is acceptable initially.

Future expansion:

- Structured outputs.
- File outputs.
- Multi-file project submission.

## 10. SDK Mode

SDK mode goal:

- Distribute selected operations (for example matrix ops) while leaving rest local.

### Zero-manual-change requirement

For no user source edits, use:

- AST transform as default in SDK mode.
- In-memory transform, do not overwrite original file.
- Hybrid mode: rewrite supported ops, keep unsupported local.
- Strict mode: fail if unsupported construct is encountered.

### Runtime backend switching

SDK operation path:

- local backend for non-distributed path
- distributed backend for offloaded path

Same API shape should be preserved.

### Why not only explicit namespace

- Explicit namespace is easier to forget.
- AST-based transparent mode avoids user mistakes.
- Keep explicit namespace as optional manual mode.

## 11. Data Plane and File Handling

### Control plane vs data plane separation

Control plane:

- registry, scheduling, lease, status

Data plane:

- artifacts, large input/output transfer, logs, model files

### File/path handling model

Do not rely on host absolute paths across machines.

Recommended flow:

1. Build input manifest.
2. Upload files as artifacts.
3. Executor materializes files into job workspace.
4. Run task with workspace as cwd.
5. Collect outputs and publish as artifacts.

Relative paths should resolve inside workspace.

Current CLI implementation notes:

- Foreground runs stream stdout/stderr and sync outputs back to the local workspace.
- Detached runs create a job record immediately and sync artifacts asynchronously.
- Job history stores metadata, output logs, and a received-files manifest for later inspection.

### Content-addressed artifact cache

For large files/models:

- Hash content (for example SHA-256).
- Artifact ID based on hash.
- Check if destination already has artifact.
- Transfer only if missing.
- Reuse cached copy for future tasks.

This gives transfer-once reuse semantics.

### Locality metadata

Add data location metadata in task planning:

- nodes currently storing artifact
- optional region/zone/network tags
- size and freshness/version

Scheduler should prefer nodes with local copies.

Optional replication policy:

- replicate hot artifacts proactively.
- keep replication factor by usage frequency.

## 12. GPU Scheduling Model

### Capability model

Track per node:

- GPU vendor/model
- VRAM
- driver/runtime versions
- compute capability
- MIG partition info (if used)

### Pooling strategy

Create logical pools:

- CPU only
- small GPU
- medium GPU
- large GPU
- specialized accelerators

### Locality for GPU workloads

Large models and datasets should run where already cached.

Scoring should account for:

- model already present on node
- dataset shard local availability
- transfer cost for model/data
- expected queue delay

## 13. Optimizer Inputs and Scoring

Good scoring features:

- hard requirement match
- free CPU/RAM/GPU/VRAM
- queue depth
- node reliability score
- network latency/bandwidth estimate
- data locality score
- cache hit probability
- historical runtime by op type

A practical approach:

- start with weighted heuristic
- add learning layer later from historical telemetry

## 14. Recommended Libraries and Frameworks

## Go service and APIs

- cobra: CLI commands and subcommands ✅ (implemented)
- chi: lightweight HTTP API server with middleware ✅ (implemented)
- zerolog: structured logging (zero-allocation, JSON + console modes) ✅ (implemented)
- viper: configuration with env var support ✅ (implemented)
- gopsutil: cross-platform system metrics ✅ (implemented)
- google/uuid: RFC-compliant UUID generation ✅ (implemented)
- go-humanize: human-readable byte formatting ✅ (implemented)

## Reliability and messaging

- NATS JetStream: queueing, retries, request/reply, durable events ✅ (implemented)
- NATS embedded server: zero-config for single-node / dev ✅ (implemented)
- Dual transport: NATS preferred, HTTP fallback ✅ (implemented)

## Registry and state

- NATS JetStream KV: registry backend with TTL, watches, persistence ✅ (implemented)
- In-memory backend: fallback when NATS unavailable ✅ (implemented)
- Future: etcd for multi-coordinator leader election if HA coordinator is needed

## Data and artifact storage

- MinIO (S3-compatible) for artifact blobs
- ORAS for OCI-style artifact handling
- Build content-addressed object layout by hash

## Python SDK side

- libcst or parso: robust AST/source transforms
- pydantic: schema validation for task payloads
- httpx or grpc client for transport

## Runtime and execution

- bubblewrap for per-job sandbox namespace isolation (CPU tasks)
- Nix on host for deterministic package/runtime builds
- Docker or Podman only for GPU jobs requiring OCI containers (future)

## Observability (deferred — not needed for current milestone)

- OpenTelemetry for traces/metrics
- Prometheus metrics endpoint
- Loki or standard log aggregation for executor logs
- Implement when multi-node production deployments begin and monitoring becomes necessary

## 15. Suggested Milestone Plan

Phase 1:

- Dual-role node process (submit + execute)
- Registry lease heartbeats
- Script mode only
- Lease-based reservations
- Task lifecycle state machine

Phase 2:

- Artifact store and content-addressed cache
- Data locality-aware scheduling
- Retry/reschedule policies

Phase 3:

- GPU capability model and pool-aware scheduling
- Model/data cache-aware optimization

Phase 4:

- SDK mode with AST transparent transform
- Hybrid fallback for unsupported operations
- Distributed op library (small stable op set first)

Phase 5:

- Optimizer learning from telemetry
- Replication policies and hotspot handling

Phase 6 (deferred — low priority):

- OpenTelemetry distributed tracing across nodes
- Prometheus metrics endpoint on daemon
- NATS header propagation for trace context
- Implement when production monitoring is required

## 16. Non-Negotiable Design Rules

- Node identity must be stable and not IP-based.
- Reservations must be lease-based with expiry.
- Task submission must support idempotency key handling.
- Control and data planes must be separated.
- Executor must run in isolated workspace/sandbox.
- Data transfer should be content-addressed and cache-aware.
- Self node is always an implicit scheduling candidate.

## 17. Practical Defaults for v1

- Runtime: Nix + bubblewrap on host (CPU), OCI containers for GPU (future)
- Registry: replicated service with lease heartbeats
- Scheduler: weighted heuristic + lease reservation fanout
- Data plane: S3-compatible artifact store + SHA-256 IDs
- Modes: script mode first, SDK mode next
- SDK transform: hybrid mode default, strict optional

## 18. GPU Execution Strategy and Automation

### Final GPU split

Use a hybrid backend policy:

- CPU jobs: run directly in the Nix-managed execution path.
- GPU jobs: run as OCI containers built from Nix.

This keeps CPU execution lightweight while still using OCI where GPU runtime compatibility matters.

### Why this split is recommended

- Nix gives reproducibility and cache reuse.
- OCI gives portability and standard GPU runtime deployment.
- GPU driver setup remains an infra concern, where it belongs.
- You avoid forcing OCI overhead onto every CPU task.

### GPU dependency automation approaches

#### Option A: Kubernetes-managed GPU automation

If the cluster runs on Kubernetes, use:

- NVIDIA GPU Operator
- NVIDIA Device Plugin
- Node Feature Discovery

These automate:

- driver lifecycle
- container runtime/toolkit setup
- GPU resource advertising
- node labeling and capability discovery

This is the cleanest option for larger GPU fleets.

#### Option B: Non-Kubernetes bootstrap automation

If the cluster is not Kubernetes-based, use a bootstrap script or config management tool:

- cloud-init
- Ansible
- shell bootstrap script

The script should:

- install or verify GPU drivers
- install or verify the container runtime GPU support
- enable the node agent service/container
- register the node with registry
- expose GPU capability metadata

### Suggested bootstrap script responsibilities

The bootstrap script can handle:

- OS prereqs
- container runtime installation
- GPU driver validation
- node identity setup
- registry enrollment
- heartbeat startup

This keeps setup one-command and avoids manual per-machine configuration.

### Node-side GPU capabilities to advertise

Track at least:

- GPU vendor/model
- VRAM
- driver version
- CUDA compatibility
- MIG partition info if available
- current GPU utilization

### GPU scheduling rules

- Use GPU nodes only for tasks that require GPU.
- Prefer nodes where the model/data is already cached.
- Keep the same optimizer layer for CPU and GPU tasks, but feed different capability constraints.
- Use OCI image pre-pull or registry mirror on GPU nodes to reduce cold-start cost.

### Practical deployment default

- CPU path: direct Nix execution on host with bubblewrap isolation.
- GPU path: Nix-built OCI image launched by the node on a GPU-capable host.
- Shared cache: Nix store cache for builds plus OCI layer cache for GPU images.

---

This blueprint captures the full design discussion and keeps the system aligned with your goals: distributed execution, Nix-based reproducibility, minimal host dependencies, and gradual path from script execution to rich SDK-driven distributed compute.
