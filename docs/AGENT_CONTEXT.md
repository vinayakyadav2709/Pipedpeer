# Agent Context — Pipedpeer

> Last updated: 2026-04-19
> This file summarizes the state of the codebase for continuity across agent sessions.

## Project Overview

Pipedpeer is a **decentralized compute mesh** — a CLI tool that dispatches Python/shell scripts to remote nodes for execution via SSH, with Nix-based environment isolation and bubblewrap sandboxing. Nodes discover each other via a registry + mDNS, communicate over NATS (with HTTP fallback), and a coordinator selects the best node based on available resources.

## Repository Structure

```
Pipedpeer/
├── cmd/pipedpeer/
│   ├── main.go                          # Cobra CLI entry (run, start, stop, status, jobs, job, init, registry, nodes)
│   ├── main_cli_test.go                 # CLI integration tests (spawn real processes)
│   ├── main_integration_test.go         # Docker-based E2E tests (nix+SSH, only for CI)
│   └── internal/
│       ├── app/
│       │   ├── run.go                   # Core execution: nix build + SSH + artifact sync
│       │   └── detached_sync.go         # Background job output collection
│       ├── coordinator/
│       │   ├── coordinator.go           # Node selection, placement, ExecuteWithRetry (NATS + HTTP)
│       │   ├── coordinator_test.go      # Unit tests (scoring, filtering, self-node)
│       │   ├── cluster_integration_test.go # Integration tests with real daemons
│       │   └── coordinator_nats_test.go # NATS transport integration tests
│       ├── daemonapi/
│       │   ├── server.go                # chi HTTP + NATS dual-transport daemon
│       │   ├── server_test.go           # HTTP endpoint tests
│       │   └── server_nats_test.go      # NATS transport tests
│       ├── daemonctl/
│       │   └── manager.go              # Daemon lifecycle management + remote lease calls (HTTP)
│       ├── discovery/
│       │   └── discovery.go            # mDNS/LAN peer discovery
│       ├── execution/
│       │   └── remote_exec.go          # SSH command building
│       ├── heartbeat/
│       │   ├── heartbeat.go            # Registry heartbeat client (NATS pub/sub or HTTP)
│       │   └── load.go                 # System load collection (gopsutil — cross-platform)
│       ├── identity/
│       │   └── identity.go            # Stable UUID-based node identity (google/uuid)
│       ├── jobhistory/
│       │   ├── history.go             # Job record CRUD (filesystem-based)
│       │   ├── events.go              # NATS event publishing on state changes
│       │   └── states.go              # Task state machine constants + transitions
│       ├── logging/
│       │   └── logger.go              # Shared zerolog logger (console/JSON, PIPEDPEER_LOG_LEVEL)
│       ├── natsbus/
│       │   ├── natsbus.go             # NATS connectivity: embedded server, pub/sub, request/reply, JetStream
│       │   └── natsbus_test.go        # 8 integration tests with real embedded NATS
│       ├── nixgen/
│       │   └── flake.go               # Nix flake generation
│       ├── pythondeps/
│       │   └── deps.go                # Python import → nix package resolution
│       ├── registry/
│       │   ├── registry.go            # Node directory with Backend interface (memory + NATS KV)
│       │   └── registry_nats_test.go  # NATS KV backend tests
│       ├── remote/
│       │   └── ssh.go                 # SSH connection string parsing
│       ├── resourceest/
│       │   ├── estimator.go           # Memory requirement estimation (4-tier, go-humanize)
│       │   └── vmpeak.go              # Peak memory reader (gopsutil — cross-platform)
│       └── taskqueue/
│           ├── queue.go               # JetStream durable task queue (submit, consume, retry)
│           └── queue_test.go          # 3 integration tests (submit, retry, ordering)
├── docs/
│   ├── DISTRIBUTED_SYSTEM_BLUEPRINT.md  # Architecture spec
│   └── AGENT_CONTEXT.md                 # THIS FILE
├── scripts/
│   └── test.sh                          # Docker-based test runner (CI only)
└── test_results/                        # Test output (gitignored)
```

## How to Run Tests

```bash
# Unit + integration tests (runs directly — no Docker needed for most tests):
cd cmd/pipedpeer && go test -count=1 -timeout 120s ./...

# Full E2E tests (Docker — includes SSH + nix integration, CI only):
./scripts/test.sh

# Build check:
cd cmd/pipedpeer && go build ./...

# Static analysis:
cd cmd/pipedpeer && go vet ./...
```

**Important**: The `go.mod` is at `cmd/pipedpeer/go.mod`, NOT at the repo root. All Go commands must be run from `cmd/pipedpeer/`.

## Current Test Count: 18 packages, ALL PASS

| Package | Notes |
|---------|-------|
| pipedpeer (root) | CLI integration tests |
| coordinator | 32 tests — scoring, filtering, placement, NATS lifecycle |
| daemonapi | HTTP + NATS lease lifecycle |
| discovery | mDNS discovery |
| execution | SSH command building |
| heartbeat | HTTP + NATS heartbeat lifecycle |
| identity | UUID generation, persistence |
| jobhistory | State machine transitions |
| natsbus | 8 tests — embedded server, pub/sub, request/reply, cross-bus |
| nixgen | Flake generation |
| pythondeps | Import → package resolution |
| registry | Memory + NATS KV backend tests |
| remote | SSH string parsing |
| resourceest | Memory estimation tiers |
| taskqueue | 3 tests — submit, retry with backoff, ordering |

## Architecture

### Transport Layer

**Dual transport: NATS preferred, HTTP fallback.**

- Daemon starts an embedded NATS server by default (or connects to `PIPEDPEER_NATS_URL`)
- All lease operations (accept, commit, complete, cancel) work over both transports
- Coordinator uses NATS request/reply when a bus is available, falls back to HTTP
- Heartbeat uses NATS pub/sub when bus available, falls back to HTTP POST

### Key Libraries

| Library | Purpose |
|---------|---------|
| `spf13/cobra` | CLI command framework |
| `spf13/viper` | Configuration (PIPEDPEER_ env vars) |
| `rs/zerolog` | Structured logging |
| `nats-io/nats.go` | NATS client (pub/sub, request/reply) |
| `nats-io/nats-server/v2` | Embedded NATS server |
| `go-chi/chi/v5` | HTTP router |
| `shirou/gopsutil/v4` | Cross-platform system metrics |
| `google/uuid` | UUID generation |
| `dustin/go-humanize` | Byte formatting/parsing |

### Lease-Based Reservation System

**Endpoints** on the daemon (`daemonapi/server.go`):
- `POST /v1/accept` → reserves resources, returns `lease_id` + `expires_at`
- `POST /v1/commit` → re-checks resources, transitions `reserved → running`
- `POST /v1/complete` → releases lease with `succeeded`/`failed` status
- `POST /v1/cancel` → submitter-only cancellation
- `GET /health` → reports `active_jobs`, `active_leases`, `reserved_mem`

**NATS equivalents**: `pipedpeer.daemon.<nodeID>.accept/commit/complete/cancel`

### Task Lifecycle State Machine (`jobhistory/states.go`)

```
queued → reserved → running → succeeded
                  ↘ expired     ↘ failed
         ↘ cancelled            ↘ cancelled
```

### Execution Fault Recovery (`ExecuteWithRetry`)

```
place → commit → execute → succeed? done.
                           fail?   → complete as failed → re-place → repeat
```

- Never auto-cancelled — only user ^C (SIGINT) stops the loop
- Failed commit → immediate re-placement
- Failed execution → reschedule on next node

### Runtime

- **CPU tasks**: Nix + bubblewrap directly on host (no Docker in production)
- **GPU tasks**: OCI containers from Nix (future)
- **Docker**: Only used in `main_integration_test.go` for CI/E2E testing

## Design Decisions

1. **Self-node is NOT exempt from memory filtering** — if the local node doesn't have capacity, it's rejected
2. **`--remote` mode is single-shot** — user explicitly chose a node, no auto-retry
3. **Auto-placement uses `ExecuteWithRetry`** — full lifecycle with signal handling
4. **NATS preferred, HTTP fallback** — graceful degradation when NATS unavailable
5. **Embedded NATS by default** — zero-config for single-node and development
6. **zerolog for warnings/errors, fmt.Printf for CLI progress** — user-facing output stays human-readable
7. **`/v1/complete` is idempotent** — completing a non-existent lease returns 200

## Blueprint Phase Status

| Phase | Description | Status |
|-------|-------------|--------|
| **Phase 1** | Dual-role nodes, registry heartbeats, script mode, lease reservations, state machine | ✅ **COMPLETE** |
| **Infra Migration** | Cobra CLI, NATS transport, zerolog, chi, gopsutil, JetStream task queue | ✅ **COMPLETE** |
| Phase 2 | Artifact store, content-addressed caching, data locality | Not started |
| Phase 3 | GPU capability model, pool-aware scheduling | Not started |
| Phase 4 | SDK mode with AST transform | Not started |
| Phase 5 | Optimizer learning from telemetry | Not started |
| Phase 6 | OpenTelemetry observability (deferred — low priority) | Not started |

## Key Files for Future Work

- **Blueprint**: `docs/DISTRIBUTED_SYSTEM_BLUEPRINT.md` — architecture spec
- **Coordinator**: `internal/coordinator/coordinator.go` — scheduling logic (NATS + HTTP)
- **Daemon**: `internal/daemonapi/server.go` — node-side lease management (chi + NATS)
- **NATS Bus**: `internal/natsbus/natsbus.go` — transport backbone
- **Task Queue**: `internal/taskqueue/queue.go` — JetStream durable queue
- **Main**: `cmd/pipedpeer/main.go` — Cobra CLI, daemon NATS wiring
- **App**: `internal/app/run.go` — nix build + SSH execution pipeline

## Gotchas

1. **go.mod is at `cmd/pipedpeer/go.mod`** — not repo root. Always `cd cmd/pipedpeer` for Go commands.
2. **Most tests run without Docker** — only `main_integration_test.go` needs Docker (gated by `PIPEDPEER_INTEGRATION` env var).
3. **All NATS tests use embedded servers** — each test creates an isolated instance with `t.TempDir()`. Zero external dependencies.
4. **`LeaseResult.DaemonPort`** — always use this field when constructing daemon URLs from a `LeaseResult`.
5. **`configSnapshot()`** copies `RetryInterval` — if you add new configurable fields to `Config`, also add them there.
6. **Daemon forked subprocess** — `runDaemon()` in main.go uses manual arg parsing (not Cobra) because it's invoked via `__daemon__` fork, not CLI.
