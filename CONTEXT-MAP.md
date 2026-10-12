# Context Map

## Contexts

- [Joining across networks](./src/internal/internet/CONTEXT.md): introducer, hole punching, direct QUIC links, forwarders
- [Nodes and placement](./src/internal/coordinator/CONTEXT.md): discovery, node store, scoring, leases, admission
- [Execution on a node](./src/internal/daemonapi/CONTEXT.md): upload, sandbox, memory ceilings, results, job dirs; `internal/clusterstats` builds the snapshot for `pipedpeer stats`, served with the /ui page by `/v1/stats` (`src/internal/daemonapi/ui.go`)
- [Interception](./src/internal/nixgen/CONTEXT.md): the Python shim, pool spill, shares, DDP sync
- [Environments and closures](./src/internal/nixstore/CONTEXT.md): import scan, flakes, stores, closure transfer, signing trust
- [Membership and trust](./src/internal/authtoken/CONTEXT.md): token, proof, pinned TLS, node keys

## Relationships

- **Joining → Nodes**: a linked internet peer enters the node store as `127.0.0.1:<forwarder port>` through `server.AddPeer` (`OnPeer` in `src/main.go` `runDaemon`); everything downstream treats it as an ordinary node.
- **Nodes → Execution**: the coordinator takes a lease on the chosen node, then the submitter uploads and execs against that node's daemon.
- **Execution → Interception**: the daemon injects the shim and `PIPEDPEER_*` env into each job (`src/internal/daemonapi/execution.go` `handleJobExec`); the shim spills back to its own daemon.
- **Interception → Environments**: spill goes only to peers holding the job's closure, and seeding is what gives it to them.
- **Interception → Nodes**: `run --offload` passes this machine's kernel boot id (`heartbeat.Machine`) as `PIPEDPEER_OFFLOAD`; pool spill (`src/internal/daemonapi/pool.go`) and DDP rank placement (`ExcludeMachine`, `src/internal/coordinator/coordinator.go`) skip every daemon on that machine.
- **Execution → Nodes**: when an exec connection drops, `networkUp` (`src/internal/app/lostconn.go`) probes a peer, the introducer and the default gateway; if any answers the node is gone and `ExecuteWithRetry` reruns the job (at most 3 failed runs; a node that refused the closure is skipped), otherwise the CLI detaches and waits.
- **Membership ↔ everything**: the token gates every daemon route; the node key addresses direct links and signs closures.

## Decisions

System-wide decisions are in [docs/adr/](./docs/adr/).
