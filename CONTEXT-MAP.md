# Context Map

## Contexts

- [Joining across networks](./src/internal/internet/CONTEXT.md): invite/connect, introducers (one in every daemon), hole punching, direct QUIC links, roaming, forwarders
- [Nodes and placement](./src/internal/coordinator/CONTEXT.md): discovery, address book (`internal/nodestore`: every path per node id), signed address gossip, roles, scoring, leases, admission
- [Execution on a node](./src/internal/daemonapi/CONTEXT.md): upload, sandbox, memory ceilings, results, job dirs; `internal/clusterstats` builds the snapshot served with the /ui page by `/v1/stats`, and `/v1/history` serves run summaries and run records (`src/internal/daemonapi/ui.go`); `internal/runrec` is the run record (simple and detailed views, CSV/JSON export)
- [Interception](./src/internal/nixgen/CONTEXT.md): the Python shim (`src/internal/nixgen/shim/*.py`, embedded), pool spill, shares, DDP sync and the one-GPU-or-split decision
- [Environments and closures](./src/internal/nixstore/CONTEXT.md): import scan, flakes, stores, closure transfer, signing trust
- [Membership and trust](./src/internal/authtoken/CONTEXT.md): `cluster.json` (token, introducers, roles), invite codes, proof, one identity (node id = key fingerprint), TLS pinned by node id

## Relationships

- **Joining → Nodes**: a linked internet peer enters the node store as `127.0.0.1:<forwarder port>` through `server.AddPeer` (`OnPeer` in `src/daemon_cmd.go` `runDaemon`) as an `internet` path of that node id; a LAN path for the same node id wins once it is verified. Everything downstream treats it as an ordinary node.
- **Nodes → Execution**: the coordinator takes a lease on the chosen node, then the submitter uploads and execs against that node's daemon.
- **Execution → Interception**: the daemon injects the shim and `PIPEDPEER_*` env into each job (`src/internal/daemonapi/execution.go` `handleJobExec`); the shim spills back to its own daemon.
- **Interception → Environments**: spill goes only to peers holding the job's closure, and seeding is what gives it to them.
- **Interception → Nodes**: `run --offload` passes this machine's kernel boot id (`heartbeat.Machine`) as `PIPEDPEER_OFFLOAD`; pool spill (`src/internal/daemonapi/pool.go`) and DDP rank placement (`ExcludeMachine`, `src/internal/coordinator/coordinator.go`) skip every daemon on that machine.
- **Execution → Nodes**: when an exec connection drops, `networkUp` (`src/internal/app/lostconn.go`) probes a peer, the introducers and the default gateway; if any answers the node is gone and `ExecuteWithRetry` reruns the job (at most 3 failed runs; a node that refused the closure is skipped), otherwise the CLI detaches and waits.
- **Membership ↔ everything**: the token gates every daemon route; the node key addresses direct links and signs closures.

## Decisions

System-wide decisions are in [docs/adr/](./docs/adr/).
