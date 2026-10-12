# Nodes and placement

Who is in the cluster, what each node can take, and how one job gets one node under a lease. Covers `discovery`, `nodestore`, `heartbeat`, `gpu`, `registry`, `resourceest`, `coordinator` and the lease handlers in `daemonapi`. Design detail: [system report 4.1, 4.4, 4.5](../../../docs/system-report.md#41-discovery-on-a-lan).

## Language

### Membership

**Node**:
One daemon, identified by its node ID, with an endpoint, capabilities and last observed load. Two daemons on one machine are two nodes.
_Avoid_: peer (when you mean the record), worker, host

**Node store**:
The daemon's SQLite table of every node it knows; the single source of truth that the CLI and dashboard read through `GET /v1/nodes`.
_Avoid_: peer list, registry

**Discovery**:
Finding LAN nodes by UDP broadcast/multicast, falling back to a TCP `/health` sweep of the local /24 when broadcast finds nothing.
_Avoid_: mDNS (it is not mDNS)

**Manual node**:
A node added with `pipedpeer nodes add`; persisted and never pruned (`nodestore/store.go` `PruneStale`).

**Registry**:
The optional standalone directory (`pipedpeer registry`, HTTP or NATS) a daemon can heartbeat to; not needed by a LAN or introducer cluster.

**Capabilities**:
The static facts a node advertises: cores, memory, arch, GPUs and their VRAM (`heartbeat.Capabilities`).

**Healthy**:
A node in state `healthy` whose last successful poll is inside the cutoff; only healthy nodes are placed on. Stale auto-discovered nodes are later pruned, manual ones kept.

**Roommates**:
Daemons that share one physical machine, told apart by machine key; their promised memory is counted together.

### Placement

**Hard filter**:
A requirement a node must meet to be considered: architecture, GPU presence or VRAM, memory.

**Score**:
The ranking among nodes that pass the filters, from free cores, free memory, load and GPU headroom (`scoreNode`, `scoreGPUNode`).

**Lease**:
A node's promise to run one job: requested by `accept`, made `running` by `commit`, kept alive by `renew`, ended by `complete` or `cancel`, and swept when it expires.
_Avoid_: reservation (the lease state is `reserved`; the thing is a lease), booking

**Admission**:
The daemon's atomic check, under one lock, of concurrency cap, memory and GPU before it grants a lease.

**Memory estimate**:
The memory a job is expected to need, from the first tier that answers: user flag, historical peak, input file size, imports.

## Invariants

- One job, one lease, one node; a job is re-placed only when its lease expires ([ADR-0002](../../../docs/adr/0002-never-slower-than-local.md)).
- Admission is one critical section from cap check to lease insert (`71d2ced`); splitting it over-admits under concurrent submitters.
- Placement and display both respect freshness: a dead node is never reported healthy (`fc58a14`).
- The coordinator runs in the submitting CLI; there is no scheduler process ([ADR-0003](../../../docs/adr/0003-no-head-node.md)).

## Entry points

- Choose a node: `coordinator/coordinator.go` `FindNode`; retry loops `PlaceWithRetry`, `ExecuteWithRetry`; scoring `scoreNode`, `scoreGPUNode`.
- Lease routes: `daemonapi/server.go` `buildRouter` (`/v1/accept` … `/v1/cancel`); admission `processAccept`; expiry `sweepExpired`; roommates `reservedOnThisMachine`.
- Peer polling into the node store: `daemonapi/server.go` `pollAllNodes`, `pollOne`.
- Node store: `nodestore/store.go` `UpsertNode`, `AddManual`, `ListHealthy`, `PruneStale`.
- Discovery: `discovery/discovery.go` `Discover`, `discovery/tcp.go` `DiscoverTCP`; wired in `src/main.go` `runDaemon` (`SetDiscoverFn`).
- Capabilities: `heartbeat/heartbeat.go` `Capabilities`; GPU detection `gpu.Detect`.
- Memory estimate: `resourceest/estimator.go` (tiers `user`, `historical`, `filesize`, `import`).
- Multi-task fan-out (`pipedpeer map`): `app/mapset.go`, `src/main.go` `newMapCmd`.
- DDP ring choice: `ddpplace/ddpplace.go` `Select`, `src/main.go` `runDDP`.
