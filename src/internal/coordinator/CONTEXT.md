# Nodes and placement

Who is in the cluster, what each node can take, and how one job gets one node under a lease. Covers `discovery`, `nodestore`, `heartbeat`, `gpu`, `resourceest`, `coordinator`, the node poller and gossip in `daemonapi` and the lease handlers in `daemonapi`. Design detail: [system report 4.1, 4.4, 4.5](../../../docs/system-report.md#41-discovery-on-a-lan).

## Language

### Membership

**Node**:
One daemon, identified by its node ID (key fingerprint), with one or more paths, capabilities and last observed load. Two daemons on one machine are two nodes.
_Avoid_: peer (when you mean the record), worker, host

**Node store**:
The daemon's SQLite address book of every node it knows and every path to each; the single source of truth that the CLI and dashboard read through `GET /v1/nodes`.
_Avoid_: peer list, registry

**Discovery**:
Finding LAN nodes by UDP broadcast/multicast on 4242/udp, falling back to a TCP `/health` sweep of the interface's prefix (docker, veth, bridge and tailscale interfaces skipped) when broadcast finds nothing. The host is the reply's UDP source, never the address the peer advertised.
_Avoid_: mDNS (it is not mDNS)

**Path**:
One way to reach a node: kind (`lan`, `internet`, `gossip`, `self`), host, port, last seen, last OK, last error. A sighting (`SeePath`) adds a candidate; only a verified path (`PathOK`) becomes active, and an active path that still answers is kept (verified lan beats verified internet) ([ADR-0019](../../../docs/adr/0019-address-book.md)).

**Gossip**:
Each member's own addresses, signed with its node key and served at `GET /v1/addresses`; pulled from healthy peers at most once a minute; records are candidates, never truth.

**Role**:
`pool-only` (a phone: pool chunks, no whole jobs) or `introducer-only` (a hosted introducer: nothing), from `cluster.json`, advertised as the `role` capability. Coordinator, DDP and admission skip any node with a role; pool spill skips only `introducer-only`.

**Capabilities**:
The static facts a node advertises: cores, memory, arch, GPUs and their VRAM, `role`, `version` (`heartbeat.Capabilities`).

**Healthy**:
A node in state `healthy` whose last successful poll is inside the cutoff; only healthy nodes are placed on. Pruned: no verified path for 15 min, or a never-verified discovery/gossip node after 2 min; a sighting alone never refreshes a node.

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
- Lease routes: `daemonapi/server.go` `buildRouter` (`/v1/accept` … `/v1/complete`); admission `processAccept`; expiry `sweepExpired`; roommates `reservedOnThisMachine`.
- Peer polling into the node store: `daemonapi/nodes.go` `pollAllNodes`, `pollOne`; gossip `daemonapi/gossip.go`.
- Node store: `nodestore/store.go` `UpsertNode`, `SeePath`, `PathOK`, `PathFailed`, `Paths`, `PruneStale`.
- Discovery: `discovery/discovery.go` `Discover`, `discovery/tcp.go` `DiscoverTCP`; wired in `src/daemon_cmd.go` `runDaemon` (`SetDiscoverFn`).
- Capabilities: `heartbeat/heartbeat.go` `Capabilities`; GPU detection `gpu.Detect`.
- Memory estimate: `resourceest/estimator.go` (tiers `user`, `historical`, `filesize`, `import`).
- Multi-task fan-out (hidden `map` command): `app/mapset.go`, `src/main.go` `newMapCmd`.
- DDP ring choice: `ddpplace/ddpplace.go` `Select`, `src/ddp_cmd.go` `runDDP`.
