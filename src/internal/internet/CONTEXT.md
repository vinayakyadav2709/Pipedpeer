# Joining across networks

How a daemon reaches peers that are not on its LAN: meet through an introducer, then open a direct, authenticated QUIC link. Covers `internet`, `direct`, `rendezvous`, `portmap`, `nattype`, `clustercfg` and the diagnostics in `src/netcheck.go`. Design detail: [system report 4.2](../../../docs/system-report.md#42-joining-across-networks).

## Language

**Introducer**:
A machine with a public address running `pipedpeer rendezvous`; an address book that also forwards punch triggers, and carries no job bytes ([ADR-0004](../../../docs/adr/0004-introducer-only-rendezvous.md)).
_Avoid_: relay, signalling server, coordinator

**Join**:
Pointing a daemon at an introducer and remembering the address, so a restarted daemon rejoins (`pipedpeer join`, `clustercfg`).
_Avoid_: connect, register

**Cluster ID**:
The hash of the shared token that partitions the introducer's address book; machines with different tokens never see each other.

**Candidate**:
One address a peer might be reachable at, tagged by how it was learned: `lan`, `mapped` or `reflex`, tried in that order.
_Avoid_: endpoint, address (unqualified)

**Reflexive address**:
This node's address as the introducer saw it; the least likely candidate to work.

**Mapped port**:
An external port a router agreed to forward, obtained through PCP, NAT-PMP or UPnP-IGD and renewed at half its granted lifetime.

**Punch**:
Both sides probing each other's candidates at the same moment, started by a `connect` datagram the introducer forwards.

**Collision**:
The fallback for a symmetric NAT: one side opens many sockets and the other sprays probes across the port space until one lands (birthday paradox).
_Avoid_: port prediction

**Symmetric NAT**:
A router that allocates a fresh external port per destination, so it has no address to publish.

**Direct link**:
A QUIC connection over the shared UDP port 38447, authenticated by the hello.
_Avoid_: tunnel, relay connection

**Hello**:
The signed message on a direct link that proves the node key, the TLS session and the cluster secret (ALPN `pipedpeer-direct/2`).

**Forwarder**:
A loopback TCP port opened per linked peer that forwards to that peer's daemon, so the rest of the system speaks plain HTTP to `127.0.0.1:<port>`.

**Unreachable peer**:
A peer the introducer lists but no direct path reaches; recorded with a reason, shown in `pipedpeer nodes`, and offered no work.

**Path**:
How a peer is reached: the candidate kind that answered, `punched` when a collision found it, or `punched-in` when the peer dialled us; shown per node.

## Invariants

- No job traffic crosses the introducer or a relay ([ADR-0005](../../../docs/adr/0005-no-relay.md)).
- A live direct link is never expired because the address book stopped listing the peer (`1cabedd`).
- A peer at `127.0.0.1` on a forwarder port is remote; tell it apart from this machine by machine key, never by host (`95e0712`).
- Poll interval stays under a NAT's ~30 s idle timeout (default 20 s), because registration keeps this node's own mapping alive.
- Hard version cut on the hello: a `/1` peer is refused with a version reason ([ADR-0006](../../../docs/adr/0006-hard-cuts-for-security-wire-changes.md)).

## Ports

| Port | Use |
|---|---|
| 38445/udp | Introducer address book (`clustercfg.DefaultPort`) |
| 38447/udp | Direct links (`internet.DefaultPort`) |
| 38443-38444/udp | `net-check` reflectors |
| 38446/udp | `relay-test` / `relay-connect` diagnostics only |

## Entry points

- Daemon wiring: `src/main.go` `runDaemon` (`internet.New`, `OnPeer`/`OnPeerGone` into `daemonapi`).
- Peer manager: `internet/internet.go` `Manager.Run`, `connect`, `raceOrCollide`, `linkUp`, `noPath` (backoff, capped at 5 min).
- Teardown: `expired()` picks peers to drop (never one with a live link), `dropLink` closes the link and its forwarder.
- Candidates and race: `direct/direct.go` `Gather`, `Prober.Race`; reasons are the `Reason*` constants.
- Collision: `direct/birthday.go` `SprayToward`, `OpenMany`.
- QUIC endpoint and hello: `direct/transport.go` `Listen`, `Endpoint.Dial`, `signHello`.
- Introducer: `rendezvous/rendezvous.go` `Server.Handle`, `RegisterWith`, `RequestPunch`.
- Port mapping: `portmap/portmap.go` `Map`, `NewKeeper`.
- Remembered introducer: `clustercfg/clustercfg.go` `Effective`.
- CLI: `src/netcheck.go` (`net-check`, `rendezvous`, `relay-test`, `relay-connect`); `src/main.go` `newJoinCmd`.
