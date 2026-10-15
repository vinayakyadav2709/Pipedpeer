# Joining across networks

How a daemon reaches peers that are not on its LAN: meet through an introducer, then open a direct, authenticated QUIC link. Covers `internet`, `direct`, `rendezvous`, `portmap`, `nattype`, `clustercfg`, `invite` and the `invite` / `connect` commands. Design detail: [system report 4.2](../../../docs/system-report.md#42-joining-across-networks).

## Language

**Introducer**:
An address book that also forwards punch triggers and carries no job bytes; every daemon with a token serves one for its own cluster id on 4244/udp, and a public server can run as `start --introducer-only` (takes no jobs) ([ADR-0004](../../../docs/adr/0004-introducer-only-rendezvous.md)).
_Avoid_: relay, signalling server, coordinator

**Invite code**:
`pp1-` + base64url(JSON of the cluster secret and up to 6 introducer addresses) + crc32, printed by `pipedpeer invite` as one `pipedpeer connect <code>` line (`invite.Encode`). A damaged or newer-format code is refused with its own error.

**Connect**:
Saving the code's secret and introducers to `cluster.json`, restarting the daemon once and reporting connected (exit 0), partly (exit 2) or failed (exit 1). The daemon remembers verified member addresses as further introducers (at most 8), so a restarted daemon rejoins.
_Avoid_: join (the old command), register

**Cluster ID**:
The hash of the shared token that partitions the introducer's address book; an introducer answers only its own; machines with different tokens never see each other.

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
A QUIC connection over the shared UDP port 4243, authenticated by the hello. It migrates to a new local address when the interface changes (quic-go `AddPath`/`Probe`/`Switch`); a hello from a node key that already has a link replaces the old link.
_Avoid_: tunnel, relay connection

**Hello**:
The signed message on a direct link that proves the node key, the TLS session and the cluster secret (ALPN `pipedpeer-direct/3`).

**Forwarder**:
A loopback TCP port opened per linked peer that forwards to that peer's daemon, so the rest of the system speaks plain HTTP to `127.0.0.1:<port>`.

**Unreachable peer**:
A peer the introducer lists but no direct path reaches; recorded with a reason on its node row, shown in `pipedpeer status` and `doctor`, and offered no work.

**Link path**:
How a direct link was made: the candidate kind that answered, `punched` when a collision found it, or `punched-in` when the peer dialled us. Not the node store's path kind (`lan`/`internet`/`gossip`/`self`, [Nodes and placement](../coordinator/CONTEXT.md)).

**Introducer state**:
Per introducer: last reply, last error, members it listed (`Manager.Introducers`, `GET /v1/introducers`). The daemon logs once when one has not answered for 60 s.

## Invariants

- No job traffic crosses the introducer or a relay ([ADR-0005](../../../docs/adr/0005-no-relay.md)).
- A live direct link is never expired because the address book stopped listing the peer (`1cabedd`).
- A peer at `127.0.0.1` on a forwarder port is remote; tell it apart from this machine by machine key, never by host (`95e0712`).
- Registration with every introducer every ~30 s and at once after a local address change; it keeps this node's own NAT mapping alive.
- Hard version cut on the hello: an older ALPN is refused with a version reason ([ADR-0006](../../../docs/adr/0006-hard-cuts-for-security-wire-changes.md)).

## Ports

| Port | Use |
|---|---|
| 4244/udp | Introducer (`clustercfg.DefaultPort`) |
| 4243/udp | Direct links (`internet.DefaultPort`) |

## Entry points

- Daemon wiring: `src/daemon_cmd.go` `runDaemon` (`internet.New`, `OnPeer`/`OnPeerGone` into `daemonapi`).
- Peer manager: `internet/internet.go` `Manager.Run`, `connect`, `raceOrCollide`, `linkUp`, `noPath` (backoff, capped at 5 min).
- Back after an outage: `kick` lifts a peer's penalty and restarts an attempt begun in the outage when a lost introducer answers again, the peer asks us to punch, or its probe reaches us; a lost introducer is re-registered with every `lostAfter` (5 s).
- Teardown: `expired()` picks peers to drop (never one with a live link), `dropLink` closes the link and its forwarder.
- Candidates and race: `direct/direct.go` `Gather`, `Prober.Race`; reasons are the `Reason*` constants.
- Collision: `direct/birthday.go` `SprayToward`, `OpenMany`.
- QUIC endpoint and hello: `direct/transport.go` `Listen`, `Endpoint.Dial`, `signHello`.
- Introducer: `rendezvous/rendezvous.go` `Server.Handle`, `RegisterWith`, `RequestPunch`.
- Port mapping: `portmap/portmap.go` `Map`, `NewKeeper`.
- Introducers: `internet.Config.Introducers`, `ServeIntroducer`, `Manager.Introducers`, `Manager.Hint` (gossiped candidates join the punch set).
- Config: `clustercfg/clustercfg.go` `Load`, `Save`, `RememberIntroducers`; codes `invite/invite.go` `Encode`, `Decode`.
- CLI: `src/invite_cmd.go` `newInviteCmd`, `src/connect_cmd.go` `newConnectCmd`.
