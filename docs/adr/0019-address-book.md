# The address book keeps every path per node id and switches only to a verified one

The node store held one row per node with one host and port, and every source wrote it: LAN discovery overwrote a working internet link with an unreachable LAN address (experiment E5e), a peer that changed IP stayed at its old address until someone joined again by hand (E3a, E4), and a discovery sighting refreshed a dead row forever so it was never pruned. Now `src/internal/nodestore` keeps, per node id, every known path with its kind (`lan`, `internet`, `gossip`, `self`), last seen, last success and last error. A sighting (`SeePath`) only adds a candidate; a path becomes active only after it answers and proves the node id (`PathOK` plus `tlsid.Expect`), and an active path that still answers is kept (a verified LAN path beats a verified internet path). Discovery takes the host from the UDP packet's source, not from the address the peer advertised at start. Members gossip their own addresses, signed with their node key, over the token-gated API (`/v1/addresses`); gossip only adds candidates and hints for hole punching. Internet links survive a local address change by QUIC connection migration, and a hello from a node key that already has a link replaces the dead link at once.

Built on the project's own stack rather than libp2p, tsnet or iroh (research R7): the gap was bookkeeping, not transport.

## Consequences

- `/v1/nodes` keeps every field and adds `host`, `path` and `reason`; `/v1/peers/unreachable` keeps its shape and is served from node reasons.
- Pruning: no verified path for 15 min drops a node; a never-verified discovery or gossip node goes after 2 min; LAN paths unseen for 10 min and internet/gossip paths unseen for 1 h go; `self` is never pruned.
- An older node store schema is dropped and recreated with one log line.
- Adding nodes by hand and joining by IP are gone: addresses are learned, never typed.
