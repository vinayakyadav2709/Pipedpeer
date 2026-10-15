# Introducers introduce and carry nothing; every daemon is one

An introducer is an address book plus one `connect` op that forwards a single punch-trigger datagram; after that, peers talk directly and the introducer can go away (`bb412e3`, `src/internal/rendezvous/rendezvous.go` `Server.Handle`). A live direct link is never expired because an introducer stopped answering (`1cabedd`), and the integration check transfers with the introducer suspended to prove nothing passes through it (`0d5d12d`).

Revised for the address-book phase: the single standalone rendezvous process made one machine's IP the cluster's single point of failure, and when that IP changed the cluster never recovered (experiment E3b, 2026-10-15). Now every daemon with a cluster token serves the introducer for its own cluster id on 4244/udp (`ServeIntroducer` in `src/internal/internet`), and a node registers with every introducer it knows: the addresses in its invite code plus member addresses it has verified, remembered in `cluster.json` (at most 8). A public server can join as `pipedpeer start --introducer-only`, which introduces and takes no jobs ([hosted introducer](../hosted-introducer.md)).

## Consequences

- A new machine joins as long as any one introducer in its list answers; `pipedpeer status` / `doctor` show each introducer's last reply or error, and the daemon logs once when one stops answering for 60 s.
- An introducer answers only its own cluster id; a request for another cluster is refused.
- `connect` is the only op that sends to a caller-chosen address, so it is rate limited per source, refused across clusters, and forwards only candidates the named node registered itself.
