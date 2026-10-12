# The rendezvous server introduces and carries nothing

`pipedpeer rendezvous` is an address book plus one `connect` op that forwards a single punch-trigger datagram; after that, peers talk directly and the introducer can go away (`bb412e3`, `src/internal/rendezvous/rendezvous.go` `Server.Handle`). A live direct link is never expired because the introducer stopped answering (`1cabedd`), and the integration check transfers with the introducer suspended to prove nothing passes through it (`0d5d12d`).

## Consequences

- One introducer per cluster and no failover: a new join needs it up ([known issues](../known-issues.md#limitations-by-design)).
- `connect` is the only op that sends to a caller-chosen address, so it is rate limited per source, refused across clusters, and forwards only candidates the named node registered itself.
