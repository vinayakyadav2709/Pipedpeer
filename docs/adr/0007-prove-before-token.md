# A host gets the cluster token only after proving it already holds it

The LAN sweep and a stale address book reach hosts nobody vetted, and the token used to travel to all of them. The outbound transport now sends a challenge first and attaches the token only after an HMAC answer, bound to the TLS exporter when there is a TLS session; proofs are cached per process and loopback is exempt (`4885a42`, `src/internal/authtoken/token.go` `transport.RoundTrip`, `proof`). The token is attached only to `/health` and `/v1/` paths (`2eb3ce7`, `isDaemonAPI`).
