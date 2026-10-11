# Peers pin self-signed certificates and are addressed by their key, not a CA

Each daemon makes its own certificate and peers pin it on first use, with the token making first contact meaningful; a CA would be infrastructure to run for machines the operator already identified by giving them the token (`49aac62`, `src/internal/tlsid/tlsid.go` `CheckOrPin`). Every node has an ed25519 key and its address across networks is that key's fingerprint, so taking a node's place needs its private half (`25ad54a`, `src/internal/identity/key.go`). The direct-link hello signs the TLS exporter of its own session and proves the cluster secret, so it cannot be replayed to a third node (`b5b943c`).

## Consequences

- A changed certificate is refused, never downgraded; `pipedpeer auth forget` clears a pin.
- A corrupt key file is refused rather than regenerated, since a fresh identity would look like a stranger to every peer.
