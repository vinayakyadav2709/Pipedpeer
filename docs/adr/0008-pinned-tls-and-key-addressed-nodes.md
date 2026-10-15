# One identity: a node is its key, and peers pin that key, not a CA

Each daemon makes its own certificate and peers pin it, with the token making first contact meaningful; a CA would be infrastructure to run for machines the operator already identified by giving them the token (`49aac62`, `src/internal/tlsid/tlsid.go`). Every node has an ed25519 key (`25ad54a`, `src/internal/identity/key.go`), so taking a node's place needs its private half. The direct-link hello signs the TLS exporter of its own session and proves the cluster secret, so it cannot be replayed to a third node (`b5b943c`).

Revised for the address-book phase: there used to be three identities (a random UUID node id, the ed25519 key's fingerprint for internet links, and a separate P-256 TLS certificate pinned by `host:port`), and the internet manager reported peers by fingerprint while the node store keyed rows by UUID, which produced phantom rows (audit B3); a pin keyed by address also refused a different machine that later got a known IP. Now the node id **is** the key fingerprint (16 hex), the TLS certificate is made from the same ed25519 key, and pins are keyed by node id (`tlsid.Expect(addr, nodeID)`): a certificate is accepted at any address when its key matches the expected node id.

## Consequences

- A changed key for a known node id is refused, never downgraded; a new machine at an old peer's IP is just a new node.
- Upgrading to this identity is a hard cut ([ADR-0006](0006-hard-cuts-for-security-wire-changes.md)): old node ids and pins do not carry over, so every node of a cluster upgrades together.
- A corrupt key file is refused rather than regenerated, since a fresh identity would look like a stranger to every peer.
