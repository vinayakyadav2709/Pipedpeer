# Membership and trust

Who counts as a member of a cluster, and how members recognise each other on the daemon port and on direct links. Covers `authtoken`, `tlsid`, `identity`, the hello in `direct/transport.go`, and `clustercfg` (where the token is stored). Closure signing trust lives in [Environments and closures](../nixstore/CONTEXT.md). Design detail: [system report 4.3](../../../docs/system-report.md#43-security-and-identity); posture: [SECURITY.md](../../../SECURITY.md).

## Language

**Cluster**:
The set of daemons holding the same token. Membership is the trust boundary: a job is arbitrary code, so any member can run code on any other.
_Avoid_: network, swarm, tailnet

**Token**:
The shared secret every daemon route requires, stored in `cluster.json` (mode 0600, with the introducers and roles; an old `auth_token` file migrates on first read). The first machine's `setup` or `invite` creates it before any daemon starts; `connect <code>` copies it. `PIPEDPEER_TOKEN` overrides it.
_Avoid_: password, API key

**Proof**:
A host's HMAC answer to a challenge, bound to the TLS session when there is one, that it must give before a client sends it the token ([ADR-0007](../../../docs/adr/0007-prove-before-token.md)).

**Pin**:
The node id a peer's certificate key must match (`tlsid.Expect(addr, nodeID)`); keyed by node id, not address, so a node is accepted at any address and a changed key for a known node id is refused ([ADR-0008](../../../docs/adr/0008-pinned-tls-and-key-addressed-nodes.md)).
_Avoid_: CA, certificate chain

**Node ID**:
The node key's fingerprint (16 hex); the one name a node has in the node store, on direct links and in pins.

**Node key**:
The ed25519 keypair in `<data>/pipedpeer/node.key`; its fingerprint is the node id, the daemon's TLS certificate is made from it, and it is the root of the nix signing key and signs gossiped addresses.

**Hello**:
The first message on a direct link: signs the TLS exporter of that session and proves the token, so it is good for one session and one cluster.

## Invariants

- The token is attached only to `/health` and `/v1/` paths, and only after the host proved it holds it; loopback is exempt.
- Token comparison is constant-time.
- Jobs inherit the token so intercepted calls reach their own daemon (`daemonapi/execution.go` `handleJobExec`, `authtoken.EnvVar`); without it they would fall back to local work silently.
- `tlsid.Install` and `authtoken.Install` run before the `__daemon__` branch (`src/main.go` `main`), or the daemon authenticates none of its own peer calls.
- A corrupt node key is refused, never regenerated.
- Security-relevant protocol changes refuse older builds ([ADR-0006](../../../docs/adr/0006-hard-cuts-for-security-wire-changes.md)).

## Entry points

- Token: `authtoken/token.go` `Current`, `Middleware`, `transport.RoundTrip`, `proof`, `Prove`, `Install`.
- TLS: `tlsid/tlsid.go` `EnsureCert`, `Expect`, `Forget`; one port for TLS and HTTP `tlsid/listener.go` `NewListener`; scheme memory `tlsid/dial.go`.
- Keys: `identity/identity.go` `GetOrCreate` (node ID = `Key().Fingerprint()`), `identity/key.go` `Key`, `KeyPair.Sign`.
- Hello: `direct/transport.go` `helloMAC`, `signHello`.
- Storage: `clustercfg/clustercfg.go` `Load`, `Save`; `authtoken.Current`, `authtoken.Set`.
- Tests isolate the token store: `src/main_test_isolation_test.go`.
