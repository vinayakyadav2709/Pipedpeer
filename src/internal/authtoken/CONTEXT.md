# Membership and trust

Who counts as a member of a cluster, and how members recognise each other on the daemon port and on direct links. Covers `authtoken`, `tlsid`, `identity`, the hello in `direct/transport.go`, and `src/auth_cmd.go`. Closure signing trust lives in [Environments and closures](../nixstore/CONTEXT.md). Design detail: [system report 4.3](../../../docs/system-report.md#43-security-and-identity); posture: [SECURITY.md](../../../SECURITY.md).

## Language

**Cluster**:
The set of daemons holding the same token. Membership is the trust boundary: a job is arbitrary code, so any member can run code on any other.
_Avoid_: network, swarm, tailnet

**Token**:
The shared secret set with `pipedpeer auth set` (file mode 0600) that every daemon route requires once set.
_Avoid_: password, API key

**Open daemon**:
A daemon with no token: every route answers anyone who reaches the port.

**Proof**:
A host's HMAC answer to a challenge, bound to the TLS session when there is one, that it must give before a client sends it the token ([ADR-0007](../../../docs/adr/0007-prove-before-token.md)).

**Pin**:
A peer certificate's fingerprint recorded on first contact; a later mismatch is refused, not downgraded. Cleared with `pipedpeer auth forget`.
_Avoid_: CA, certificate chain

**Node ID**:
The UUID a daemon is listed under in the node store.

**Node key**:
The ed25519 keypair in `<data>/pipedpeer/node.key`; its fingerprint is the node's address across networks and the root of its nix signing key.
_Avoid_: node ID (a different value)

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
- TLS: `tlsid/tlsid.go` `EnsureCert`, `CheckOrPin`, `Forget`; one port for TLS and HTTP `tlsid/listener.go` `NewListener`; scheme memory `tlsid/dial.go`.
- Keys: `identity/identity.go` `GetOrCreate` (node ID), `identity/key.go` `Key`, `KeyPair.Sign`.
- Hello: `direct/transport.go` `helloMAC`, `signHello`.
- CLI: `src/auth_cmd.go` (`auth set|show|clear|forget`).
- Tests isolate the token store: `src/main_test_isolation_test.go`.
