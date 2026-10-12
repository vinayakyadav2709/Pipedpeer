# Security-relevant wire changes are hard cuts; everything else is additive

Ordinary protocol changes only add fields and both sides ignore what they do not recognise, so mixed versions interoperate (`bb412e3`; TLS and HTTP share a port so a cluster encrypts as nodes upgrade, `49aac62`; signed closures are sent only to peers that advertise them, `4c3ca3b`). A change that closes an impersonation or token-leak hole is not negotiated: the direct hello moved to ALPN `pipedpeer-direct/2` and refuses `/1` peers with a version reason (`b5b943c`, `ALPN` in `src/internal/direct/transport.go`), and token proof refuses builds that cannot answer the challenge (`4885a42`).

## Consequences

Upgrade every node of a cluster together across these commits. A downgrade path would hand the attacker the old behaviour.
