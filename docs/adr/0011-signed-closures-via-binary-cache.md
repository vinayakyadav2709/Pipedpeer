# Closures are signed by the node key and moved as a binary cache

`require-sigs = false` switched off signature checking for everything on a machine so one program could import unsigned closures. Exports are now signed with a nix key derived from the node's ed25519 identity, so the key that signs is the one that authenticated the link (`19bd494`, `src/internal/nixsign/nixsign.go`). `nix-store --export` drops signatures (`f0fe093`), so closures move as `nix copy` binary-cache archives with narinfos (`cdfcecc`, `src/internal/narpack/narpack.go`), sent only to peers that advertise the format (`4c3ca3b`). Setup trusts the cluster keys and leaves `require-sigs` on (`cf2e3ae`).

## Consequences

- A published key must hash to the fingerprint it is published under, and the trusted list is rewritten every poll so departed peers lose trust.
- Gaps remain: the legacy import path and the submitter's own upload use `--no-check-sigs`, and trusted keys are collected from the whole node store ([known issues](../known-issues.md#read-in-the-code-not-exercised)).
