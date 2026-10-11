# Closures are signed by the node key and moved as a binary cache

`require-sigs = false` switched off signature checking for everything on a machine so one program could import unsigned closures. Exports are now signed with a nix key derived from the node's ed25519 identity, so the key that signs is the one that authenticated the link (`19bd494`, `src/internal/nixsign/nixsign.go`). `nix-store --export` drops signatures (`f0fe093`), so closures move as `nix copy` binary-cache archives with narinfos (`cdfcecc`, `src/internal/narpack/narpack.go`), sent only to peers that advertise the format (`4c3ca3b`). Setup trusts the cluster keys and leaves `require-sigs` on (`cf2e3ae`).

## Consequences

- A published key must hash to the fingerprint it is published under, and the trusted list is rewritten every poll so departed peers lose trust.
- Nix learns the keys per import: `narpack.Import` passes the trusted-keys file, plus the sender's key named in the archive manifest (checked with `nixsign.Verify` against the signer it names; the request is token-authenticated, the same bar polling sets), as `--option extra-trusted-public-keys`, so a fresh worker accepts peers it met after setup ran, with no nix.conf edit and signatures still required. A multi-user nix-daemon ignores that option from a user outside `trusted-users`; there the keys come only from nix.conf (`pipedpeer setup`, as root), so a key learned later needs setup again. A refused import is returned to the submitter in the upload response.
- Gaps remain: the legacy import path and the submitter's own upload use `--no-check-sigs`, and trusted keys are collected from the whole node store ([known issues](../known-issues.md#read-in-the-code-not-exercised)).
