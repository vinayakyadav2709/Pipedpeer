# Peers are a fallback Nix binary cache, asked per store path

A path that cache.nixos.org lacks, or cannot be reached for (an offline LAN, a slow link), is built again on every machine, although a same-architecture peer often holds it. Nix is told, per command, to ask this machine's daemon after cache.nixos.org; the daemon asks the peers.

- **The local layer** (`/nix-cache`, `internal/daemonapi/nixcache.go`, `nixcache_peers.go`): serves `nix-cache-info` (`Priority: 60`, behind cache.nixos.org's 40, `peercache.CacheInfo`), `<hash>.narinfo` and `nar/<name>`. Nix cannot send the cluster token, so GET and HEAD on `/nix-cache/` are exempt from it only when the caller is on loopback (`exceptLocalNixCache`). A loopback caller is this machine's nix, another local user, or an internet peer spliced in through the QUIC forwarder, which the cluster secret already authenticated. The store is world-readable on the machine anyway.
- **The peer layer** (`/v1/store/narinfo/{hash}`, `/v1/store/nar/{hash}`, `internal/daemonapi/peerstore.go`): token-gated like every other route; answers only from this node's store, with a narinfo signed by the node key, and never asks further peers, so a lookup cannot loop. The two routes share an 8-slot semaphore and return 503 when it is full; the asker moves on.
- **Fan-out**: one lookup asks, in parallel, the peers whose `Arch` equals this machine's `identity.System()` (store paths differ by system; a peer reporting no `Arch` is an old build and is not asked). The first narinfo that passes `peercache.Accept` wins; the NAR comes from the winner, then from the other peers that answered. Hits are remembered for 10 minutes; a lookup that every peer answered 404 or had rejected is remembered for 30 seconds; timeouts are not remembered. The 3 s narinfo budget bounds the wait. A 404 from an old peer is a miss, so the change needs no hard cut ([ADR-0006](0006-hard-cuts-for-security-wire-changes.md)): the peer layer's authentication is unchanged. Phones, introducer-only and pool-only nodes serve and ask like any other node.
- **Acceptance** (`peercache.Accept`, `internal/peercache/peercache.go`): the narinfo names the requested hash and store path, has a nonzero size, and carries a signature that verifies against a key in the cluster's trusted list, or the path is content-addressed. Nix then checks the NAR against the narinfo as for any cache.
- **Options per command, never nix.conf** (`peercache.NixArgs`, appended last to the nix argv in `internal/app/env.go`): a private store and a single-user nix get `extra-substituters`, `extra-trusted-public-keys` (the cluster keys), `narinfo-cache-negative-ttl 0` and `fallback true`. A multi-user nix-daemon ignores those options from an untrusted client, so it gets only `extra-substituters` and `fallback`, and only after the root-owned helper ([ADR-0018](0018-root-owned-nix-key-helper.md)) has written `extra-trusted-substituters = http://127.0.0.1:4242/nix-cache` (its `sync` verb; setup asks for sudo once to reinstall an older helper). `NixArgs` returns nothing, and logs why, when the daemon does not answer, the port is not the default one on a multi-user nix, or the helper has not run. A build with the cache off behaves exactly as before.
- **Phones** run the same code. A phone is aarch64, so only aarch64 peers ask it and it asks.

## Alternatives rejected

- **peerix, harmonia, nix-cache-beacon**: LAN-only, unencrypted, or one host serving one store. They would need a second discovery and trust system beside the cluster's authenticated links.
- **Editing the user's substituters or nix.conf**: a global, persistent change to a configuration pipedpeer does not own, and uninstall would have to undo it.
- **Putting peers into `substituters` directly**: nix cannot send the token and does not know the cluster membership.
- **Chunking one path across peers**: more protocol for a gain only on very large paths. The first valid hit wins.

## Consequences

- On a multi-user nix a miss is cached by nix for its default hour: the nix-daemon ignores an untrusted client's `narinfo-cache-negative-ttl`. cache.nixos.org is unaffected; only a path that a peer gains later waits.
- Setup's first Debian and Nix build on a phone runs before any daemon exists and uses cache.nixos.org only.
- On an offline LAN nix first waits out its connect timeout to cache.nixos.org for each path; the peer answers after that.
- Any local user can read the loopback route without the token; the store is world-readable on the machine already. Remote callers need the token on the peer layer.
- The warm-up that makes the common environment now starts from `pipedpeer setup`, so every machine, including the one that made the invite, has the environment before a script runs; a later machine finds those paths on its peers.
