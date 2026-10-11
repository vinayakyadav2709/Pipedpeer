# Environments and closures

How a script's Python environment is built as a Nix closure, where the store lives, and how closures move between nodes and are trusted there. Covers `pythondeps`, `nixgen/flake.go`, `nixgen/wheelpin.go`, `nixstore`, `nixsign`, `narpack`, `tarcodec`, `app/env.go`, `daemonctl/closurediff.go`, the store handlers in `daemonapi`, and the nix steps of `setup`. Design detail: [system report 4.6](../../../docs/system-report.md#46-reproducing-the-environment) and [4.7](../../../docs/system-report.md#47-closure-transfer-and-caches).

## Language

### Building

**Environment**:
The interpreter and packages one script needs, built once per submit as a Nix flake and run as `bin/run` from its store path.
_Avoid_: venv, image, container

**Import scan**:
Reading the script's import lines to name its top-level modules.

**Package table**:
The fixed map from module name to nixpkgs attribute (35 entries); an import not in it adds nothing to the environment.
_Avoid_: resolver

**uv.lock**:
When present in the project root, its package list replaces the import scan's external imports (`app/env.go` `BuildEnvironment`); both still go through the package table.

**Wheel pin**:
A table row that fetches a prebuilt wheel (torch CUDA) as a fixed-output derivation, because nixpkgs builds it only from source.

**Pinned nixpkgs**:
The single nixpkgs revision every generated flake uses ([ADR-0014](../../../docs/adr/0014-pinned-nixpkgs.md)).

### Stores

**Store path**:
`/nix/store/<hash>-<name>`; the cache key for every closure transfer between nodes.

**Closure**:
A store path plus everything it references; the unit that travels.

**System store**:
A `/nix` the machine already has, single- or multi-user.

**Private store**:
A store in the user's data directory bound at `/nix` inside a user namespace, used only when no system store is usable ([ADR-0013](../../../docs/adr/0013-rootless-private-nix-store.md)).

**Seed**:
Downloading a static nix into a private store.

### Moving

**Closure diff**:
Asking a peer which paths of a closure it lacks (`/v1/store/missing`) and sending only those.

**Signed archive**:
A tar of binary-cache entries (narinfo plus nar) that keeps signatures; sent only to peers that advertise it ([ADR-0011](../../../docs/adr/0011-signed-closures-via-binary-cache.md)).
_Avoid_: NAR export (the legacy form)

**Legacy export**:
A gzipped `nix-store --export` stream; drops signatures and is imported with `--no-check-sigs`.

**Closure cache**:
The daemon's on-disk LRU of closure archives, bounded at startup and on every transfer.

**Seeding**:
Pushing a closure to peers after an upload so pool spill can use them; only peers that hold the closure get spill work.
_Avoid_: broadcast (the function name, not the concept)

**Trusted keys**:
The nix public keys of cluster nodes, each proved against the fingerprint it is published under, rewritten every poll. Each signed import hands them to nix on the command line (`extra-trusted-public-keys`); a multi-user nix-daemon ignores that from an untrusted user, so there setup writes them to nix.conf.

## Invariants

- Same script, same pin, same store path on every node: the store path is the cache key.
- A received signed archive imports the root this node asked for, never the one the sender's manifest names (`4c3ca3b`).
- A signed archive holds only the paths one peer lacked, so it is never cached or forwarded as a closure.
- Setup never writes `require-sigs = false`; a machine that already has it is left alone (`cf2e3ae`).
- A signed import never passes `--no-check-sigs`; it adds the cluster keys to what nix already trusts, for that one command.
- Builds name the flake as `path:` (`app/env.go` `flakeRef`), so a `$HOME` that is a git checkout cannot make nix refuse the untracked `flake.nix` (`35a0050`).

## Entry points

- Build: `app/env.go` `BuildEnvironment`; flake `nixgen/flake.go` `nixpkgsRef`, `GenerateFlake`; wheel pins `nixgen/wheelpin.go` `pinFor`.
- Imports: `pythondeps/deps.go` `nixpkgsMapping` (the package table), `ExtractImportScan`, `ParseUVLock`, `ResolvePackages` (unmapped dropped).
- Store: `nixstore/nixstore.go` `Private`, `Cmd`; `nixstore/seed.go` `Seed`.
- Submitter upload and diff: `daemonctl/execution.go` `UploadJob`, `daemonctl/closurediff.go` `seedClosure`.
- Daemon store routes: `daemonapi/narcache.go` `handleStoreMissing`, `handleStoreCheck`, `handleStoreImport`, `importSignedClosure`; seeding `daemonapi/server.go` `broadcastClosure`, `importStoreOnPeer`; signed path `daemonapi/signedclosure.go` `signedClosureFor`.
- Archives: `narpack/narpack.go` `Publish`, `Pack`, `Import`; pruning `narpack/prune.go` `Prune`; compression sniffing `tarcodec/`.
- Signing and trust: `nixsign/nixsign.go` `SignPaths`, `WriteTrustedKeys`; `nixsign/collect.go` `Collect`; `daemonapi/server.go` `refreshTrustedKeys`; setup `setup/setup.go` `trustClusterKeys`.
