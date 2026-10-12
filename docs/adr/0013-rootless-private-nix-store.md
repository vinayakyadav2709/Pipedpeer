# Without a usable /nix, keep a private store and bind it at /nix

Creating `/nix` needs root. When the system store is unusable, the store lives in the user's data directory and every nix command runs in a user+mount namespace that binds it at `/nix` (`d0c557c`, `src/internal/nixstore/nixstore.go`). Because it is bound at `/nix`, store paths stay identical across peers and remain valid cache keys. Machines with a working nix are left on it, since moving would re-fetch every closure.

## Consequences

- nix's own build sandbox is off in the private store: that costs build purity, not job isolation (`ba64e57`).
- The seed download is x86_64-linux only (`src/internal/nixstore/seed.go` `Seed`).

## Superseded in part (2026-10-12)

Setup used to try the private store first and run the Determinate installer only when seeding failed. It now prefers a system nix: an installed nix is used, and where root is available (running as root, passwordless sudo, or sudo with a terminal) the Determinate installer runs first. The private store remains the fallback for a machine without root, or after the installer fails, and `PIPEDPEER_FORCE_PRIVATE_NIX=1` still forces it (`setup.chooseNix`, `installNix`). Reason: the system nix needs no user namespaces, builds with nix's sandbox on, and is what the cluster's other setup steps (the key helper, [ADR-0018](0018-root-owned-nix-key-helper.md)) are built around.
