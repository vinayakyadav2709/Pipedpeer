# Without a usable /nix, keep a private store and bind it at /nix

Creating `/nix` needs root. When the system store is unusable, the store lives in the user's data directory and every nix command runs in a user+mount namespace that binds it at `/nix` (`d0c557c`, `src/internal/nixstore/nixstore.go`). Because it is bound at `/nix`, store paths stay identical across peers and remain valid cache keys. Machines with a working nix are left on it, since moving would re-fetch every closure.

## Consequences

- nix's own build sandbox is off in the private store: that costs build purity, not job isolation (`ba64e57`).
- The seed download is x86_64-linux only (`src/internal/nixstore/seed.go` `Seed`).
