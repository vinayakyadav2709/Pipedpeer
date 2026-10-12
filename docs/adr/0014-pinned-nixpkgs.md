# Generated flakes pin one nixpkgs revision

The store path is the cache key for closure transfer, and tracking `nixos-unstable` made two nodes build different store paths for the same script on different days, re-uploading hundreds of megabytes per job. Flakes pin an exact revision (`14d03e9`, `nixpkgsRef` in `src/internal/nixgen/flake.go`); bumping it invalidates every cached closure in the cluster.
