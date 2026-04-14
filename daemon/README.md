# Daemon Package Scaffold

This folder is the target home for the blueprint-aligned refactor.

Planned package boundaries:

- `internal/registry`: node registration, leases, liveness
- `internal/scheduler`: candidate selection, reservation, dispatch
- `internal/executor`: local execution backends (nix-native, oci-gpu)
- `internal/artifacts`: upload, fetch, materialization, cache
- `internal/sdkmode`: sdk transform orchestration and execution routing

Current runnable implementation remains in `cmd/pipedpeer`.
