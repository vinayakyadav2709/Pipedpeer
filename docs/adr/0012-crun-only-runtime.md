# crun is the only job runtime

Jobs run in an OCI bundle under `crun`: a single static binary that installs into `~/.local/bin` without root, with no container daemon (`d8bbede`, `src/internal/setup/crun.go` `installCrunRelease`). Docker and podman are not runtime options. With a system nix store, a node without crun or user namespaces runs the job unisolated with a warning (`3609328`); with a private store the sandbox is what mounts `/nix`, so the job fails instead (`d0c557c`, `src/internal/daemonapi/execution.go` `handleJobExec`, `sandboxRequired`).

## Consequences

The sandbox is a hardening layer, not a security boundary: uid 0 inside, no network namespace, no seccomp ([system report 6.2](../system-report.md#62-limitations)).
