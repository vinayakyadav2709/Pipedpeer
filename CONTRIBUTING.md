# Contributing

Thanks for taking the time to contribute.

## Where to send changes

Pull requests target **`dev`**, not `main`.

| Branch | Purpose |
|---|---|
| `main` | Stable, tagged releases only. Updated by promoting `dev`. |
| `dev` | Development stream. All PRs land here; nightly builds come from here. |
| `poc` | Internal experiments. Not a place to send PRs. |

## Before you open a PR

```bash
make fmt    # gofmt -w
make lint   # go vet
make test   # unit tests
```

CI runs the same checks plus `go test -race` and cross-compiles for
linux/darwin on amd64/arm64. A PR needs a green CI run to merge.

Integration tests need podman or docker and are not run on PRs:

```bash
make test-integration
```

For anything touching job placement or execution, `make lab-up` starts a
three-worker container cluster you can dispatch real jobs to.

## Code layout

```
src/                     the CLI and daemon (single Go module)
  internal/coordinator/  node selection, scoring, placement, retries
  internal/daemonapi/    the node's HTTP/WS API, leases, admission control
  internal/daemonctl/    client side: upload, execute, download results
  internal/nixgen/       environment (flake) generation
  internal/nodestore/    persistent view of known nodes (sqlite)
  internal/registry/     optional standalone node registry
scripts/                 build, test, lint, bootstrap, install
bench/                   sandbox and GPU benchmarks (separate module)
lab/                     container cluster for local multi-node testing
```

## Conventions

- Write tests for behaviour you would not want silently broken — especially
  around leases, admission control, and placement.
- Avoid tests that depend on wall-clock races. If a test must exercise expiry,
  give it a window wide enough to survive a loaded CI runner.
- Commit messages: a short imperative subject line. Conventional-commit
  prefixes (`feat:`, `fix:`) are picked up by the release changelog.

## Reporting bugs

Include your OS, whether the job ran isolated (`--isolate`), the relevant
output of `pipedpeer status` and `pipedpeer nodes list`, and the job ID so the
history under `~/.local/share/pipedpeer/jobs/<id>/` can be inspected.

Security issues go through [SECURITY.md](SECURITY.md), not the public tracker.
