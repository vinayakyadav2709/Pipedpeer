# Pipedpeer

`pipedpeer run main.py` runs an unmodified Python script on other machines. Each machine runs one daemon (Go); the CLI builds the script's environment as a Nix closure, picks a node, ships closure and workspace, and runs the job in a crun sandbox. A Python shim injected into the job spreads `Pool`, joblib, pandas, numpy and torch training work across nodes when that is faster. Machines join with `pipedpeer connect <code>` (from `pipedpeer invite`); on different networks they meet through an introducer (every daemon is one for its own cluster) and then connect directly; nothing is relayed.

## Find your way

Coming back to the project? Start with [docs/next-steps.md](docs/next-steps.md).

1. [CONTEXT-MAP.md](CONTEXT-MAP.md): the six areas, their vocabulary, invariants and file:line entry points. Start here for any code change.
2. [docs/adr/](docs/adr/): why things are the way they are. Read the matching ADR before reversing anything that looks odd.
3. [docs/capabilities.md](docs/capabilities.md): what the system can and cannot do, with the reason.
4. [docs/system-report.md](docs/system-report.md): full design, by section; [3.1](docs/system-report.md#31-architecture) has a one-line responsibility for every package.
5. `git log -S <symbol>` and the commit bodies: each commit explains its why at length, and is the primary record.

## Layout

| Path | What |
|---|---|
| `src/` | The Go module: CLI and daemon in one binary. `main.go` (root and small commands), `*_cmd.go` one file per command (`run_cmd.go`, `ddp_cmd.go`, `setup_cmd.go`, `daemon_cmd.go` with `runDaemon`, …) |
| `src/internal/` | Packages, grouped by area in [CONTEXT-MAP.md](CONTEXT-MAP.md) |
| `bench/` | Sandbox and GPU benchmarks; a separate Go module |
| `scripts/` | Build, test, lab and benchmark harnesses; `install-pipedpeer.sh` |
| `lab/`, `docker/` | Container clusters for local multi-node runs |
| `pipedpeer-demo/` | Demo workloads and the three-network walkthrough (`DEMO.md`) |
| `docs/` | Report and paper sources, benchmarks, known issues, ADRs |
| `landing/` | The project website (Vite) |

`cmd/` holds only a stale untracked binary.

## Build and test

Run Go commands from `src/`, the module root.

```sh
cd src && go build -o ../bin/pipedpeer .
cd src && go vet ./... && gofmt -l .
cd src && go test -count=1 ./...            # CI adds -race
PIPEDPEER_INTEGRATION=1 go test ./...      # or make test-integration; needs podman or docker
scripts/check-skips.sh                      # fails when the distribution tests skipped
scripts/test-on-host.sh <ssh-host>          # build tests here, run where the Python deps are
make lab-up                                 # three worker containers on this host
make test-net                               # network scenarios (IP change, dead introducer, UDP-only router)
scripts/shim-check.sh                       # ruff + pytest on the shim modules
```

The shim tests need pandas, scikit-learn, joblib and torch; without them they skip and the package still reports `ok`.

## Traps

- The Python shim lives in `src/internal/nixgen/shim/*.py`, embedded with go:embed; keep it importable without torch/numpy/pandas. `scripts/shim-check.sh` runs ruff and pytest on it.
- Every `PIPEDPEER_*` variable the shim names must be read somewhere; a test fails otherwise.
- Never name a temp dir: ask `internal/userdir` for paths. `src/tmpfs_guard_test.go` enforces it.
- A node is its node id (the node key's fingerprint), never its address: it can have several paths (lan, internet at `127.0.0.1:<forwarder port>`, gossip). Never decide "this machine" from the host; use the machine key.
- Security-relevant wire changes refuse older builds ([ADR-0006](docs/adr/0006-hard-cuts-for-security-wire-changes.md)); anything else must stay additive. Ports are 4242/tcp API, 4242/udp discovery, 4243/udp direct, 4244/udp introducer ([ADR-0020](docs/adr/0020-ports-block-hard-cut.md)).
- Write tests that survive a loaded 2-core CI runner: wide expiry windows anchored to recorded timestamps. GPU and host telemetry shell out and are slow when cold.
- The plain lab (three containers on one host) proves work crosses nodes, never speedup; the quota-plus-netem lab (`scripts/lab-emu.sh`) is the emulation method, and its results are always labelled emulated.
- With podman, `lab-up.sh` needs `podman pull docker.io/nixos/nix:2.31.3` once.
- PRs target `dev`; `main` is for tagged releases ([CONTRIBUTING.md](CONTRIBUTING.md)).
