# Pipedpeer

Distributed execution runtime with a Nix-first packaging model, dual-role nodes (submitter + executor), and optional GPU execution via OCI containers for GPU-bound tasks.

## Prerequisites

**Host Requirements (Required)**
- Go 1.21+ (build, lint, and unit tests)
- Nix with flakes enabled (reproducible builds, dependency management)
- SSH (for remote job execution)
- bubblewrap/bwrap (job isolation via namespaces)

**Optional (Future GPU Tasks)**
- Docker or Podman (container runtime for GPU job execution via OCI)
- GPU drivers (NVIDIA, AMD, or Intel Arc)

**Setup**
Run the automated bootstrap:
```bash
sudo ./scripts/bootstrap/all.sh
```

This installs all prerequisites and validates the setup with `pipedpeer doctor`.

## Architecture Overview

- **Default Execution:** Host Nix builds with direct SSH execution to remote nodes
- **GPU Tasks:** Planned OCI container runtime with Nix inside (not implemented in bootstrap yet)
- **Integration Tests:** Docker-based lab in `lab/` (Nix runs inside workers)

Current runtime scope:
- **Execution Modes**: Supports `script` (default syncing of workspace), `sdk`, and `script+sdk`.
- **Workspace Sync**: Synchronizes the user's current working directory (`pwd`) directly to the remote node, ensuring paths act locally. Respects `.pipedpeerignore`.
- **Terminal Modes**: 
  - Attached (default): live streaming of remote execution stdout/stderr and graceful `Ctrl+C` cancellation.
  - Detached (`--detach`): returns immediately and syncs outputs asynchronously.
- Multi-node scheduling/optimizer is not implemented yet (explicit remote target is used).

Job history is stored under `$XDG_DATA_HOME/pipedpeer/jobs` or `~/.local/share/pipedpeer/jobs` and records metadata, stdout/stderr, manifests, and synced artifacts.

## Current Entry Point

The active CLI implementation is under `cmd/pipedpeer`.

## Repository Structure

- `cmd/pipedpeer`: current runnable Go CLI and tests
- `daemon/internal`: blueprint-aligned internal package scaffold for next refactor
- `proto`: wire contract scaffold
- `sdk/python/pipedpeer`: SDK scaffold
- `examples`: example scripts
- `lab`: Docker-based worker lab
- `docs`: architecture and planning docs
- `scripts`: build/test helper scripts
- `bin`: local build outputs

## Quick Commands

- Build: `./scripts/build.sh`
- Install locally (user bin): `./scripts/install-pipedpeer.sh`
- Unit tests: `./scripts/test.sh`
- Integration tests: `./scripts/test-integration.sh`
- Lab up: `./scripts/lab-up.sh`
- Lab down: `./scripts/lab-down.sh`

CLI runtime commands:
- `./bin/pipedpeer start --node-id <local-node-id> --daemon-port 38080`
- `./bin/pipedpeer status`
- `./bin/pipedpeer stop`
- `./bin/pipedpeer init` (generates `.pipedpeerignore`)
- `./bin/pipedpeer run --script <script.py> --remote <user@host:port> --target-id <remote-node-id> [flags] -- [script_args...]`
- `./bin/pipedpeer jobs`
- `./bin/pipedpeer job --id <job-id> --output`

**Unified CLI `run` Command Example:**
```bash
# 1. Terminal Var | 2. Pipedpeer Command | 3. Pipedpeer Flags                         | 4. Script  | 5. Forward Env | 6. Separator | 7. Script Args
API_KEY=xyz         pipedpeer run          --remote root@host --python 3.10 --pkg req.txt script.py  -e API_KEY       --             --epochs 50 --batch 32
```

## Install Pattern

`scripts/install-pipedpeer.sh` follows the common CLI install pattern:

1. Try downloading a prebuilt release artifact (URL placeholder for now).
2. If download is unavailable, build locally with Go.
3. Install to `$HOME/.local/bin` (or `PIPEDPEER_INSTALL_DIR`).
4. Print PATH export instructions if needed.

Release URL is currently a placeholder and can be set with:

```bash
PIPEDPEER_RELEASE_BASE_URL="https://your-release-host/path" ./scripts/install-pipedpeer.sh
```

## First-Run Workflow

1. Build the CLI:
	- `./scripts/build.sh`
2. Run fast checks:
	- `./scripts/test.sh`
3. Run isolated integration test (Docker-based):
	- `./scripts/test-integration.sh`

## Execution Paths

### Primary: Host Nix → SSH → Remote Execution
- `cmd/pipedpeer/main.go` performs local `nix build` via host Nix
- Copies build result to remote node via SSH
- Remote executes via bubblewrap-isolated namespace
- Best for CPU-bound and typical workloads
- Before run, CLI checks remote daemon acceptance using explicit `target-id`
- If local daemon is stopped, CLI auto-starts it and prints `started daemon`

### Optional: GPU Jobs via OCI Container
- Detected at runtime if GPU is configured
- Builds OCI image with Nix inside
- Mounts host `/nix` volume for cache reuse
- Spawns container on remote worker with GPU device pass-through
- Planning phase only; current bootstrap registers CPU-only nodes

### Testing: Lab with Docker Workers
- Uses Docker workers in `lab/`
- Nix runs inside worker containers (isolated test environment)
- Provides clean cache between test runs
