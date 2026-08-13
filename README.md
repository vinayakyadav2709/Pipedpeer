# Pipedpeer

Run your Python workloads on other machines without changing your code.

```bash
pipedpeer python train.py
```

Pipedpeer builds the exact environment your script needs, ships it to a peer on
your network, runs the job there, and streams the output and result files back.
No cluster manager, no head node, no YAML.

> **Status: experimental.** Pipedpeer is under active development and has not
> had a stable release yet. Interfaces and on-disk formats can change.

## How it works

Every machine runs a small daemon. Daemons find each other on the LAN, publish
what they have (cores, memory, GPUs, current load), and accept work from peers.

When you submit a job:

1. Your imports (or `uv.lock`) are resolved into a Nix closure — the exact
   interpreter and packages your script needs.
2. The coordinator picks a node: architecture and GPU requirements are hard
   filters, then nodes are ranked on free cores, free memory, load and GPU
   headroom.
3. The node reserves a lease, receives the closure and your project files, and
   runs the script inside a `crun` sandbox.
4. stdout/stderr stream back live; result files come back when the job ends.

If a node dies mid-job, its lease expires and the job is placed somewhere else.

## Install

```bash
# stable (tagged releases)
./scripts/install-pipedpeer.sh

# nightly (rolling build from the dev branch)
./scripts/install-pipedpeer.sh --channel nightly
```

Requires Linux to *run* jobs (`crun` sandbox, Linux namespaces, GPU device
nodes). macOS builds work as a submitting client.

## Quickstart

```bash
# On every machine that should accept work:
pipedpeer setup            # installs prerequisites (nix, crun) and starts the daemon

# On your machine:
pipedpeer nodes list       # see who is out there
pipedpeer python main.py   # run your script on the best available node
```

Useful commands:

| Command | What it does |
|---|---|
| `pipedpeer setup` | Check prerequisites, install what's missing, start the daemon |
| `pipedpeer start` / `stop` / `status` | Manage the local daemon |
| `pipedpeer python <script>` | Run a script on the cluster |
| `pipedpeer run --script <s> [flags]` | Same, with full control (GPU, memory, target node) |
| `pipedpeer nodes list\|add\|remove` | Inspect and manage known nodes |
| `pipedpeer tasks` (alias `ps`) | Live view of what is running cluster-wide |
| `pipedpeer jobs` / `job --id <id>` | Job history and details |
| `pipedpeer dashboard` | Live TUI of nodes and jobs |
| `pipedpeer registry` | Run a standalone node registry |

Run `pipedpeer <command> --help` for the flags.

## Security

**Run Pipedpeer only on networks you trust.**

The daemon accepts jobs from any peer that can reach it, and a job is arbitrary
code. There is no authentication, authorization, or transport encryption today.
Treat an open daemon port the same way you would treat handing someone a shell
on that machine.

Do not expose the daemon port (default `38080`) to the internet or to any
network you do not control. See [SECURITY.md](SECURITY.md).

## Branches and releases

| Branch | Stream | What it is |
|---|---|---|
| `main` | stable | Tagged releases. Nothing released yet. |
| `dev` | nightly | Development stream; nightly prereleases are built from here. |
| `poc` | — | Internal experiments, no guarantees. |

Pull requests go to `dev`. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Development

```bash
make build            # build ./bin/pipedpeer
make test             # unit tests
make test-integration # integration tests (needs podman/docker)
make fmt lint         # gofmt and go vet
make lab-up           # 3-worker container cluster for local testing
```

## License

[Apache-2.0](LICENSE)
