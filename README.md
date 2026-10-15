# Pipedpeer

Run your Python script on more machines without changing it.

```bash
pipedpeer run train.py --epochs 3
```

Pipedpeer builds the exact environment your script needs, ships it to other
machines in your cluster, runs the work there, and streams output and result
files back. Plain `run` prints what `python train.py` would print; the details
are in `pipedpeer ui`, `pipedpeer jobs` and `run -v`. No cluster manager, no
head node, no YAML.

> **Status: experimental.** Pipedpeer is under active development and has not
> had a stable release yet. Interfaces and on-disk formats can change.

## How it works

Every machine runs a small daemon. Daemons publish what they have (cores,
memory, GPUs, current load) and accept work from peers.

On a LAN they find each other by themselves. Across networks, every daemon is
also an **introducer** for its own cluster: a machine joining with
`pipedpeer connect <code>` reaches any member named in the code, learns the
other members' addresses, and then connects **directly** to them, hole-punching
outbound through both routers. Introducers hand out addresses and nothing else:
no job data, results or traffic pass through them, and nothing is relayed. For
clusters with no reachable member, a small public server can run as an
introducer that takes no jobs ([docs/hosted-introducer.md](docs/hosted-introducer.md)).

Each node is known by its key, not its address. A node keeps every address it
has seen for a peer (LAN, internet, learned from other members) and switches to
a new one only after it has answered, so a laptop that changes Wi-Fi comes back
on its own.

When you submit a job:

1. Your imports (or `uv.lock`) are resolved into a Nix closure: the exact
   interpreter and packages your script needs.
2. The coordinator picks a node: architecture and GPU requirements are hard
   filters, then nodes are ranked on free cores, free memory, load and GPU
   headroom.
3. The node reserves a lease, receives the closure and the project files it
   lacks, and runs the script inside a `crun` sandbox.
4. stdout/stderr stream back live; result files come back when the job ends;
   the script's exit code is pipedpeer's exit code.

If a node dies mid-job, its lease expires and the job is placed somewhere else.

Some limits are by design (no relay, no head node): see
[docs/known-issues.md](docs/known-issues.md#limitations-by-design).

## Transparent cluster acceleration

`pipedpeer run` embeds a shim that routes your parallel primitives across the
cluster with zero code changes. Each primitive is intercepted only when the
cost model says the cluster wins (measured link speed and per-machine speed,
kept across runs); otherwise it runs locally, exactly as before.

| Your code | What happens instead |
|---|---|
| `multiprocessing.Pool.map/starmap/imap/apply` | Work is split into chunks sized from the measured link, sent to each node as fast as it takes them, results stream back in order |
| A `Pool` kernel that is a lambda, closure, method or partial | Shipped by value. Stock `multiprocessing` refuses these (`PicklingError`); here they run cluster-wide |
| `pandas.read_csv/read_parquet` (large files) | Chunked out-of-core reads across nodes, streamed back on demand |
| `df.groupby(...).agg(...)` / `df.merge/join` | Hash-shuffle: each node reduces its share of the keys, partials are combined exactly at the origin |
| `joblib.Parallel` / `sklearn` (e.g. `RandomForestClassifier(n_jobs=-1)`) | Same path as `Pool.map`: the first tasks run here to measure them, the rest go to the cluster; calls too small to be worth a round trip stay local |
| `np.matmul` / `np.dot` / `np.tensordot` (large arrays) | Block-row slicing: each worker computes its share, origin concatenates |
| `np.linalg.svd` / `np.linalg.eig` (large 2-D) | Whole matrix offloaded to one worker |
| `torch` training | Either one GPU or a ring of ranks, decided from the first steps; the script's batch is split across ranks (its maths is unchanged). `--ddp N` forces N ranks; `--ddp-sync fast` syncs every few steps (DiLoCo) instead of every step |

The shim is a set of Python modules injected into the job's environment;
nothing is installed on the nodes or in your interpreter.

```bash
pipedpeer run train.py                      # cluster-parallel primitives
pipedpeer run train.py --ddp 2              # force a 2-rank training ring
pipedpeer run train.py -v                   # also show placement, stages and per-step timings
```

## Install

```bash
# latest build
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash

# an exact build (every machine in a cluster should run the same one)
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <version>
```

The installer verifies the checksum, puts the binary in `/usr/local/bin` (or
`~/.local/bin` without sudo, with an ACTION NEEDED line for your PATH) and runs
`pipedpeer setup`. `pipedpeer --version` prints the build to pass to
`--version` on the other machines.

Requires Linux to *run* jobs (`crun` sandbox, Linux namespaces, GPU device
nodes). macOS builds work as a submitting client. Android phones can join as
CPU-only pool workers through Termux ([docs/phone.md](docs/phone.md)).

## Quickstart

```bash
# First machine: setup creates the cluster and prints the line for the others
pipedpeer setup
pipedpeer invite            # prints: pipedpeer connect pp1-...

# Every other machine
pipedpeer setup
pipedpeer connect pp1-...   # says connected (exit 0), partly (exit 2) or failed (exit 1), and why

# Anywhere
pipedpeer status            # machines, paths, running work
pipedpeer run main.py       # run your script, using the whole cluster
```

`setup` uses a nix you already have, or installs the multi-user nix when sudo
is available; without root it falls back to a private store in your data
directory. It creates the cluster secret before the daemon first starts, opens
the firewall ports, and asks whether to start pipedpeer at boot (default yes).
After `connect`, the common environments are built in the background so the
first run starts fast.

Commands:

| Command | What it does |
|---|---|
| `pipedpeer setup` | Check prerequisites, install what's missing, start the daemon |
| `pipedpeer invite` | Print the `pipedpeer connect <code>` line for another machine |
| `pipedpeer connect <code>` | Join the cluster in the code and say whether it worked |
| `pipedpeer run <script> [script args]` | Run a script on the cluster (`-v`, `--gpu`, `--remote`, `--ddp N`, `--ddp-sync`) |
| `pipedpeer status` | Machines, their paths and state, running tasks, last runs |
| `pipedpeer dashboard` | Live terminal view of nodes and jobs |
| `pipedpeer ui` | Open the web view on this machine (no token to type) |
| `pipedpeer jobs [id]` | Run history; `--steps` and `--export csv\|json` for one run, `--export-all <dir>` for all |
| `pipedpeer attach <id>` | Follow a running job's output |
| `pipedpeer start` / `stop` | Start or stop the daemon; `--boot` turns autostart on or off |
| `pipedpeer doctor` | Check network, firewall, sandbox, GPU driver; each problem with its fix |
| `pipedpeer uninstall` | Remove pipedpeer, its data, service and firewall rules (keeps Nix) |

Run `pipedpeer <command> --help` for the flags.

| Port | Used for |
|---|---|
| 4242/tcp | Daemon API (token-gated) |
| 4242/udp | LAN discovery (multicast 239.255.0.99) |
| 4243/udp | Direct encrypted links between members (QUIC) |
| 4244/udp | Introducer |

## Security

**A job is arbitrary code, so cluster membership is the trust boundary.**
Everything below is about who gets to be a member, not about sandboxing a peer
you should not have admitted.

What is in place:

- **A shared secret defines the cluster.** It protects the daemon API; requests
  without it are refused. Machines with different secrets are in different
  clusters and never see each other; an introducer answers only its own
  cluster. The invite code carries the secret: share it like a password.
- **One identity per machine.** The node id is the fingerprint of its ed25519
  key; the TLS certificate is made from the same key, and peers are pinned by
  node id, so a link cannot be taken over by whoever answers at an address.
- **Direct links are authenticated and encrypted** (QUIC).
- **Address gossip is signed** by each member's key; gossiped addresses are
  only candidates until they answer.
- **Closures are signed** by the node key and travel in a form that keeps the
  signature, so a machine can leave `require-sigs` on.
- Jobs run inside a `crun` sandbox with a memory limit from cgroups.

What is not:

- The sandbox is not a security boundary against a hostile peer: there is no
  seccomp profile or capability dropping yet.

See [SECURITY.md](SECURITY.md).

## Branches and releases

| Branch | Stream | What it is |
|---|---|---|
| `main` | stable | Tagged releases. |
| `dev` | nightly | Every push publishes a build under its own version (`nightly-<version>`) plus the rolling `nightly`. |

Pull requests go to `dev`. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Development

```bash
make build            # build ./bin/pipedpeer
make test             # unit tests
make test-integration # integration tests (needs podman/docker)
make test-net         # network scenarios (IP changes, dead introducer, UDP-only router) in containers
make lab-up           # 3-worker container cluster for local testing
```

## License

[Apache-2.0](LICENSE)
