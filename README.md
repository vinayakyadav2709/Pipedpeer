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

Every machine runs a small daemon. Daemons publish what they have (cores,
memory, GPUs, current load) and accept work from peers.

On a LAN they find each other by themselves. Across the internet — three
machines on three unrelated networks, each behind its own router — they meet
through an **introducer**: any machine with a public address, running
`pipedpeer rendezvous`. It hands out addresses and nothing else. Peers then
connect **directly to each other**, hole-punching outbound through both
routers, so no job data, results or traffic pass through it, and it can go down
once everyone has met. Nothing is port-forwarded and no VPN is involved.

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

## Transparent cluster acceleration

`pipedpeer run` embeds a shim that routes your parallel primitives
across the cluster with zero code changes. Each primitive is intercepted only
when the cost model says the cluster wins (measured latency/bandwidth probe,
cached per peer); otherwise it runs locally, exactly as before.

| Your code | What happens instead |
|---|---|
| `multiprocessing.Pool.map/starmap/imap/apply` | Work is split into batches and fanned out to cluster nodes; results stream back in order |
| A `Pool` kernel that is a lambda, closure, method or partial | Shipped by value. Stock `multiprocessing` refuses these outright (`PicklingError`), because it sends functions by name; here they run cluster-wide |
| `pandas.read_csv/read_parquet` (large files) | Chunked out-of-core reads across nodes, streamed back on demand |
| `df.groupby(...).agg(...)` / `df.merge/join` | Hash-shuffle: each node reduces its share of the keys, partials are combined exactly at the origin |
| `torch` training with `pipedpeer run --ddp K` | `K` ranks placed on the cluster; DDP process group, `DistributedDataParallel` wrapping and gradient sync happen transparently |
| `joblib.Parallel` / `sklearn` (e.g. `RandomForestClassifier(n_jobs=-1)`) | Job batches routed through the same cluster pool; sklearn's thread preference is overridden only inside the shim |
| `np.matmul` / `np.dot` / `np.tensordot` (large arrays) | Block-row slicing: each worker computes its share, origin concatenates |
| `np.linalg.svd` / `np.linalg.eig` (large 2-D) | Whole matrix offloaded to one worker |

The shim is a `sitecustomize` injected into the job's environment; nothing is
installed on the nodes or in your interpreter. On constrained nodes, payloads
that exceed 40% of free RAM run as sequential micro-chunks; if even one
micro-chunk cannot fit locally, the request is forwarded to a healthy peer
instead of failing.

```bash
pipedpeer run train.py           # cluster-parallel primitives (interception always on)
pipedpeer run train.py --ddp 2   # transparent 2-rank DDP
make lab-fail                                        # demo: kills a worker mid-pool-map, run survives
```

## Install

```bash
# nightly (rolling build, republished on every push to dev)
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --channel nightly

# stable (tagged releases)
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash
```

From a checkout, the same script works directly:

```bash
./scripts/install-pipedpeer.sh                    # stable
./scripts/install-pipedpeer.sh --channel nightly  # nightly
```

Requires Linux to *run* jobs (`crun` sandbox, Linux namespaces, GPU device
nodes). macOS builds work as a submitting client.

## Quickstart

```bash
# On every machine that should accept work:
pipedpeer setup            # installs prerequisites (nix, crun) and starts the daemon

# On your machine:
pipedpeer nodes            # see who is out there
pipedpeer run main.py      # run your script, using the whole cluster
```

Machines on the same network find each other. Machines on **different**
networks join through an introducer — one command each, and the address is
remembered so later starts need no arguments:

```bash
pipedpeer rendezvous               # on any machine with a public IP
pipedpeer join <that-machines-ip>  # on each of the others
```

For a full walkthrough on three machines across three networks, including what
to watch while it runs, see **[pipedpeer-demo/DEMO.md](pipedpeer-demo/DEMO.md)**.

Useful commands:

| Command | What it does |
|---|---|
| `pipedpeer setup` | Check prerequisites, install what's missing, start the daemon |
| `pipedpeer start` / `stop` / `status` | Manage the local daemon |
| `pipedpeer python <script>` | Run a script on the cluster |
| `pipedpeer run <s> [flags]` | Same, with full control (GPU, memory, target node) |
| `pipedpeer join <address>` | Join the cluster at an introducer, and remember it |
| `pipedpeer rendezvous` | Be the introducer (needs a public address) |
| `pipedpeer auth set\|show` | The shared secret that defines the cluster |
| `pipedpeer nodes list\|add\|remove` | Inspect and manage known nodes |
| `pipedpeer traffic` | What work this node ran for peers, and for whom |
| `pipedpeer net-check` | What this machine's router does to inbound UDP |
| `pipedpeer tasks` (alias `ps`) | Live view of what is running cluster-wide |
| `pipedpeer jobs` / `job --id <id>` | Job history and details |
| `pipedpeer dashboard` | Live TUI of nodes and jobs |
| `pipedpeer registry` | Run a standalone node registry |

Run `pipedpeer <command> --help` for the flags.

## Security

**A job is arbitrary code, so cluster membership is the trust boundary.**
Everything below is about who gets to be a member — not about sandboxing a peer
you should not have admitted.

What is in place:

- **A shared secret defines the cluster.** `pipedpeer auth set` protects the
  daemon API; requests without the token are refused. Machines with different
  secrets are in different clusters and never see each other, even through the
  same introducer.
- **Direct links are authenticated and encrypted.** Peers connect over QUIC and
  each proves possession of the ed25519 identity key its fingerprint is derived
  from, so a link cannot be taken over by whoever answers at an address.
- **Closures are signed**, by a key derived from that same node identity, and
  they travel in a form that keeps the signature — so a machine can leave
  `require-sigs` on and still receive work, rather than switching signature
  checking off for everything it installs.
- Jobs run inside a `crun` sandbox with a memory limit from cgroups.

What is not:

- **With no token set, the daemon is open** to anyone who can reach the port.
  Set one before putting a machine anywhere untrusted.
- The sandbox is not a security boundary against a hostile peer: there is no
  seccomp profile or capability dropping yet.
- Set a token before exposing the daemon port (default `38080`).

See [SECURITY.md](SECURITY.md).

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
make lab-fail         # failure-injection demo (kills a worker mid-run)
```

## License

[Apache-2.0](LICENSE)
