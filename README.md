# DC -- P2P Distributed Computing with Network-Aware Routing

A peer-to-peer distributed computing framework built on gRPC that partitions and distributes matrix computations across a swarm of worker nodes. A lightweight coordinator handles node registration and discovery while all task dispatch, availability checking, reservation, and computation happen directly between peers.

## Architecture

```
                 ┌──────────────┐
                 │  Coordinator │   (registry + fault-tolerance metadata)
                 │  :50051      │
                 └──────┬───────┘
          register /    │    \ discover
                 /      │     \
    ┌───────────┐  ┌───────────┐  ┌───────────┐
    │  Worker 1 │──│  Worker 2 │──│  Worker N │   P2P task dispatch
    │  :50061   │  │  :50062   │  │  :5006N   │
    └───────────┘  └───────────┘  └───────────┘
```

- **Coordinator** -- Maintains a node registry, tracks task replicas for primary-backup fault tolerance, and handles backup promotion on failure. Does not participate in computation.
- **Worker nodes** -- Register with the coordinator, then communicate peer-to-peer. Each node can act as both a **client** (partitioning and dispatching work) and a **worker** (computing assigned blocks). Large tasks are recursively redistributed across available peers.

## Features

- Async gRPC (grpc.aio) for non-blocking P2P communication
- Network-aware routing with latency measurement and node selection
- P2P availability checking and task reservation before dispatch
- Recursive work redistribution for large matrices
- Primary-backup fault tolerance with coordinator-managed failover
- Configurable simulated network latency per node
- MPI collective operations support (broadcast, scatter, gather)

## Experiments

| #  | Topic                        | Script         |
|----|------------------------------|----------------|
| 1  | Basic gRPC communication     | `test_client.py` |
| 2  | Multithreading & async tasks | `test_node.py` |
| 3  | Clock synchronization        | --             |
| 4  | Bully election algorithm     | --             |
| 5  | Data replication             | `exp5.py`, `run_exp5.py` |
| 6  | Load balancing               | `exp6.py`, `run_exp6.py` |
| 7  | MapReduce with PySpark       | `exp7.py`, `run_exp7.py` |
| 8  | Fault tolerance              | `exp8.py`, `run_exp8.py` |
| 9  | MPI collectives              | `exp9.py`, `run_exp9.py` |
| 10 | Parallel matrix multiply     | `exp10.py`, `run_exp10.py` |

## Requirements

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Setup

```bash
# Install dependencies with uv
uv sync

# Or with pip
pip install -e .

# Dev dependencies (pytest)
uv sync --group dev
```

## Usage

### Local

Start the coordinator, then launch one or more worker nodes, then run a client or experiment.

```bash
# Terminal 1 -- coordinator
uv run python src/coordinator.py

# Terminal 2..N -- worker nodes
uv run python start_node.py worker1 50061 0.01
uv run python start_node.py worker2 50062 0.02

# Terminal N+1 -- run a client
uv run python test_client.py
```

Node arguments: `<node_id> <port> <simulated_latency_seconds>`

### CLI

A unified CLI is also available:

```bash
dc coordinator                   # start coordinator
dc node <id> <port> [latency]    # start a worker node
dc test                          # run test suite
```

### Docker / Podman

The included `docker-compose.yml` spins up a coordinator and 10 worker nodes on an isolated bridge network.

```bash
# Docker
docker compose up --build

# Podman
podman-compose up --build
```

Logs are mounted to `./logs/` on the host.

## Project Structure

```
.
├── src/
│   ├── compute.proto        # gRPC service & message definitions
│   ├── compute_pb2.py       # generated protobuf code
│   ├── compute_pb2_grpc.py  # generated gRPC stubs
│   ├── config.py            # central configuration (ports, thresholds, gRPC opts)
│   ├── coordinator.py       # coordinator server (registry + fault tolerance)
│   ├── cli.py               # unified CLI entry point
│   └── node/
│       ├── core.py          # UnifiedNode & MPINode implementation
│       ├── network.py       # NetworkManager (P2P comms, latency, dispatch)
│       └── servicer.py      # async gRPC servicer for incoming tasks
├── start_node.py            # node boot script (used in containers)
├── test_client.py           # basic distributed matrix multiply client
├── test_matrix_client.py    # matrix multiplication test client
├── exp[5-10].py             # experiment implementations
├── run_exp[5-10].py         # experiment runners
├── tests/                   # pytest suite
├── docker-compose.yml       # multi-node container orchestration
├── Dockerfile.coordinator   # coordinator container image
├── Dockerfile.node          # worker node container image
└── pyproject.toml           # project metadata & dependencies
```

## Configuration

Key settings in `src/config.py`:

| Variable                         | Default       | Description                                    |
|----------------------------------|---------------|------------------------------------------------|
| `COORDINATOR_PORT`               | `50051`       | Coordinator listen port                        |
| `NODE_BASE_PORT`                 | `50052`       | Starting port for worker nodes                 |
| `DEFAULT_NUM_NODES`              | `10`          | Nodes to request from coordinator              |
| `ENABLE_RECURSION`               | `True`        | Allow recursive task redistribution            |
| `MAX_RECURSION_DEPTH`            | `2`           | Max redistribution depth                       |
| `GOOD_CONNECTION_LATENCY`        | `0.05s`       | Latency threshold for "good" connections       |
| `REDISTRIBUTION_SIZE_THRESHOLD`  | `1,000,000`   | Min element count to trigger redistribution    |

The coordinator address can be overridden via the `COORDINATOR_ADDRESS` environment variable (used by Docker workers to resolve `coordinator:50051`).

## Testing

```bash
uv run pytest
```
