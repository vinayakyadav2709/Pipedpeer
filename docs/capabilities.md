# Capabilities and limits

What the system does today and where it stops. Each row names its evidence (path under `src/`, or a commit). Measured numbers are in [system-report.md chapter 5](system-report.md#chapter-5--results); defects are in [known-issues.md](known-issues.md).

## Can

| Capability | How, and its limit | Evidence |
|---|---|---|
| Run an unmodified Python script on another node | `pipedpeer run <script> [script args]`; the script's flags pass through, its exit code is the CLI's; output is the script's own unless `-v`; environment from imports or `uv.lock` | `run_cmd.go` `newRunCmd`, `internal/app/env.go` `BuildEnvironment` |
| Scatter one script over many inputs | `pipedpeer map` (hidden command) with `--inputs`, `--args-file` or `--split`, optional local `--reduce` | `main.go` `newMapCmd`, `internal/app/mapset.go` |
| Spread `multiprocessing.Pool` / `ProcessPoolExecutor` work | Only when the cost model says it wins; lambdas, closures and partials ship by value. Single calls (`executor.submit`, `Pool.apply_async`/`apply`) are batched after a 20 ms idle window and spread like a map; calls with keyword arguments stay local | [ADR-0002](adr/0002-never-slower-than-local.md), [ADR-0017](adr/0017-kernels-ship-as-source-then-cloudpickle.md) |
| Spread joblib / scikit-learn `n_jobs` | joblib backend routed through the same pool | `internal/nixgen/shim/pool.py` |
| Distribute pandas groupby/merge and large CSV/parquet reads | Hash-shuffle and out-of-core partitioned reads; categorical keys included (`observed=False` restores the empty categories) | `internal/nixgen/shim/pandas_ops.py` |
| Offload large numpy matmul/dot/tensordot, svd/eig | Module functions only; the `@` operator is not intercepted | `internal/nixgen/shim/numpy_ops.py`, [known-issues](known-issues.md#read-in-the-code-not-exercised) |
| Train torch across nodes, or on one GPU when that is faster | Automatic: after warm-up the lead compares the split step time with the fastest rank alone and keeps the ring or releases the rest; `--ddp N` fixes N ranks. The script's batch is split by speed (its maths unchanged); int8 gradients with error feedback; the lead is a GPU node whenever one exists; mismatched models abort every rank with one message; a sync closes without a dead rank after 45 s; `--ddp-sync fast` = DiLoCo | [ADR-0010](adr/0010-gradient-sync-through-the-daemon.md), `5f807f7` |
| Start DDP training only when every rank is ready | The first sync is a barrier (`PIPEDPEER_DDP_START_WAIT`, default 5 min); rank 0 names whom it waits for; ranks still missing are dropped and can rejoin later through a DataLoader, running one untrained catch-up batch per missed epoch | `internal/daemonapi/ddpsync.go`, `c567369`, `9b47502`, `82596fe` |
| Keep this machine's CPU, GPU and memory out of the spread work | `run --offload`: the script runs here, every distributed piece goes to other machines (by kernel boot ID); a piece nobody else can take runs here with a warning. The summary's usage lines show this machine's and the node's load | `main.go`, `internal/daemonapi/pool.go`, `internal/app/selfuse.go`, `ef68f02`, `ca70da4` |
| Find LAN peers with no configuration | UDP broadcast, TCP `/health` sweep when broadcast is filtered | `internal/discovery/` |
| Join machines on different networks with no VPN or port forwarding | `pipedpeer invite` / `connect <code>`; every daemon is an introducer for its cluster, a node registers with all it knows; port mapping, hole punching, collision for one symmetric side | [ADR-0004](adr/0004-introducer-only-rendezvous.md), `internal/direct/birthday.go` |
| Keep working when an introducer goes down or changes IP | Existing links survive; new joins use any other introducer in the list | `1cabedd`, `internal/internet/` |
| Follow a machine that changes network | Every path per node id is kept; a new address is used once it answers; QUIC connections migrate; a reconnecting node's hello replaces its dead link | [ADR-0019](adr/0019-address-book.md) |
| Tell what is wrong with the network or the machine | `pipedpeer doctor`: daemon, token, each introducer, peers with reason, firewall, TCP vs UDP reachability, sandbox, nix, GPU driver, disk, clock; each with the fix | `internal/doctor/` |
| Survive a reboot | Optional user service (`setup` asks; `start --boot` / `stop --boot`) | `internal/service/` |
| Take CPU pool work on an Android phone | Termux, pool-only role, no Nix or crun | [phone.md](phone.md), `internal/phone/` |
| Re-place a job whose node died | Gateway, introducer and peer probes tell a dead node from this machine's lost network; then `ExecuteWithRetry`, at most 3 failed runs, untried nodes first; a node that refuses the closure is skipped | `internal/app/lostconn.go` `networkUp`, `internal/coordinator/coordinator.go` `ExecuteWithRetry`, `879b86c`, `1e07d56` |
| Wait for room with `--remote` | Up to 10 min, saying every 30 s why each node is busy (with `-v`); a job that fits no node, or whose script does not exist, fails at once | `run_cmd.go`, `1e07d56` |
| Stop everything on Ctrl+C, the submitter's own Nix export and pack included | Nix runs in its own process group: SIGTERM, then SIGKILL after 5 s | `internal/nixstore/nixstore.go` `CmdContext`, `99cf0ad` |
| Cap job memory without root | cgroup v2 in a user systemd scope | [ADR-0016](adr/0016-memory-caps-in-a-user-scope.md) |
| Install and run without root | crun in `~/.local/bin`, private nix store | [ADR-0012](adr/0012-crun-only-runtime.md), [ADR-0013](adr/0013-rootless-private-nix-store.md) |
| Use a worker's NVIDIA GPU inside the sandbox | Driver libraries bind-mounted into the job | `92191d3`, `internal/daemonapi/execution.go` `gpuDriverLibDir` |
| Ship an environment and workspace once | Only store paths a peer lacks travel, paths cache.nixos.org serves are fetched by the peer itself; only workspace files the node lacks are sent; signed where the peer supports it | [ADR-0011](adr/0011-signed-closures-via-binary-cache.md), `193e1bf` |
| Accept closures from peers that join after setup, with no prompt | On a multi-user nix, setup's one sudo installs a root-owned key helper the daemon calls with `sudo -n`; a private store or single-user nix takes the keys with each import | [ADR-0018](adr/0018-root-owned-nix-key-helper.md), `internal/nixtrust/` |
| Return only what a job produced | Results are files created or modified by the job | `2befd26` |
| Submit from macOS | Submit-only client; jobs run on Linux | [README](../README.md#install) |
| See where the time went | Every run keeps a record: total, script and pipedpeer overhead, where it ran; `-v`, `jobs <id> --steps`, the web run view and `jobs --export-all` show every step of every chunk | `internal/runrec/` |

## Cannot

| Limit | Reason | Evidence |
|---|---|---|
| Two peers with no direct path cannot work together | No relay, by design | [ADR-0005](adr/0005-no-relay.md) |
| Two peers both behind symmetric NATs cannot connect | Neither side has an address to publish | `internal/direct/direct.go` `ReasonBothSymmetric` |
| Port mapping does not help behind carrier-grade or double NAT | The device that answers is not the outer NAT | `fe43933` |
| Mixed-version clusters across security cuts | Older builds cannot answer the hello or the token proof | [ADR-0006](adr/0006-hard-cuts-for-security-wire-changes.md) |
| Workers must be Linux; private-store seed is x86_64 only | crun, namespaces, cgroups; the seed URL names x86_64 | `internal/nixstore/seed.go` `Seed` |
| Python only | The environment builder and shim are Python-specific | `internal/pythondeps/`, `internal/nixgen/shim/` |
| The submitting CLI must outlive the job | The coordinator runs in the CLI | [ADR-0003](adr/0003-no-head-node.md) |
| Imports outside the 35-entry package table are dropped silently | Fixed module-to-nixpkgs table | `internal/pythondeps/deps.go` `ResolvePackages` |
| The sandbox is not a boundary against a hostile member | uid 0 inside, no network namespace, no seccomp; unisolated without crun or user namespaces on a system store | [ADR-0012](adr/0012-crun-only-runtime.md), [system report 6.2](system-report.md#62-limitations) |
| No per-user authority | One token per cluster | `internal/authtoken/token.go` `Current`, [SECURITY.md](../SECURITY.md) |
| DiLoCo (`--ddp-sync fast`) is not chosen automatically | Auto may pick it only after its loss is within 1% of one GPU on every test model | [ADR-0010](adr/0010-gradient-sync-through-the-daemon.md), [benchmarks/diloco.md](benchmarks/diloco.md) |
| GPUs are NVIDIA only in practice; Intel integrated graphics never count | CUDA wheels and NVIDIA driver passthrough; AMD is detected but has no ROCm torch | `internal/gpu/`, `6fbd06e` |
| Phones run pool work only | No Nix or crun on Android; whole jobs are refused | [phone.md](phone.md) |
| Thread pools are not distributed | They share memory; only process pools promise picklable, share-nothing work | [ADR-0001](adr/0001-interception-not-an-sdk.md) |
