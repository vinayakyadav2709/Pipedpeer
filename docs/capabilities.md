# Capabilities and limits

What the system does today and where it stops. Each row names its evidence (path under `src/`, or a commit). Measured numbers are in [system-report.md chapter 5](system-report.md#chapter-5--results); defects are in [known-issues.md](known-issues.md).

## Can

| Capability | How, and its limit | Evidence |
|---|---|---|
| Run an unmodified Python script on another node | `pipedpeer run` / `pipedpeer python`; environment from imports or `uv.lock` | `main.go` `newRunCmd`, `internal/app/env.go` `BuildEnvironment` |
| Scatter one script over many inputs | `pipedpeer map` with `--inputs`, `--args-file` or `--split`, optional local `--reduce` | `main.go` `newMapCmd`, `internal/app/mapset.go` |
| Spread `multiprocessing.Pool` / `ProcessPoolExecutor` work | Only when the cost model says it wins; lambdas, closures and partials ship by value. Single calls (`executor.submit`, `Pool.apply_async`/`apply`) are batched after a 20 ms idle window and spread like a map; calls with keyword arguments stay local | [ADR-0002](adr/0002-never-slower-than-local.md), [ADR-0017](adr/0017-kernels-ship-as-source-then-cloudpickle.md) |
| Spread joblib / scikit-learn `n_jobs` | joblib backend routed through the same pool | `internal/nixgen/shim.go` `_install_joblib` |
| Distribute pandas groupby/merge and large CSV/parquet reads | Hash-shuffle and out-of-core partitioned reads; categorical keys included (`observed=False` restores the empty categories) | `internal/nixgen/shim.go` `_install_pandas`, `_install_io` |
| Offload large numpy matmul/dot/tensordot, svd/eig | Module functions only; the `@` operator is not intercepted | `internal/nixgen/shim.go` `_install_numpy`, [known-issues](known-issues.md#read-in-the-code-not-exercised) |
| Train torch DDP across nodes | `--ddp K`, sync through the daemon, ranks chosen by measured throughput; a sync closes without a dead rank after 45 s | [ADR-0010](adr/0010-gradient-sync-through-the-daemon.md), `5f807f7` |
| Find LAN peers with no configuration | UDP broadcast, TCP `/health` sweep when broadcast is filtered | `internal/discovery/` |
| Join machines on different networks with no VPN or port forwarding | One introducer; port mapping, hole punching, collision for one symmetric side | [ADR-0004](adr/0004-introducer-only-rendezvous.md), `internal/direct/birthday.go` |
| Keep working when the introducer goes down | Existing links survive; new joins wait for it | `1cabedd` |
| Re-place a job whose node died | Lease expiry, then `ExecuteWithRetry` | `internal/coordinator/coordinator.go` `ExecuteWithRetry` |
| Cap job memory without root | cgroup v2 in a user systemd scope | [ADR-0016](adr/0016-memory-caps-in-a-user-scope.md) |
| Install and run without root | crun in `~/.local/bin`, private nix store | [ADR-0012](adr/0012-crun-only-runtime.md), [ADR-0013](adr/0013-rootless-private-nix-store.md) |
| Use a worker's NVIDIA GPU inside the sandbox | Driver libraries bind-mounted into the job | `92191d3`, `internal/daemonapi/execution.go` `gpuDriverLibDir` |
| Ship an environment once | Only store paths a peer lacks travel; signed where the peer supports it | [ADR-0011](adr/0011-signed-closures-via-binary-cache.md), `193e1bf` |
| Accept closures from peers that join after setup, with no prompt | On a multi-user nix, setup's one sudo installs a root-owned key helper the daemon calls with `sudo -n`; a private store or single-user nix takes the keys with each import | [ADR-0018](adr/0018-root-owned-nix-key-helper.md), `internal/nixtrust/` |
| Return only what a job produced | Results are files created or modified by the job | `2befd26` |
| Submit from macOS | Submit-only client; jobs run on Linux | [README](../README.md#install) |

## Cannot

| Limit | Reason | Evidence |
|---|---|---|
| Two peers with no direct path cannot work together | No relay, by design | [ADR-0005](adr/0005-no-relay.md) |
| Two peers both behind symmetric NATs cannot connect | Neither side has an address to publish | `internal/direct/direct.go` `ReasonBothSymmetric` |
| Port mapping does not help behind carrier-grade or double NAT | The device that answers is not the outer NAT | `fe43933` |
| No introducer failover | One introducer per cluster | [known-issues](known-issues.md#limitations-by-design) |
| Mixed-version clusters across security cuts | Older builds cannot answer the hello or the token proof | [ADR-0006](adr/0006-hard-cuts-for-security-wire-changes.md) |
| Workers must be Linux; private-store seed is x86_64 only | crun, namespaces, cgroups; the seed URL names x86_64 | `internal/nixstore/seed.go` `Seed` |
| Python only | The environment builder and shim are Python-specific | `internal/pythondeps/`, `internal/nixgen/shim.go` |
| The submitting CLI must outlive the job | The coordinator runs in the CLI | [ADR-0003](adr/0003-no-head-node.md) |
| Imports outside the 35-entry package table are dropped silently | Fixed module-to-nixpkgs table | `internal/pythondeps/deps.go` `ResolvePackages` |
| The sandbox is not a boundary against a hostile member | uid 0 inside, no network namespace, no seccomp; unisolated without crun or user namespaces on a system store | [ADR-0012](adr/0012-crun-only-runtime.md), [system report 6.2](system-report.md#62-limitations) |
| No per-user authority | One token per cluster; with no token the daemon is open | `internal/authtoken/token.go` `Current`, [SECURITY.md](../SECURITY.md) |
| Auto-tuned DDP sync is off by default | It measured a worse final loss | [ADR-0010](adr/0010-gradient-sync-through-the-daemon.md) |
| Thread pools are not distributed | They share memory; only process pools promise picklable, share-nothing work | [ADR-0001](adr/0001-interception-not-an-sdk.md) |
