# Never slower than local

A workload not worth distributing must run exactly as a plain local `multiprocessing.Pool` would: the shim starts local, measures per-item cost, spills only when a cost model says the cluster wins, and keeps local cores pulling from the same queue, with idle cores re-running the remote straggler tail (`d2dee3f`, `_race` in `src/internal/nixgen/shim.go`). Peers get shares by measured throughput, and a peer that cannot lower the makespan is declined (`8c9284c`, `src/internal/schedule/schedule.go` `Compute`); dividing by headcount measured 1.6x slower than not distributing.

## Consequences

- `--distribute force` / `PIPEDPEER_DISTRIBUTE=force` is the only way to override the cost model (`51a951d`).
- CI holds the line with the shim-overhead and pool benchmarks (`.github/workflows/ci.yml`, `20a6e5d`).
- One task runs on one node under one lease. It runs again only when its lease expires (node died); a task that fails in user code is reported failed. The straggler re-run is the single exception.
