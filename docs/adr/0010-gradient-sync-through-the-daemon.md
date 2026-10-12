# DDP gradients sync through the daemon, which averages them

torch's gloo mesh needs rank-to-rank TCP on ports of its own, which failed on every cross-network attempt, so ranks sync by `POST /v1/ddp/sync` on the lead rank's daemon (`6abe4df`; gloo/nccl stay reachable via `PIPEDPEER_DDP_BACKEND`). The daemon folds contributions into one float64 accumulator and returns one model, instead of a blackboard that sent every rank N models (`145db30`, `src/internal/daemonapi/ddpreduce.go`). A sync completes with the ranks that answered after 45 s, so one dead machine does not fail the run (`5f807f7`).

## Consequences

Auto-tuned sync intervals (`PIPEDPEER_DDP_SYNC_EVERY=auto`) gave 10x speed and a measurably worse loss, so exact per-step sync stays the default and the daemon hands every rank one interval (`145db30`, `_SYNC_EVERY` and `_SYNC_AUTO` in `src/internal/nixgen/shim.go` `_install_ddp`).
