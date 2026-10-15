# DDP gradients sync through the daemon, which averages them

torch's gloo mesh needs rank-to-rank TCP on ports of its own, which failed on every cross-network attempt, so ranks sync by `POST /v1/ddp/sync` on the lead rank's daemon (`6abe4df`). The daemon folds contributions into one float64 accumulator and returns one model, instead of a blackboard that sent every rank N models (`145db30`, `src/internal/daemonapi/ddpreduce.go`). A sync completes with the ranks that answered after 45 s, so one dead machine does not fail the run (`5f807f7`).

Revised for the address-book phase: the gloo/nccl backend (`PIPEDPEER_DDP_BACKEND`, the master-port probing and the training-port firewall rule) was removed, so the daemon path is the only one. Gradients travel as int8 with per-block scales and error feedback by default. The script's batch is split across ranks by speed (the slices sum to the script's batch), so the learning rate and the maths stay the script's own. After warm-up the lead compares the measured split step time with the fastest rank alone and keeps the ring or releases all but the fastest rank (exit 77), unless `--ddp N` fixed the ring.

## Consequences

- Exact per-step sync stays the default. The earlier "auto interval gave 10x speed and a worse loss" result was measured with fp16 weight averaging, no outer optimizer and every rank running the full batch, so it does not judge DiLoCo; `--ddp-sync fast` (DiLoCo: exact warm-up, drift-driven interval, outer Nesterov) is opt-in, and auto mode may pick it only after its loss benchmark passes.
- A rank whose script fails stops every rank at once; a lost machine is dropped and the ring continues; a lost lead stops the survivors with exit 76.
