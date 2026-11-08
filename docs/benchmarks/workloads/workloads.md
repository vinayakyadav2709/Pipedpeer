# Campaign workloads

The workloads, which campaign systems run them, and how a run's output is
judged (`result_ok`, brief S8 / `docs/benchmarks/campaign-cases.md` §3). Sizes
are fixed for the whole campaign and come from `results/lab/sizes.env`
(`calibrate.sh` writes it; never change one mid-campaign). The runner
(`scripts/campaign/run.sh`) exports that file's variables before each run.

## Workload table

| Id | Script (in `docs/benchmarks/workloads/`) | Exercises | Result line |
|---|---|---|---|
| W1 | `pool_map.py` | `multiprocessing.Pool` map, CPU bound | `POOLMAP-OK items=%d seconds=%.4f checksum=%.6f` |
| W2 | `pipedpeer-demo/cpu.py --items $W2_ITEMS` | mixed: Pool, joblib, NumPy, pandas | `Pool:` / `joblib:` / `numpy:` / `pandas:` lines |
| W3 | `w3_rf.py` | joblib / scikit-learn | `accuracy: {:.4f}` |
| T1 | `t1_mlp.py` | data-parallel training on CPU | `final loss:` / `weight checksum:` |
| idle | `pool_idle.py` | a Pool map the shim's cost model must decline | `POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f` |
| trivial | `trivial.py` | pipeline overhead, not compute | `TRIVIAL-OK` |

## Sizes

From `sizes.env` (one set for the campaign): `BENCH_PROCS` = pc00's `nproc`
(J1 runs), `W1_ITEMS` (strong) and `W1_WEAK_PER_PC` (weak, items × N),
`W2_ITEMS`, `W3_TREES`, `T1_EPOCHS`. The runner exports them as the workload
scripts' own names (`BENCH_ITEMS`, `BENCH_TREES`, `BENCH_EPOCHS`).

## Which system runs which workload

| Workload | plain | pipedpeer | ray | dask | ssh | pipedpeer-noshim |
|---|---|---|---|---|---|---|
| W1 | REF, E1 | E1, E1b, E4, E5, E6, E7, E11 | E1, E1b | E1, E1b | E1 | — |
| W2 | REF, E1 | E1 (smoke gate) | E1 | E1 | E1 | — |
| W3 | REF, E1 | E1 | E1 | E1 | E1 | — |
| T1 | REF | pipedpeer-exact/-fast (E8) | — | — | — | — |
| idle | REF | E7 | — | — | — | E7 |
| trivial | REF | FC, E4 (E11) | FC | FC | FC | — |

torchrun runs T1 only (E8, `baselines/t1_torchrun.sh`).

## Baseline ports

`baselines/` holds the Ray, Dask and plain-ssh ports (systems of the same
name). Each port's header says what is distributed, what stays local, and the
docs page it follows; each prints the original's result line byte-for-byte.
Pool-shaped code: `ray.util.multiprocessing.Pool` / `client.map` + `gather`;
joblib code: `register_ray` / `joblib.parallel_backend("dask")`; the ssh port
splits the item list (`w#_ssh_part.py --part i --parts n`) and joins the
part outputs on pc00 (`w#_ssh.sh`), so the checksum equals the original's bit
for bit. How many lines each port changes from its original:
`results/lab/port-lines.csv` (`scripts/campaign/port-lines.sh`).

## result_ok (how the runner judges a run)

The runner extracts a run's result line by the workload's regex and compares
the job's own reference (`$LAB/reference/<workload>-<sizekey>.txt`, the plain
N=1 output; sizes without one compare against the majority across systems,
`ref=majority`):

| Workload | Rule |
|---|---|
| W1, idle | the `checksum=` field equals the reference exactly |
| W2 | all four result lines exact |
| W3 | `accuracy:` exact; the ssh port within \|Δ\| ≤ 0.005 (disjoint tree subsets) |
| T1 | `final loss` within 1% (relative) of the one-rank run |
| trivial | `TRIVIAL-OK` present |

Extra log lines around a result are ignored (only lines matching the regex are
kept); a missing result line is `result_ok: false` and still recorded.
