# Baseline ports

Ray and Dask versions of the lab campaign's workloads ([lab-campaign.md](../../lab-campaign.md) 1.3), and a torchrun version of the training workload. Each port is its original with the fewest lines changed that a Ray or Dask user would write. Every other line, including imports the port no longer uses, stays byte-identical so the diff shows only the port. `scripts/campaign/port-lines.sh` counts the changed lines into `results/lab/port-lines.csv`. plain and pipedpeer run the original file, so their count is 0.

| File | Original | Changes |
|---|---|---|
| `w1_ray.py` | `docs/benchmarks/workloads/pool_map.py` | `ray.util.multiprocessing.Pool` in place of `multiprocessing.Pool`; `ray.init(address=$RAY_ADDRESS or "auto")` |
| `w1_dask.py` | same | `Client($DASK_SCHEDULER)`; `client.gather(client.map(work, range(n)))` in place of `Pool.map` |
| `w2_ray.py` | `pipedpeer-demo/cpu.py` | Pool section as W1; `register_ray()` and `parallel_backend("ray")` around the joblib section; `Pool()` without the `os.cpu_count()` argument |
| `w2_dask.py` | same | Pool section as W1; `parallel_backend("dask")` around the joblib section |
| `w3_ray.py` | `pipedpeer-demo/01_sklearn_rf.py` | `ray.init`, `register_ray()`, `parallel_backend("ray")` around `clf.fit` |
| `w3_dask.py` | same | `Client($DASK_SCHEDULER)`, `parallel_backend("dask")` around `clf.fit` |
| `w4_ray.py` | `docs/benchmarks/workloads/grid_search.py` | `ray.init`, `register_ray()`, `parallel_backend("ray")` around `search.fit` |
| `w4_dask.py` | same | `Client($DASK_SCHEDULER)`, `parallel_backend("dask")` around `search.fit` |
| `w5_ray.py` | `docs/benchmarks/workloads/group_fits.py` (E20) | `ray.init`, `register_ray()`, `parallel_backend("ray")` around the `Parallel(n_jobs=-1)` over the groups |
| `w5_dask.py` | same | `Client($DASK_SCHEDULER)`, `parallel_backend("dask")` around the same line |
| `w6_ray.py` | `docs/benchmarks/workloads/cross_val.py` (E20) | `ray.init`, `register_ray()`, `parallel_backend("ray")` around `cross_val_score` |
| `w6_dask.py` | same | `Client($DASK_SCHEDULER)`, `parallel_backend("dask")` around `cross_val_score` |
| `t1_train.py` | not a port | A verbatim copy of the `diloco.py` heredoc in `scripts/bench-diloco.sh`, which holds MLP-C (`BENCH_MODEL=ddp04`) and the transformer (`BENCH_MODEL=tfm`). `port-lines.sh` fails if the two drift apart. |
| `t1_torchrun.py` | `t1_train.py` | `init_process_group("gloo")`, `DistributedDataParallel`, `DistributedSampler` with `set_epoch`, per-rank batch `batch // world` (the same global batch pipedpeer's ring trains), and `RANK`/`WORLD_SIZE` in place of `PIPEDPEER_RANK`/`PIPEDPEER_WORLD_SIZE` |

Why these changes and no others:

- **Pool code.** `ray.util.multiprocessing.Pool` is Ray's documented drop-in for `multiprocessing.Pool`. Dask has no Pool, so its idiom is `client.map` plus `gather`, which keeps the input order, so W1's checksum sums the same floats in the same order.
- **joblib code.** Ray and Dask both plug in as joblib backends: Ray through `ray.util.joblib.register_ray`, Dask through the `"dask"` backend joblib ships, which uses the current `distributed.Client`. scikit-learn's forest asks joblib for threads, and an explicit `parallel_backend` context takes precedence over that hint, so the trees go to the cluster.
- **Connection.** Ray ports call `ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))` explicitly. Without that call Ray falls back to a private local instance when it finds no cluster, and the run would look valid while using one machine. Dask ports fail with a `KeyError` if `DASK_SCHEDULER` is unset.
- **W2's `Pool()` on Ray.** The original sizes its pool to `os.cpu_count()`, the submitting machine's cores. On Ray that would cap the pool at one PC's worth of actors. `Pool()` with no argument takes the whole cluster's CPUs, as a Ray user would write it. W1 keeps `Pool(BENCH_PROCS)`, and the runner sets `BENCH_PROCS` per system (`results/lab/workloads.md`).
- **T1.** The plan names `scripts/bench-train.py`, but that script trains a different, wider MLP (d 512, 122,880 samples) that is neither MLP-C nor the transformer. The paper's MLP-C and transformer are `ddp04` and `tfm` in `bench-diloco.sh`'s `diloco.py`. This directory keeps a copy of that file so the runner has a path to run. The DDP port needs 18 changed lines; the full list is in the table above.

Running them: start the cluster, export the address, run with the campaign env's python.

```sh
E=$HOME/.local/share/pipedpeer-lab/env/bin
$E/ray start --head --num-cpus=0 --disable-usage-stats            # pc00
$E/ray start --address=pc00:6379 --num-cpus=$(nproc)              # each worker
RAY_ADDRESS=pc00:6379 BENCH_PROCS=<sum of worker cores> $E/python3 w1_ray.py

$E/dask scheduler                                                  # pc00
$E/dask worker tcp://pc00:8786 --nworkers $(nproc) --nthreads 1    # each worker
DASK_SCHEDULER=tcp://pc00:8786 $E/python3 w1_dask.py

$E/torchrun --nnodes=<n> --nproc-per-node=1 --rdzv-backend=c10d \
  --rdzv-endpoint=pc00:29500 t1_torchrun.py                        # each rank's PC
```

`scripts/campaign/env/smoke.py` runs plain, Ray and Dask versions of W1–W3 on one machine and checks the results agree.
