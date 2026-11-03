# Lab campaign runner

The runner side of [docs/benchmarks/lab-campaign.md](../../docs/benchmarks/lab-campaign.md) (section 1.4): it runs each experiment on the lab PCs and writes one JSON line per run.

```sh
scripts/campaign/run.sh <experiment> [--rehearsal] [--inventory FILE] [--n 1,2] [--repeats K]
                                     [--seed S] [--size W1:BENCH_ITEMS=48] [--workloads W1,W2]
scripts/campaign/analyse.py [--runs results/lab/runs.jsonl]
scripts/campaign/rehearse.sh [--keep] [--only "e1 e4"]
```

| Path | What |
|---|---|
| `run.sh` | Entry point. On the first run it installs Ansible. |
| `lib/ansible.sh` | Installs ansible-core, `community.docker`, `ansible.posix` and matplotlib in a venv at `$HOME/.local/share/pipedpeer-lab/ansible` (no root needed). It is the only way the runner reaches a PC (`sh`, `bg`, `put`, `ping`, `hosts`). |
| `lib/campaign/` | The runner, as stdlib Python. `runner.py` holds the context, the per-run record and the scaling loop. `systems.py` holds the four systems and how each one is held to N. `workloads.py` reads `workloads.json`. `record.py` writes `runs.jsonl`. `remote.py` holds timed remote commands. `guided.py` is the E3/E10 timer. |
| `experiments/<id>.py` | One file per experiment. The plan's section 3 table lists them. |
| `analyse.py` | Writes the CSVs, figures and the E8 table. |
| `rehearse.sh` | Runs every experiment on the container lab. |
| `tests/` | pytest suite: the runner against a fake cluster, and `analyse.py` against synthetic data. |

Test with `$HOME/.local/share/pipedpeer-lab/ansible/bin/python -m pytest scripts/campaign/tests`.

## Inputs

- **`results/lab/inventory.ini`**, from the setup scan. It has `[head] pc00` and `[workers] pc01..` with `ansible_host` and `machine_id` per host. An optional `lab_ip` gives the address the other PCs use for that host; the default is `ansible_host`.
- **`scripts/campaign/workloads.json`**, from lab/baselines. `lib/campaign/workloads.py` documents every field the runner reads. A size of `"0"` means the size is not calibrated yet. A run with such a size is recorded as failed and the experiment continues; pass `--size` to override it. Without the file, the runner falls back to a built-in W1 and logs a warning.
- **The campaign env on every PC** at `$HOME/.local/share/pipedpeer-lab/env` (`bin/python3`, `ray`, `dask`, `torchrun`). `PL_REMOTE_ENV` points elsewhere.

## runs.jsonl

The runner appends one line per run to `results/lab/runs.jsonl` and flushes it as the run ends. Failed runs get a line too. Logs go to `results/lab/logs/<run_id>/`: `output.log`, plus `record.json` for PipedPeer runs.

| Field | Meaning |
|---|---|
| `run_id` | `<experiment>-<UTC time>-<6 hex>` |
| `experiment`, `workload`, `system`, `n`, `repeat`, `warmup` | Which run this is. `repeat` 0 is the warm-up, and `analyse.py` excludes runs with `warmup: true`. |
| `start_ns`, `end_ns`, `wall_s` | Timed on the PC that ran the command, with that PC's own clock. Ansible's start-up time is never included. |
| `result` | Every match of the workload's `result_regex`, joined with ` \| ` |
| `result_ok` | `result` equals the plain run's result at the same size (`result_compare`). The plain result is cached per (workload, size) in `results/lab/result-cache.json`. If no plain result exists yet, the runner does one plain run first and does not record it. E1b and E7 use the first successful result at a size instead. `result_ref` names the source of the reference. |
| `hosts` | PC ids the system ran on (pc00 included) |
| `git_commit`, `pipedpeer_version` | The driver's checkout, and the version `pipedpeer --version` reports on pc00 |
| `receipt` | PipedPeer runs only: the shim's receipt (`PIPEDPEER_RECEIPT`). `record_path` points to `pipedpeer jobs last --export json`. |
| `size`, `mode`, `caps`, `seed`, `script_s`, `rc` | The size env, strong or weak, core caps (E4), the order seed, the script's own timing (`timing_regex`) and the exit code |
| `error`, `failing_pc`, `log_path` | Set when the run failed. A failed run never stops the experiment. |

Some experiments add fields: E5 has `phase`, `mb_down`, `all_have_s` and `per_pc_s`. E6 has `fault`, `killed_pc`, `kill_at_s`, `kill_landed`, `ctrlc_latency_s` and `interrupt_landed`. E8 has `variant` and `bytes_per_rank`. E9 has `paths` and `introducer_bytes`. E11 has `join_s`, `bytes_per_node_per_min` and `bytes_source`. E3 and E10 have `minutes`, `commands_typed`, `script_lines_changed`, `participant` and `notes`. A rehearsal line carries `rehearsal: true`.

## Order and warm-ups

For each workload and each N, every system first gets one warm-up run. Plain warms up first so that its result can serve as the reference. Then each repeat runs the systems in a new random order. The order comes from `--seed`; each line records the seed, so a run can be replayed. Plain runs only at N=1.

## Holding each system to N

| System | How |
|---|---|
| pipedpeer | Daemons run on pc00..pc(N-1). Everywhere else the runner stops them with `pipedpeer stop`. The runner then waits until pc00's API (`/v1/nodes`) lists exactly N-1 other healthy nodes. **pc00 is one of the N.** Every daemon is restarted after the experiment. |
| ray | pc00 runs the head with `--num-cpus=0`: it submits and does no work. pc01..pcN run `ray start --address=pc00:6379 --num-cpus=<cores>`. The runner waits until N nodes with CPUs are alive. |
| dask | pc00 runs the scheduler. pc01..pcN each run `dask worker --nworkers <cores> --nthreads 1`. The workloads are pure-Python CPU loops that hold the GIL, so one single-threaded process per core is the only configuration that can use every core. The runner waits until N hosts and every process have joined. |
| plain | The campaign env's `python3` runs on pc00, at N=1 only. |
| torchrun (E8) | pc00..pc(N-1) each run one rank, with c10d rendezvous on pc00. These are the same PCs that PipedPeer's `--ddp N` uses. |

Starting Ray or Dask first stops whichever of the two is running, so neither idles while the other is measured. Both stops are idempotent: `ray stop --force`, and `pkill` of the Dask scheduler and worker processes, checked with `pgrep`. PipedPeer's daemons stay up through an N block, as they would in normal use.

**Core caps (E4)** use CPU affinity for every system: `taskset -c 0-<c-1>`, or `CPUAffinity=` in a drop-in when the daemon runs as a systemd user service. The plan suggested `systemd-run --scope -p AllowedCPUs`. That does not hold the daemon, because the daemon starts itself in its own `systemd-run --user` scope and so leaves a root scope behind. Affinity is inherited by every child process (crun, the raylet, Dask's nanny), so all four systems are capped the same way. **The headcount split** in E4 is a daemon started with `PIPEDPEER_POOL_SPLIT=headcount`. That is the even split that commit 8c9284c replaced, restored as a benchmark baseline (`schedule.Even`).

## Telemetry and R2

At the end of a campaign experiment the runner calls `scripts/campaign/sync-r2.sh`, which uploads through the ingest Worker (or R2 keys). A rehearsal never does: with `--rehearsal`, or with `PL_RESULTS` anywhere but `results/lab`, nothing is uploaded, because `~/.config/pipedpeer-lab/ingest.env` on the machine would send rehearsal data to the campaign's bucket. `PL_SYNC=1` forces the upload (point `PIPEDPEER_LAB_INGEST_ENV` at a stand-in first); `PL_SYNC=0` leaves it to the caller, as the lab day does.

PipedPeer runs keep the shim's receipt whole in `logs/<run_id>/receipt.json.gz`; the `receipt` field in runs.jsonl is a summary (`lib/campaign/receipts.py`): the receipt's small fields and its steps per (step, node).

## Environment knobs

| Variable | Default | Effect |
|---|---|---|
| `PL_RESULTS` | `results/lab` | Output directory |
| `PL_REMOTE_ENV` | `$HOME/.local/share/pipedpeer-lab/env` | The campaign env on the PCs |
| `PL_RUN_TIMEOUT` / `PL_WAIT_TIMEOUT` | 3600 / 300 s | Time limit per run, and per wait for a cluster to reach N |
| `PL_NPROC` | PC's `nproc` | Cores each PC offers to Ray and Dask, and the value of `{nproc_*}` |
| `PL_SYSTEMS` | all | Run only these system kinds, for example `plain,pipedpeer` |
| `PL_RAY_START_ARGS`, `PL_DASK_WORKER_ARGS` | rehearsal: small object store, `--memory-limit 0` | Extra arguments for `ray start` and `dask worker` |
| `PL_E9_HOST` | `local` (rehearsal: last PC) | Where E9's laptop steps run |
| `PL_E11_IDLE_S` | 600 (rehearsal: 30) | E11 idle window per cluster size |

`--size W1:BENCH_ITEMS=48` replaces workloads.json's size. In a weak-scaling experiment (E1b) it is the size per PC, multiplied by N like `weak_per_pc`.

## Rehearsal limits

`rehearse.sh` runs every experiment at N=1 and N=2 on three 2 GB containers that share one host. It uses the workloads' `smoke` sizes and `PL_NPROC=2`. The rehearsal proves that each code path runs; its timings are not measurements. These are the limits of a 3-container lab:

- **E11:** sizes 8, 16, 32 and 60 are capped at the 3 containers. The rehearsal runs the join-and-idle path at sizes 1 to 3 with a 30 s idle window. Containers have no `nft`, so bytes come from the NIC counters (`bytes_source: nic`).
- **E9:** there is no second network. The "laptop" is pc02 on the lab bridge, so the path type is `lan`, and `net-verify.sh` is skipped because its introducer kill needs a real introducer.
- **E5:** this runs at N=2 with a small package set (`e5_env_small.py`: numpy and joblib). Shipping torch three times would not fit in 2 GB containers. pip installs joblib only: the lab image is Nix-only, so a manylinux wheel with C code, such as numpy, installs but cannot load `libstdc++`. The lab PCs have that library.
- **E6:** this runs at N=2, so the run kills the one worker other than pc00. It runs W1 at `BENCH_ITEMS=4000`, ten times the smoke size. At the smoke size a run ends in about a second, before Ansible can deliver the kill. Each line's `kill_landed` (and `interrupt_landed` for Ctrl-C) says whether the job was still running when the signal arrived.
- **E4:** caps are 2/1 instead of 8/4/2/1, at N=2.
- **E3 and E10:** these are guided timers with no PC involved. The rehearsal feeds their answers through stdin.
