# Runner rehearsal, 2026-11-03 (container lab, emulated)

`scripts/campaign/rehearse.sh` on `LAB_NAME=runner`: three podman containers of 2 GB each, all on one 20-thread
host shared with other labs. The tree was lab/runner merged with lab/baselines `f34a036f` (env, ports,
`workloads.json`). The rehearsal used smoke sizes, `PL_NPROC=2`, N=1 and N=2, and one repeat after a warm-up.
**These timings are not measurements.** They show that each code path runs, and nothing else.

| Exp | Runs | Failed | What ran |
|---|---|---|---|
| e1 | 42 | 0 | W1, W2, W3 × plain, pipedpeer, ray, dask at N=1,2; every result matched plain's |
| e1b | 14 | 0 | weak scaling, W1 |
| e4 | 16 | 0 | caps 2/1 with taskset, measured vs headcount split, ray, dask |
| e5 | 6 | 0 | pipedpeer, pip and ray runtime_env, cold and +tqdm; small package set; pip used joblib only (see README) |
| e6 | 10 | 0 | W1 at `BENCH_ITEMS=4000`; each kill landed mid-run on a live process (`kill_landed`, `killed_processes`); Ctrl-C interrupted (rc 130), all work stopped in 4.8 s |
| e7 | 4 | 0 | trivial.py with `--remote`, shim on vs `PIPEDPEER_SHIM=0` |
| e8 | 12 | 0 | T1 mlp-c and transformer, pipedpeer exact/fast vs torchrun gloo, 2 ranks |
| e9 | 1 | 0 | "laptop" = pc02 on the lab bridge (path `lan`); net-verify.sh skipped |
| e11 | 4 | 0 | join and 30 s idle at sizes 1, 2, 3, NIC bytes; one W1 at 3 |
| e3, e10 | 3, 2 | 0, 1 | guided timers fed through stdin; the e10 failure is a scripted "gave up" |

Container artefacts that do not apply to the lab:
- With `PL_NPROC=2`, plain runs 2 processes. The PipedPeer shim sizes its pool from `cpu_count()`, which is 20 inside a container. PipedPeer's N=1 "speedup" of 2.3 on W1 comes from that difference, not from the system.
- The E5 PipedPeer "cold" run was warm, because the env had been built in an earlier attempt.
- E11 and E5 bytes are NIC totals.

This rehearsal found the following runner bugs; each is fixed in the commit that names it:
- Dask workers outlived their nanny and rejoined the next scheduler.
- awk, pgrep and pkill are missing from the image. Because of that, E6 kills and Ctrl-C were silent no-ops and E5 read 0 MB.
- A venv has `pip3` but no `pip`.
