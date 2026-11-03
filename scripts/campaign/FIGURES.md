# Headline figures: E12, E13, E14, E15, and W4 in E1

What each headline experiment records, and the figure `analyse_headline.py` draws from it. `analyse.py` calls it, so the figures land in `results/lab/figures/` beside the others, and the CSVs in `results/lab/csv/`. The visualisation agent (`sample_figures.py`) can redraw any of them from the fields below. Times are seconds since the job's `start_ns`, unless a figure says otherwise.

## Where the per-chunk records come from

`lib/campaign/timeline.py` turns each system's own records into chunks `{pc, t0, t1, items}`. It writes them to `logs/<run_id>/chunks.json` and bins them into the line's `timeline`. Each chunk's items are spread evenly over `[t0, t1]`; a Dask task is a point in time.

| System | Source (`timeline_source`) | Placement | Items per chunk |
|---|---|---|---|
| pipedpeer | The shim's receipt (`PIPEDPEER_RECEIPT`), key `steps`: every `compute` step with `machine` (node id), `start_ns` and `dur_ns`. `receipt-steps` | The node id, mapped to a PC through each PC's own `/health` after the run | `items` on the step. This needs the shim change in commit "shim: count the items each compute step finished". With an older nightly, items are estimated from busy time (`receipt-steps-estimated`), which is fair for W1 because every item costs the same. A part the daemon re-ran on the origin after a peer failed has a step marked `"fallback": true`, built from the part's `ms`. A map the shim kept on pc00 (no peer when it started) has no steps: `receipt-local-only`, one chunk on pc00 over the whole run |
| dask | A scheduler plugin (`experiments/scripts/timeline_probe.py`), put in before the job starts. It counts tasks finished per second per worker (processing to memory, on the scheduler's clock, which is pc00's), tasks taken back from a worker that left, and worker add/remove times. `dask-scheduler-plugin` | Worker address, mapped to a PC by IP | 1 (`client.map`, one task per item) |
| ray | `ray.timeline()` after the job: each `PoolActor.run_batch` with start, duration and node IP. `ray-timeline`. The state API needs the dashboard, which the runner turns off | Node IP | `ceil(items / (4 × actors))`, Ray's own `Pool` chunk size |

`items_counted` is the sum over chunks. Below `items_total`, some work went unrecorded. Above it, some items were computed twice.

## E12 elastic join: `e12_join`

**Data (runs.jsonl, experiment `e12`, one line per system):** `system`, `start_ns`, `wall_s`, `hosts` (the PCs at the start; pc00 included), `n_start`, `n_final`, `join_every_s`, `timeline` (`{bin_s: 1, t0_ns, per_pc: {pc: [items per second, ...]}}`), `timeline_source`, `items_total`, `items_counted`, `chunks_path`, `events`, `hosts_final`, `result_ok`. Each element of `events` is `{kind: "join", t_ns, done_ns, planned_s, pcs: [...], seen_ns}`. `seen_ns` is when the system first listed the new PCs: pipedpeer, pc00's `/v1/nodes` healthy count; Dask, the plugin's worker-add time; Ray, null. CSV: `csv/e12_timeline.csv` (`run_id, system, t_s, pc, cohort, items_per_s`).

**Figure:** one panel per system (pipedpeer, ray, dask), side by side, with a shared y axis.

- x: seconds since the job started. y: items finished per second, summed over PCs.
- Marks: a stacked step area, one layer per join cohort: the PCs the run started on, then each group of 4 that joined. All layers use the system's colour, with opacity falling for later cohorts.
- Annotations: a dashed vertical line at each join's `t_ns`, labelled `+4`, and a small tick on the x axis at `seen_ns`. The title gives the system and its wall. The legend sits below each panel.
- Reading: the stack should step up after each join. A cohort that stays flat at zero is a system that does not use PCs that join mid-run. Ray's `Pool` is expected to do this, because its actors were made at the start.

## E13 churn: `e13_churn`

**Data (experiment `e13`, one line per system):** `system`, `n`, `size` (E1b's weak size at N), `wall_s`, `result`, `result_ok`, `error`, `fault: "churn"`, `churn_every_s`, `churn_down_s`, `kills`, `items_rerun`, `rerun_source`, `timeline`, `timeline_source`, `items_counted`, `events`. Each element of `events` is `{kind: "kill"|"restart", t_ns, done_ns, pc, restarted_by: "runner"|"systemd", after_job}`. The no-churn wall is the median `wall_s` of usable `e1b` lines with the same `workload`, `system`, `n` and `size`. CSV: `csv/e13_summary.csv` (`wall_s, no_churn_wall_s, slowdown, result_ok, kills, items_rerun, rerun_source, ...`).

`items_rerun` by system:

- pipedpeer: the items of all compute steps minus `mapped_items` (`receipt-steps`). With an older shim: duplicates from the counters plus the items of failed parts (`receipt-counters`).
- dask: tasks taken back while running, plus finished results that died with a worker and were computed again (`items_counted - items_total`).
- ray: batches counted beyond the items total.

**Figure:** one panel per system.

- x: seconds since the job started. y: items finished per second, all PCs, as a step line in the system's colour.
- Annotations: a solid vertical line with a filled ▼ at the top for each kill, and a dotted line with a hollow ▲ for each restart.
- Two-line title: `system: result ok | result WRONG | FAILED` and `<wall> s vs <no-churn wall> s no churn, <items_rerun> items re-run`.
- Reading: dips at kills, and recovery after restarts. A Ray run that ends in FAILED is a result; Ray's `Pool` does not survive losing an actor.

## E14 curl to cluster: `e14_staircase`, `e14_summary.json`

**Data:** `results/lab/timeline.jsonl`, one line per (PC, event): `{pc, event, t_ns, clock: "pc"|"pc00", machine_id, source}`. `day/timeline.sh` writes these events:

| Event | When |
|---|---|
| `head_start` | `lab.sh head` began on pc00 |
| `setup_start` | `lab.sh` began on a PC |
| `labpc` | the LAB-PC line printed |
| `ssh_ok` | the first discover scan that logged in. Keyed by `machine_id`, because pc numbers settle only after discover; at most one scan (about 15 s) late |
| `pc_map` | `machine_id` to the final pc id |
| `install_start`, `install_end` | `install-pipedpeer.sh` on the PC |
| `join_end` | `pipedpeer connect` returned |
| `cluster_ready` | pc00's `/v1/nodes` lists every worker healthy |

`experiments/e14.py` adds `first_work` (the start of the first compute step each PC ran in the day's first distributed job), `first_job_start` and `first_job_end`. Its runs.jsonl line has `phase: "first-job"`, `first_work {pc: ns}` and `pcs_with_work`. `labpc`, `setup_start`, install and join times are on each PC's own clock; the rest are on pc00's. `clocks.txt` has the offsets, and chrony was turned on where an offset was over 10 ms.

CSV: `csv/e14_pcs.csv`, one row per PC, with seconds from `head_start` to each of `setup_start, labpc, ssh_ok, install_end, join_end, first_work`. `e14_summary.json` has `pcs`, `pcs_with_first_work`, `head_to_last_labpc_s`, `head_to_cluster_ready_s`, `head_to_first_job_start_s`, `head_to_all_pcs_working_s` and `head_to_first_job_end_s`. Worker times can be negative: the workers are set up before the head line is typed.

**Figure:** the per-PC staircase.

- One row per PC, sorted by `labpc` time, pc ids on the y axis, top to bottom.
- x: minutes since the head line on pc00.
- Marks per PC: a thin grey line from its first to its last event, with ● LAB-PC, ■ answers SSH, ▲ pipedpeer installed, ◆ joined, and a blue ★ for first work.
- Annotations: dashed vertical lines at `cluster ready` and `first job`, each labelled with its minute.
- Title: `Curl to cluster: N PCs, first distributed job at T min`.
- Headline number: `head_to_all_pcs_working_s`, the time from the head line until every PC has run work.

## E15 network sensitivity: `e15_network`

**Data (experiment `e15`):** `system`, `n`, `rtt_added_ms` (0, 20, 80), `netem_hosts`, `wall_s`, `repeat`, `warmup`, `result_ok`. Each PC in `netem_hosts` had `RTT/2` of netem delay on its lab interface, so every pair of PCs sees the full RTT added. analyse.py groups the E15 CSV by `rtt_added_ms` too. CSV: `csv/e15_network.csv` (`system, n, rtt_added_ms, runs, median_wall_s, slowdown_vs_0ms`).

**Figure:** one panel.

- x: added RTT in ms, with ticks at 0, 20 and 80. y: median wall divided by the same system's median wall at 0 ms.
- Marks: one line with point markers per system, in the system colours, and a dashed reference line at 1.0.
- Title: `Network sensitivity, W1 at N=4`.

## W4 in E1

W4 (`GridSearchCV`) is one more workload in `e1` (`workload: "W4"`). The existing `e1_speedup` figure gets a W4 panel, and `csv/e1.csv` gets W4 rows; nothing new is needed. The result line compared across systems is `best_params={...} | best_score=...`.
