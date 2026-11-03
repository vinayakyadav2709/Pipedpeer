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

**Data (runs.jsonl, experiment `e12`, one line per system):** `system`, `start_ns`, `wall_s`, `hosts` (the PCs at the start; pc00 included), `n_start`, `n_final`, `join_every_s`, `timeline` (`{bin_s: 1, t0_ns, per_pc: {pc: [items per second, ...]}}`), `timeline_source`, `items_total`, `items_counted`, `chunks_path`, `events`, `hosts_final`, `result_ok`, and the join's outcome from the chunks: `items_per_pc`, `joiner_share` (the joined PCs' part of the items), `joiner_first_work_s` (`{pc: seconds from its join's start to its first chunk}`, null for a joiner that got none) and `joiner_present_frac` (the part of the wall after the first joiner's first work). Each element of `events` is `{kind: "join", t_ns, done_ns, planned_s, pcs: [...], seen_ns}`. `seen_ns` is when the system first listed the new PCs: pipedpeer, pc00's `/v1/nodes` healthy count; Dask, the plugin's worker-add time; Ray, null. CSV: `csv/e12_timeline.csv` (`run_id, system, t_s, pc, cohort, items_per_s`).

**Figure:** one panel per system (pipedpeer, ray, dask), side by side, with a shared y axis.

- x: seconds since the job started. y: items finished per second, summed over PCs.
- Marks: a stacked step area, one layer per join cohort: the PCs the run started on, then each group of 4 that joined. All layers use the system's colour, with opacity falling for later cohorts.
- Annotations: a dashed vertical line at each join's `t_ns`, labelled `+4`, and a small tick on the x axis at `seen_ns`. The title gives the system and its wall. The legend sits below each panel.
- Reading: the stack should step up after each join. A cohort that stays flat at zero is a system that does not use PCs that join mid-run. Ray's `Pool` is expected to do this, because its actors were made at the start.

**Size, and why the job outlasts the joins.** 13 × W1's calibrated strong size (`PL_JOIN_SCALE`), in the rehearsal too (a smoke size ends before pc00 lists the joiner). Lab day: 13 × 60 s = 780 s of one PC. pipedpeer starts on 4 PCs (pc00 included); joins of 4 are started at 20, 40 and 60 s, and a joiner computes about 5 s after its start command (detection 3 s, first chunk 2 s; `docs/benchmarks/work-reaches-every-pc.md`). At 80 % per-PC efficiency: 80 PC-s done by 25 s, 208 by 45 s, 400 by 65 s, the remaining 380 on 16 PCs in 30 s, so the job ends near 95 s (82 s at 100 %). The first cohort then works for about 74 % of the run, the last for about 32 %. The rehearsal (2-core containers, 10 s calibration, 130 s of one PC, 2 → 3 PCs with the join at 10 s) puts the joiner in for about 70 % of the run; `test/gonogo.py` fails it below 50 %.

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

## E5 shipping the environment: cold is cold

**Data (experiment `e5`):** one line per method and phase (`system` pipedpeer, pip or ray; `phase` cold or update; `extra_package`), `wall_s`, `mb_down` (bytes every PC received, summed), `rx_bytes`, `result_ok`. pipedpeer lines add `all_have_s` (until every PC holds the closure), `closure_on_pc00` (built / already built), `closure_paths` and `closure_mb` (the job closure, `nix-store -qR` and `--size` on pc00), `present_before` (`{pc: {paths, mb, python_packages}}`: the part of the closure that PC held, from a `/nix/store` listing each PC takes just before the run; `python_packages` names the `python3.x-*` paths among them), `store_entries_before` and `before_campaign_env`.

**Why the cold ship runs early.** The campaign env (`env/flake.nix`) is on the same nixpkgs revision as pipedpeer's closures, so once `prep.nixenv` has pulled it onto the workers they hold torch, numpy and the rest, and a pipedpeer "cold" ship at the e5 row would send almost nothing while pip and Ray download everything. Three ways out were weighed: evicting the paths on the workers (they are rooted by the env every other experiment needs, so evicting means pulling them again: minutes of day time and a broken env if it fails), a closure on another Python (not the environment pip and Ray install), or measuring before the env arrives. The day does the last: step `prep.e5cold` (between `prep.firstjob` and `prep.nixenv`) runs `run.sh e5 <the e5 row's arguments> --part pipedpeer-cold`. At that point the workers hold only what `pipedpeer setup` and the first job (W1, standard library only) brought: python and its libraries, no numpy or torch. pc00 builds E5's closure from cache.nixos.org there (in the lab it has no campaign env yet either), and `prep.nixenv` later finds those paths present, so the day is no longer. The e5 row then runs pip's and Ray's cold cases, cold by construction (a fresh venv with `--no-cache-dir`; a fresh Ray session), and the updates, and skips pipedpeer's cold run because its line (`before_campaign_env: true`, `result_ok: true`) is in runs.jsonl. If `prep.e5cold` failed, the row runs it, and `present_before` shows how warm it was. The early run has no telemetry traces (the collectors start at `prep.otel`); its receipt, record and `/proc/net/dev` bytes are all the figure uses.

In the rehearsal of 2026-11-04 (numpy and joblib, the rehearsal's set) the closure was 42 paths, 471 MB; each worker held 24 paths, 220 MB, before the cold ship: the Python 3.14 interpreter, glibc and the other libraries the first job had brought, and none of numpy, joblib, OpenBLAS, LAPACK or gfortran. In the lab the closure adds torch, pandas and scikit-learn, so the share held is smaller still. `test/gonogo.py` fails the day's E5 if any worker held a Python package of the closure.

The day joins with `pipedpeer connect --no-warmup`. Without it, connect builds pipedpeer's common environment (numpy, pandas, scipy, scikit-learn, torch, ...) on every PC in the background from cache.nixos.org: on the same nixpkgs, so the workers held 40 of those 42 paths, and the build ran during the first job (E14).

**Figure** (`e5_env`, `e5_env_mb`): median wall and MB per method and phase. The caption quotes `present_before` (MB and packages) and `closure_mb` per worker for the cold pipedpeer bar.

# More benchmarks: E16-E20 (lab/more-bench)

`analyse_more.py` (called by `analyse.py`) writes the data below; it draws no figures. Each section names the records, their fields, and the figure they are for. Every E16-E20 line also has the usual runs.jsonl fields (README.md).

## E16 energy to solution: `e16_energy`

**Source.** `otel/energy.sh` on every PC: the package counters (`/sys/class/powercap/intel-rapl:<n>/energy_uj`, top-level zones; hwmon `amd_energy` `Esocket<n>` on old AMD kernels) read once a second as root (`sudo -n`), appended to `~/.local/share/pipedpeer-lab/logs/energy.jsonl`. `node.sh start` starts it with the collector, so every run of the day is sampled, and the collector ships the file (`lab.source: energy`). E16 starts it on any PC where it is not running. A PC that cannot read the counters writes one line with `available: false` and the reason: no RAPL zone, no root, or a container (the counters there are the host's package, shared by every container; `PL_ENERGY_IN_CONTAINER=1` reads them anyway).

`energy.jsonl` lines (`ts_ns` on the PC's own clock): `{kind: "energy-start", available, source: "powercap"|"hwmon-amd_energy", period_s, zones: {id: {name, max_uj}}}` or `{kind: "energy-start", available: false, reason}`, then `{kind: "energy", ts_ns, uj: {id: counter}}`. E16 copies each PC's whole file to `results/lab/energy/<pc>.jsonl` at its end.

**Joules** (`lib/campaign/energy.py`): per PC, the growth of every `package-*` zone between a run's `start_ns` and `end_ns`, each end interpolated between the two samples around it, counter wraps (`max_uj`) undone. psys and the subzones (core, uncore, dram) are not added: they overlap the package.

**Data (experiment `e16`):**

- One line `phase: "idle-power"`, `workload: "E16-idle"`, `system: "idle"`: `power_w {pc: W}` over `PL_ENERGY_IDLE_S` (10 s) with the daemons up and Ray and Dask stopped; `energy_per_pc_j`, `energy_unavailable {pc: reason}`.
- W1 at its strong size: plain at N=1, pipedpeer, ray and dask at N=16 (one warm-up, `--repeats` 1). Each line adds `energy_available` (every PC the run used had samples), `energy_j` (the PCs in `hosts`: pipedpeer N, ray and dask N + 1 with their head), `energy_all_pcs_j` (every PC in the inventory, idle ones too: the lab's bill for the job, and the fairest comparison), `energy_per_pc_j {pc: J|null}`, `energy_source`, `energy_sampler {pc: started|running|unavailable: why}`, `energy_unavailable`.

CSV `csv/e16_energy.csv`, one row per run that has joules, from any experiment: `run_id, experiment, workload, system, n, repeat, warmup, result_ok, wall_s, hosts, host_ids, energy_j, energy_all_pcs_j, energy_from` (`inline`: the run line had them, E16 and E17; `archive`: computed from `results/lab/energy/`, which covers E1 and E1b because E16 runs right after them). `e16_summary.json`: `available`, `idle_power_w {pc: W}`, `unavailable {pc: reason}`, and `groups`, one per (experiment, workload, system, n): `runs, median_wall_s, median_energy_j, median_energy_all_pcs_j, median_energy_above_idle_j` (`energy_j` minus the hosts' idle power times the wall).

**Figure:** two panels. Left, W1 from E1: x N (1, 4, 16), y median `energy_all_pcs_j` (kJ), one line per system with plain as a point at N=1; the dashed line is plain's joules. Right: energy-to-solution against wall, one point per (system, N), both axes log, so "faster and cheaper" is the bottom-left corner. Title `Energy to solution, W1 (package energy, RAPL)`. When `available` is false, no figure; the summary's reasons go in the caption instead.

## E17 idle footprint and cluster start: `e17_footprint`, `e17_ready`

**Data (experiment `e17`, `workload: "E17-idle"`, one line per system):** `system, n` (workers), `hosts` (pc00 plus the workers), `commands` (`[{pcs, command, runner_command}]`: what a user types, and what the runner ran), `commands_typed` (pipedpeer 0 with `pipedpeer setup --boot`, ray and dask 1 + n), `commands_typed_note`, `start_ns, ready_ns`, `ready_s` (seconds from the first start command until the runner's check first passed), `ready_after_s` (the last failed check: the truth lies between), `start_call_s` (the start calls alone, mostly Ansible), `idle_s` (`PL_IDLE_S`, 60), `idle_start_ns, idle_end_ns`, `footprint {pc: {cpu_s, cpu_pct, rss_mb_avg, rss_mb_max, pss_mb_avg, pss_mb_max, procs, names {comm: count}, samples, seconds}}`, `cpu_s_total, cpu_pct_mean_per_pc, rss_mb_total, pss_mb_total, procs_total`, `idle_energy_j, idle_power_w` (all PCs, null without RAPL). The processes counted: pipedpeer's daemon and its children; every process carrying the runner's mark for Ray (`PL_CAMPAIGN_RAY=1`: raylet, gcs_server, the idle workers, the agents) and Dask (`PL_CAMPAIGN_DASK=1`: scheduler, nanny, worker).

CSV `csv/e17_footprint.csv`, one row per (system, PC): `run_id, system, pc, cpu_s, cpu_pct, rss_mb_avg, pss_mb_avg, rss_mb_max, procs, seconds`. `e17_summary.json` per system: the line's fields above, `commands` as a list of strings.

**Figures:**

- `e17_footprint`: two bar panels, one bar per system in its colour: idle CPU (`cpu_pct_mean_per_pc`, % of one core per PC) and memory (`pss_mb_total / (n + 1)`, MB per PC, with `rss_mb_total` as a thin outline). Title `Idle cost per PC, 60 s`.
- `e17_ready`: horizontal bars of `ready_s` per system, with a whisker down to `ready_after_s`, and the number of commands typed written at the bar's end (`0`, `17`, `17`). Title `Nothing to cluster ready, 17 PCs`.

## E18 noisy neighbour: `e18_hog`

**Data (experiment `e18`):** per system two runs (`PL_HOG_REPEATS` pairs): `fault: "none"`, then `fault: "hog"`: `PL_HOG_AT_S` (5 s) after the job starts, one busy python loop per core on half the cores (`taskset -c 0..k-1`) of `hog_pcs` (pc01; `PL_HOG_PCS` more). W1 at N=4, twice its strong size (`PL_HOG_SCALE`). Fields: `fault, hog_pcs, hog_cores, hog_at_s, events` (`{kind: "hog", t_ns, done_ns, pcs, cores}`), the throughput fields of E12/E13 (`timeline, timeline_source, items_total, items_counted, chunks_path`), `items_per_pc {pc: items}`, `hog_share` (the hogged PCs' part of the items), `fair_share` (their part of the PCs that do work: pipedpeer counts pc00, Ray's and Dask's head does none).

CSV `csv/e18_hog.csv` (`run_id, system, n, repeat, fault, hog_pcs, hog_cores, wall_s, result_ok, hog_share, fair_share, items_counted, error`). `e18_summary.json` per system: `wall_none_s, wall_hog_s, slowdown, hog_share_none, hog_share_hog, fair_share`.

**Figure:** two panels. Left: grouped bars per system, wall without and with the hog (light and full system colour), the slowdown printed above. Right: the hog run's timeline as in E13, items per second per PC stacked, the hogged PC's layer hatched, a vertical line at the hog's `t_ns`. Reading: a system that moves work off the loaded PC shows `hog_share` falling below `fair_share` and a small slowdown; Ray's Pool, whose actors take fixed batches, waits for the slow PC.

## E19 cold vs warm ship: `e19_ship`

**Data (experiment `e19`):** pipedpeer and ray at N=16, four lines each, `phase` (also `variant`) in order `cold` (a networkx Pool.map never run on the cluster: a new environment), `warm` (the same again), `edit` (one constant changed: a new workspace, the same environment), `newdep` (plus `import rich`: a new environment). `ttfr_s` (time to first result: the wall from the command to the result; the script prints only at the end and computes little), `result`, `result_ok` (pipedpeer and ray agree per phase). pipedpeer adds `env_build_s, env_send_s, env_send_bytes, script_step_s` from `pipedpeer jobs last --export json`. The generated scripts are in `results/lab/e19/<phase>/`. Ray uses `runtime_env={"working_dir": ".", "pip": [...]}`. Dask is not run (no per-job environment; E5 measures pip).

CSV `csv/e19_ship.csv` (`run_id, system, n, phase, ttfr_s, wall_s, script_s, env_build_s, env_send_s, env_send_bytes, script_step_s, result, result_ok, error`). `e19_summary.json`: `{system: {phase: ttfr_s}}`.

**Figure:** grouped bars, x the four phases, one bar per system, y `ttfr_s` (s). pipedpeer's bars split into `env_build_s`, `env_send_s` and the rest. Title `Time to first result while iterating, 16 PCs`.

## E20 one-liner real workloads: `e20_speedup`

**Data (experiment `e20`):** W5 (`group_fits.py`: pandas groupby, each group to `joblib.Parallel(n_jobs=-1)`) and W6 (`cross_val.py`: `cross_val_score(n_jobs=-1)`, 64 fits). plain at N=1, pipedpeer, ray and dask at N=16, one warm-up and 2 repeats. Each line adds `script_lines_changed` (0 for plain and pipedpeer, which run the original; the port's changed lines for Ray and Dask). `result_ok` is the equality check against plain (W5 within 1e-9 relative, W6 exact). `csv/e20.csv` comes from analyse.py's generic table.

`e20_summary.json`: one entry per (workload, system, n): `runs, ok_runs, median_wall_s, speedup_vs_plain, result_ok_all, script_lines_changed, result`.

**Figure:** one panel per workload, bars per system of `speedup_vs_plain` at N=16, a ✓ or ✗ above each bar from `result_ok_all`, and `script_lines_changed` under the system name (`0 lines`, `6 lines`). Title `Unmodified pandas and scikit-learn, 16 PCs`.

# Paper figures

`paper_figures.py` draws the IEEE paper's evaluation figures (E1 to E20, telemetry and receipts) from the same inputs, in one style (`paperfig/style.py`, `paperfig/paper.mplstyle`), at IEEEtran's column widths. `docs/paper/FIGURES.md` lists every figure with its file, width, paper section and draft caption; `docs/paper/build-preview.sh` compiles the preview document. `--sample` reads the container sample, including `results/lab/sample/data/more/` for E16-E20.
