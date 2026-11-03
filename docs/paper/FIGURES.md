# Paper figures

Every figure for `docs/paper/paper.tex`, at its final size: single column 252 pt (3.49 in), double column 516 pt (7.14 in, `figure*`), which are IEEEtran's own `\columnwidth` and `\textwidth` (IEEE's nominal 3.5 in and 7.16 in overflow the class by 1-1.5 pt). Include them at their natural size (`\includegraphics{...}` with no width) so nothing is scaled and the 8 pt labels stay 8 pt.

- Evaluation figures: `scripts/campaign/paper_figures.py` writes `docs/paper/figures/<name>.pdf` (and a 600 dpi PNG) from `results/lab/` (the lab day). `--sample` draws the container sample; `--draft` writes `docs/paper/figures-draft/` with an "emulated" stamp, for the preview only.
- Design diagrams: TikZ sources in `docs/diagrams/tikz/`, built by `docs/diagrams/tikz/build.sh` into `docs/diagrams/tikz/pdf/`.
- Preview: `docs/paper/build-preview.sh` regenerates everything and compiles `docs/paper/figures-preview.pdf` (IEEEtran, every figure at its real size with its draft caption); page renders in `docs/paper/preview-pages/`.
- Style: `scripts/campaign/paperfig/style.py` and `paper.mplstyle` (plots), `docs/diagrams/tikz/ppstyle.tex` (diagrams). One mapping everywhere: PipedPeer blue #0072B2, solid, circle; Ray vermilion #D55E00, dashed, square, `////`; Dask green #009E73, dash-dot, triangle, `\\`; plain Python grey #595959, dotted, diamond; torchrun purple #CC79A7, triangle down, `xx`.

After the lab day: `python3 scripts/campaign/paper_figures.py` reads `results/lab/` (runs.jsonl, otel/, logs/, timeline.jsonl, energy/, node-ids.csv) and writes the final figures to `docs/paper/figures/`, unstamped; `docs/paper/build-preview.sh --lab` rebuilds the preview from them. A figure whose data is missing is listed in `skipped.txt` with the reason. To check layouts at lab scale first, `python3 scripts/campaign/paperfig/synth.py DIR` writes an invented 17-PC results directory (outside the repository; never for the paper).

This file and the preview's figure list are generated (`paper_figures.py --docs`); edit captions in `scripts/campaign/paperfig/figs_*.py` or `docs/diagrams/tikz/diagrams.json`.

## System Design

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `arch` | `docs/diagrams/tikz/pdf/arch.pdf` | double | 7.14 x 2.43 | Every machine runs the same binary: a short-lived client that builds, places and ships the job, and a daemon that admits it, caches its closure and runs it, with peers on the LAN or over direct links. | System design. Both roles are one binary. The client builds the closure, takes a lease (1) and sends only the store paths the worker lacks (2); output and changed files stream back (3). The worker pushes the signed closure to peers (4) and the shim spreads parts and gradients over them (5). Introducers carry addresses, never job data. |

## System Design > Placement

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `placement` | `docs/diagrams/tikz/pdf/placement.pdf` | single | 3.49 x 2.16 | How the client picks a node: candidates, ordered hard filters, the score of Eq. 1, the three-phase lease, and the two retry paths. | Placement in the submitting client. Hard filters run in order; survivors are ranked by the score S of Equation 1. A refused lease moves to the next node (a); a failed run is placed again, at most three times (b). |
| `lease` | `docs/diagrams/tikz/pdf/lease.pdf` | single | 3.49 x 1.31 | The worker's lease state machine: accept reserves, commit runs, and expiry, completion or a silent submitter releases. | Lease states on the worker. A reservation expires 30 s after its last renewal plus a 5 s grace; a running lease is kept while its job lives and reaped only after 5 minutes without renewal. |

## System Design > Environment Reproduction

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `closure` | `docs/diagrams/tikz/pdf/closure.pdf` | single | 3.49 x 1.88 | The store path as cache key: what the client asks the worker and what crosses the wire when warm, partly cached or cold. | Environment transfer. The client asks whether the worker can run the closure (1) and which store paths it lacks (2), and sends only those (3). Adding one package sent 7 of 43 paths, 28.6 MB of 206.0 MB. |

## System Design > Joining Across Networks

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `nat` | `docs/diagrams/tikz/pdf/nat.pdf` | single | 3.49 x 2.12 | Message sequence of a join across networks, from the invite code to a direct QUIC link, with the introducer carrying only addresses. | Joining across networks. Both nodes register their candidates with an introducer (2), which forwards one punch request (3); both sides then probe every candidate pair (4) until a direct QUIC link forms (5). No job data passes through the introducer. |
| `trust` | `docs/diagrams/tikz/pdf/trust.pdf` | single | 3.49 x 1.42 | The two roots of trust, the cluster secret and the node key, and the four places each one is checked. | Trust. The cluster secret guards every daemon route and the QUIC hello; the node key makes the TLS certificate, signs the hello and signs closures. |

## System Design > Transparent Interception

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `shim` | `docs/diagrams/tikz/pdf/shim.pdf` | double | 7.14 x 1.92 | The shim loaded before the script patches parallel entry points, and each call either leaves the machine or runs as written. | Transparent interception. The shim is imported before the script's first line (1) and patches the listed calls (2). Each call is gated (3); offloaded work is split by measured speed while local cores re-run late parts (4), and the script receives the result a local run would give (5). |
| `shuffle` | `docs/diagrams/tikz/pdf/shuffle.pdf` | single | 3.49 x 1.37 | The hash shuffle behind pandas grouping and joining: every key lands whole on one node, so partial results only need concatenation. | Hash shuffle for pandas grouping and joining. Rows are bucketed by a hash of their key, so each key is complete on one node and any aggregation is exact without a combiner; the row order of joins is not preserved. |
| `ddp` | `docs/diagrams/tikz/pdf/ddp.pdf` | single | 3.49 x 2.53 | PipedPeer's training sync through the lead daemon, its exact, fast and auto modes, against torchrun's rank-to-rank all-reduce. | Data-parallel training. (a) Each PipedPeer rank posts to the lead daemon and receives one sample-weighted average; exact mode sends int8 gradients every step, fast mode weights every H steps. (b) The torchrun baseline meets at a c10d rendezvous and all-reduces between ranks over gloo. |

## System Design > The Cost Model

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `cost` | `docs/diagrams/tikz/pdf/cost.pdf` | single | 3.49 x 2.56 | The order of the numerical gate: size and node rules, the memory rule, an optimistic bound, a bandwidth probe, then Equation 2. | The cost model as a cascade. Cheap tests run first; bandwidth is measured only when offloading could win at 10 Gbit/s. Work moves when Equation 2 holds or when it does not fit in local memory. |

## System Design > Implementation

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `sandbox` | `docs/diagrams/tikz/pdf/sandbox.pdf` | single | 3.49 x 2.17 | The layers around a job: crun container mounts and namespaces, the per-job and daemon memory ceilings, and the host. | The job sandbox. The closure is mounted read-only and the workspace read-write inside a crun container; a per-job cgroup and the daemon's own scope bound memory. The job shares the host network. |

## Evaluation > Setup

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `lab` | `docs/diagrams/tikz/pdf/lab.pdf` | double | 7.14 x 2.27 | The lab campaign topology: pc00 drives, submits and collects telemetry from 16 worker PCs on one LAN, then uploads to R2 through a capped ingest Worker. | Lab campaign. pc00 runs the experiments over Ansible, hosts the Ray head, Dask scheduler and PipedPeer submitter, and collects OpenTelemetry from a collector on every PC into files; after each experiment the results go to R2 through an ingest Worker that refuses uploads past the free tier. |
| `eval-e14-staircase` | `docs/paper/figures/eval-e14-staircase.pdf` | single | 3.49 x 1.35 | E14: from the first command on the head PC to every PC running work, one row per PC. | Curl to cluster (E14): for every PC, when it printed its LAB-PC line, answered SSH, had PipedPeer installed, joined, and first ran work of the day's first distributed job, relative to the head command on pc00. Dashed lines: cluster ready and first job. |
| `eval-e17-ready` | `docs/paper/figures/eval-e17-ready.pdf` | single | 3.49 x 1.15 | E17: time from nothing running to a usable cluster, and the commands a user types. | From nothing running to a cluster that accepts a job (E17): seconds from the first start command until the readiness check passed; the whisker reaches back to the last failed check. The number is the commands a user types after every boot. |

## Evaluation > Microbenchmarks (Q1, Q2)

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `eval-e7-overhead-cdf` | `docs/paper/figures/eval-e7-overhead-cdf.pdf` | single | 3.49 x 1.50 | E7: CDF of whole-job wall time for a trivial script with the shim on and off. | Cost of transparent interception (E7): CDF of the whole-job wall time of a script that does no parallel work, with the shim injected and without. The gap between the medians is the per-job overhead. |
| `eval-time-breakdown` | `docs/paper/figures/eval-time-breakdown.pdf` | single | 3.49 x 1.40 | Where PipedPeer's job time goes: environment build and ship (ms) against the run (s), per workload and N. | Where a PipedPeer job's time goes, from its own step records. (a) Building the Nix environment and shipping closure and workspace, in milliseconds; (b) running the script, in seconds. Median over repeats, warm environment cache. |
| `eval-e17-footprint` | `docs/paper/figures/eval-e17-footprint.pdf` | single | 3.49 x 1.50 | E17: idle cost per PC of each system's daemons over 60 s: CPU and memory. | Idle cost of an installed cluster (E17), per PC, averaged over a quiet minute with no job: (a) CPU as a percentage of one core; (b) memory, proportional set size (bar) and resident set size (outline), of every process the system leaves running. |
| `eval-e19-ship` | `docs/paper/figures/eval-e19-ship.pdf` | single | 3.49 x 1.65 | E19: time to first result while iterating: cold, warm, edited script, new dependency. | Time to first result while iterating on a script (E19), 16 PCs: the first run of a new environment (cold), the same again (warm), one constant edited, and one new dependency. PipedPeer's bar is split into environment build, ship, and the rest of the job. Median over repeats. |

## Evaluation > End-to-end runs (Q3)

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `eval-frontier` | `docs/paper/figures/eval-frontier.pdf` | single | 3.49 x 2.15 | Lines changed to distribute each workload against the speedup it bought at the largest N. | Porting cost against payoff. Each point is one workload on one system at the largest N; the vertical bar is the interquartile range over repeats. PipedPeer runs the unmodified script (zero lines changed); Ray and Dask need a port. |
| `eval-strong-scaling` | `docs/paper/figures/eval-strong-scaling.pdf` | double | 7.14 x 1.72 | Strong scaling of W1-W4: speedup over plain Python on one PC against N, log2 x axis, shared y. | Strong scaling. Speedup over plain Python on one PC (median; bars are the IQR over repeats) against the number of PCs N. The dotted line is ideal linear speedup; the grey line is parity with plain Python. Panels share the y axis. |
| `eval-weak-scaling` | `docs/paper/figures/eval-weak-scaling.pdf` | single | 3.49 x 1.75 | Weak scaling of W1: efficiency T(1)/T(N) when the work grows with N. | Weak scaling of W1: the work grows with N, so a perfect system holds efficiency T(1)/T(N) at 1.0 (dotted). Median and IQR over repeats. |
| `eval-wall-time` | `docs/paper/figures/eval-wall-time.pdf` | double | 7.14 x 1.85 | Wall time of every workload, system and N: median, IQR and each repeat. | Wall time per workload, system and N. The marker is the median, the bar the IQR, and the small dots are the individual repeats; the dotted line is plain Python on one PC. Lower is better; log y axis, each panel with its own range. |
| `eval-efficiency-heatmap` | `docs/paper/figures/eval-efficiency-heatmap.pdf` | single | 3.49 x 1.90 | Parallel efficiency speedup/N for each system and workload at each N. | Parallel efficiency (speedup over plain Python divided by N) for every system, workload and N. 1.00 is linear scaling. Darker is better; values are printed so the figure reads in greyscale. |
| `eval-e4-heterogeneous` | `docs/paper/figures/eval-e4-heterogeneous.pdf` | single | 3.49 x 1.95 | E4: W1 on PCs of unequal speed: wall time per system and the share of items each PC computed. | Unequal PCs (E4). (a) Wall time, median and IQR. (b) Share of the items each PC computed in one representative run; the black tick is the first PC's share of the cores. The even split is PipedPeer with placement by head count instead of measured speed. |
| `eval-e6-kill` | `docs/paper/figures/eval-e6-kill.pdf` | single | 3.49 x 1.45 | E6: wall time with and without one worker killed (kill -9) mid-run, per system. | One worker killed with kill -9 mid-run (E6). Hollow: no fault; filled: killed. The label is the added wall time; every run returned the correct result unless marked. |
| `eval-cpu-heatmap` | `docs/paper/figures/eval-cpu-heatmap.pdf` | double | 7.14 x 2.28 | Per-PC CPU utilisation over one W1 run at the largest N, one panel per system. | CPU utilisation of every PC during one W1 run at the largest N (OTel host or cgroup CPU time; busy cores over logical cores, 0.5 s bins). The vertical lines mark the job's start and end; PCs the run did not use are labelled in grey. |
| `eval-swimlanes` | `docs/paper/figures/eval-swimlanes.pdf` | double | 7.14 x 2.48 | Work placement: each chunk of work as a bar on the PC that ran it, one panel per system. | Where the work ran. Each bar is one chunk of items on the PC that computed it, from each system's own records (PipedPeer's receipt, Ray's timeline, Dask's scheduler). Dask records tasks per second, drawn as one-second cells shaded by count. Rows within a PC are concurrent chunks. |
| `eval-net-bytes` | `docs/paper/figures/eval-net-bytes.pdf` | single | 3.49 x 1.55 | Network bytes each PC sent and received during one W1 run per system at the largest N. | Network traffic per PC: bytes sent plus received on the lab interface during one W1 run per system at the largest N (OTel system.network.io). Log y axis; pc00 submits the job and hosts the Ray head and the Dask scheduler. |
| `eval-offload-share` | `docs/paper/figures/eval-offload-share.pdf` | single | 3.49 x 1.60 | Share of PipedPeer's items computed off the submitting PC, per workload, against N. | How much work PipedPeer moved off the submitting PC: the share of mapped items computed by peers (from the shim's receipt), median and range over repeats. The dotted line is an even share (N-1)/N. |
| `eval-e12-join` | `docs/paper/figures/eval-e12-join.pdf` | double | 7.14 x 1.75 | E12: throughput while PCs join mid-run, stacked by the cohort of PCs that did the work. | Elastic join (E12). Items finished per second, stacked by cohort: the PCs the job started on, then each group that joined (dashed line, labelled with the PCs added). A cohort that stays at zero is a system that never used the PCs that joined. Panels share the y axis. |
| `eval-e13-churn` | `docs/paper/figures/eval-e13-churn.pdf` | double | 7.14 x 1.75 | E13: throughput under churn, a worker killed and restarted periodically, per system. | Churn (E13): a worker is killed with kill -9 periodically (solid line, filled triangle) and restarted a few seconds later (dotted line, hollow triangle). Items finished per second over all PCs; each panel gives the outcome, the wall time against the same job without churn, and the items computed twice. |
| `eval-e15-network` | `docs/paper/figures/eval-e15-network.pdf` | single | 3.49 x 1.55 | E15: wall-time slowdown as round-trip time is added between every pair of PCs. | Network sensitivity (E15): W1 wall time with netem delay added between every pair of PCs, relative to the same system with no added delay. Median; bars span the IQR over repeats. |
| `eval-e16-energy` | `docs/paper/figures/eval-e16-energy.pdf` | double | 7.14 x 1.70 | E16: package energy to solution for W1, against N and against wall time. | Energy to solution (E16), W1, package energy from RAPL summed over all 17 PCs, idle ones included. (a) Energy against N; the dashed line is plain Python on one PC. (b) Energy against wall time, both log: lower left is faster and cheaper. |
| `eval-e18-straggler` | `docs/paper/figures/eval-e18-straggler.pdf` | double | 7.14 x 1.75 | E18: a noisy neighbour on one PC: wall time with and without it, and each system's throughput per PC. | Noisy neighbour (E18): busy loops on half the cores of one PC, started a few seconds into W1. (a) Wall time without (hollow) and with (filled) the hog; the label is the slowdown. (b-d) Items finished per second in the hog run, stacked per PC, the hogged PC hatched; the dashed line is the hog's start. Panels (b-d) share the y axis. |
| `eval-e20-unmodified` | `docs/paper/figures/eval-e20-unmodified.pdf` | single | 3.49 x 1.60 | E20: speedup of unmodified pandas groupby (W5) and scikit-learn cross-validation (W6) at the largest N. | Unmodified pandas and scikit-learn (E20). W5 fits a model per pandas group through joblib; W6 is cross_val_score with n_jobs=-1. Speedup over plain Python on one PC; the tick above each bar marks that every run matched plain Python, and the line under each name is the number of script lines changed. |

## Evaluation > Training (Q4)

| Figure | File | Width | Size (in) | Shows | Suggested caption |
|---|---|---|---|---|---|
| `eval-e8-training` | `docs/paper/figures/eval-e8-training.pdf` | double | 7.14 x 1.50 | E8: DDP training of T1: wall time, bytes sent per rank, and final loss, per launcher. | Data-parallel training (E8). (a) Wall time, median and IQR. (b) Bytes each rank sent. (c) Final training loss of each repeat. Exact averages gradients every step; fast averages weights every few steps. |

