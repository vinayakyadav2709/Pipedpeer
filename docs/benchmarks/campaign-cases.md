# Lab campaign: input cases

The cases the campaign scripts in `scripts/campaign/` must handle, and the simplest
handling that holds for each. Rows marked *real* come from observed output of the current
build; rows marked *foreseen* are cases this kind of input brings on other machines,
releases or networks. The scripts' tests take their rows from these tables.

## 1. `pipedpeer status` output

The first line names the daemon; the node table follows a blank line and ends at the next
blank line.

```
daemon running (pid=1600 node-id=9078b729c31d42b5 port=4242 version=0.0.1-nightly.20261104-74be7b8)

NAME  STATE    PATH  CPU     MEM FREE  GPU                   JOBS  NOTE
yeet  healthy  self  15c 1%  6.7 GiB   RTX 3060 0% 41M/6.0G  0     -

Recent runs:
```

| Case | Source | Handling | Cost |
|---|---|---|---|
| Daemon running, one node | real (above) | node-id and version taken by regex `node-id=(\S+)` and `version=(\S+)` on line 1 | none |
| Healthy, stale and unreachable rows | real (`src/nodesview.go`) | count rows whose second column is `healthy`; STATE is always column 2 | none |
| NOTE column with spaces | foreseen | only columns 1, 2 and 4 are read, so extra words do not matter | none |
| GPU column `-` or multi-word | real/foreseen | not read | none |
| Daemon not running | foreseen | line 1 does not start `daemon running`: PC counted not ready, logged | one retry |
| Daemon version differs from the CLI | foreseen (old daemon after an upgrade) | install fails the PC with `version <got> != <want>` | the PC is reinstalled |
| Two PCs with the same node-id (cloned disk images) | foreseen, common in college labs | install stops with `DUPLICATE-NODE-ID`; the operator regenerates the key on one PC | one manual step |
| A node listed twice under two paths | foreseen | counting is by NAME, once | none |
| Table header renamed in a later release | foreseen | the parser locates the `STATE` header column; a missing header is an error, never a count of 0 | fails loudly |

## 2. `pipedpeer connect` and `invite`

| Case | Source | Handling |
|---|---|---|
| `invite` prints `pipedpeer connect <code>` on stdout, a "created" note on stderr | real (`src/invite_cmd.go:48`) | code = second word of the stdout line matching `^pipedpeer connect \S+$` |
| Exit 0, connected | real (`src/joinwatch.go:50`) | recorded |
| Exit 2, partly connected (some paths failed) | real | retried up to 3 times, then counted as joined if `status` on pc00 shows it healthy |
| Exit 1, failed | real | retried up to 3 times, then the PC goes to `FAILED` |
| Exit 130, cancelled | real (`src/main.go`) | treated as failed |
| The same code used by 32 PCs at once | foreseen | allowed (no expiry or use count in the code); if joins time out, the fan-out parallelism drops to 8 |

## 3. Workload result lines (result_ok)

| Workload | Real line format | result_ok rule |
|---|---|---|
| W1 `pool_map.py` | `POOLMAP-OK items=48 seconds=12.3456 checksum=1234.567890` | `checksum=` field equals the reference exactly |
| idle `pool_idle.py` | `POOLIDLE-OK items=16 seconds=... checksum=...` | `checksum=` exact |
| trivial `trivial.py` | `TRIVIAL-OK` | line present |
| W2 `pipedpeer-demo/cpu.py` | `Pool: <n> items, checksum <s>`, `joblib: 64 Mandelbrot tiles, <n> points inside`, `numpy: largest singular value <f>`, `pandas: <rows> rows -> <n> cities, total rain <f>` | all four lines exact |
| W3 `w3_rf.py` | `accuracy: 0.8765` | exact (fixed `random_state` fixes every tree); the SSH port, which trains disjoint tree subsets with their own seeds, within 0.005 |
| T1 `t1_mlp.py` | `final loss: <f>`, `weight checksum: <f>` | `final loss` within 1% (relative) of the one-rank run |

| Case | Source | Handling |
|---|---|---|
| Output has extra log lines around the result | real (shim prints `[pipedpeer]` notes to stderr) | only lines matching the workload's regex are kept |
| Result line missing (crash, timeout) | foreseen | `result_ok: false`, the run is kept as a result |
| Float formatting differs between numpy versions | foreseen | the reference comes from the same environment family where possible; a W2 `numpy:` mismatch in the last digit is flagged, not silently forgiven |
| No reference for this size (weak scaling at N>1) | foreseen | compare with the majority output of that size across systems, recorded as `ref: majority` |
| All systems disagree | foreseen | `result_ok: null`, a warning, and a BLOCKERS.md line |

## 4. Lab PC states for `lab-pc.sh`

| Case | Source | Handling |
|---|---|---|
| Ubuntu 22.04 desktop, `ssh.service`, no openssh-server | foreseen (default desktop install) | installs openssh-server, records `pkg` lines and `started ssh` |
| Ubuntu 24.04, ssh by socket activation (`ssh.socket`) | foreseen | either unit active counts as running; nothing recorded |
| ufw inactive (Ubuntu default) | foreseen | no ufw rules added or recorded |
| ufw active with the lab's own rules | foreseen | only missing rules added and recorded; undo removes only recorded ones |
| Driver key already present without the marker | foreseen | a marked copy is added; undo removes only marked lines |
| Sleep targets already masked by the lab | foreseen | not recorded, so undo leaves them masked |
| No GNOME session (server install, other desktop) | foreseen | a warning; gsettings skipped |
| Not Ubuntu (no apt) | foreseen | refuses with a message |
| Run twice | foreseen | the state file is unchanged the second time |
| Undo run over ssh that drops | foreseen | undo runs under `systemd-run`, so it finishes; result read from the journal |
| Cloned PCs with identical hostnames | foreseen | PCs are named by ids `pc00`.., never by hostname |
| A home directory on NFS shared by all PCs | foreseen (other labs) | inventory notes `home_fs`; per-PC paths under `~/pipedpeer-lab` would collide: the operator stops before install |

## 5. Fan-out and long runs over ssh

| Case | Source | Handling |
|---|---|---|
| A PC is off or asleep | foreseen | ssh exit 255; 3 retries 5 s apart; then listed in `logs/<step>/FAILED`; the run goes on with the rest |
| WiFi drop of the laptop during a run | foreseen | runs are detached on pc00 (`systemd-run --user`); the waiter counts ssh failures as "not yet" |
| The runner itself is killed or the laptop sleeps | foreseen | a sleep inhibitor holds the laptop; on restart, run ids already in `runs.jsonl` are skipped and a started run dir is never started again |
| Run exceeds its timeout | foreseen | recorded with exit 124 and `note: timeout`; the run unit is stopped |
| A PC reboots mid-campaign | foreseen | lingering keeps the user units; the daemon restarts with the session; `cluster.sh up` waits for it or lists it `NOT-READY` |
| Clock skew between PCs | foreseen | wall times are measured on pc00 only; per-PC times are corrected by `clocks.csv` offsets |
| A PC much slower than the rest | foreseen | kept (the comparison uses the same PCs for every system); `excluded.txt` only for broken PCs, with the reason logged |

## 6. Measurements

| Case | Handling |
|---|---|
| A repeat more than 2× its group median | kept, flagged `slow-repeat` |
| Spread (IQR/median) above 10% after the first sizes | more repeats, `k = min(15, ceil(5 × (spread/0.10)²))` |
| A ratio interval that crosses both 0.9 and 1.1 | verdict `unresolved`, never "same" |
| A system missing at some N | flagged `missing`; no curve interpolates across it |
| Ray or Dask ignoring a CPU cap | Ray restarted with `--num-cpus`, logged and stated in the results |

## 7. Task-size and data-size sweeps

| Case | Source | Handling |
|---|---|---|
| Task much smaller than the framework's call overhead | foreseen | Ray and Dask batch to about 100 ms per remote task where their documentation advises it; the batch size is in the script header |
| Pipedpeer keeps small tasks on one machine | `poolrun.go:130-152` | a result, not a failure: default settings, with the remote share from the receipt in the table |
| No-op throughput and single-call latency | foreseen | raw per-task cost, unbatched; Pipedpeer runs with `--distribute force` and is labelled so |
| Identical payload objects in one list | foreseen | each item builds its own payload; pickle memoisation would otherwise hide the transfer |
| ssh baseline regenerating a payload on the remote PC | foreseen | payload files are sent with the run; the part fails if its file is missing |
| Total payload near the pool request ceiling | `poolhandlers.go:58,66-71` | 64 x 20 MB = 1.28 GB, under 2 GiB; a lower cgroup budget halves the ceiling, so a local-only decision is reported with its receipt |
| An older `sizes.env` without the sweep keys | foreseen | the run stops and names the key; re-calibrate |

## 8. Membership and failure

| Case | Source | Handling |
|---|---|---|
| Introducer killed during a job | foreseen | LAN discovery off; the daemon is killed and its unit stopped (it restarts itself in 5 s otherwise); the job must still finish with the right result |
| New PC joining after the introducer died | foreseen | invite from a surviving member; measured to healthy on both sides, then a forced job from the new PC |
| An invite issued by the dead introducer | foreseen | expected not to join within 60 s; a join is logged as unexpected |
| "New" PC already known to other members | lab | its own node list is moved aside and restored afterwards; stated as a re-join; E11 measures first-ever start |
| PC joining after a job's upload | `execupload.go:175` | no closure for that job; elasticity is measured with the environment already on the joiners |
| CPU busy during framework worker start-up | foreseen | first work = a streak of at least 5 s at half the cores or more, timed on the joining PC so clocks do not matter |
| Target PC never reported dead | foreseen | row with `timed_out=1` at 120 s |
| Another PC's state changes while waiting | foreseen | only the target's own row counts |
| Private Nix store when the sandbox is off | `execrun.go:284` | the run fails with its reason, never a silent fallback to the sandbox |
