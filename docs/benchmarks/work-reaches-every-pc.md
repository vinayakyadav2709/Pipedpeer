# Work reaches every PC: the first job and the elastic join

Two failures from the lab rehearsal, diagnosed on the container lab (`LAB_NAME=runner`, three 2 GB containers, `scripts/campaign/rehearse.sh`). Emulated, container lab; the plain lab proves work crosses nodes, never speedup.

## Failure 1: the first job ran wholly on pc00 (E14)

Rehearsal-day (`scripts/campaign/HEADLESS.md`, 2026-11-03): W1 on every PC right after the join; every worker was still importing the job's closure after a minute, and no peer ran any item.

Chain, in code:

1. The upload seeds the closure to every peer (`daemonapi/server.go` `broadcastClosure`) and waits at most `broadcastWait` (60 s) for the imports.
2. The job is then started with `PIPEDPEER_CLOSURE_NODES` = peers holding the closure + 1 (`daemonapi/execrun.go`, `withClosureNodes`). With every peer still importing that is `1`.
3. The shim reads that variable once, when the Pool is built: `_ClusterPool.__init__` sets `self._remote = _cluster_ready()`, and `_cluster_ready` is `shards >= 2` (`shim/pipedpeer_shim/core.py`). With `1` the pool is local for its whole life. A peer that finishes importing one second later never gets an item. **Root cause.**
4. Why the imports outlast a minute on a fresh lab: every seed asks the peer which paths it lacks with `"substitute": true` (`daemonapi/narimport.go` `peerMissingPaths`). A fresh peer then downloads every path cache.nixos.org has (all but the job's own two) from the internet instead of from the sender on the same LAN, and the sender waits up to `substituteWait` (30 s) after its own part is in before giving up on the download and sending the rest itself. On the lab's shared uplink, 16 PCs pull the same closure at once. In the container lab a fresh peer took 14.8 s to fetch 24 of 26 paths from the cache while the sender's two paths were in at once.

Pushing the closure at join time cannot help the first job: its closure does not exist until the job is submitted, and seeding at submit already happens. What helps is that a late import no longer matters (3), and that the import takes the LAN path (4).

## Failure 2: a PC that joins mid-map gets little or nothing (E12)

Rehearsal (`results/lab/sample/data/rehearsal`, 2026-11-03): pc02 joined during the map and ran 860 of 20000 items (4.3 %). Reproduced here (old build, `results/lab/rehearsal-before`):

| run | items | joiner first work | joiner items | pc01 (there from the start) |
|---|---|---|---|---|
| rehearse.sh size | 20000 | 14.5 s (start command at 11.3 s, wall 14.7 s) | 10 (0.1 %) | 41.7 % |
| long | 60000 | 16.1 s (start at 10.7 s, wall 42.3 s) | 10.2 % | 35.2 % |
| long | 60000 | 15.9 s (start at 11.2 s, wall 43.5 s) | 19.1 % | 32.8 % |

The joiner is present for about two thirds of the long run, and does a third to a half of what pc01 does.

Chain, in code:

1. The origin daemon re-reads the peers for every chunk (`daemonapi/poolrun.go` `runChunk` calls `peerFn`), so a ready joiner can get a share of each *new* chunk.
2. But the shim decides the race's shape once, at map start (`shim/pipedpeer_shim/pool_race.py` `_race_items`): `_cluster_pacing()` is read once, the number of remote dispatch threads is fixed at the in-flight depth of the peers present then, and FAC2 (`_fac2_size`) sizes chunks for that fixed number of pullers. **Root cause**, two parts:
   - the in-flight depth never grows, so a joiner adds no concurrency; and
   - FAC2's first round hands out about 40 % of the map at t = 0, in chunks of up to `remaining / (2 × pullers)` items (W1 at 60000: the first two remote chunks were 8331 and 6942 items, out for up to 29-31 s). Claimed work is bound to the machines present: a joiner can only take from what is still queued.
3. Seeding happens only at upload. A PC that joins without the job's closure is filtered out by `peerHasStore` for every chunk and never receives one (the E12 joiners hold it from earlier experiments, which hides this; a new machine would get nothing).
4. Detection: a restarted daemon is listed by pc00 at pc00's next poll, up to 10 s later (`StartPeerPoller(10 * time.Second)`); measured 2.6-3.2 s from the start command to `seen`, then about 2 s more to first work.

## The fix

- Shim (`core._cluster_ready`, `pool.py`, `pool_race.py`, `pool_pacing.py`): readiness is asked at each call; with `PIPEDPEER_CLOSURE_NODES=1` the daemon's pacing is asked, and a peer holding the closure or getting it (`pending`) is a cluster. The race re-reads the pacing every second and opens a dispatch slot for each chunk the depth allows (none while no peer holds the closure); chunks are capped at max(2 s, 4 target parts) of work so most of the map stays queued. A daemon that gives no pacing keeps the old rule.
- Daemon (`daemonapi/poolelastic.go`): the job's own pacing request seeds every healthy peer that lacks the closure (`seedLate`) and answers `pending`; yes-answers to "can you run it" are cached 30 s; a LAN peer takes every path from the sender instead of from cache.nixos.org (`PeerHealth.substitutesItself`).

## Before and after (container lab, emulated; one run per row unless stated)

| | before | after |
|---|---|---|
| E14, fresh lab, 20000 items: share pc00 / pc01 / pc02 | 36.4 / 32.3 / 31.3 % | 39.6 / 28.8 / 31.5 % |
| E14, fresh lab: first work (s after run start), wall | 63.2 s, 77.5 s | 45.0 s, 59.5 s |
| fresh-peer closure import | 16.5-17.3 s (24 of 26 paths from cache.nixos.org) | 2.1 s (all 26 from the sender) |
| E14, slow import emulated (pc00 egress 16 Mbit/s, peers without cache.nixos.org; imports 84-89 s, past the 60 s wait), 100000 items: share pc00 / pc01 / pc02 | 100 / 0 / 0 % | 63.5 / 19.5 / 16.9 % |
| same: script s, wall | 76.3 s, 180.2 s | 71.8 s, 177.2 s |
| same: peers' first work | never | about 1 s after each import finished |
| E12, rehearse.sh size (20000): joiner share | 0.1 % (pc00 listed it at 13.9 s of 14.7 s) | 0 % (listed at 17.5 s, job ended 14.3 s) |
| E12 long (60000, two runs): joiner share | 10.2 %, 19.1 % | 15.3 %, 18.2 % |
| E12 long: joiner first work (start command at ~10.7 s) | 16.1 s, 15.9 s | 13.5 s, 13.5 s |
| E12 long: wall | 42.3 s, 43.5 s | 42.2 s, 42.4 s |
| E6 (4000 items, kill -9 at 30 %): wall no fault / with kill, result | 3.06 / 3.36 s, correct | 2.84 / 3.31 s, correct |
| E13 churn (20000, kill pc01 at ~10 s): wall, result, items rerun | 15.1 s, correct, 1 | 16.1 s, correct, 0 |

Every run's checksum matched the reference. The three containers share one host's 20 cores, so walls say nothing about speed-up. The rehearse.sh E12 size cannot show a join on either build: the joiner is listed after, or within a second of, the end of the job.

Raw runs: `results/lab/rehearsal-before{,-std}/`, `results/lab/rehearsal{,-after}/`, `results/lab/e14-slow-{before,after}/` (not committed).
