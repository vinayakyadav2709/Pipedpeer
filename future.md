# Future ideas

Brain-dump of deferred ideas, not commitments. Revisit when the itch is real.

## Targeted closure delivery for non-DDP runs (full B)
- **Current state:** every non-DDP upload broadcasts the closure to ALL healthy
  peers, so pool spill can split a task anywhere. DDP uploads already skip the
  broadcast (`Options.SkipBroadcast`) because each rank uploads straight to its
  own node.
- **Problem:** with 10 nodes and 3 executing a job, the other 7 get a 6.6GB
  closure push they never asked for. Harmless with a shared store (peerHasStore
  short-circuits), wasteful on real networks.
- **Chosen:** spill only to peers that already hold the closure — `peerHasStore`
  + `EnablePoolSpill` already gate on that, so a peer that never received the
  closure simply isn't offered spill work, and local always runs its own part.
  Zero new code; correct because pool spill is the only reason to ship the
  closure ahead of need.
- **When to pick the other option (on-demand push at first chunk-forward):**
  when a real job actually spills to a peer that lacks the closure and the
  stall/retry is measured to hurt. Until then the extra failure modes (partial
  transfers, missing-closure chunk routing) aren't worth the complexity.

## Generic GPU-wheel table (instead of per-lib code)
- torch is currently a hardcoded `torchCUDA` branch in flake.go because nixpkgs'
  torch is CPU-only and its CUDA build is from-source (hours). Same problem will hit
  tensorflow (download.tensorflow.org index), cupy (PyPI +cu tags), etc.
- **Fix when needed:** replace the branch with a data table `{package → pip index URL}`;
  the flake generator pip-installs anything in the table and uses `ps.<pkg>` for the
  rest. Adding a GPU lib = one table row, no code. YAGNI until a second GPU lib is real.

## Delta closure transfer
- **Problem:** NAR uploads are whole-closure. Jobs 02→03→04 re-send shared numpy/python
  store paths to a peer that already has them. Per-closure cache only helps same-job
  re-runs (the demo/debug loop), not one-off training jobs.
- **Fix:** before sending a NAR, query the peer (`nix-store -q --references` on the
  closure vs paths the peer reports) and transfer only the missing diff — the
  Docker-layer-on-the-wire equivalent. Disk is already deduped by content-addressing;
  this only fixes the wire.
- **When to do:** if one-off jobs sharing deps become a real bottleneck. Not now.

## Containerd snapshot bloat (docker rig)
- `/var/lib/containerd` reached 376G; host `/` went 219G→272G used during CUDA torch
  testing. Each `docker build`/closure import adds overlay layers that never get GC'd
  on this rig. Figure out which snapshot(s) own the space and prune (`docker system
  prune`, or remove stale snapshots).
- Not yet root-caused to a specific container.

## Stable-affinity regression test (epic 03)
- The 03 combine fix hinged on stable lexicographic peer ordering for affinity routing
  (`pool.go` runChunk noSplit). Load-ranked order shifts between parse and combine →
  cache misses. A regression test asserting affinity targets don't move when peer load
  order changes is still pending.

## nvidia-container-toolkit (host, one-time)
- Host has the driver (610.57) but no toolkit → containers can't see the GPU. One-time:
  `sudo dnf config-manager addrepo --from-repofile=https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo && sudo dnf install -y nvidia-container-toolkit && sudo nvidia-ctk runtime configure --runtime=docker && sudo systemctl restart docker`.
- Only the Docker↔GPU bridge; CUDA libs live inside the nix closure.

## Disk-space audit (tests + demo)
- The demo rig ate ~50GB of the host root during CUDA torch testing: each worker's
  `/tmp` holds stale multipart/closure.nar/workspace.tar copies, `nar-cache` holds the
  6.6GB closure, and every `jobs/*/work/` re-copies data.csv (1.1GB × 6-7 per node).
- Also: containerd snapshot bloat above; and each pip/nix build re-downloads wheels
  into `/tmp` (the failed build alone pulled 751MB cudnn repeatedly).
- **When to do:** after the demo is green. Then measure per-test + per-demo footprint
  and trim (dedupe data.csv into shared workdir, prune stale nars/jobs, gc /tmp).

## Cleanup
- Remove `.wheels-dl/` (3.4GB of CUDA wheels) once GPU testing is done.
- `bin/pipedpeer` + `.freebuff/` untracked artifacts audit.