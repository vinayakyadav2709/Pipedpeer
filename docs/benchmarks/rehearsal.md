# End-to-end rehearsal in the LAN lab (2026-10-12)

The record behind the report's end-to-end table (Table 5.3, "End-to-end steps,
first run and cached"), its failure-drill table (Table 5.2) and the paper's
end-to-end paragraph (§IV-C). The original evidence files
(`~/Downloads/pipedpeer-work/evidence/v1..v3/*.txt`) were deleted when the
laptop was cleaned after the rehearsal; this file copies what survives in the
repository:

- **[S]** `docs/system-report.md` §5.11 "Demo rehearsal", rows with the evidence
  file name and build of each figure.
- **[V]** `pipedpeer-demo/VERIFY.md` and **[D]** `pipedpeer-demo/DEMO-SCRIPT.md`,
  removed in 938d8c5; read them with
  `git show 938d8c5^:pipedpeer-demo/VERIFY.md` (line numbers below are in that
  tree). Their code blocks are the rehearsal's screen output, pasted verbatim.
- **[B]** `~/.claude/briefs/pipedpeer-demo.md` (the session brief, outside the
  repo), copied here for the one figure the other two do not hold.

## Setup

- One laptop (yeet, RTX 3060). `lab/lan`: four containers (pc1-pc4) on
  one bridge for the CPU steps; `scripts/lab-gpu.sh`: two containers sharing
  the laptop's GPU through CDI for the GPU steps and the training ring.
- Containers share one CPU, so no time here is a speedup.
- Passes and builds: V1 8e14b9b, V2 31b5364, re-check cc56753, V3 9949ad0,
  V5 on the nightly-fixes branch (896f000-ef68f02).
- "Cold" is the first run on fresh containers; the five student-style scripts'
  first run came after the common packages were built ahead of time
  (`prep-4-common`).

## End-to-end steps

| Step | First run | Cached | Source |
|---|---|---|---|
| Parallel map (`00_pool.py`) | 43.5 s | 17.0 s | [S] (v1/30-host-00pool-first.txt, 8e14b9b); [V]:380 |
| Map over a lambda (`06_pool_lambda.py --remote --cold`, then without) | 20.5 s | 4.8 s | [V]:418, :423 (v2/73-step-2b-cold-pc2.txt, 31b5364) |
| GPU job from a GPU-less node (`04_torch_ddp.py --remote --gpu force`) | not used (see note) | 7.6 s | [V]:492 (v2/74b-gpu-pc1-direct.txt) |
| Two-rank ring on one shared GPU (`--ddp 2 --gpu force`) | 630.3 s | 9.5 s | [S] (v3/43-ddp-ring-cold.txt, 9949ad0); [V]:1112 (cc56753) |
| Combined script (`07_everything.py`) | 664.6 s | 26.7 s | [V]:643, :672 (9949ad0) |
| Five student-style scripts, first run after the common prep | 35.3-125.7 s | 19.6-69.5 s | [D]:191, :804-805 (31b5364) |
| Teacher-style script's environment (dry run), without / with the common prep | 54 s | 4 s | [D]:800, :1468 (pc3; v1/15-pc3-pkgprep-before-after.txt) |
| Eight jobs at once | 412.0 s, all 8 ok | - | [V]:1245 (v3/33-item3-finale-run3.txt) |

Note: the GPU step's first run (264.8 s, V1) was spent on a closure broadcast
to peers without a GPU, which a later change removed ([V]:495, [D]:545); it is
not a cost of the step and is not quoted.

Verbatim lines:

```
[V]:376-380  (00_pool.py, cached)
  script       16.5 s · peak memory 352 MB
  spread       96 items: 83 on yeet, 13 on 2 others (pc4 8, pc3 5)
  files back   none
  total        17.0 s
[V]:417-423  (lambda, --cold then cached)
  closure      sent 96 MB in 8.6 s (cold)
  total        20.5 s
  closure      already on the node
  total        4.8 s
[V]:487-492  (GPU job from pc1, cached)
  environment  built in 1.2 s
  closure      already on the node
  script       5.9 s · peak memory 1.3 GB
  total        7.6 s
[V]:668-672  (07_everything.py, cached)
  script       24.3 s · peak memory 1.7 GB
  spread       192 items: 158 on yeet, 50 on 2 others (pc3 28, pc2 22) · 16 computed twice (local cores raced slow chunks)
  total        26.7 s
```

## Failure drills

| Drill | Result | Source |
|---|---|---|
| Interrupt during a parallel map | peers stopped in under 2 s | [S] (v1/31-ctrlc-poolwatch.txt, 8e14b9b) |
| Terminal closed during a run | stopped everywhere, as an interrupt | [S] (v1/32-sighup*.txt) |
| Detach, then attach while running and after it finished | followed live; replayed in 1.0 s once finished | [S] (v1/34-attach.txt, v1/35-detach-attach-live.txt) |
| Submitter's network removed for 200 s during a remote job | job ran once; result back about 15 s after reconnecting | [S]; [D]:1046-1062 (v2/64-, 65-senderdeath-*.txt, 31b5364) |
| A training rank killed during a run | the survivor finished all 80 epochs, 90.5 s | [S] (v2/23b-ddp-down-up.txt, GPU lab) |
| A training rank's machine back during a run | rejoined at the next epoch, 4 s after its daemon started | [S] (same file) |
| A worker killed during a remote job | node reported gone 16 s after the kill | [B] line "V5 DONE": "node-gone detected in 16 s via gateway" |

```
[D]:1060-1061
About 15 s after the network came back, the sender (pc3 in V2) had the result file without any
command, and its history showed the job once
```
