# Pipedpeer: lab demo run sheet

What to type, on which machine, what the screen shows, what to say, how long it
takes, and what to do when it goes wrong. It covers one college lab on one LAN:
our laptops plus the college PCs. Set the machines up first with [SETUP.md](SETUP.md).
[VERIFY.md](VERIFY.md) has the same commands as a check list you can run
yourself.

**Where the outputs come from.** Every screen excerpt below was copied from a
real run. `<!-- V1 8e14b9b: <file> -->` marks output from rehearsal pass 1:
four containers plus this laptop, with Wi-Fi-like delay and loss added. The
evidence files are in `~/Downloads/pipedpeer-work/evidence/v1/`.
`<!-- V2 31b5364: <file> -->` and `<!-- V2 cc56753: <file> -->` mark output
from rehearsal pass 2 (same lab, evidence in
`~/Downloads/pipedpeer-work/evidence/v2/`). `<!-- confirm in final re-check -->`
marks an expected line: no rehearsal ran it, or the code changed after V2 ran
it. `…` marks lines cut from an excerpt. In the excerpts, `yeet` is the
rehearsal laptop (LAPTOP-A), and pc1–pc4 are the containers that stood in for
the PCs.

**What the container lab cannot show:**
- **Speedup.** The "PCs" were slices of one laptop's CPU, so the speed numbers
  must come from the lab visit.
- **Two physical GPUs.** The friend's real 2-GPU ring was never rehearsed (their
  laptop went offline during V2); check it in your own run.
- **The venue itself:** real Wi-Fi, Wi-Fi client isolation, the college
  firewall, and Ubuntu's AppArmor rules.
- **The demo scripts that need `python3`** (`demo/7-gpu.sh`,
  `demo/prep-3-gpu.sh`): the lab containers had no `python3`, so these skipped
  there. The venue machines have it.

---

## Machines and screens

| Name | What | Role on the day |
|---|---|---|
| **LAPTOP-A** | Your Fedora laptop, NVIDIA GPU, ethernet if possible | runs `pipedpeer rendezvous`, shows `pipedpeer dashboard`, does steps 2, 4, 10, 11c, 12 |
| **LAPTOP-B** | Teammate's Fedora laptop, NVIDIA GPU | shows `pipedpeer events -f`, does steps 7 and 11a, and is the second GPU |
| **PC-1, PC-2** | College Ubuntu PCs, no GPU | do steps 3, 5, 6 and 9 (a PC sending GPU work is the point) |
| **PC-3** | College Ubuntu PC | **left unjoined at prep**, joins live in step 1, gets stopped in step 10 |

There is no projector. The graders look at laptop screens, so before you start:

- Set the terminal font large (Ctrl+Shift+= a few times), with about 100
  columns visible.
- Keep one terminal per screen, and turn the screen toward the room when the
  step names it.

| Screen | Command | Keep it up for |
|---|---|---|
| LAPTOP-A, second terminal | `pipedpeer dashboard` | the whole demo |
| LAPTOP-B, second terminal | `pipedpeer events -f` | the whole demo |
| LAPTOP-A and LAPTOP-B, during step 6 | `watch -n1 nvidia-smi` | step 6 only |
| optional: a browser on LAPTOP-A | `pipedpeer ui --print`, then open the address it prints | if it reads better than the dashboard. V2 loaded it at 1366×768 with no console errors: tiles "Nodes healthy 5 of 5 · Cores 79 · GPU nodes 1", a node table, recent runs <!-- V2 31b5364: 68-ui-text.txt --> |

The demo scripts in `demo/` stop at `[enter to continue]` so you can talk. The
yellow `>` lines are notes for you; read them out or paraphrase them. Run
`PAUSE=0 ./demo/<step>.sh` to skip the pauses.

---

## Prep (setup visit, the day before)

Follow [SETUP.md](SETUP.md). When it is done, all of these are true:

1. Every machine runs the **same binary**. `demo/prep-0-install.sh` prints a
   `sha256`; compare it across machines.
2. `pipedpeer rendezvous` runs in its own terminal on LAPTOP-A.
3. Every machine except **PC-3** ran `pipedpeer auth set <token>` and
   `pipedpeer join <LAPTOP-A IP>`.
4. PC-3 has run `pipedpeer setup -y` only, never `join`, so that step 1 shows
   a real first join. Do not join it and then `stop` it: a stopped machine
   rejoins with no countdown.
5. `./demo/prep-1-check.sh` on LAPTOP-A ends in READY. <!-- confirm in final re-check: prep-1 was not run in V1 or V2 -->
6. `./demo/prep-4-common.sh` has run on every machine and
   `./demo/prep-3-gpu.sh` on every machine that sends or runs GPU or
   "everything" work. The common environment, CUDA torch included, took
   **1150–1261 s per machine** in V2 (four building in parallel; 1269–1315 s
   in V1) and must never be built on stage.
   <!-- V2 31b5364: 55-prep4-pc1.txt, 55-prep4-pc4.txt -->
7. `./demo/prep-2-rehearse.sh` on LAPTOP-A writes
   `demo/rehearsal-results.txt`: the cold, warm and speedup columns. **A step
   under 1.2× goes to "if asked"**; don't present a slowdown as a feature.
   <!-- confirm in final re-check: prep-2 was not run in V2; in V1 it stopped at 02_numpy (600M cap, fixed: 02_numpy ran in 37.2 s in V2's ./setup.sh pass, 70-setup-firstpass.txt) -->

## On the day

- **T−30 min, every machine:** run `pipedpeer status`. Expected
  (<!-- V1 8e14b9b: 03-host-start.txt -->):
  ```
  daemon running (pid=433799 node-id=06b9a112-9739-4e3d-9728-103872ac5f67 port=38080)
  ```
  If it says the daemon is not running, run `pipedpeer start`. It rejoins on
  its own.
- **T−30 min, LAPTOP-A:** run `./demo/prep-1-check.sh`. It should say READY,
  with the same hashes as at prep.
- **T−10 min:** open the two screens above and run `./demo/1-cluster.sh` once.
  Close anything that uses the network heavily (cloud sync, updates).
- **Be honest about cold and warm.** Prep built and shipped everything, so on
  stage nothing waits for a Nix build. Step 5 shows the first-run cost on
  purpose.

---

## The flow

| # | Step | Where | Cached | Cold (first ever) |
|---|---|---|---|---|
| 0 | the problem (talk) | — | 1 min | — |
| 1 | live join | PC-3 | 1.8 s (V2), 5.2 s (V1) | same |
| 2 | the cluster | LAPTOP-A | instant | — |
| 3 | dry run (`--check-only`) | PC-1 | 0.8 s | 42 s for a new environment on an empty store |
| 4 | headline: python3 vs pipedpeer | LAPTOP-A | 27 s (9.5 s python3 + 17.0 s pipedpeer) | — (prep builds it) |
| 5 | cold vs cached | PC-2 | 4.8 s | 20.5 s send only; 169 s with build, empty store, 0.5 % loss (V1) |
| 6 | GPU from a PC with no GPU | PC-1 | 7.6 s (`--remote`); `--ddp 2` 9.5 s | 1150–1261 s CUDA build, 635 s first `--ddp 2` ship: prep only |
| 7 | pandas out-of-core (optional) | LAPTOP-B | 12.8 s | 34.0 s (first run after prep) |
| 8 | the teacher's own script | any | 20–70 s for the students/ scripts | 39–54 s check without the common prep (V1) |
| 9 | everything in one script | PC-2 | about 30 s expected <!-- confirm in final re-check --> | 700.6 s (470 s build): prep only |
| 10 | failover: stop a PC mid-job | LAPTOP-A + PC-3 | 16.7 s (V2), 19.5 s (V1) | — |
| 11a | Ctrl+Z, then `attach` | LAPTOP-B | job length + about 1 s | — |
| 11b | the sender loses its network | PC-1 | job length; result back ~15 s after reconnecting | — |
| 11c | Ctrl+C stops everything | LAPTOP-A | about 10 s | — |
| 12 | finale: every script at once | LAPTOP-A | 534 s in the containers (6 of 8 ok in V2) | — |
| 13 | receipts | LAPTOP-A, then PC-2 | instant | — |
| 14 | capstone (optional) | LAPTOP-A | 288 s | — |

The order follows what impresses most (DECIDED): (a) an unchanged script
running across the room is the headline, (b) failover is the cherry on top,
(c) the teacher's script shows it doesn't fall apart. If time is short, drop
7, 14 and 11b first. Steps 1, 4, 6, 8 and 10 carry the argument.

---

### 0 — The problem (talk)

> "Most of us have more than one computer, but a slow script uses exactly one
> of them. The tools that fix that (Ray, Dask, Spark, Kubernetes) want the
> code rewritten for them and a cluster manager running. Pipedpeer runs an
> ordinary Python script, unedited, across every machine in this room."

### 1 — Live join · PC-3 · `./demo/0-join.sh <LAPTOP-A IP> <token>`

Get the token on LAPTOP-A with `pipedpeer auth show`. Leave the token argument
out if PC-3 already ran `auth set`.

**Screen** (<!-- V2 31b5364: 56-join-pc3.txt -->):
```
$ pipedpeer join 10.88.0.1
restarting the daemon to join through 10.88.0.1:38445
this node is b7b73c2d
  waiting for peers · 90 s left · 0 found
connected (3 peer(s))

NODE       HOST           STATE    JOBS  CPU      MEM FREE  GPU  PATH  SOURCE
06b9a112   192.168.0.200  unknown  0     0%       0 B       -    lan   discovery
0b591fdc   pc2            healthy  0     16c 9%   1008 MiB  -    lan   discovery
28197ccf   pc4            healthy  0     16c 9%   6.7 GiB   -    lan   discovery
b7b73c2d*  pc3            healthy  0     16c 12%  9.3 GiB   -    self  self
e3ec139a   pc1            healthy  0     16c 9%   1004 MiB  -    lan   discovery
```
V1 also streamed JOIN lines under the countdown (`22:21:19  JOIN   pc2 is up
(10.88.0.12:38080)`, 20-join-pc3.txt); V2's join was too quick (1.8 s) to
show any.
Then turn LAPTOP-A round: its dashboard has a new row, and LAPTOP-B's events
feed shows the join. In V2 that line still used the node id
(`JOIN   b7b73c2d-aad0-41e5-b9c5-49a46da18d08 is up (10.88.0.13:38080)`,
56-join-host-events.txt); dad7e42 names peers by hostname, so expect
`JOIN   pc3 is up (10.88.0.13:38080)`. <!-- confirm in final re-check -->
The dashboard needs a TTY and was not captured in V1 or V2. <!-- confirm in final re-check -->

**Say:** "It found the others by itself. The laptop only introduced them."

**Time:** 1.8 s in V2, 5.2 s in V1.

**Watch out for:**
- **The countdown runs out with "no peers found".** The output prints the
  checks to make:
  - `ping <LAPTOP-A IP>` from PC-3.
  - The firewall: `sudo ufw status` on Ubuntu, `sudo firewall-cmd --list-ports`
    on Fedora. See SETUP.md, "Firewall".
  - Wi-Fi client isolation: move PC-3 to ethernet.
  - `pipedpeer net-check` for the details.
- **`auth set failed`:** the token is wrong. Copy it again from
  `pipedpeer auth show` on LAPTOP-A.
- **A row shows `unreachable` with an IP for its host name:** that is an old
  identity of the same machine. Ignore it on stage.
- **On a multi-user Nix**, `join` may ask for sudo once, to install the key
  helper ("installing pipedpeer's key helper … (sudo)"). Type the password.
  Without a terminal, V1 printed `could not make system nix trust the
  cluster's keys` (04-host-join.txt); the fix is `pipedpeer trust`.

### 2 — The cluster · LAPTOP-A · `./demo/1-cluster.sh`

**Screen** (<!-- V2 31b5364: 71-step-1-cluster.txt -->):
```
$ pipedpeer nodes
NODE       HOST  STATE    JOBS  CPU     MEM FREE  GPU                   PATH  SOURCE
06b9a112*  yeet  healthy  0     15c 5%  8.5 GiB   RTX 3060 0% 41M/6.0G  self  self
0b591fdc   pc2   healthy  0     16c 5%  721 MiB   -                     lan   manual
28197ccf   pc4   healthy  0     16c 5%  6.5 GiB   -                     lan   manual
b7b73c2d   pc3   healthy  0     16c 5%  1.0 GiB   -                     lan   manual
e3ec139a   pc1   healthy  0     16c 5%  874 MiB   -                     lan   manual

  > One row per machine, all 'healthy'. The '*' row is this laptop.
  > CPU = cores and how busy they are now; GPU = card, load and memory in use.
…
  > Second screen: the live page every daemon serves. Open it now and leave it up.
$ pipedpeer ui --print
This machine:    http://127.0.0.1:38080/ui
From elsewhere:  http://192.168.0.200:38080/ui
```
V1's stale `manual-1 127.0.0.1 unreachable` rows are gone.

**Say:** "Each machine reports its cores, free memory and GPU. That is what the
scheduler weighs. PATH `lan` means a direct link; no VPN, no cluster manager."

**Time:** instant.

**Watch out for:**
- **A machine is missing:** wait one poll (10 s) and run it again. Still
  missing: on that machine run `pipedpeer stop && pipedpeer start`.
- **`unreachable` rows:** these are old identities of machines that were reset.
  Say "old entries" and move on.
- **The GPU column shows `-` on a laptop:** run `nvidia-smi` there. If that
  fails, the driver is broken and step 6 runs on the other laptop.

### 3 — Dry run · PC-1 · `pipedpeer run 00_pool.py --remote --check-only`

**Screen** (<!-- V1 8e14b9b: 12-checkonly-pc4-00pool.txt, run on the Ubuntu stand-in; V2 did not rerun this command, but its check-only runs in step 8 print the same layout: 100-c-teacher-cached-primes.txt -->):
```
CHECK 00_pool.py — nothing will run
  imports      standard library only
  spreads      multiprocessing.Pool (line 33)
               split decided at run time by measured cost
  memory       ~512 MiB (tier=import)
  gpu          not used
  placement    yeet (score=0.772, source=manual)
  environment  building (first build of a new env takes minutes)
…
  environment  built in 0.7 s
  store        /nix/store/w6cjl84hmjmqkw6hsva72i26kmk909jc-run
  closure      already on yeet (26 paths)
checks complete
```

**Say:** "Before it runs anything, it says what it will do: what the script
imports, what it can spread, where it would go, and whether that machine
already has the environment."

**Time:** 0.8 s with the environment cached. On an empty store, the first
build of this environment took 42.4 s (step 5's run on a fresh pc3).

**Watch out for:**
- **`warning: ignoring the client-specified setting 'build-users-group'`
  lines:** a multi-user Nix prints these. They are harmless.
- **`not packaged X`:** expected only for unusual imports; see step 8.
- **No placement / "no eligible node":** check the step 2 table.

### 4 — Headline · LAPTOP-A · `./demo/2-headline.sh`

The script shows the file, runs it with plain `python3`, then runs the same
file with `pipedpeer run`.

**Screen** (<!-- V2 31b5364: 71-step-2-headline.txt -->):
```
$ grep -n Pool\|import 00_pool.py
…
33:    with multiprocessing.Pool(os.cpu_count()) as pool:
…
============================================================
 00_pool.py - this machine only (plain python3)
============================================================
scoring 96 items (16 local cores) ...
checksum 4319999744
TOTAL 9.5s

============================================================
 00_pool.py - across the cluster (pipedpeer run)
============================================================
…
[pipedpeer] local 13 items
[pipedpeer] remote ok: 13 items in 8178 ms on 10.88.0.13:38080, 10.88.0.14:38080
checksum 4319999744
TOTAL 16.4s
…
=== Summary ===
  ran on       yeet (127.0.0.1)
  environment  built in 0.4 s
  closure      already on the node
  script       16.5 s · peak memory 352 MB
  spread       96 items: 83 on yeet, 13 on 2 others (pc4 8, pc3 5)
  files back   none
  total        17.0 s

============================================================
 this machine only:           9.5s
 across the cluster:          16.4s
 speedup:                     0.58x
============================================================
```
The 0.58× is the container lab (every "PC" is a slice of the same laptop CPU),
not a result: at the venue the numbers come from `rehearsal-results.txt`. The
cluster half also prints the 7-stage build log (`[1/7] Detecting imports…`)
before the script's output.

**Say:** "Ordinary Python, `multiprocessing.Pool`, nothing about pipedpeer in
it. The same file, not edited between the runs, and both times are printed by
the script itself." Point at the dashboard: every row's CPU column rises.

**Time:** 27 s for the whole step in V2 (9.5 s python3, 17.0 s pipedpeer).
The container lab can't measure speedup; use `demo/rehearsal-results.txt`
from the lab visit.

**Watch out for:**
- **Speedup under 1.2× at prep:** present it as "it spread", not as speed.
- **A peer is missing from `spread`:** say so and continue; it has dropped
  (step 2).
- **Two `using N workers, not the 16 requested` lines:** the shim sized the
  local pool. This is normal.

### 5 — Cold vs cached · PC-2 · `./demo/2b-cold.sh`

**Screen** (<!-- V1 8e14b9b: 14-pc3-truecold-2b.txt, empty store, 0.5 % loss -->):
```
$ pipedpeer run 06_pool_lambda.py --remote --cold
…
      Cold run: sending the whole closure even if the node has it
      Packed closure: 96 MB in 6.8 s
      Sent closure: 96 MB in 114.9 s (834 kB/s)
…
=== Summary ===
  ran on       yeet (192.168.0.200)
  environment  built in 42.4 s
  closure      sent 96 MB in 114.9 s (cold)
  script       4.6 s · peak memory 407 MB
…
  total        169.1 s

  > Same command without --cold: only the script travels.
$ pipedpeer run 06_pool_lambda.py --remote
…
      Closure already on the node; nothing to send
…
=== Summary ===
  ran on       pc4 (10.88.0.14)
  environment  built in 0.5 s
  closure      already on the node
  script       5.2 s · peak memory 401 MB
…
  total        5.8 s

  first run (cold): 169 s     every run after: 6 s
```
The progress bar draws in place during "Sending closure". That's why the
script isn't piped.

**Say:** "The first run ships the environment once. After that only the
script travels." Also: "This script uses a lambda, which plain
`multiprocessing` can't send to another process at all."

**Time:** 169 s cold and 5.8 s cached in V1, over 0.5 % packet loss (about
0.8 MB/s). In V2, on pc2 with the environment already built, only the send
was cold: `Sent closure: 96 MB in 8.6 s (11 MB/s)`, 20.5 s cold and 4.8 s
cached (`first run (cold): 21 s     every run after: 5 s`)
(<!-- V2 31b5364: 73-step-2b-cold-pc2.txt -->). Real Wi-Fi will differ, so
time it at the lab visit. Since cc56753 the script exits non-zero if either
run fails.

**Watch out for:**
- **The bar stalls:** the Wi-Fi is the problem. Press Ctrl+C and show only the
  cached half.
- **The cold half takes over 3 minutes at prep:** run only the cached half on
  stage and quote the prep number.
- **`--cold` resends the environment but does not undo this machine's build**
  (DECIDED). A truly first run also builds; quote the 42 s.

### 6 — GPU from a PC with no GPU · PC-1 · `./demo/7-gpu.sh`

Before you start: run `watch -n1 nvidia-smi` on LAPTOP-A and LAPTOP-B, and
turn those screens round.

The lab containers had no `python3`, so `7-gpu.sh` skipped there
(`No GPU machine among the peers - skipping`, 74-step-7-gpu-pc1.txt). V2 ran
the script's command directly on pc1. The script filters the output down to
the lines below.

**Screen** (<!-- V2 31b5364: 74b-gpu-pc1-direct.txt -->):
```
$ pipedpeer run 04_torch_ddp.py --remote --gpu force
…
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
training ...
epoch 0: 30 steps, loss 0.2452
epoch 1: 30 steps, loss 0.1147
epoch 2: 30 steps, loss 0.0592
training done in 1.3s
final loss: 0.05621044710278511
TOTAL 2.9s
…
  ran on       yeet (192.168.0.200)
  environment  built in 1.2 s
  closure      already on the node
  script       5.9 s · peak memory 1.3 GB
  files back   none
  total        7.6 s
```
Then the `--ddp 2 --gpu force` half, one rank on each laptop GPU. V2 ran it
in the GPU lab: two containers sharing this laptop's one RTX 3060, sent from
a GPU-less container (<!-- V2 cc56753: 113-c-ddp-ring-cached.txt -->):
```
$ pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force
=== Distributed training: 2 ranks ===
rank 0: a43e931b (10.89.0.12:38080)
rank 1: 29ffe602 (10.89.0.11:38080)
sync: daemon channel via 10.89.0.12:38080 (no extra ports)
…
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
[pipedpeer] ddp: sharding 60000 samples across 2 ranks
…
final loss: 0.18032728135585785
TOTAL 5.5s
[pipedpeer] ddp: 43 sync(s), 1.6s total (36 ms each), 66.2 MiB sent, 66.2 MiB received
[pipedpeer] ddp: the ring did NOT pay for itself — 2 ranks on 1 machine(s), …
```
On the two real laptops, expect the two `rank N:` lines to name LAPTOP-A and
LAPTOP-B and the two `using GPU:` lines to show different cards (RTX 3060 and
RTX 3050). "did NOT pay for itself" was right in the lab (one GPU shared);
on two laptops it may say so too, because the demo model is tiny. The real
2-GPU ring was never rehearsed: check it in your own run.
<!-- confirm in final re-check -->

**Say:** "This PC has no GPU. The job went to the laptop that has one, and the
script didn't change. With `--ddp 2` it trains on both laptops' GPUs at once
and keeps their gradients in sync."

**Time:** 7.6 s with `--remote` (V2; V1's 264.8 s was the broadcast FX-NAR
removed). `--ddp 2` cached: 9.5 s; its first run took 635 s, packing and
sending 3.1 GB to each rank (112-c-ddp-ring-cold.txt). The CUDA build is
1150–1261 s per machine. Both are prep only.
`run 04_torch_ddp.py --gpu force` without `--remote` also works from a GPU-less
PC (`--gpu force: this machine has no GPU; placing the run on a GPU node...`,
95-c-gpuforce-from-gpuless-pc1.txt); V2's total there was 212.0 s, about
3 min 24 s of it packing the GPU closure for peers that never got it. f182b0f
fixes that, so expect about 8 s. <!-- confirm in final re-check -->

**Watch out for:**
- **No `using GPU:` line (it says `using CPU`):** `--gpu force` was left out, or
  the GPU laptop is not healthy (step 2).
- **"no eligible node":** the GPU laptop's daemon is down. Run
  `pipedpeer start` there.
- **`Warning: peer … did not take this environment`:** V1 showed this when a
  CPU-only PC ran out of memory taking the CUDA closure. FX-NAR fixed it. If
  it comes back, it is harmless for this step.
- **`nvidia-smi` stays at 0 %:** expected. The demo model trains in under a
  second (V1's nvidia-smi log never moved); point at "using GPU" instead.
  <!-- V1 8e14b9b: 13-gpu-nvidia-smi.txt -->
- **Never run this cold on stage.**
- **`7-gpu.sh` prints `No GPU machine among the peers - skipping`** while a
  GPU laptop is healthy: check `python3` exists on this PC (the script uses
  it to count nodes), then type the command by hand.
- **A GPU rank dies mid-run (`--ddp 2`):** the run prints
  `[ddp] rank 1 on gpu1 died; continuing on 1` and carries on; when that
  machine's daemon comes back it rejoins at the next epoch
  (`rank 2 joins the ring … with rank 0's model and optimizer`,
  114-c-ddp-down-up.txt). If the LEAD rank's machine dies (the one in the
  `sync: daemon channel via` line), it says
  `its machine leads the ring (rendezvous and sync), so the run can't continue without it`
  and exits 1 (115-c-ddp-lead-dies.txt): say so and rerun.

### 7 — pandas out-of-core (optional) · LAPTOP-B · `./demo/6-pandas.sh`

**Screen** (<!-- V2 31b5364: 71-step-6-pandas.txt -->):
```
$ pipedpeer run 03_pandas_ooc.py -e PIPEDPEER_OOC_MIN=1e8
[pipedpeer] read_csv: streaming 329 MB out-of-core
read data.csv: 3,000,000 rows x 7 cols in 8.0s
[pipedpeer] groupby: combining 5 chunk partials from the cluster
groupby('cat').mean(): 1.0s
cat
alpha  1.498151e+06  0.499574  0.500078  0.500743  0.500440  0.500033
beta   1.501307e+06  0.500057  0.499720  0.499787  0.499942  0.500040
delta  1.499895e+06  0.500472  0.500003  0.499678  0.499779  0.500668
gamma  1.500641e+06  0.499842  0.499994  0.499707  0.500116  0.500480
TOTAL 9.0s
```

**Say:** "No machine held the whole file. Each one parsed its own chunks and
sent back tiny per-group sums, so the means are exact."

**Time:** 12.8 s for the step in V2 (script 9.0 s, 3,000,000 rows, 314 MB
file); 34.0 s the first time after prep (70-setup-firstpass.txt).

**Watch out for:**
- **A red "fell back" line:** one machine has a different binary (compare the
  hashes). The answer is still correct; say it ran on one machine.

### 8 — The teacher's own script · any machine · `./demo/11-teacher.sh their.py [data files]`

It copies the script into a fresh folder, runs `--check-only`, pauses, then
runs it.

**Screen**, `./demo/11-teacher.sh students/primes.py` on pc3
(<!-- V2 cc56753: 100-c-teacher-cached-primes.txt -->):
```
 Your script: primes.py
================================================================
  > Nothing edited. First a dry run: what it imports, what it would spread, where it would go.
$ pipedpeer run primes.py --check-only
CHECK primes.py — nothing will run
  imports      standard library only
  spreads      ProcessPoolExecutor (line 36)
               split decided at run time by measured cost
  memory       ~512 MiB (tier=import)
  gpu          not used
  placement    this machine (no --remote)
  lease        remote daemon accepted job (lease=99cc3806), cancelled
…
  environment  built in 0.4 s
  store        /nix/store/w6cjl84hmjmqkw6hsva72i26kmk909jc-run
  closure      already on this machine (26 paths)
checks complete
…
$ pipedpeer run primes.py
…
checking 16000000 numbers in 100 ranges
[pipedpeer] local 13 items
[pipedpeer] remote ok: 13 items in 4565 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.14:38080, 192.168.0.200:38080
…
primes below 16000000 : 1031130
TOTAL 38.6s
…
=== Summary ===
  ran on       pc3 (127.0.0.1)
  environment  built in 0.4 s
  closure      already on the node
  script       38.8 s · peak memory 352 MB
  spread       100 items: 26 on pc3, 74 on 4 others (yeet 35, pc2 17, pc4 12, pc1 10)
  files back   none
  total        39.3 s
  > Files it wrote are in: /tmp/pipedpeer-teacher.VPlnA8
```

A script with a package outside the built-in table. From V1's stand-in, which
imports `humanize` and a made-up package (<!-- V1 8e14b9b: 40-pkgauto-probe.txt, 41-teacher-unpackaged-run.txt, 42-teacher-humanize-run.txt -->).
V1's check-only also listed `not packaged humanize (add --pkg)`; FX-CLI 1ef1058
makes check-only ask nixpkgs, so expect only the made-up one there
<!-- confirm in final re-check: not rerun in V2 -->:
```
CHECK teacher.py — nothing will run
  imports      humanize, notarealpkg_xyz
  not packaged notarealpkg_xyz (add --pkg)
  spreads      multiprocessing.Pool (line 6)
…
[2/7] Parsing dependencies...
      Not in the built-in table, but nixpkgs has python3Packages.humanize: using it
      Not packaged (the job will fail to import it): notarealpkg_xyz — add it with --pkg <nixpkgs name>
…
ModuleNotFoundError: No module named 'notarealpkg_xyz'
```
With the made-up import removed:
```
332,833,500
…
…
  total        1.3 s
```
(V1's `spread` line said 1476 items for this 1000-item job; the count is fixed,
so expect `spread       1000 items: …`. <!-- confirm in final re-check -->)

**Say:** "Hand us anything with a Pool, a ProcessPoolExecutor or `n_jobs`. It
finds the packages by itself, even ones it has never seen."

**Time:** check-only took 5.7 s, including fetching the new package. On one
`students/grid_search.py`, the check took **54 s → 4 s** before and after the
common prep (pc3), and **39 s → 6 s** on LAPTOP-A.
<!-- V1 8e14b9b: 15-pc3-pkgprep-before-after.txt, 17-prep2-rehearse-host.txt -->
The whole step (check + run) for each `students/` script, cached, from pc3 in
V2: `image_blur.py` 19.6 s, `monte_carlo_pi.py` 35.9 s, `primes.py` 39.3 s,
`word_count.py` 42.5 s, `grid_search.py` 69.5 s
(<!-- V2 cc56753: 100-c-teacher-cached-*.txt -->).

**Watch out for:**
- **`not packaged: X`:** rerun with `--pkg <nixpkgs name>`
  (`python3Packages.X`), or switch to a script in `students/`.
- **`Error: NOT READY: refused: … insufficient memory` and
  `the run failed (exit 1)`:** the target's free memory dipped for a moment
  (V2 saw ~240 MB on a container right after a big run;
  100-c-teacher-refused-*.txt). Wait a few seconds and rerun.
- **No `spreads` line:** the script has nothing parallel in it. It runs on one
  machine; say so.
- **The build runs for minutes:** the script uses packages outside the common
  set. Talk through "how it works" (below) while it builds.

### 9 — Everything in one script · PC-2 · `./demo/12-everything.sh`

**Screen:** `section <name> <s>` lines for each part: Pool, lambda Pool,
sklearn, numpy, pandas, torch. A tqdm bar redraws in place, then the summary.
The cached run from PC-2 did not pass in V2 (below), so this is the first,
cold run of the same file sent from pc3 with `--remote`
(<!-- V2 cc56753: 98-c-everything-pc3-cold.txt -->):
```
$ pipedpeer run 07_everything.py --remote
[coordinator] GPU-accelerated libraries detected — preferring GPU nodes
…
everything: 16 local cores
--- pool
[pipedpeer] remote ok: 16 items in 3268 ms on 10.88.0.12:38080, 10.88.0.13:38080
checksum 1151999856
section pool 7.5s
--- lambda
…
checksum 143999880
section lambda 2.7s
--- sklearn
[pipedpeer] joblib: 44 tasks, 0.165s each; racing this machine against the cluster
accuracy 0.9599
section sklearn 1.5s
--- numpy
[pipedpeer] remote ok: 16 items in 5698 ms on 10.88.0.12:38080, 10.88.0.13:38080
…
largest singular value 1152.7576
section numpy 8.9s
--- pandas
…
section pandas 0.5s
--- torch
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
epoch 0: 100%|██████████| 78/78 [00:00<00:00, 260.11it/s]
…
final loss 0.0282
section torch 1.9s
wrote results/everything.txt
TOTAL 23.0s
…
=== Summary ===
  ran on       yeet (192.168.0.200)
  environment  built in 470.0 s
  closure      already on the node
  script       26.7 s · peak memory 7.6 GB
  spread       192 items: 170 on yeet, 50 on 2 others (pc2 31, pc3 19) · 28 computed twice (local cores raced slow chunks)
  files back   1 (1 new, 0 updated)
  total        700.6 s
```
(The tqdm line is shown after its last redraw.)

In V2 the next runs failed before starting: the 7.6 GB peak, summed over
forked workers, became an 8.5 GiB estimate that no node could take
(`Error: no eligible node found`, 99-c-everything-pc3-cached.txt; and on
PC-2 `insufficient memory: need 9111920640 bytes` then
`the run failed (exit 1): read the Error line above`,
97-c-12-everything-pc2.txt). FX-PEAK fixes the estimate, so expect the
output above with `environment  built in` about 1 s, `closure  already on the
node` and a total near the script's 27 s. <!-- confirm in final re-check -->

**Say:** "Every kind of work in one plain file."

**Time:** cold 700.6 s (470 s of it building the environment, CUDA torch
included): prep only. Cached: about 30 s expected. <!-- confirm in final re-check -->

**Watch out for:**
- **`insufficient memory` or `no eligible node` with an estimate of several
  GiB:** V2's estimate bug (above) is back. Skip the step.
- **It starts building torch:** prep-3-gpu was not run on PC-2. Skip the step.
- **The tqdm bar prints many lines instead of redrawing:** the output is
  piped; run the script directly.

### 10 — Failover · LAPTOP-A `./demo/9-failover.sh`, then on PC-3 `pipedpeer stop`

Type `pipedpeer stop` on PC-3 as soon as the first `remote ok` line appears.

**Screen** (<!-- V2 31b5364: 72-step-9-failover.txt, pc3 stopped at 07:23:25, about 1 s into the run, 72-failover-stop.txt -->):
```
  > Expected checksum (from the headline step):  checksum 4319999744
  > When the 'remote ok' lines start, on PC-3 run:   pipedpeer stop
  > Keep the dashboard / 'pipedpeer events -f' in view: PC-3 goes away there.
$ pipedpeer run 00_pool.py
[pipedpeer] local 12 items
[pipedpeer] remote ok: 12 items in 9097 ms on 10.88.0.11:38080, 10.88.0.14:38080
checksum 4319999744
TOTAL 15.9s
  spread       96 items: 84 on yeet, 12 on 2 others (pc4 9, pc1 3)
  total        16.5 s

  > Same checksum as the undisturbed run.
```
pc3 is missing from `spread`: it was stopped before it took a batch.
The events feed (on LAPTOP-B, or `pipedpeer events` on LAPTOP-A) shows
`LEAVE  pc3 is gone`, and later `JOIN pc3 is up` once PC-3 restarts.
<!-- V1 8e14b9b: 51-failover-events.txt -->

**Say:** "I just stopped one of the machines mid-job. Same answer. Losing a
machine costs time, never correctness."

**Time:** 16.7 s in V2 (16.5 s undisturbed in the same session), 19.5 s in V1.

**Afterwards, on PC-3:** `pipedpeer start`. It rejoins with no arguments.

**Watch out for:**
- **No `remote ok` lines:** nothing went out. Check step 2.
- **No `remote failed … local covers it` line:** PC-3 held no batch at that
  moment. It is still the same checksum; say so. V1 showed no such line.
- **A different checksum:** this is a real bug. Stop and say so.

### 11a — Background a job, then attach · LAPTOP-B

Type `pipedpeer run 07_everything.py`, press **Ctrl+Z** after the first
`remote ok` line, then run `pipedpeer jobs`, then `pipedpeer attach <id>`.

**Screen** (<!-- V1 8e14b9b: 33-ctrlz.txt, 33-ctrlz-jobs.txt, 34-attach.txt, run on 00_pool.py --> <!-- confirm in final re-check: Ctrl+Z on 07_everything.py was not run in V1 or V2; expect the same lines with that script's name and output -->):
```
[pipedpeer] remote ok: 12 items in 5193 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.13:38080, 10.88.0.14:38080
^Z
detached — job 20896323 keeps running
  reattach: pipedpeer attach 20896323

$ pipedpeer jobs
ID	STATUS	MODE	JOB	…
20896323	running	bg	job-1791757565020894049	…

$ pipedpeer attach 20896323
attaching to job 20896323 (00_pool.py) on 127.0.0.1:38080

scoring 96 items (16 local cores) ...
…
checksum 4319999744
TOTAL 14.9s
…
=== Summary ===
  ran on       yeet (127.0.0.1)
…
  total        15.7 s
```
`pipedpeer run <script> --detach` does the same without the keypress: it
returns in 0.5 s with the same two lines (35-detach.txt).

You don't have to attach to get the files: once a background job finishes,
its machine's results come back by themselves. In V2 a `--detach` job from
pc3 finished on the laptop, and pc3's daemon logged
`job collected bytes=5481 … files=1 from=10.88.0.1:38080 … status=succeeded`;
`pipedpeer jobs` there showed it `succeeded	bg`. Even with the laptop's daemon
then stopped, `pipedpeer attach` on pc3 printed the saved output
(<!-- V2 31b5364: 60-fetch-detach.txt, 61-fetch-collected.txt, 62-fetch-attach-node-gone.txt, 65-senderdeath-collected.txt -->):
```
job 71581038 (word_count.py) finished on 10.88.0.1:38080; its output, saved on this machine:

counting words in 400 documents
…
TOTAL 29.0s
…
=== Summary ===
  ran on       10.88.0.1
…
  files back   1 (1 new, 0 updated)
  total        50.7 s
```
That run also printed one raw JSON `withholding the cluster token` warning;
since 3664049 warnings are plain text. <!-- confirm in final re-check -->

**Say:** "Like a shell job: put it in the background and close the lid if you
like. The work keeps going on every machine, and the results come back."

**Time:** the job's own length. `attach` replays the output so far, then
follows live.

**Watch out for:**
- **`job not found`:** you are on the wrong machine. The job history lives on
  the machine that started the job.
- **Do NOT close the terminal or press Ctrl+C here.** Either one stops the job
  everywhere, by design.
- **"detach is not supported for DDP/map runs":** expected for `--ddp`. Ctrl+C
  is the only way to stop those.

### 11b — The sender loses its network · PC-1

Run `pipedpeer run 04_torch_ddp.py --remote --gpu force` (or any long
`--remote` job). Pull PC-1's cable or turn its Wi-Fi off. Plug it back in, then
run `pipedpeer attach <id>`.

**Screen** (<!-- V2 31b5364: 64-senderdeath-long-run.txt; V2 ran `students/word_count.py --host <laptop>` from pc3 and cut pc3 off the network for 200 s -->):
```
[7/7] Executing on remote...
counting words in 400 documents
[pipedpeer] local 14 items
[pipedpeer] remote ok: 14 items in 3116 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.13:38080, 10.88.0.14:38080

lost the connection to 10.88.0.1:38080 (connection lost: read tcp 10.88.0.13:57302->10.88.0.1:38080: read: connection timed out); checking whether the node or this machine went away...
…
neither the node (…) nor any other peer answers, so this machine is the one offline; the job keeps running there

detached — job 53517360 keeps running
  reattach: pipedpeer attach 53517360
```
About 15 s after the network came back, the sender (pc3 in V2) had the result file without any
command, and its history showed the job once
(<!-- V2 31b5364: 65-senderdeath-collected.txt -->):
```
$ pipedpeer jobs
ID	STATUS	MODE	JOB	…
53517360	succeeded	bg	job-1791768757453515387	…
```
V2 also printed four raw JSON lines (`{"level":"warn",…"message":"withholding
the cluster token"}`) while offline; since 3664049 these are plain-text
warnings. <!-- confirm in final re-check --> V2 did not repeat this with the
GPU job named above. <!-- confirm in final re-check -->

**Say:** "The job lives where it runs. The sender can disappear and come back
for the answer."

**Time:** the job's length. The CLI noticed the loss after about 2 min 40 s
(TCP timeout), and the result arrived about 15 s after reconnecting.

**Watch out for:**
- **The job ran twice, or `attach` says "was not started in the background":**
  this was V1's bug (90-senderdeath-*.txt), fixed in V2. If it comes back, drop
  the step.
- **Closing the terminal is not a network loss:** it sends SIGHUP and stops the
  job everywhere.
- **A Pool job can't do this:** its script runs on the sender.

### 11c — Ctrl+C stops everything · LAPTOP-A

Type `pipedpeer run 00_pool.py` and press **Ctrl+C** after the first
`remote ok` line.

**Screen** (<!-- V1 8e14b9b: 31-ctrlc.txt, 32-sighup.txt; V1 then printed a ~45-line Python traceback, which FX-CLI 1ef1058 removes --> <!-- confirm in final re-check: Ctrl+C mid-run was not rerun in V2 -->):
```
[pipedpeer] remote ok: 13 items in 5492 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.13:38080, 10.88.0.14:38080
^C
…
=== Summary ===
  ran on       yeet (127.0.0.1)
…
  total        cancelled after 10.0 s
cancelled — remote work stopped
Error: execution failed: job cancelled (exit code 137): context canceled
```
On the dashboard, every row's JOBS drops back to 0. In V1, each PC's pool
counters stopped growing within about 2 s of the ^C (31-ctrlc-poolwatch.txt).

Ctrl+C also works while the environment is still uploading. V2 pressed it
during `[6/7] Uploading` of a GPU job to pc3; the CLI exited 0.06 s later, the
lease was freed at once and the next job on pc3 was accepted
(<!-- V2 cc56753: 94-c-ctrlc-mid-upload.txt -->):
```
[6/7] Uploading to daemon...
…
      Upload stopped: cancelled
…
=== Summary ===
  ran on       pc3 (10.88.0.13)
  environment  built in 0.8 s
  total        cancelled after 8.9 s
cancelled — remote work stopped
```

**Say:** "Stop means stop, on every machine."

**Time:** about 10 s.

**Watch out for:**
- **Rows still busy after 5 s:** a cancel regression. Note it and move on.
- **A traceback after ^C:** V1's cosmetic bug, fixed since. If it shows,
  ignore it; the job did stop.

### 12 — Finale · LAPTOP-A · `./demo/13-finale.sh`

You can also start one job by hand on each PC at the same time.

**Screen** (<!-- V2 cc56753: 101-c-13-finale.txt -->):
```
 Finale - 8 jobs at once
================================================================
  > Watch the dashboard: every row busy.
  started pool  (log: demo/finale-logs/pool.log)
  started sklearn  (log: demo/finale-logs/sklearn.log)
…
  started everything  (log: demo/finale-logs/everything.log)

LEASE      NODE             JOB                  STATE      SUBMITTER  GPU      AGE
66ab22de   pc1              finale-pool          running    06b9a112   -        1s
8e728430   pc1              finale-numpy         running    06b9a112   -        1s
0f490736   pc4              finale-pandas        running    06b9a112   -        1s
92e26f28   pc3              finale-sklearn       running    06b9a112   -        1s
fad64406   yeet             finale-torch         running    06b9a112   -        1s
…
 Finale results
================================================================
  JOB          RAN ON                 TOTAL      RESULT
  pool         pc1                    428.8s     ok
  sklearn      pc3                    42.5s      ok
  numpy        pc1                    514.0s     ok
  pandas       pc4                    ?          fail (exit 1)
  torch        yeet                   2.7s       ok
  files        pc4                    0.0s       ok
  lambda       pc2                    6.1s       ok
  everything   ?                      ?          fail (exit 1)

  > A failed job: read its log in demo/finale-logs/. 'no eligible node' = max-concurrent or memory; rerun that one alone.
```
The middle part is `pipedpeer tasks --watch`, redrawn every second. Two jobs
failed in V2: pandas on a pool HTTP 500 (FX-SHIM now combines locally) and
everything on the 8.5 GiB estimate (FX-PEAK, see step 9). With both fixes,
expect all eight `ok` and the script to exit 0 (it exits 1 if any job fails).
<!-- confirm in final re-check -->

**Say:** "All of it at once."

**Time:** 534 s in V2, because eight jobs shared containers that were slices
of one laptop (pool 428.8 s and numpy 514.0 s, against 18.9 s and 37.2 s
alone, 70-setup-firstpass.txt). Real PCs each have their own CPU; time it
at the lab visit.

**Watch out for:**
- **A job says `fail` with "no eligible node":** that is the concurrency or
  memory limit. Rerun that job alone with `./demo/13-finale.sh <name>`.
- **RAN ON shows `?`:** read that job's log.
- **To stop the finale,** press Ctrl+C once. It stops every job.

### 13 — Receipts · LAPTOP-A `./demo/10-proof.sh`, then on PC-2 `pipedpeer traffic`

**Screen on LAPTOP-A:**
- `pipedpeer jobs`.
- `pipedpeer job --last --timeline` and `--receipt`.
- `pipedpeer events --limit 15`.

(<!-- V2 31b5364: 77-step-10-proof.txt -->):
```
  > The last run, stage by stage: build, upload, seeding the peers, the script, the sync.
$ pipedpeer job --last --timeline
job 1791770005089791190  00_pool.py  succeeded

== Timeline ==
STAGE             WHERE        START      TIME
detect imports    local         +0ms       0ms  █
…
nix build         local         +0ms     386ms  █
export closure    local       +386ms       7ms  █
upload            06b9a112    +394ms       5ms  █
seed peers        06b9a112    +400ms      70ms  █
sandbox setup     06b9a112    +470ms      23ms   █
script            06b9a112    +493ms     16.0s   ██████████████████████████████████████
sync results      local       +16.5s       1ms                                         █
total                                    16.5s

  > What the shim sent to which machine, what it kept here, and the cost model's reason for each.
$ pipedpeer job --last --receipt
…
== Receipt ==
Items: 96 total
  on other machines       12  ███
  on the running node     84  ██████████████████████████

By peer:
  10.88.0.14:38080            9  ██
  10.88.0.11:38080            3  █

Cost model decisions:
  shipped     pool.map             84 items x 1.626s = 136.61s of work, over the 0.50s worth a round trip

$ pipedpeer events --limit 15
07:26:30  POOL   received pool/map: 6 items
…
07:28:23  POOL   spawning warm worker for store /nix/store/cl5nqb9z7y77jw6zpk6q3rj46q9qmhp9-run
…
07:30:10  WORK   lease granted: job  from pc2
```
`pipedpeer stats` puts the cluster and the recent runs on one screen
(<!-- V2 31b5364: 79-stats.txt -->):
```
Cluster: 5 node(s), 5 healthy · 79 cores · 4.3 GiB free memory · 1 GPU node(s) · 1 task(s) running
…
1791770005089791190  00_pool.py         succeeded  06b9a112      16.5s  script 16s             12/96 items remote
```

**Screen on PC-2** (<!-- V1 8e14b9b: 60-proof-receipts.txt -->):
```
$ pipedpeer traffic
2026/10/11 22:27:20 [pool] received pool/map from 10.88.0.1:50986: items=49 b64=false cache_keys=0 required_mem=40 forwarded=""
2026/10/11 22:27:20 [pool] received pool/map from 10.88.0.1:48414: items=57 b64=false cache_keys=0 required_mem=40 forwarded=""
…
```

**Say:** "Each machine keeps its own receipts. This is PC-2's record of the
work it did for the laptop. The laptop can't fake it."

**Watch out for:**
- **Empty traffic:** that PC took no pool work this session. Pick another PC.

### 14 — Capstone (optional) · LAPTOP-A · `./demo/14-capstone.sh`

**Screen** (<!-- V2 31b5364: 78-step-14-capstone.txt -->):
```
$ pipedpeer run 07_capstone.py -e PIPEDPEER_OOC_MIN=5e7
[1/5] pandas: read data.csv and average each category ...
[pipedpeer] read_csv: streaming 196 MB out-of-core
[pipedpeer] groupby: combining 3 chunk partials from the cluster
  pandas: 216.5s
[2/5] numpy: features and target ...
  numpy: 58.3s
[3/5] sklearn: RandomForest(n_estimators=80, n_jobs=-1) ...
[pipedpeer] joblib: 64 tasks, 0.331s each; racing this machine against the cluster
  accuracy 0.8962
  sklearn: 2.9s
[4/5] Pool: bootstrap the accuracy (48 tasks x 100 resamples) ...
  95% of resamples between 0.8929 and 0.8997
  pool: 4.5s
[5/5] files: write results/ ...
…
  files: 0.8s
TOTAL 283.0s
      Synced 3 file(s) to …/07_capstone (3 new, 0 updated, 0 deleted)
  spread       288 items, all on yeet

$ ls results/
by_category.csv
feature_importance.csv
summary.json
```
In V2 the `spread` row said `288 items, all on yeet`: the cost model kept
every tree and resample local on the shared-CPU containers, and only the CSV
chunks went out.

**Say:** "pandas, numpy, scikit-learn and Pool, all in one file, each
intercepted on its own. It shows breadth, not speed."

**Time:** 288 s for the step in V2 (pandas out-of-core 216.5 s of it); 101.7 s
without `PIPEDPEER_OOC_MIN` in the `./setup.sh` pass (70-setup-firstpass.txt).
Optional for that reason.

---

## While a long step runs: how it works

- **The environment comes from the imports.** Nix builds an exact
  environment once, caches it, and ships it as a closure: only the store paths
  a machine lacks.
- **Interception, not rewriting.** A shim loaded before the script replaces
  `multiprocessing.Pool`, `ProcessPoolExecutor`, `joblib` (scikit-learn), some
  NumPy and pandas calls, and the PyTorch step with versions that can use the
  cluster.
- **Scheduling.** Each machine reports cores, free memory and GPUs. A cost
  model splits work by measured speed and keeps work local when shipping it
  would be slower.
- **Finding each other.** On one LAN, daemons find each other by UDP
  broadcast. LAPTOP-A's `rendezvous` introduces machines that broadcast can't
  reach (Wi-Fi and wired on different subnets). After that, traffic goes
  directly between the machines.
- **Isolation and trust.** Jobs run in a `crun` sandbox. Machines share a
  cluster secret, and a machine accepts only environments signed by keys it
  trusts.

## If asked

**Measured in V1** (containers on one laptop, 0.5 % loss; these are not speed
claims):

| What | Number | Evidence |
|---|---|---|
| live join, command to "connected" | 5.2 s | 20-join-pc3.txt |
| first run, empty store: build 42.4 s + ship 96 MB in 114.9 s | 169.1 s | 14-pc3-truecold-2b.txt |
| the same run once cached | 5.8 s | same |
| CUDA torch environment, first build per machine | 1269–1315 s | 11-*, 10-prep4-*.txt |
| teacher check, before / after the common prep | 54 s → 4 s (pc3), 39 s → 6 s (laptop) | 15-*, 17-*.txt |
| failover run vs undisturbed | 19.5 s vs 17.7–19.7 s | 50-*, 30-*, 51-failover.txt |
| `--detach` returns | 0.5 s | 35-detach.txt |

**Measured in V2** (same lab; evidence in `evidence/v2/`):

| What | Number | Evidence |
|---|---|---|
| live join, command to "connected" | 1.8 s | 56-join-pc3.txt |
| `--cold` send of the 96 MB environment, then cached | 20.5 s → 4.8 s | 73-step-2b-cold-pc2.txt |
| common environment incl. CUDA torch, first build per PC | 1150–1261 s | 55-prep4-*.txt |
| GPU job from a GPU-less PC, cached | 7.6 s | 74b-gpu-pc1-direct.txt |
| `--ddp 2` ring, first run (3.1 GB to each rank) / cached | 635 s / 9.5 s | 112-, 113-c-ddp-ring-*.txt |
| a rank dies and rejoins mid-run | rejoins with the model and optimizer (6.0 MiB) at the next epoch | 114-c-ddp-down-up.txt |
| 07_everything.py, first run from a PC (470 s build) | 700.6 s | 98-c-everything-pc3-cold.txt |
| students/ scripts through 11-teacher.sh, cached | 19.6–69.5 s | 100-c-teacher-cached-*.txt |
| Ctrl+C during the upload: CLI exits | 0.06 s, lease freed | 94-c-ctrlc-mid-upload.txt |
| sender offline 200 s: result back after reconnecting | about 15 s, ran once | 64-, 65-senderdeath-*.txt |
| a node stopped mid-job: rescheduled and finished | 50.9 s wall (32.7 s on the second node) | 67-node-killed-reschedule.txt |
| test suite at cc56753 | 38 packages ok | fix5/go-test-full-2.txt |

**Speedup:** quote only `demo/rehearsal-results.txt` from the lab visit.

**"Isn't this just Ray or Dask?"** Those are excellent if you write for them.
Pipedpeer's bet is the other end: the code stays as it is, and there is no
cluster manager.

**Honest limits:**
- Work that ships a lot of data for little compute can be slower than one
  machine. That is why the cost model exists.
- The machines doing the work must run Linux.
- A Pool job dies with its sender. A `--remote` job doesn't.

## Something went wrong

| Symptom | Do this |
|---|---|
| A step prints a traceback | Say what it says and move on. Every step stands alone. |
| `pipedpeer nodes` lost a machine | On that machine, run `pipedpeer stop && pipedpeer start`. Continue in the meantime. |
| A step hangs a minute past its prep time | Press Ctrl+C (it stops everything), check `pipedpeer tasks`, and move on. |
| `retrying in …` keeps repeating | Press Ctrl+C. A machine probably has another binary; compare the hashes after the demo. |
| The network dies | Show `python3 00_pool.py` locally and talk through the rest from this sheet. |

## After the demo

Run `pipedpeer stop` on each college PC. To leave the college cluster, run
`pipedpeer join <other address>` later; one join resets it. On LAPTOP-A, stop
`pipedpeer rendezvous` with Ctrl+C.
