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
`<!-- refresh from V2 -->` marks a step that changed after V1 or that V1 did not
run. Its output is still a placeholder until rehearsal pass 2 fills it in. `…`
marks lines cut from an excerpt. In the excerpts, `yeet` is the rehearsal
laptop (LAPTOP-A), and pc1–pc4 are the containers that stood in for the PCs.

**What the container lab cannot show:**
- **Speedup.** The "PCs" were slices of one laptop's CPU, so the speed numbers
  must come from the lab visit.
- **Two physical GPUs.**
- **The venue itself:** real Wi-Fi, Wi-Fi client isolation, the college
  firewall, and Ubuntu's AppArmor rules.

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
| optional: a browser on LAPTOP-A | `pipedpeer ui --print`, then open the address it prints | if it reads better than the dashboard <!-- refresh from V2: /ui not run in V1 --> |

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
5. `./demo/prep-1-check.sh` on LAPTOP-A ends in READY. <!-- refresh from V2: prep-1 not run in V1 -->
6. `./demo/prep-4-common.sh` has run on every machine and
   `./demo/prep-3-gpu.sh` on every machine that sends or runs GPU or
   "everything" work. The CUDA torch environment took **1269–1315 s per
   machine** in V1 (three machines building in parallel) and must never be
   built on stage. <!-- V1 8e14b9b: 11-pc1-gpu-checkonly.txt, 10-prep4-*.txt -->
7. `./demo/prep-2-rehearse.sh` on LAPTOP-A writes
   `demo/rehearsal-results.txt`: the cold, warm and speedup columns. **A step
   under 1.2× goes to "if asked"**; don't present a slowdown as a feature. <!-- refresh from V2: prep-2 stopped at 02_numpy in V1 (out of memory at the old 600M default cap; FX-PERF) -->

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
| 1 | live join | PC-3 | 5.2 s | same |
| 2 | the cluster | LAPTOP-A | instant | — |
| 3 | dry run (`--check-only`) | PC-1 | 0.8 s | 42 s for a new environment on an empty store |
| 4 | headline: python3 vs pipedpeer | LAPTOP-A | refresh from V2 | refresh from V2 |
| 5 | cold vs cached | PC-2 | 5.8 s | 169 s |
| 6 | GPU from a PC with no GPU | PC-1 | refresh from V2 | 1269 s CUDA build: prep only |
| 7 | pandas out-of-core (optional) | LAPTOP-B | refresh from V2 | refresh from V2 |
| 8 | the teacher's own script | any | 1–6 s check + the run | 39–54 s check without the common prep |
| 9 | everything in one script | PC-2 | refresh from V2 | prep only (torch) |
| 10 | failover: stop a PC mid-job | LAPTOP-A + PC-3 | 19.5 s | — |
| 11a | Ctrl+Z, then `attach` | LAPTOP-B | job length + about 1 s | — |
| 11b | the sender loses its network | PC-1 | refresh from V2 | — |
| 11c | Ctrl+C stops everything | LAPTOP-A | about 10 s | — |
| 12 | finale: every script at once | LAPTOP-A | refresh from V2 | — |
| 13 | receipts | LAPTOP-A, then PC-2 | instant | — |
| 14 | capstone (optional) | LAPTOP-A | refresh from V2 | — |

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

**Screen** (<!-- V1 8e14b9b: 20-join-pc3.txt --> <!-- refresh from V2: FX-EVENTS 5a5e5a5 renamed event lines; V1 showed one peer as a raw uuid -->):
```
$ pipedpeer join 10.88.0.1
restarting the daemon to join through 10.88.0.1:38445
this node is a88718a2
  waiting for peers · 90 s left · 0 found
22:21:19  JOIN   pc2 is up (10.88.0.12:38080)
22:21:19  JOIN   pc4 is up (10.88.0.14:38080)
22:21:19  JOIN   06b9a112-9739-4e3d-9728-103872ac5f67 is up (192.168.0.200:38080)
22:21:19  JOIN   pc1 is up (10.88.0.11:38080)
connected (4 peer(s))

NODE       HOST  STATE    JOBS  CPU      MEM FREE  GPU                   PATH  SOURCE
043e7463   pc4   healthy  0     16c 31%  6.7 GiB   -                     lan   discovery
06b9a112   yeet  healthy  0     15c 17%  6.7 GiB   RTX 3060 0% 41M/6.0G  lan   discovery
a88718a2*  pc3   healthy  0     16c 37%  9.1 GiB   -                     self  self
…
```
Then turn LAPTOP-A round: its dashboard has a new row, and LAPTOP-B's events
feed shows `JOIN pc-3 is up`. <!-- refresh from V2: dashboard not captured in V1 (needs a TTY) -->

**Say:** "It found the others by itself. The laptop only introduced them."

**Time:** 5.2 s in V1.

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

**Screen** (<!-- V1 8e14b9b: 16-1-cluster.txt --> <!-- refresh from V2: FX-NODES aca41d1 removes the stale `manual-1 127.0.0.1 unreachable` rows V1 showed; s2's version of the script also prints `pipedpeer ui --print` -->):
```
$ pipedpeer nodes
NODE       HOST           STATE        JOBS  CPU      MEM FREE  GPU                   PATH  SOURCE
043e7463   pc4            healthy      0     16c 14%  6.7 GiB   -                     lan   manual
06b9a112*  yeet           healthy      0     15c 13%  10 GiB    RTX 3060 0% 41M/6.0G  self  self
9bc88ca3   192.168.0.200  unreachable  0     0%       0 B       -                     lan   discovery
a88718a2   pc3            healthy      0     16c 15%  719 MiB   -                     lan   manual
aa6e1138   pc1            healthy      0     16c 14%  1.1 GiB   -                     lan   manual
c993ba76   pc2            healthy      0     16c 14%  1.1 GiB   -                     lan   manual
manual-1   127.0.0.1      unreachable  0     0%       0 B       -                     -     manual
…

  > One row per machine, all 'healthy'. The '*' row is this laptop.
  > CPU = cores and how busy they are now; GPU = card, load and memory in use.
```

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

**Screen** (<!-- V1 8e14b9b: 12-checkonly-pc4-00pool.txt, run on the Ubuntu stand-in --> <!-- refresh from V2: FX-CLI 1ef1058 touched check-only -->):
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

**Screen** <!-- refresh from V2: 2-headline.sh (the python3 half) was not run in V1 -->. What V1
saw from the cluster half, on its own (<!-- V1 8e14b9b: 30-host-00pool-first.txt -->):
```
scoring 96 items (16 local cores) ...
[pipedpeer] local 12 items
[pipedpeer] remote ok: 12 items in 6504 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.13:38080, 10.88.0.14:38080
…
checksum 4319999744
TOTAL 16.8s
…
=== Summary ===
  ran on       yeet (127.0.0.1)
  environment  built in 0.5 s
  closure      already on the node
  script       17.7 s · peak memory 328 MB
  spread       120 items: 84 on yeet, 36 on 4 others (pc1 9, pc2 9, pc3 9, pc4 9)
  files back   1 (1 new, 0 updated)
  total        43.5 s
```
<!-- refresh from V2: FX-CLI 1ef1058 fixed the spread count (V1 counted 120 for 96 items) -->

**Say:** "Ordinary Python, `multiprocessing.Pool`, nothing about pipedpeer in
it. The same file, not edited between the runs, and both times are printed by
the script itself." Point at the dashboard: every row's CPU column rises.

**Time:** refresh from V2. The container lab can't measure speedup; use
`demo/rehearsal-results.txt` from the lab visit.

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
0.8 MB/s). Real Wi-Fi will differ, so time it at the lab visit.

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

**Screen** (<!-- V1 8e14b9b: 13-gpu-step.txt, the one-GPU half; the script's filter keeps these lines --> <!-- refresh from V2: FX-NAR 29acd9b stopped the 260 s broadcast of the CUDA closure to CPU-only peers; DDP-UP changed 04_torch_ddp.py to a DataLoader; `--ddp 2` not run in V1 -->):
```
$ pipedpeer run 04_torch_ddp.py --remote --gpu force
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
training done in 0.4s
final loss: 0.06403607875108719
TOTAL 1.7s
  ran on       yeet (192.168.0.200)
  closure      already on the node
  total        264.8 s
```
Then the `--ddp 2 --gpu force` half, one rank on each laptop GPU: <!-- refresh from V2: 2-GPU ring (this laptop + friend's RTX 3050) -->

**Say:** "This PC has no GPU. The job went to the laptop that has one, and the
script didn't change. With `--ddp 2` it trains on both laptops' GPUs at once
and keeps their gradients in sync."

**Time:** script 4.5 s. V1's total of 264.8 s was almost all the broadcast
FX-NAR removed (refresh from V2). Cold is 1269 s of CUDA build, so it is prep
only.

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

### 7 — pandas out-of-core (optional) · LAPTOP-B · `./demo/6-pandas.sh`

**Screen:** `streaming … MB out-of-core`, `combining N chunk partials`, the
result table. <!-- refresh from V2: not run in V1 -->

**Say:** "No machine held the whole file. Each one parsed its own chunks and
sent back tiny per-group sums, so the means are exact."

**Time:** refresh from V2.

**Watch out for:**
- **A red "fell back" line:** one machine has a different binary (compare the
  hashes). The answer is still correct; say it ran on one machine.

### 8 — The teacher's own script · any machine · `./demo/11-teacher.sh their.py [data files]`

It copies the script into a fresh folder, runs `--check-only`, pauses, then
runs it.

**Screen**, from V1's stand-in script, which imports `humanize` (not in the
built-in table) and a made-up package (<!-- V1 8e14b9b: 40-pkgauto-probe.txt, 41-teacher-unpackaged-run.txt, 42-teacher-humanize-run.txt --> <!-- refresh from V2: FX-CLI 1ef1058 makes check-only ask nixpkgs, so `humanize` should no longer show as "not packaged"; 11-teacher.sh itself not run in V1 -->):
```
CHECK teacher.py — nothing will run
  imports      humanize, notarealpkg_xyz
  not packaged humanize (add --pkg)
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
  spread       1476 items: 1000 on yeet, 476 on 4 others (pc3 137, pc4 124, pc1 109, pc2 106)
…
  total        1.3 s
```

**Say:** "Hand us anything with a Pool, a ProcessPoolExecutor or `n_jobs`. It
finds the packages by itself, even ones it has never seen."

**Time:** check-only took 5.7 s, including fetching the new package. On one
`students/grid_search.py`, the check took **54 s → 4 s** before and after the
common prep (pc3), and **39 s → 6 s** on LAPTOP-A.
<!-- V1 8e14b9b: 15-pc3-pkgprep-before-after.txt, 17-prep2-rehearse-host.txt -->

**Watch out for:**
- **`not packaged: X`:** rerun with `--pkg <nixpkgs name>`
  (`python3Packages.X`), or switch to a script in `students/`.
- **No `spreads` line:** the script has nothing parallel in it. It runs on one
  machine; say so.
- **The build runs for minutes:** the script uses packages outside the common
  set. Talk through "how it works" (below) while it builds.

### 9 — Everything in one script · PC-2 · `./demo/12-everything.sh`

**Screen:** `section <name> <s>` lines for each part: Pool, lambda Pool,
sklearn, numpy, pandas, torch. A tqdm bar redraws in place, then the summary.
<!-- refresh from V2: not run in V1 -->

**Say:** "Every kind of work in one plain file."

**Time:** refresh from V2.

**Watch out for:**
- **It starts building torch:** prep-3-gpu was not run on PC-2. Skip the step.
- **The tqdm bar prints many lines instead of redrawing:** the output is
  piped; run the script directly.

### 10 — Failover · LAPTOP-A `./demo/9-failover.sh`, then on PC-3 `pipedpeer stop`

Type `pipedpeer stop` on PC-3 as soon as the first `remote ok` line appears.

**Screen** (<!-- V1 8e14b9b: 50-failover.txt --> <!-- refresh from V2: spread count (FX-CLI) -->):
```
  > Expected checksum (from the headline step):  checksum 4319999744
$ pipedpeer run 00_pool.py
[pipedpeer] local 13 items
[pipedpeer] remote ok: 13 items in 6865 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.14:38080
[pipedpeer] remote ok: 13 items in 12839 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.14:38080
checksum 4319999744
TOTAL 16.3s
…
  total        19.4 s
```
The events feed (on LAPTOP-B, or `pipedpeer events` on LAPTOP-A) shows
`LEAVE  pc3 is gone`, and later `JOIN pc3 is up` once PC-3 restarts.
<!-- V1 8e14b9b: 51-failover-events.txt -->

**Say:** "I just stopped one of the machines mid-job. Same answer. Losing a
machine costs time, never correctness."

**Time:** 19.5 s in V1, against 17.7–19.7 s undisturbed.

**Afterwards, on PC-3:** `pipedpeer start`. It rejoins with no arguments.

**Watch out for:**
- **No `remote ok` lines:** nothing went out. Check step 2.
- **No `remote failed … local covers it` line:** PC-3 held no batch at that
  moment. It is still the same checksum; say so. V1 showed no such line.
- **A different checksum:** this is a real bug. Stop and say so.

### 11a — Background a job, then attach · LAPTOP-B

Type `pipedpeer run 07_everything.py`, press **Ctrl+Z** after the first
`remote ok` line, then run `pipedpeer jobs`, then `pipedpeer attach <id>`.

**Screen** (<!-- V1 8e14b9b: 33-ctrlz.txt, 33-ctrlz-jobs.txt, 34-attach.txt, run on 00_pool.py --> <!-- refresh from V2: on 07_everything.py; FETCH 7f20314 now brings a finished background job's results back without attach -->):
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

**Screen:** <!-- refresh from V2: V1 FAILED this (90-senderdeath-*.txt): the CLI rescheduled the job, it ran twice, history said failed, and `attach` refused. V-F2 fix: a lost connection after start = detached, collected by FETCH. -->

**Say:** "The job lives where it runs. The sender can disappear and come back
for the answer."

**Watch out for:**
- **The job ran twice, or `attach` says "was not started in the background":**
  this is V1's bug. Drop the step until V2 passes it.
- **Closing the terminal is not a network loss:** it sends SIGHUP and stops the
  job everywhere.
- **A Pool job can't do this:** its script runs on the sender.

### 11c — Ctrl+C stops everything · LAPTOP-A

Type `pipedpeer run 00_pool.py` and press **Ctrl+C** after the first
`remote ok` line.

**Screen** (<!-- V1 8e14b9b: 31-ctrlc.txt, 32-sighup.txt --> <!-- refresh from V2: FX-CLI 1ef1058 removes the ~45-line Python traceback V1 printed after ^C -->):
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

**Say:** "Stop means stop, on every machine."

**Time:** about 10 s.

**Watch out for:**
- **Rows still busy after 5 s:** a cancel regression. Note it and move on.
- **A traceback after ^C:** this is V1's cosmetic bug. Ignore it; the job did
  stop.

### 12 — Finale · LAPTOP-A · `./demo/13-finale.sh`

You can also start one job by hand on each PC at the same time.

**Screen:** `started <name> (log: demo/finale-logs/<name>.log)` for each job,
then `pipedpeer tasks --watch`, then a table:
`JOB · RAN ON · TOTAL · RESULT`. <!-- refresh from V2: not run in V1 -->

**Say:** "All of it at once."

**Time:** refresh from V2.

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

<!-- refresh from V2: s2's 10-proof.sh (--timeline, --receipt) not run in V1; FX-EVENTS changed the events lines -->

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

**Screen:** the five `[n/5]` stage lines with times, `results/` appearing, and
the summary's `spread` row. <!-- refresh from V2: s2 step, not run in V1 -->

**Say:** "pandas, numpy, scikit-learn and Pool, all in one file, each
intercepted on its own. It shows breadth, not speed."

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
