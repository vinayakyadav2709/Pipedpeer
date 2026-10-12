# Check it yourself

These are the commands Claude ran in the rehearsal, in the same order. Each
check has the expected output, copied from that run. Run them and compare what
you see. Nothing is listed here that Claude did not actually run.

- `<!-- V1 8e14b9b: <file> -->`: output from rehearsal pass 1. The full
  output is in `~/Downloads/pipedpeer-work/evidence/v1/<file>`.
- `<!-- V2 31b5364: <file> -->` / `<!-- V2 cc56753: <file> -->`: output from
  rehearsal pass 2, in `~/Downloads/pipedpeer-work/evidence/v2/<file>`.
- `<!-- V3 9949ad0: <file> -->`: output from the final re-check, in
  `~/Downloads/pipedpeer-work/evidence/v3/<file>`.
- `<!-- V4 c567369: <file> -->`: output from the V4 re-check of lines a fix made
  after V3 changed, in `~/Downloads/pipedpeer-work/evidence/v4/<file>`.
- **Pass if** lists the lines that must appear exactly. Times, ids, hashes and
  CPU percentages vary from run to run. Names like `yeet` (this laptop) and
  pc1–pc4 (the containers) are what the lab uses.

**Where this runs:** this laptop plus four containers on one docker network
(`scripts/lab-lan.sh`). The laptop plays LAPTOP-A at 10.88.0.1, with the GPU
and the rendezvous. pc1–pc3 are single-user Nix; pc4 stands in for Ubuntu
(multi-user Nix, user `pp`).

**What this does not prove:** speedup (the containers share this laptop's CPU),
two real GPUs, real Wi-Fi, or the college firewall and Ubuntu's AppArmor rules.
The lab visit covers those. The containers also have no `python3`, so the demo
scripts that call it (`demo/7-gpu.sh`, `demo/prep-3-gpu.sh`) skip there unless
run inside `nix shell …#python3`; the venue machines have it. The containers
have about 1.2 GB each, too little for `07_everything.py` run on the container
itself (check 21). The friend's real 2-GPU ring (this laptop's RTX 3060 +
their RTX 3050) was never completed: in V3 their laptop dropped off the network
mid-ship. Check it in your own run (check 7c).

**Before you start:**
- Have at least 4 GB of memory free (`free -g`).
- Make sure no other lab is up (`docker ps`).
- Plan about 2.5 hours (V2's lab and GPU-lab passes took about 2 h 20 min,
  06:52–09:14). The CUDA build in check 7 takes about 21 minutes and runs in
  the background.

---

## Bring the lab up

**1. Build and check the code** (repo root):
```bash
cd src && go build -o ../bin/pipedpeer . && go vet ./... && gofmt -l . ; cd ..
```
Pass if: no output from vet, and none from gofmt. <!-- V1 8e14b9b: 00-static.txt (VET-OK, gofmt empty) -->
The full test run is `cd src && go test -count=1 -p 1 ./...`. Pass if every
package says `ok` (or `[no test files]`): at cc56753 that was 38 `ok` lines and
no FAIL (<!-- fix5/go-test-full-2.txt -->). V1 had `internal/daemonapi` FAIL in
two memory tests, fixed since (01-gotest.txt).

The integration tests (need docker or podman, about 3 minutes):
```bash
cd src && PIPEDPEER_INTEGRATION=1 go test -v -run "TestConcurrentJobsAndSharedNumpyCache|TestIntegrationFullSyncAndExecute" -timeout 15m . ; cd ..
```
Pass if (<!-- V2 31b5364: 12-integration.txt, 12b-integration-fullsync.txt -->):
```
--- PASS: TestConcurrentJobsAndSharedNumpyCache (83.86s)
…
PASS
ok  	github.com/pipedpeer/pipedpeer	82.777s
```
V2's first run failed `TestIntegrationFullSyncAndExecute` on a test bug; the
second excerpt is the rerun after the test fix.

**2. Start the containers:**
```bash
PIPEDPEER=$PWD/bin/pipedpeer scripts/lab-lan.sh up
```
Pass if (<!-- V1 8e14b9b: 02-lab-up.txt -->, 26 s):
```
  pc1 up at 10.88.0.11 (not joined)
  pc2 up at 10.88.0.12 (not joined)
  pc3 up at 10.88.0.13 (not joined)
  pc4 up at 10.88.0.14 (not joined)
next: scripts/lab-lan.sh join all   (or leave one out for the live join)
```

**3. Make this laptop LAPTOP-A.** Open a second terminal for
`bin/pipedpeer rendezvous` and leave it running. Then, in the first terminal:
```bash
bin/pipedpeer auth set
bin/pipedpeer start
bin/pipedpeer join 10.88.0.1
```
Pass if (<!-- V1 8e14b9b: 03-host-start.txt, 04-host-join.txt -->):
```
started daemon (node-id=06b9a112, port=38080)
restarting the daemon to join through 10.88.0.1:38445
no peers found
```
"no peers found" is normal for the first machine. `join` then asks for your
sudo password, to install the Nix key helper. Type it. V1 had no terminal for
the prompt and printed `could not make system nix trust the cluster's keys`.
With a terminal you should not see that line.

**4. Join three of the PCs. Leave pc3 out:**
```bash
PIPEDPEER=$PWD/bin/pipedpeer scripts/lab-lan.sh join pc1 pc2 pc4
```
Pass if each of pc1, pc2 and pc4 prints `connected (N peer(s))`, and its table
shows the others `healthy` with PATH `lan` (<!-- V1 8e14b9b: 05-join-pcs.txt -->):
```
this node is aa6e1138
connected (3 peer(s))
NODE       HOST           STATE        JOBS  CPU     MEM FREE  GPU  PATH  SOURCE
043e7463   pc4            healthy      0     16c 3%  6.7 GiB   -    lan   discovery
```

From here on, "on pcN" means `scripts/lab-lan.sh x pcN <command>`. It runs in
`/work`, a copy of `pipedpeer-demo/`. "On the laptop" means: run it from
`pipedpeer-demo/`, with `export PATH=$PWD/../bin:$PATH` set first.

---

## The demo steps

**5. Live join (step 1).** On the laptop, run `pipedpeer events -f` in a third
terminal. Then:
```bash
scripts/lab-lan.sh x pc3 ./demo/0-join.sh 10.88.0.1 "$(bin/pipedpeer auth show)"
```
Pass if (<!-- V2 31b5364: 56-join-pc3.txt -->, 1.8 s; V1 took 5.2 s):
```
$ pipedpeer join 10.88.0.1
restarting the daemon to join through 10.88.0.1:38445
this node is b7b73c2d
  waiting for peers · 90 s left · 0 found
connected (3 peer(s))
…
b7b73c2d*  pc3            healthy  0     16c 12%  9.3 GiB   -    self  self
```
The other pcs show `healthy` with PATH `lan`. A `192.168.0.200  unknown` row
is this laptop's other address; ignore it.
The events terminal shows a JOIN line for pc3. In V2 it still named pc3 by
its node id (`JOIN   b7b73c2d-aad0-41e5-b9c5-49a46da18d08 is up (10.88.0.13:38080)`,
56-join-host-events.txt); since dad7e42 it names the host
(<!-- V3 9949ad0: docs-VERIFY-141-events-f.txt -->):
```
09:38:16  JOIN   pc3 is up (10.88.0.13:38080)
09:38:17  TRUST  system nix now trusts these cluster keys (added through the pipedpeer key helper)
```
`pipedpeer dashboard` (run it in a real terminal) gets a pc3 row and the same
two lines under EVENTS (<!-- V3 9949ad0: docs-VERIFY-142-dashboard.txt -->):
```
pc3                  healthy     0       2%      1.2 GiB       -                              broadcast
…
EVENTS
…
  09:38:16  JOIN   pc3 is up (10.88.0.13:38080)
```
V3's own `join` on pc3 printed the laptop by its full node id
(`JOIN   06b9a112-9739-4e3d-9728-103872ac5f67 is up (192.168.0.200:38080)`,
10-live-join-pc3.txt) and the other pcs by hostname.

**6. The cluster (step 2).** On the laptop: `PAUSE=0 ./demo/1-cluster.sh`.
Pass if pc1–pc4 and `yeet` are all `healthy`, the `yeet*` row shows the GPU,
there are no `unreachable` rows, and the script ends with the `/ui` addresses
(<!-- V2 31b5364: 71-step-1-cluster.txt -->):
```
$ pipedpeer nodes
NODE       HOST  STATE    JOBS  CPU     MEM FREE  GPU                   PATH  SOURCE
06b9a112*  yeet  healthy  0     15c 5%  8.5 GiB   RTX 3060 0% 41M/6.0G  self  self
0b591fdc   pc2   healthy  0     16c 5%  721 MiB   -                     lan   manual
28197ccf   pc4   healthy  0     16c 5%  6.5 GiB   -                     lan   manual
b7b73c2d   pc3   healthy  0     16c 5%  1.0 GiB   -                     lan   manual
e3ec139a   pc1   healthy  0     16c 5%  874 MiB   -                     lan   manual
…
$ pipedpeer ui --print
This machine:    http://127.0.0.1:38080/ui
From elsewhere:  http://192.168.0.200:38080/ui
```
pc4's 6.5 GiB came from the host; since 9ca58a9 and da6a8ef it shows its
container limit (V2 re-check: about 1.1 GiB). <!-- V2 cc56753 -->

**7. GPU from a PC with no GPU (step 6).** First the CUDA environment on pc1.
This is a 21-minute build; start it and carry on with checks 8–11 in another
terminal:
```bash
scripts/lab-lan.sh x pc1 pipedpeer run 04_torch_ddp.py --remote --gpu force --check-only
```
Pass if (<!-- V1 8e14b9b: 11-pc1-gpu-checkonly.txt -->, 1269 s):
```
  gpu          required · GPU nodes: yeet
  ddp          1 eligible rank(s) — runs on one node
  environment  built in 1269.0 s
checks complete
```
Then the run itself:
```bash
scripts/lab-lan.sh x pc1 pipedpeer run 04_torch_ddp.py --remote --gpu force
```
Pass if (<!-- V2 31b5364: 74b-gpu-pc1-direct.txt -->, 7.8 s):
```
[coordinator] --remote: discovering a node other than this machine...
[coordinator] Placed on node 06b9a112 (lease=8d3d595b)
…
      Closure already on the node; nothing to send
…
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
training ...
epoch 0: 30 steps, loss 0.2452
epoch 1: 30 steps, loss 0.1147
epoch 2: 30 steps, loss 0.0592
training done in 1.3s
…
=== Summary ===
  ran on       yeet (192.168.0.200)
  environment  built in 1.2 s
  closure      already on the node
  script       5.9 s · peak memory 1.3 GB
  files back   none
  total        7.6 s
[coordinator] Task completed successfully on node 06b9a112
```
No `Warning: peer … did not take this environment` lines (V1 spent 264.8 s on
that broadcast). In the lab, run `pipedpeer run` directly as above:
`./demo/7-gpu.sh` needs `python3`, which the containers lack, so there it only
prints `No GPU machine among the peers - skipping` (74-step-7-gpu-pc1.txt).

ca70da4 made the script bigger and added two usage lines to the summary. V5's
pc1 run stalled on its first CUDA download
(40-item11-gpu-step-pc1-cold-download-stalled.txt), so the resized script was
captured **running locally on the laptop** instead
(<!-- V5 ef68f02: docs-new-gpu-step-host-local.txt -->):
```
$ pipedpeer run 04_torch_ddp.py --gpu force
…
epoch 0: 74 steps, loss 0.0360
…
training done in 13.4s
…
  this machine CPU 12% avg · RAM +28 MB (peak +1.1 GB) · GPU 55% avg
  on yeet      CPU 12% avg · GPU 74% avg · VRAM 665 MB peak
  files back   none
  total        18.9 s
```
`nvidia-smi` sampled once a second read 100 % for 13 s at 632–634 MiB
(<!-- V5 ef68f02: docs-new-gpu-step-host-nvidia-smi.csv -->). Run locally, both
usage lines name the same machine. Sent with `--remote`, they name the sender
and the node; V5, `00_pool.py --remote` from pc2
(<!-- V5 ef68f02: docs-new-usage-remote-pool-pc2.txt -->):
```
  ran on       pc3 (10.88.0.13)
…
  this machine CPU 22% avg · RAM +69 MB (peak +249 MB) · no GPU
  on pc3       CPU 31% avg
```

**7b. `--gpu force` without `--remote`, from the GPU-less pc1:**
```bash
scripts/lab-lan.sh x pc1 pipedpeer run 04_torch_ddp.py --gpu force
```
Pass if (<!-- V2 cc56753: 95-c-gpuforce-from-gpuless-pc1.txt -->):
```
[coordinator] --gpu force: this machine has no GPU; placing the run on a GPU node...
[coordinator] Placed on node 06b9a112 (lease=977b0522)
…
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
…
  ran on       yeet (192.168.0.200)
```
V2's total was 212.0 s, of which about 3 min 24 s was packing the 3.6 GB GPU
closure for peers that would not get it. f182b0f decides who gets a closure
before packing it; V3 took 7.0 s (<!-- V3 9949ad0: docs-VERIFY-225-gpuforce-pc1.txt -->):
```
[coordinator] Resource estimate: 2.1 GiB (tier=import)
[coordinator] --gpu force: this machine has no GPU; placing the run on a GPU node...
[coordinator] Placed on node 06b9a112 (lease=87f52228)
…
      Closure already on the node; nothing to send
…
  ran on       yeet (192.168.0.200)
  environment  built in 1.0 s
  closure      already on the node
  script       5.9 s · peak memory 1.2 GB
  files back   none
  total        7.0 s
```
The same with `--check-only` (no `--remote`) said `placement    this machine
(no --remote)` and `lease        refused: job rejected by remote daemon:
insufficient mem…` in V3 (05-dl-pc1-torch-checkonly.txt), although the real
run goes to the GPU node. FX-CLI2 fixed the check. V4 printed
(<!-- V4 c567369: docs-VERIFY-259-checkonly-gpuforce-pc1.txt -->):
```
  gpu          required · GPU nodes: yeet
  ddp          1 eligible rank(s) — runs on one node
  placement    yeet (--gpu force, this machine has no GPU; score=1.332,…
```
with no `refused` line.

**7c. Two GPU ranks (`--ddp 2`), DDP-DOWN and DDP-UP.** These ran in the GPU
lab: two containers (gpu1, gpu2) sharing this laptop's one RTX 3060 and a
GPU-less submitter (sub1). Take the lab-lan down first (only one lab at a time).
```bash
PIPEDPEER=$PWD/bin/pipedpeer scripts/lab-gpu.sh up
PIPEDPEER=$PWD/bin/pipedpeer scripts/lab-gpu.sh join all
scripts/lab-gpu.sh x sub1 pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force
```
Pass if (<!-- V2 cc56753: 113-c-ddp-ring-cached.txt -->, 9.5 s cached; the
first run took 635 s, packing and sending 3.1 GB to each rank, 112-c-ddp-ring-cold.txt):
```
=== Distributed training: 2 ranks ===
rank 0: a43e931b (10.89.0.12:38080)
rank 1: 29ffe602 (10.89.0.11:38080)
sync: daemon channel via 10.89.0.12:38080 (no extra ports)
…
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
[pipedpeer] ddp: sharding 60000 samples across 2 ranks
…
[pipedpeer] ddp daemon-channel sync ready (rank 0/2, every 1 step)
[pipedpeer] ddp daemon-channel sync ready (rank 1/2, every 1 step)
…
final loss: 0.18032728135585785
TOTAL 5.5s
[pipedpeer] ddp: 43 sync(s), 1.6s total (36 ms each), 66.2 MiB sent, 66.2 MiB received
[pipedpeer] ddp: the ring did NOT pay for itself — 2 ranks on 1 machine(s), …
```
"did NOT pay for itself" is right here: both ranks share one GPU.

DDP-DOWN and DDP-UP: run 60 epochs, and once `epoch 3:` appears, stop the
daemon of the rank that is NOT named in the `sync: daemon channel via` line,
wait about 25 s, then start it again:
```bash
scripts/lab-gpu.sh x sub1 pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force 60
scripts/lab-gpu.sh x gpu1 pipedpeer stop      # the non-lead rank in V2
scripts/lab-gpu.sh x gpu1 pipedpeer start
```
Pass if (<!-- V3 9949ad0: docs-VERIFY-284-ddp-down-up.txt -->, 72 s in all):
```
[ddp] rank 1 on gpu1 died; continuing on 1
[ddp]   cause: execution failed: connection lost: websocket: close 1006 (abnormal closure): unexpected EOF
[pipedpeer] ddp: rank 1 stopped (execution failed: connection lost: websocket: close 1006 (abnormal closure): unexpected EOF); its share of the data moves to the 1 rank(s) left at the next epoch
epoch 5: 14 steps, loss 0.0672
[pipedpeer] ddp: rank 0 of 1 now takes 57344 samples, batch 4096
…
[ddp] rank 2: starting on gpu1 (127.0.0.1:38883); it joins at the ring's next epoch
…
[pipedpeer] ddp: rank 2 is ready; waiting for the ring's next epoch to join
[pipedpeer] ddp: rank 2 joins; the ring grows to 2 rank(s) at the next epoch, each taking a share of the data
epoch 23: 14 steps, loss 0.0003
[pipedpeer] ddp: handed the model and optimizer to joining rank 2 (6.0 MiB)
[pipedpeer] ddp: the ring grows from 1 to 2 ranks at this epoch
[pipedpeer] ddp: rank 0 of 2 now takes 27648 samples, batch 3072
[pipedpeer] ddp: rank 2 joins the ring of 2 with rank 0's model and optimizer, skipping the 24 epoch(s) the ring already did
…
[ddp] finished on 2 of 3 ranks (lost: [1])
```
V2 (114-c-ddp-down-up.txt) counted the dead rank: `rank 0 of 2 now takes
57344 samples`, `the ring grows from 2 to 3 ranks`, `joins the ring of 3`.
FX-SHIM counts live ranks only, as above.

The lead dying: same run, but stop the lead (the rank named in the `sync:` line).
Pass if (<!-- V2 cc56753: 115-c-ddp-lead-dies.txt -->) it ends, exit 1, with:
```
[ddp] rank 0 on gpu1 died: its machine leads the ring (rendezvous and sync), so the run can't continue without it; the other ranks will stop
…
Error: rank 0 on gpu1 failed: execution failed: connection lost: websocket: close 1006 (abnormal closure): unexpected EOF
```
The surviving rank also prints a Python traceback ending in
`http.client.RemoteDisconnected`; that is the expected stop.

Finish with `scripts/lab-gpu.sh down`.

**The real 2-GPU ring** (this laptop + the friend's RTX 3050, a third machine
submitting) was not completed. V3 sent it from sub1 with the friend joined over
Tailscale (<!-- V3 9949ad0: 53-friend-ring-cold.txt -->):
```
=== Distributed training: 2 ranks ===
rank 0: 06b9a112 (192.168.0.200:38080)
rank 1: 94bd5f98 (127.0.0.1:41293)
shares: rank 0 56%, rank 1 44% — measured, so a slower rank takes less rather than setting the pace for everyone
sync: daemon channel via 192.168.0.200:38080 (no extra ports)
…
[pipedpeer] ddp: every rank produced the SAME gradients, so every rank is training on the same data. …
…
[ddp] rank 1 on fedora died; continuing on 1
…
TOTAL 794.7s
[ddp] finished on 1 of 2 ranks (lost: [1])
```
The friend's laptop dropped off Tailscale while its ~1 GB closure was still
arriving over a relay (~2.1 MB/s), and rank 0 had started training without
rank 1, waiting ~45 s per sync. FX-DDPREADY makes rank 0 wait until every rank
has its closure. Check it in your own run: from a GPU-less machine, with prep
done, `pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force`; expect the two
`rank N:` lines to name the two laptops, two `using GPU:` lines with
different GPU names, no `SAME gradients` or `died` line, and both
`nvidia-smi` screens busy.

V4 ran this in the GPU lab, not on two laptops: two containers sharing one RTX 3060
(<!-- V4 c567369: docs-VERIFY-359-ring-gpulab-substitute.txt -->):
```
$ (sub1) pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force
=== Distributed training: 2 ranks ===
rank 0: 96d92571 (10.89.0.12:38080)
rank 1: fe7332e0 (10.89.0.11:38080)
sync: daemon channel via 10.89.0.12:38080 (no extra ports)
…
       Sent 43 of 43 store paths: 3.1 GB in 25.2 s
…
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
```
`SAME gradients` and `died` did not appear. The real two-laptop run is still to be
checked during your own run.

**What you'll see at the start (DDP start barrier).** Rank 0 waits at step 1 until every
rank has its closure and has started, up to 5 minutes (`PIPEDPEER_DDP_START_WAIT`),
before it trains. With one slow machine that wait is visible: V4 on a 1 Mbit link
printed `training ...` at 12:51:42 and the first `daemon-channel sync ready` lines at
12:54:07, about 146 s later (<!-- V4 c567369: 80-item1-ddp-start-barrier-default.txt -->).
With `PIPEDPEER_DDP_START_WAIT=30` the line is
(<!-- V4 c567369: 82-item1-ddp-start-wait-30s.txt -->):
```
[pipedpeer] ddp: rank 1 not ready after 30s (closure still on its way, or the script not started); training started on the 1 rank(s) that were, and their data moves to them at the next epoch. …
```

**8. Dry run (step 3):**
```bash
scripts/lab-lan.sh x pc4 pipedpeer run 00_pool.py --remote --check-only
```
Pass if (<!-- V1 8e14b9b: 12-checkonly-pc4-00pool.txt -->, 0.8 s):
```
CHECK 00_pool.py — nothing will run
  imports      standard library only
  spreads      multiprocessing.Pool (line 33)
  closure      already on yeet (26 paths)
checks complete
```
The `warning: ignoring the client-specified setting 'build-users-group'`
lines are normal on pc4's multi-user Nix. V2 did not rerun this exact
command; its check-only runs through `11-teacher.sh` (check 10b) print the
same layout (`CHECK … — nothing will run`, `spreads`, `closure already on …
(26 paths)`, `checks complete`) (<!-- V2 cc56753: 100-c-teacher-cached-primes.txt -->).

**9. Cold vs cached (step 5).** pc3 must not have run anything yet besides the
join.
```bash
scripts/lab-lan.sh x pc3 env PAUSE=0 ./demo/2b-cold.sh
```
Pass if (<!-- V1 8e14b9b: 14-pc3-truecold-2b.txt -->, 175 s with netem on):
```
      Cold run: sending the whole closure even if the node has it
      Sent closure: 96 MB in 114.9 s (834 kB/s)
  closure      sent 96 MB in 114.9 s (cold)
  total        169.1 s
      Closure already on the node; nothing to send
  closure      already on the node
  total        5.8 s
  first run (cold): 169 s     every run after: 6 s
```
Both runs print `checksum 191999840`. V1 had Wi-Fi-like delay and 0.5 % loss
on each pc. The evidence records the resulting qdisc on every pc, not the
command (<!-- V2 31b5364: 57-netem.txt -->):
```
pc1: qdisc netem 8007: root refcnt 17 limit 1000 delay 4ms  3ms loss 0.5% seed 16053757782790428889
```
The commands that set it on every pc and remove it again, run inside the
containers (this laptop has no `tc`, so a `sudo nsenter … tc` form fails with
`nsenter: failed to execute tc: No such file or directory`):
```bash
for pc in pc1 pc2 pc3 pc4; do docker exec pp-lan-$pc tc qdisc add dev eth0 root netem delay 4ms 3ms loss 0.5%; done
for pc in pc1 pc2 pc3 pc4; do docker exec pp-lan-$pc tc qdisc show dev eth0; done     # check
for pc in pc1 pc2 pc3 pc4; do docker exec pp-lan-$pc tc qdisc del dev eth0 root; done
```
Pass if the check prints, per pc, and after the removal `qdisc noqueue 0: root refcnt 2`
(<!-- V3 9949ad0: docs-VERIFY-349-netem.txt -->):
```
pc1: qdisc netem 800b: root refcnt 17 limit 1000 delay 4ms  3ms loss 0.5% seed 12941513410463435892
```

V2 ran this step on pc2, whose store already had the environment, so only the
send was cold (<!-- V2 31b5364: 73-step-2b-cold-pc2.txt -->, 25.8 s):
```
      Cold run: sending the whole closure even if the node has it
      Packed closure: 96 MB in 6.9 s
      Sent closure: 96 MB in 8.6 s (11 MB/s)
…
  closure      sent 96 MB in 8.6 s (cold)
  total        20.5 s
…
      Closure already on the node; nothing to send
…
  closure      already on the node
  total        4.8 s
  first run (cold): 21 s     every run after: 5 s
```
Since cc56753 the script exits non-zero when either run fails.

**10. Packages a script never named (step 8).** On the laptop, in a new
folder, create `teacher.py`:
```python
import humanize
import notarealpkg_xyz
from multiprocessing import Pool
def f(x): return x * x
if __name__ == "__main__":
    with Pool(4) as p:
        print(humanize.intcomma(sum(p.map(f, range(1000)))))
```
Then run `pipedpeer run teacher.py --check-only`, then `pipedpeer run teacher.py`.
Pass if (<!-- V1 8e14b9b: 40-pkgauto-probe.txt, 41-teacher-unpackaged-run.txt -->):
```
  not packaged notarealpkg_xyz (add --pkg)
  spreads      multiprocessing.Pool (line 6)
checks complete
      Not in the built-in table, but nixpkgs has python3Packages.humanize: using it
      Not packaged (the job will fail to import it): notarealpkg_xyz — add it with --pkg <nixpkgs name>
ModuleNotFoundError: No module named 'notarealpkg_xyz'
```
V1's check-only also said `not packaged humanize (add --pkg)`. Since FX-CLI
1ef1058 check-only asks nixpkgs; V3's check-only listed only the made-up one
(<!-- V3 9949ad0: docs-VERIFY-392-teacher-checkonly.txt -->, 1.2 s):
```
CHECK teacher.py — nothing will run
  imports      humanize, notarealpkg_xyz
  packages     humanize
  not packaged notarealpkg_xyz (not in nixpkgs; the job will fail to im…
  spreads      multiprocessing.Pool (line 6)
…
checks complete
```
and the run ended in `ModuleNotFoundError: No module named 'notarealpkg_xyz'`
and `Error: execution failed: remote job exited with code 1`
(docs-VERIFY-392-teacher-run.txt).
Now delete the `import notarealpkg_xyz` line and run `pipedpeer run teacher.py`
again. Pass if (<!-- V1 8e14b9b: 42-teacher-humanize-run.txt -->, 1.4 s):
```
332,833,500
```
V1's `spread` line counted 1476 items for this 1000-item job. Since FX-CLI and
da6a8ef it counts each item once, as it ran; V3, 1.5 s
(<!-- V3 9949ad0: docs-VERIFY-401-teacher-humanize-run.txt -->):
```
332,833,500
…
  spread       1000 items: 269 on yeet, 731 on 4 others (pc3 185, pc1 182, pc2 182, pc4 182)
  files back   none
  total        1.5 s
```

**10b. Student scripts through `11-teacher.sh` (step 8).**
```bash
scripts/lab-lan.sh x pc3 env PAUSE=0 ./demo/11-teacher.sh students/primes.py
```
Pass if (<!-- V2 cc56753: 100-c-teacher-cached-primes.txt -->, 40 s):
```
CHECK primes.py — nothing will run
  imports      standard library only
  spreads      ProcessPoolExecutor (line 36)
               split decided at run time by measured cost
…
  closure      already on this machine (26 paths)
checks complete
…
$ pipedpeer run primes.py
…
primes below 16000000 : 1031130
TOTAL 38.6s
…
  ran on       pc3 (127.0.0.1)
  spread       100 items: 26 on pc3, 74 on 4 others (yeet 35, pc2 17, pc4 12, pc1 10)
  total        39.3 s
```
The other four, same command, cached totals in V2: `monte_carlo_pi.py` 35.9 s,
`word_count.py` 42.5 s, `grid_search.py` 69.5 s, `image_blur.py` 19.6 s
(100-c-teacher-cached-*.txt). If a node's free memory dips (V2 saw ~240 MB
right after a big run), the script prints
`Error: … insufficient memory: need 536870912 bytes, available …` and
`the run failed (exit 1)` (100-c-teacher-refused-*.txt): wait a few seconds and
rerun.

**11. A run on the laptop that spreads (steps 4 and 10).** On the laptop:
`pipedpeer run 00_pool.py`. Pass if (<!-- V1 8e14b9b: 30-host-00pool-first.txt -->):
```
[pipedpeer] remote ok: 12 items in 6504 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.13:38080, 10.88.0.14:38080
checksum 4319999744
  ran on       yeet (127.0.0.1)
  closure      already on the node
```
The summary's `spread` line counts 96 items and names the pcs that took some
(V1 said 120 items for this 96-item job; FX-CLI fixed the count).

The full headline: on the laptop, `PAUSE=0 ./demo/2-headline.sh` (python3,
then pipedpeer, same file). Pass if (<!-- V2 31b5364: 71-step-2-headline.txt -->, 27 s):
```
 00_pool.py - this machine only (plain python3)
…
checksum 4319999744
TOTAL 9.5s
…
 00_pool.py - across the cluster (pipedpeer run)
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
The same checksum both times is the pass. 0.58× is what containers sharing
one CPU give; the real speedup comes from the lab visit.

**12. Failover (step 10).** On the laptop: `PAUSE=0 ./demo/9-failover.sh`.
After the first `remote ok` line, in another terminal:
```bash
scripts/lab-lan.sh x pc3 pipedpeer stop
```
Pass if (<!-- V1 8e14b9b: 50-failover.txt, 51-failover-events.txt -->, 19.5 s):
```
stopped daemon
checksum 4319999744
```
After that, `pipedpeer events` on the laptop shows `LEAVE  pc3 is gone`. Bring
pc3 back with `scripts/lab-lan.sh x pc3 pipedpeer start`; events then shows
`JOIN   pc3 is up (10.88.0.13:38080)`.

**13. Ctrl+C (step 11c).** In a second terminal, run
`~/Downloads/pipedpeer-work/tmp/poolwatch.sh 45`, which prints each pc's pool
counters every second. On the laptop: `pipedpeer run 00_pool.py`, then press
Ctrl+C after the first `remote ok` line. Pass if
(<!-- V3 9949ad0: docs-VERIFY-505-ctrlc.txt, docs-VERIFY-505-ctrlc-poolwatch.txt -->):
```
^C/nix/store/…-python3-3.14.7-env/lib/python3.14/multiprocessing/resource_tracker.py:475: UserWarning: resource_tracker: There appear to be 6 leaked semaphore objects to clean up at shutdown: {…}
  warnings.warn(
History: /home/yeet/.local/share/pipedpeer/jobs/1791778845175987352  (pipedpeer job --id 75987352 --timeline)

=== Summary ===
  ran on       yeet (127.0.0.1)
  environment  built in 0.4 s
  closure      already on the node
  script       8.1 s · peak memory 162 MB
  total        cancelled after 8.6 s
cancelled — remote work stopped
```
and in poolwatch every pc's `ex=` stops growing and its `can=` goes up
(V3: `pc1 ex=286 can=2` became `pc1 ex=286 can=5` and stayed there).
V1 also printed a ~45-line Python traceback (ConnectionRefusedError) after ^C;
since FX-CLI 1ef1058 there is none. The two `resource_tracker` lines are
Python's notice about the killed pool's semaphores.

**13b. Ctrl+C while the environment uploads.** Send a GPU job to pc3, which
lacks the 6.4 GB torch closure, and press Ctrl+C during `[6/7] Uploading`:
```bash
pipedpeer run 04_torch_ddp.py --host 10.88.0.13:38080 --mem 900M
```
Pass if (<!-- V2 cc56753: 94-c-ctrlc-mid-upload.txt -->; the CLI exited
0.06 s after the signal):
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
Right after, `pipedpeer tasks` says `no tasks running`, and
`pipedpeer run 00_pool.py --host 10.88.0.13:38080` is accepted at once and
prints `checksum 4319999744` (the lease was freed).

**14. Closing the terminal stops the job too.** Same run. Instead of Ctrl+C,
close the terminal window. Pass if, in `pipedpeer jobs` from another terminal,
that job is `cancelled`, and poolwatch stops growing as in check 13.
(<!-- V1 8e14b9b: 32-sighup.txt, 32-sighup-poolwatch.txt; V1 sent SIGHUP to the CLI, which is what closing the window does -->)

**15. Ctrl+Z, jobs, attach (step 11a).** On the laptop:
`pipedpeer run 00_pool.py`, then press Ctrl+Z after the first `remote ok`
line. Pass if (<!-- V1 8e14b9b: 33-ctrlz.txt, 33-ctrlz-jobs.txt, 34-attach.txt -->):
```
detached — job 20896323 keeps running
  reattach: pipedpeer attach 20896323
```
Next, `pipedpeer jobs` shows the job as `running	bg`. Then run
`pipedpeer attach <that id>`. Pass if:
```
attaching to job 20896323 (00_pool.py) on 127.0.0.1:38080
checksum 4319999744
=== Summary ===
```

**15b. A background job's results come back without attach.** A detached
`--host` job from pc3 to the laptop, then the laptop's daemon goes away:
```bash
scripts/lab-lan.sh x pc3 pipedpeer run students/word_count.py --host 10.88.0.1:38080 --detach
# wait ~60 s for it to finish, then on the laptop:
pipedpeer stop
scripts/lab-lan.sh x pc3 pipedpeer attach <id>
```
Pass if `run` ends with (<!-- V2 31b5364: 60-fetch-detach.txt -->):
```
detached — job 71581038 keeps running
  reattach: pipedpeer attach 71581038
```
`pipedpeer jobs` on pc3 then shows it `succeeded	bg`, pc3's daemon.log has
`job collected bytes=5481 … files=1 from=10.88.0.1:38080 job=…1581038 status=succeeded`
(65-senderdeath-collected.txt), and `attach`, with the laptop's daemon
stopped, still prints the saved output (<!-- V2 31b5364: 62-fetch-attach-node-gone.txt -->):
```
job 71581038 (word_count.py) finished on 10.88.0.1:38080; its output, saved on this machine:

counting words in 400 documents
…
wrote counts.txt
TOTAL 29.0s
…
=== Summary ===
  ran on       10.88.0.1
  script       peak memory 940 MB
  spread       400 items: 244 on 10.88.0.1, 156 on 1 other (other machines 156)
  files back   1 (1 new, 0 updated)
  total        50.7 s
```
V2 also printed one raw JSON line (`{"level":"warn",…"message":"withholding the
cluster token"}`) before the summary. V3 ran the same check from pc4
(job 40568475, 128 s to detach because pc4 first fetched its Nix inputs); the
warning is plain text now, with two more lines about the stopped node
(<!-- V3 9949ad0: docs-VERIFY-583-detach-pc4.txt, docs-VERIFY-583-attach-node-gone.txt -->):
```
wrote counts.txt
TOTAL 28.0s
04:14:14 WRN withholding the cluster token error="10.88.0.1:38080 could not prove it holds the cluster token; not sending it: dial tcp 10.88.0.1:38080: connect: connection refused" addr=10.88.0.1:38080
warning: could not ask 10.88.0.1 for its hostname: Get "http://10.88.0.1:38080/health": 10.88.0.1:38080 could not prove it holds the cluster token; not sending it: dial tcp 10.88.0.1:38080: connect: connection refused
warning: could not list 10.88.0.1's peers for the summary: Get "http://10.88.0.1:38080/v1/nodes": 10.88.0.1:38080 could not prove it holds the cluster token; not sending it: dial tcp 10.88.0.1:38080: connect: connection refused

=== Summary ===
  ran on       10.88.0.1
  script       peak memory 756 MB
  spread       400 items: 231 on 10.88.0.1, 169 on 1 other (other machines 169)
  files back   1 (1 new, 0 updated)
  total        28.0 s
```
A second `attach` printed the same and did not apply the files again:
`counts.txt` kept its time (`04:13:54.821959135`) across both.
The `spread` line says `1 other` although pc1–pc4 all took work (the
`remote ok` lines name 10.88.0.11–14); FX-CLI2 names them. V4 printed
(<!-- V4 c567369: docs-VERIFY-696-attach-spread-pc4.txt -->):
```
  spread       400 items: 218 on yeet, 182 on 4 others (pc2 52, pc4 51, pc3 47, pc1 32)
```
Start the laptop's daemon again: `pipedpeer start`.

**16. --detach.** On the laptop: `pipedpeer run 00_pool.py --detach`. It
returns in about 0.5 s. Then run `pipedpeer attach <id>`. Pass if
(<!-- V1 8e14b9b: 35-detach.txt, 35-detach-attach-live.txt -->):
```
detached — job 63417177 keeps running
  reattach: pipedpeer attach 63417177
attaching to job 63417177 (00_pool.py) on 127.0.0.1:38080
checksum 4319999744
```

**17. The Nix key helper on the Ubuntu stand-in (pc4):**
```bash
scripts/lab-lan.sh x pc4 cat /etc/nix/pipedpeer-trusted-keys.conf
scripts/lab-lan.sh x pc4 pipedpeer run 00_pool.py
```
Pass if (<!-- V1 8e14b9b: 22-fixtrust-pc4.txt, 22b-fixtrust-from-pc4.txt -->):
```
# Written by /usr/local/libexec/pipedpeer-nix-trust (pipedpeer); do not edit.
extra-trusted-public-keys = pipedpeer-…
checksum 4319999744
  ran on       pc4 (127.0.0.1)
```
The second command's `spread` line names the other machines.

**18. Receipts (step 13).** On the laptop, run `PAUSE=0 ./demo/10-proof.sh`.
Pass if, after the `pipedpeer jobs` table, it prints the last run's timeline,
receipt and recent events (<!-- V2 31b5364: 77-step-10-proof.txt -->):
```
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
07:30:10  WORK   lease granted: job  from pc2
```
Then run `scripts/lab-lan.sh x pc2 pipedpeer traffic`. Pass if
(<!-- V1 8e14b9b: 60-proof-receipts.txt -->) its lines read:
```
… [pool] received pool/map from 10.88.0.1:…: items=… b64=false cache_keys=0 required_mem=… forwarded=""
```
Each line is work pc2 did for the laptop (10.88.0.1).

**19. A join that needs no rendezvous.** With UDP 38445 dropped on the
laptop's side of the bridge, `scripts/lab-lan.sh x pc3 pipedpeer join 10.88.0.1`
still printed `connected (4 peer(s))`, because LAN discovery found the peers
(<!-- V1 8e14b9b: 70-firewall-join.txt; rule in 70-firewall-rule.txt -->).
The rule V1 used (70-firewall-rule.txt) and the commands that add and remove it:
```bash
sudo nft add table inet ppdrill
sudo nft add chain inet ppdrill input '{ type filter hook input priority filter - 10; policy accept; }'
sudo nft add rule inet ppdrill input iifname "pp-venue" udp dport 38445 drop
scripts/lab-lan.sh x pc3 pipedpeer join 10.88.0.1      # expect: connected (4 peer(s))
sudo nft delete table inet ppdrill
```
Pass if (<!-- V3 9949ad0: docs-VERIFY-669-nft-drill.txt -->, 1 s):
```
restarting the daemon to join through 10.88.0.1:38445
this node is 77dd3fd6
connected (4 peer(s))

NODE       HOST  STATE    JOBS  CPU     MEM FREE  GPU                   PATH  SOURCE
06b9a112   yeet  healthy  0     15c 2%  6.7 GiB   RTX 3060 0% 41M/6.0G  lan   discovery
77dd3fd6*  pc3   healthy  0     16c 2%  1.1 GiB   -                     self  self
…
```

## Added in V2

**20. pandas out-of-core (step 7).** On the laptop: `PAUSE=0 ./demo/6-pandas.sh`.
Pass if (<!-- V2 31b5364: 71-step-6-pandas.txt -->, 12.8 s):
```
$ pipedpeer run 03_pandas_ooc.py -e PIPEDPEER_OOC_MIN=1e8
[pipedpeer] read_csv: streaming 329 MB out-of-core
read data.csv: 3,000,000 rows x 7 cols in 8.0s
[pipedpeer] groupby: combining 5 chunk partials from the cluster
groupby('cat').mean(): 1.0s
…
alpha  1.498151e+06  0.499574  0.500078  0.500743  0.500440  0.500033
…
TOTAL 9.0s
```

**21. Everything in one script (step 9).**
```bash
scripts/lab-lan.sh x pc3 pipedpeer run 07_everything.py --remote
```
The first run on pc3 built the whole environment, CUDA torch included
(`environment  built in 470.0 s`), and took 700.6 s. Pass if
(<!-- V2 cc56753: 98-c-everything-pc3-cold.txt -->):
```
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
…
largest singular value 1152.7576
section numpy 8.9s
--- pandas
…
section pandas 0.5s
--- torch
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
…
final loss 0.0282
section torch 1.9s
wrote results/everything.txt
TOTAL 23.0s
…
  ran on       yeet (192.168.0.200)
  environment  built in 470.0 s
…
  total        700.6 s
```
In V2 the second (cached) run then failed: the recorded peak memory (7.6 GB,
summed over forked workers) became an 8.5 GiB estimate and every node turned
it down (`Error: no eligible node found`, 99-c-everything-pc3-cached.txt).
FX-PEAK fixed the estimate. V3 ran it cold (664.6 s, `environment  built in
637.7 s`, 21-item2-everything-pc3-cold.txt), then cached. Pass if the cached
run reads (<!-- V3 9949ad0: docs-VERIFY-732-everything-pc3-cached.txt -->, 26.7 s):
```
[coordinator] GPU-accelerated libraries detected — preferring GPU nodes
[coordinator] Resource estimate: 1.9 GiB (tier=historical)
[coordinator] --remote: discovering a node other than this machine...
[coordinator] Placed on node 06b9a112 (lease=28683710)
…
      Closure already on the node; nothing to send
…
checksum 1151999856
section pool 7.8s
…
accuracy 0.9599
…
largest singular value 1152.7576
…
final loss 0.0282
section torch 1.7s
wrote results/everything.txt
TOTAL 21.0s
…
  ran on       yeet (192.168.0.200)
  environment  built in 2.3 s
  closure      already on the node
  script       24.3 s · peak memory 1.7 GB
  spread       192 items: 158 on yeet, 50 on 2 others (pc3 28, pc2 22) · 16 computed twice (local cores raced slow chunks)
  files back   1 (0 new, 1 updated)
  total        26.7 s
```
`./demo/12-everything.sh` on pc2 runs it without `--remote`, on pc2 itself,
which needs about 3.1 GiB free (`tier=import`: pc2 never ran it). A 1.2 GB
container can't, so in the lab it exits 1
(<!-- V3 9949ad0: docs-VERIFY-736-12-everything-pc2.txt -->, 15.1 s):
```
$ pipedpeer run 07_everything.py
[coordinator] GPU-accelerated libraries detected — preferring GPU nodes
[coordinator] Resource estimate: 3.1 GiB (tier=import)
[coordinator] job rejected by remote daemon: insufficient memory: need 3300917248 bytes, available 673480704 bytes; waiting up to 15s for memory to free...
Error: job rejected by remote daemon: insufficient memory: need 3300917248 bytes, available 675500168 bytes

  the run failed (exit 1): read the Error line above
```
That is the expected result in the lab. On a venue PC with 4 GB or more free
the script runs on the PC itself.

**22. Finale (step 12).** The 1.2 GB containers can't hold the finale's
1.9 GiB numpy and everything jobs (inside prep-2 it ran 5 of 8). Resize them
first, 6.0 GB in total, as V3 did (<!-- V3 9949ad0: 30-finale-lab-resize.txt -->):
```bash
docker update --memory 2600m --memory-swap 2600m pp-lan-pc1
docker update --memory 1900m --memory-swap 1900m pp-lan-pc2
docker update --memory 900m --memory-swap 900m pp-lan-pc3
docker update --memory 600m --memory-swap 600m pp-lan-pc4
```
Other sizes OOM-killed pc daemons in V3 (pc3 at 700m, pc4 at 700m, pc2 at
2000m while numpy ran on pc1); if a pc drops out of `pipedpeer nodes`, run
`scripts/lab-lan.sh x <pc> pipedpeer start`.
Then on the laptop: `PAUSE=0 ./demo/13-finale.sh`. Since 50bae3e it sets
`PIPEDPEER_REMOTE_WAIT=10m`, so each job waits up to 10 min for a machine with
room instead of `--remote`'s default 15 s (with 15 s V3's first finale ran 6
of 8: 31-item3-finale-run1-15s-wait.txt). Pass if
(<!-- V3 9949ad0: docs-VERIFY-763-finale.txt -->, 412.0 s, exit 0):
```
 Finale - 8 jobs at once
…
  started pool  (log: demo/finale-logs/pool.log)
…
  started everything  (log: demo/finale-logs/everything.log)
…
 Finale results
================================================================
  JOB          RAN ON                 TOTAL      RESULT
  pool         pc4                    40.3s      ok
  sklearn      pc2                    21.9s      ok
  numpy        pc1                    340.0s     ok
  pandas       pc2                    57.3s      ok
  torch        yeet                   3.3s       ok
  files        pc3                    0.0s       ok
  lambda       pc4                    4.2s       ok
  everything   pc1                    43.1s      ok

  > RAN ON '?' = that job printed no summary; its log says where it went.
```
V2 ran 6 of 8 in 534 s (101-c-13-finale.txt): pandas failed on a pool HTTP
500 (FX-SHIM now combines locally) and everything on the 8.5 GiB estimate
(FX-PEAK).

**23. Capstone (step 14).** On the laptop: `PAUSE=0 ./demo/14-capstone.sh`.
Pass if (<!-- V2 31b5364: 78-step-14-capstone.txt -->, 288 s):
```
[1/5] pandas: read data.csv and average each category ...
[pipedpeer] read_csv: streaming 196 MB out-of-core
[pipedpeer] groupby: combining 3 chunk partials from the cluster
  pandas: 216.5s
[2/5] numpy: features and target ...
  numpy: 58.3s
[3/5] sklearn: RandomForest(n_estimators=80, n_jobs=-1) ...
…
  accuracy 0.8962
  sklearn: 2.9s
[4/5] Pool: bootstrap the accuracy (48 tasks x 100 resamples) ...
  95% of resamples between 0.8929 and 0.8997
  pool: 4.5s
[5/5] files: write results/ ...
  files: 0.8s
TOTAL 283.0s
      Synced 3 file(s) to …/07_capstone (3 new, 0 updated, 0 deleted)
…
$ ls results/
by_category.csv
feature_importance.csv
summary.json
```

**24. `pipedpeer stats`.** On the laptop. Pass if (<!-- V2 31b5364: 79-stats.txt -->):
```
Cluster: 5 node(s), 5 healthy · 79 cores · 4.3 GiB free memory · 1 GPU node(s) · 1 task(s) running

NODE       HOST           STATE       CORES  CPU%   MEM FREE JOBS     CHUNKS FROM PEER ITEMS RUN TO PEERS FAILED
06b9a112   yeet           healthy*       15    40    1.1 GiB    1        153       32       447      209      0
e3ec139a   pc1            healthy        16    49    506 MiB    0        145      104       442        0      0
…
Recent runs from this machine:
ID                   SCRIPT             STATUS     NODE          TOTAL  SLOWEST STAGE          DISTRIBUTED
1791771026451497843  07_capstone.py     succeeded  06b9a112    4m47.3s  script 4m44.3s         0/288 items remote
1791770005089791190  00_pool.py         succeeded  06b9a112      16.5s  script 16s             12/96 items remote
…
```

**25. The `/ui` page.** On the laptop: `pipedpeer ui --print`, open the first
address in a browser window about 1366×768. Pass if the page reads
(<!-- V2 31b5364: 68-ui-text.txt, page text from headless Chromium; no console errors -->):
```
Pipedpeer
127.0.0.1:38080
live · updated 7:09:49 AM
…
Nodes healthy
5of 5
Cores
79
…
GPU nodes
1
…
Node	Host	State	Cores	CPU	Free memory	Tasks	Items run	From peers	Sent to peers	Failed
06b9a112	yeet	healthy · this machine	15
…
Recent runs from this machine
```

**26. The sender loses its network (step 11b).** Start a job from pc3 on the
laptop, then cut pc3 off for 200 s:
```bash
scripts/lab-lan.sh x pc3 pipedpeer run students/word_count.py --host 10.88.0.1:38080
# right after the first "remote ok" line, in another terminal:
docker network disconnect pipedpeer-lan_venue pp-lan-pc3; sleep 200; docker network connect --ip 10.88.0.13 pipedpeer-lan_venue pp-lan-pc3
```
Pass if the run ends with (<!-- V2 31b5364: 64-senderdeath-long-run.txt -->):
```
lost the connection to 10.88.0.1:38080 (connection lost: read tcp 10.88.0.13:57302->10.88.0.1:38080: read: connection timed out); checking whether the node or this machine went away...
…
neither the node (…) nor any other peer answers, so this machine is the one offline; the job keeps running there

detached — job 53517360 keeps running
  reattach: pipedpeer attach 53517360
```
and, about 15 s after pc3 reconnects, pc3 has the result file and its job
history says it ran once (<!-- V2 31b5364: 65-senderdeath-collected.txt -->):
```
53517360	succeeded	bg	job-1791768757453515387	…
01:36:17 INF job collected bytes=2664 component=daemon files=1 from=10.88.0.1:38080 job=1791768757453517360 status=succeeded
```
V2 also printed four raw JSON `withholding the cluster token` lines while pc3
was offline. V3 ran the same check from pc4 (`pp-lan-pc4`, `--ip 10.88.0.14`;
the network commands above ran as written, rc=0). The lines are plain text
now; the CLI detached 2 min 41 s after the cut, and pc4 had the result 1 s
after reconnecting
(<!-- V3 9949ad0: docs-VERIFY-852-senderdeath-run-pc4.txt, docs-VERIFY-853-senderdeath-net-pc4.txt, docs-VERIFY-852-senderdeath-collected-pc4.txt -->):
```
lost the connection to 10.88.0.1:38080 (connection lost: read tcp 10.88.0.14:59582->10.88.0.1:38080: read: connection timed out); checking whether the node or this machine went away...
04:17:13 WRN withholding the cluster token error="192.168.0.200:38080 could not prove it holds the cluster token; not sending it: dial tcp 192.168.0.200:38080: connect: network is unreachable" addr=192.168.0.200:38080
…
neither the node (job status: Get "http://10.88.0.1:38080/v1/jobs/4d9a76bf-82e5-406d-818d-ddbc349a7ff7/status": dial tcp 10.88.0.1:38080: connect: network is unreachable) nor any other peer answers, so this machine is the one offline; the job keeps running there

detached — job 73428572 keeps running
  reattach: pipedpeer attach 73428572
$ pipedpeer jobs | grep 73428572
73428572	succeeded	bg	job-1791778468373426478		06b9a112-9739-4e3d-9728-103872ac5f67		29626	2026-10-12T04:14:28.373542827Z
04:17:54 INF job collected bytes=1298 component=daemon files=1 from=10.88.0.1:38080 job=1791778468373428572 status=succeeded
```

**26b. The same with a job that ends while the sender is offline.** From pc1,
cut off for 90 s once `epoch 1` shows:
```bash
scripts/lab-lan.sh x pc1 pipedpeer run 04_torch_ddp.py --remote --gpu force 60
docker network disconnect pipedpeer-lan_venue pp-lan-pc1; sleep 90; docker network connect --ip 10.88.0.11 pipedpeer-lan_venue pp-lan-pc1
```
V3 (<!-- V3 9949ad0: docs-DEMO-806-senderdeath-gpu-pc1.txt -->, 127 s):
```
epoch 3: 30 steps, loss 0.0320

lost the connection to 192.168.0.200:38080 (connection lost: read tcp 10.88.0.11:58404->192.168.0.200:38080: read: connection reset by peer); checking whether the node or this machine went away...
the job ended on the node (done); collecting it
      No files changed on the remote

=== Summary ===
  ran on       yeet (192.168.0.200)
  script       peak memory 1.2 GB
  files back   none
  total        21.2 s
[coordinator] Task completed successfully on node 06b9a112
```
Epochs 4–59 were not printed; `pipedpeer attach <id>` on pc1 printed them all,
through `training done in 18.3s` and `TOTAL 19.5s`
(docs-DEMO-806-senderdeath-gpu-attach.txt). FX-CLI2 makes the collect print the rest of the
output too. V4 printed from `epoch 2`, not `epoch 4`, because epochs 0–1 had already
shown live before the connection dropped (<!-- V4 c567369: docs-VERIFY-1071-reconnect-collect-pc1.txt -->):
```
epoch 1: 30 steps, loss 0.1147

lost the connection to 192.168.0.200:38080 (connection lost: …); checking whether the node or this machine went away...
the job ended on the node (done); collecting it
      No files changed on the remote
epoch 2: 30 steps, loss 0.0592
…
epoch 59: 30 steps, loss 0.0000
training done in 19.1s
final loss: 2.151369881175924e-05
TOTAL 20.3s

=== Summary ===
```

**27. A node killed mid-job.** Run a `--remote` job from pc3 that lands on pc4,
then `scripts/lab-lan.sh x pc4 pipedpeer stop` mid-run. Pass if
(<!-- V2 31b5364: 67-node-killed-reschedule.txt -->):
```
lost the connection to 10.88.0.14:38080 (connection lost: websocket: close 1006 (abnormal closure): unexpected EOF); checking whether the node or this machine went away...
…
[coordinator] Execution failed on 28197ccf: … — retrying in 2s
[coordinator] Rescheduling (attempt 2)...
[coordinator] Placed on node 06b9a112 (lease=9d0f6157)
…
distinct words: 133  total words: 400000000
…
  total        32.7 s
[coordinator] Task completed successfully on node 06b9a112
```

**Still not run in any rehearsal:** the friend's real 2-GPU ring to the end
(check 7c). V3 ran `prep-0-install.sh`, `setup-machine.sh`, `prep-1-check.sh`,
`prep-2-rehearse.sh` and `prep-3-gpu.sh` (see SETUP.md), `pipedpeer dashboard`
(check 5) and Ctrl+Z on `07_everything.py` (DEMO-SCRIPT.md backup B1).

**V1 failed these; V2 passed them:**
- `02_numpy_heavy.py` was killed at the 600M default memory cap. In V2 it ran
  in 37.2 s (70-setup-firstpass.txt).
- A sender that lost its network got its job run twice. In V2 it detached and
  collected the result once (check 26).

## Not re-checked on real machines yet

These were built after the last full re-check. Unit tests cover them, but no
real run has. The demo does not depend on them; run them after the demo, or
during your own run with the friend's laptop.

1. A slow helper machine: its piece is raced on the sender, the first result
   wins and the other is stopped (8541355). Slow one PC's link with
   `docker exec pp-lan-pcN tc qdisc add dev eth0 root netem delay 400ms rate 1mbit`.
2. A machine that refuses the environment is skipped with
   `fix: run pipedpeer trust on <node>`, and the job lands on another (1e07d56).
3. `--remote` with every machine busy prints a line every 30 s and places the
   job when one frees up, within 10 min (1e07d56).
4. A script killed by a signal on every machine fails after 3 attempts
   (1e07d56).
5. A machine rejoining two-GPU training, with a script that prints `loss`
   after its loop: no `NameError`, catch-up lines start with
   `[rank N catch-up, not trained]` (82596fe).
6. `pipedpeer start --max-concurrent 2`, then `pipedpeer auth set <token>`:
   `curl -s 127.0.0.1:38080/health` still shows `"max_concurrent":2` (76a0c3b).
7. Two-GPU runs (`--ddp 2`) print one `on <node>` usage line per machine
   (98e8684), and the start barrier names who it waits for (9b47502).
8. `--offload` across two physical machines: the spread line shows
   `0 on <this machine>` (ef68f02). Partly checked in V5 on pc2
   (<!-- V5 ef68f02: docs-new-offload-pc2-LAB-ARTIFACT.txt -->): the daemon
   sent every part to peers (pc2 is in no `remote ok` list), but the spread
   line said `28 on pc2` and the script printed `local 14 items`. The
   containers share the laptop's kernel; the sandboxed script read the
   laptop's machine id, not pc2's (52-item12-sandbox-bootid-pc2.txt), so it
   counted itself as a peer. Still to check on two physical machines.
9. The real two-laptop GPU run with the friend's laptop.

Known gaps found in V5, not fixed yet:

10. A rescheduled job with no reachable node retries every 30 s with no
    limit (`No node has capacity (no node reachable) — task queued`, then
    `Queued — retrying in 30s (attempt N)...`), and resumes when the node
    returns. V5 killed pc2 under a running job; the CLI was still queueing
    ~5 min later, and placed the rerun on pc2 once it was started again
    (11-item1-kill-node-pc1.txt, 11-item1-kill-node-drill.txt).
11. Ctrl+C during `[4/7] Building locally...` of a `--remote` run is not acted
    on until the build ends (21-item2-ctrlc-pack-pc3.txt). Ctrl+C during the
    upload stops at once (`Upload stopped: cancelled`, 22-item2-ctrlc-pack-pc2.txt).

## Afterwards

```bash
PIPEDPEER=$PWD/bin/pipedpeer scripts/lab-lan.sh down
```
Pass if it ends with this line (<!-- V1 8e14b9b: 99-lab-down.txt -->):
```
this laptop is still joined to 10.88.0.1; at the venue: pipedpeer join <real rendezvous IP>
```
Stop `pipedpeer rendezvous` with Ctrl+C.
