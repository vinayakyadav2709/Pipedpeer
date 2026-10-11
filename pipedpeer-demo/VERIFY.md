# Check it yourself

These are the commands Claude ran in the rehearsal, in the same order. Each
check has the expected output, copied from that run. Run them and compare what
you see. Nothing is listed here that Claude did not actually run.

- `<!-- V1 8e14b9b: <file> -->`: output from rehearsal pass 1. The full
  output is in `~/Downloads/pipedpeer-work/evidence/v1/<file>`.
- `<!-- refresh from V2 -->`: the check changed after V1, or V1 did not run it.
  The expected output stays a placeholder until rehearsal pass 2 fills it in.
  Run the check anyway and note what you see.
- **Pass if** lists the lines that must appear exactly. Times, ids, hashes and
  CPU percentages vary from run to run. Names like `yeet` (this laptop) and
  pc1–pc4 (the containers) are what the lab uses.

**Where this runs:** this laptop plus four containers on one docker network
(`scripts/lab-lan.sh`). The laptop plays LAPTOP-A at 10.88.0.1, with the GPU
and the rendezvous. pc1–pc3 are single-user Nix; pc4 stands in for Ubuntu
(multi-user Nix, user `pp`).

**What this does not prove:** speedup (the containers share this laptop's CPU),
two real GPUs, real Wi-Fi, or the college firewall and Ubuntu's AppArmor rules.
The lab visit covers those.

**Before you start:**
- Have at least 4 GB of memory free (`free -g`).
- Make sure no other lab is up (`docker ps`).
- Plan about 1.5 hours. The CUDA build in check 7 takes about 21 minutes and
  runs in the background.

---

## Bring the lab up

**1. Build and check the code** (repo root):
```bash
cd src && go build -o ../bin/pipedpeer . && go vet ./... && gofmt -l . ; cd ..
```
Pass if: no output from vet, and none from gofmt. <!-- V1 8e14b9b: 00-static.txt (VET-OK, gofmt empty) -->
The full test run is `cd src && go test -count=1 -p 2 ./...`. V1 had 36
packages `ok` and `internal/daemonapi` FAIL in two memory tests, which FX-SMALL
fixed (01-gotest.txt). <!-- refresh from V2: expect every package ok -->

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
Pass if (<!-- V1 8e14b9b: 20-join-pc3.txt -->, 5.2 s):
```
  waiting for peers · 90 s left · 0 found
connected (4 peer(s))
a88718a2*  pc3   healthy  0     16c 37%  9.1 GiB   -                     self  self
```
The events terminal shows a `JOIN … is up (10.88.0.13:38080)` line for pc3.
<!-- refresh from V2: FX-EVENTS 5a5e5a5 names every peer; V1 showed `06b9a112-9739-…` as a raw uuid and printed `("no)` in NOPATH lines -->
`pipedpeer dashboard` gets a pc3 row. <!-- refresh from V2: not captured in V1 -->

**6. The cluster (step 2).** On the laptop: `PAUSE=0 ./demo/1-cluster.sh`.
Pass if pc1–pc4 and `yeet` are all `healthy`, and the `yeet*` row shows the GPU
(<!-- V1 8e14b9b: 16-1-cluster.txt -->):
```
06b9a112*  yeet           healthy      0     15c 13%  10 GiB    RTX 3060 0% 41M/6.0G  self  self
```
<!-- refresh from V2: FX-NODES aca41d1. V1 also listed ten stale `manual-1 127.0.0.1 unreachable` rows; there should be none now. The script now ends with `pipedpeer ui --print` (s2) -->

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
Pass if (<!-- V1 8e14b9b: 13-gpu-step.txt -->):
```
[coordinator] --remote: discovering a node other than this machine...
using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
  ran on       yeet (192.168.0.200)
[coordinator] Task completed successfully on node 06b9a112
```
<!-- refresh from V2: V1 took 264.8 s, almost all of it spent broadcasting the CUDA closure to CPU-only pcs, which ran out of memory (`Warning: peer … did not take this environment`). FX-NAR 29acd9b fixed that, so the warnings should be gone and the total close to the script's 4.5 s. 04_torch_ddp.py now uses a DataLoader (DDP-UP). -->
The two-GPU `--ddp 2 --gpu force` run is in V2's GPU lab and on the friend's
laptop. <!-- refresh from V2 -->

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
lines are normal on pc4's multi-user Nix. <!-- refresh from V2: FX-CLI touched check-only; the lines above should not change -->

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
on each pc (`tc qdisc … netem delay 4ms 3ms loss 0.5%`, 80-netem.txt). Without
it the cold half is faster. <!-- refresh from V2: record the exact netem command -->

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
<!-- refresh from V2: V1's check-only ALSO said `not packaged humanize (add --pkg)`; FX-CLI 1ef1058 makes check-only ask nixpkgs, so that line should be gone -->
Now delete the `import notarealpkg_xyz` line and run `pipedpeer run teacher.py`
again. Pass if (<!-- V1 8e14b9b: 42-teacher-humanize-run.txt -->, 1.4 s):
```
332,833,500
  spread       1476 items: 1000 on yeet, 476 on 4 others (pc3 137, pc4 124, pc1 109, pc2 106)
```
<!-- refresh from V2: spread counts (FX-CLI) -->

**11. A run on the laptop that spreads (steps 4 and 10).** On the laptop:
`pipedpeer run 00_pool.py`. Pass if (<!-- V1 8e14b9b: 30-host-00pool-first.txt -->):
```
[pipedpeer] remote ok: 12 items in 6504 ms on 10.88.0.11:38080, 10.88.0.12:38080, 10.88.0.13:38080, 10.88.0.14:38080
checksum 4319999744
  ran on       yeet (127.0.0.1)
  closure      already on the node
```
The summary's `spread` line names pc1–pc4. <!-- refresh from V2: V1 said 120 items for a 96-item job (FX-CLI fixed the count) -->
The full headline, `PAUSE=0 ./demo/2-headline.sh` (python3 against
pipedpeer), was not run in V1. <!-- refresh from V2 -->

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
(<!-- V1 8e14b9b: 31-ctrlc.txt, 31-ctrlc-poolwatch.txt -->):
```
  total        cancelled after 10.0 s
cancelled — remote work stopped
```
and in poolwatch, every pc's `ex=` stops growing within about 2 s (V1:
`pc1 ex=18 can=1 …` stayed there from the ^C onward).
<!-- refresh from V2: V1 printed a ~45-line Python traceback (ConnectionRefusedError) after ^C; FX-CLI 1ef1058 should leave only the lines above -->

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
<!-- refresh from V2: FETCH 7f20314 now syncs a finished background job's files without attach; check `files back` and that a second attach doesn't apply them again -->

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
<!-- refresh from V2: s2's version (job --last --timeline / --receipt) not run in V1 -->
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
<!-- refresh from V2: record the exact nft commands that add and remove the drill rule, so this check can be repeated -->

## Not run in V1 (V2 will add expected output)

Each of these is marked `<!-- refresh from V2 -->`:
- the headline (`2-headline.sh`).
- pandas (`6-pandas.sh`).
- the teacher script through `11-teacher.sh`.
- `12-everything.sh` and `07_everything.py`.
- `13-finale.sh`, `14-capstone.sh`, and the `students/` scripts.
- the `prep-2-rehearse.sh` table and `prep-1-check.sh`.
- `pipedpeer dashboard`, `/ui` and `pipedpeer stats`.
- `--ddp 2`, plus DDP-DOWN and DDP-UP on two GPUs.
- the sender losing its network (step 11b).
- the integration tests.
- the PKG-PREP before/after on a fresh pc. V1 measured 54 s → 4 s, but the
  common build then failed its memory check in the 1.2 GB containers
  (15-pc3-pkgprep-before-after.txt).

**V1 failed these; they were fixed since, and V2 checks them again:**
- `02_numpy_heavy.py` was killed at the 600M default memory cap, and its SVD
  offload hung for over 15 minutes (17-, 18-*.txt).
- A sender that lost its network got its job run twice
  (90-senderdeath-*.txt).

## Afterwards

```bash
PIPEDPEER=$PWD/bin/pipedpeer scripts/lab-lan.sh down
```
Pass if it ends with this line (<!-- V1 8e14b9b: 99-lab-down.txt -->):
```
this laptop is still joined to 10.88.0.1; at the venue: pipedpeer join <real rendezvous IP>
```
Stop `pipedpeer rendezvous` with Ctrl+C.
