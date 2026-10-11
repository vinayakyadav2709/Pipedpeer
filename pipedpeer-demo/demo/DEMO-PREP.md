# Demo prep — what to run, in order

The commands only. [DEMO-SCRIPT.md](../DEMO-SCRIPT.md) is the full run sheet
(what to say, what to point at, what to do when something breaks), and
[DEMO.md](../DEMO.md) is the reference behind it.

Every command below runs from `pipedpeer-demo/` unless it says otherwise.

| Machine | Role |
|---|---|
| **box-a** | your laptop: every demo command is typed here |
| **box-b**, **box-c** | workers (ideally with an NVIDIA GPU) |
| **introducer** | any machine with a public IP; an address book only |

---

## Before the demo (do it a day ahead)

**1. On box-a, build the binary and copy it to the other two machines:**

```bash
cd ~/Projects/Pipedpeer
make build
scp bin/pipedpeer user@box-b:/tmp/pipedpeer
scp bin/pipedpeer user@box-c:/tmp/pipedpeer
```

The demo needs fixes the nightly build does not have yet, so every machine runs
this build, not the nightly.

**2. On every machine, install it** (each machine needs a copy of `pipedpeer-demo/`):

```bash
./demo/prep-0-install.sh ../bin/pipedpeer     # on box-a
./demo/prep-0-install.sh /tmp/pipedpeer       # on box-b and box-c
```

It prints a `sha256` at the end. It must be the same on all three machines.

**3. On the introducer:**

```bash
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --channel nightly
nohup pipedpeer rendezvous > rendezvous.log 2>&1 &
```

Open UDP ports 38443–38446 in its cloud firewall.

**4. Join the machines into one cluster:**

```bash
./join-machine.sh <introducer-ip>             # box-a first; it prints a token
./join-machine.sh <introducer-ip> <token>     # then box-b, then box-c
```

**5. On box-a, check and rehearse:**

```bash
./demo/prep-1-check.sh        # must end with READY
./demo/prep-2-rehearse.sh     # 15-25 min; saves timings to demo/rehearsal-results.txt
```

Read `demo/rehearsal-results.txt` afterwards. Any step less than about 1.2×
faster on your real networks comes out of the live demo. The capstone row
(step 10) is there for the time it takes, not to win: keep the step either
way, and quote the number honestly.

If pandas is slow over your links, rehearse again with a smaller file:

```bash
rm 03_pandas_ooc/data.csv
DEMO_CSV_ROWS=3000000 ./demo/prep-2-rehearse.sh    # ~330 MB instead of 1.1 GB
```

---

## On the day

**About 30 minutes before:** on each machine, `pipedpeer status` (and
`pipedpeer start` if it isn't running; it rejoins the cluster on its own).
Then on box-a:

```bash
./demo/prep-1-check.sh
```

**Screens:** the demo terminal on the projector; the live page in a browser on
the second screen (`pipedpeer ui` opens it; step 1 prints the address); and
`pipedpeer dashboard` (plus `watch -n1 nvidia-smi` for step 7) on box-b and
box-c.

**The demo**, all on box-a. Each script pauses at `[enter to continue]` so you
can talk; the yellow `>` lines are notes for you.

| Step | Run | What it shows |
|---|---|---|
| 1 | `./demo/1-cluster.sh` | 3 machines, 3 networks, connected directly |
| 2 | `./demo/2-headline.sh` | The same script on one machine vs the cluster |
| 3 | `./demo/3-lambda.sh` | A script stock Python can't run, working under pipedpeer |
| 4 | `./demo/4-sklearn.sh` | scikit-learn across the cluster *(optional)* |
| 5 | `./demo/5-numpy.sh` | NumPy: the cost model deciding what to ship *(optional)* |
| 6 | `./demo/6-pandas.sh` | pandas reading a file in chunks across the machines |
| 7 | `./demo/7-gpu.sh` | GPU training *(skips itself with no GPU machine)* |
| 8 | `./demo/8-files.sh` | Files the job changed coming back to the laptop |
| 9 | `./demo/9-failover.sh` | Stopping a machine mid-job, still the right answer |
| 10 | `./demo/10-capstone.sh` | One plain script: pandas, NumPy, scikit-learn, Pool and files together |
| 11 | `./demo/11-proof.sh` | The last run's timeline and receipt, then the other machines' own record |

- **Step 9:** when the `remote ok` lines start, run `pipedpeer stop` on box-c.
  Afterwards, `pipedpeer start` on box-c.
- **Step 11:** also run `pipedpeer traffic` on box-b and box-c.
- **Short on time:** drop 4 and 5 first, then 7.
- **Step 8 says "no eligible node found":** read the "nodes turned down" lines it
  prints; it has already waited 15 s.

**Afterwards:** `pipedpeer stop` on each machine.
