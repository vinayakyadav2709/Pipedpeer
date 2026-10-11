# Pipedpeer — mentor demo run sheet

The script for the live demo: what to prepare, what to type, what to say, and
what to do when something goes wrong. Every command on stage is one file in
[`demo/`](demo/); you type `./demo/2-headline.sh`, not the commands inside it.

[DEMO.md](DEMO.md) is the reference behind this — how each piece works and the
full troubleshooting list. This page is the order of events.
[demo/DEMO-PREP.md](demo/DEMO-PREP.md) is the short version: the commands only.

**Length:** about 20 minutes of demo, then questions.
**Machines:** three real machines on three different networks, plus the introducer.

---

## The machines

| Name here | What it is | Needs |
|---|---|---|
| **introducer** | Any machine with a public IP (a free cloud VM). An address book only — no data passes through it. | UDP 38443–38446 open to the internet |
| **box-a** | Your laptop. Every demo command is typed here. | This repo checked out; `python3` with numpy, pandas, scikit-learn (for the "this machine only" half of the comparisons) |
| **box-b** | Worker, ideally with an NVIDIA GPU | Linux |
| **box-c** | Worker, ideally with an NVIDIA GPU | Linux |

The steps adapt to what you have: with no GPU machine, step 7 skips itself;
with one, it trains on that one.

## Screen layout

| Screen | Shows | Command |
|---|---|---|
| Projector (box-a) | the demo terminal, large font | `cd pipedpeer-demo` |
| Second window on box-a, or a second projector | live cluster view | `pipedpeer dashboard` |
| box-b / box-c | the worker side — proves it is peer-to-peer | `pipedpeer dashboard`, and `watch -n1 nvidia-smi` for step 7 |

---

## Before the day

Do all of this at least a day ahead, on the real networks you will present from.

### 1. One binary, the same on every machine

The demo depends on three fixes that are on the `nightly-fixes` branch and not
yet in the nightly build:

- `a7bb9a5` — finished jobs were reported as failed and retried forever
- `7128212` — warm runs used fewer and fewer workers and got slower
- `049a1aa` — the pandas out-of-core read silently fell back to one machine

So build it yourself and copy it to every machine:

```bash
# on box-a, in the repo root
make build                     # -> bin/pipedpeer (linux/amd64)
scp bin/pipedpeer user@box-b:/tmp/pipedpeer
scp bin/pipedpeer user@box-c:/tmp/pipedpeer
```

(If the machines cannot reach each other over ssh, a USB stick or any file
transfer works — it is one file.)

Then **on each machine**, from a checkout or a copy of `pipedpeer-demo/`:

```bash
./demo/prep-0-install.sh /tmp/pipedpeer        # on box-a: ../bin/pipedpeer
```

It installs the binary to `~/.local/bin`, runs `pipedpeer setup -y` (Nix, crun,
identity, firewall, daemon) and prints a `sha256`. **The three hashes must
match.** A machine with a different binary is the most likely cause of a step
misbehaving on the day.

> Once these fixes are merged to `dev` and the nightly has rebuilt,
> `./demo/prep-0-install.sh` with no argument installs the nightly instead.

### 2. The introducer

On the public VM:

```bash
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --channel nightly
nohup pipedpeer rendezvous > rendezvous.log 2>&1 &
```

Open UDP 38443–38446 in the cloud firewall (see DEMO.md Part 0). Note the
public IP.

### 3. Join the three machines

```bash
./join-machine.sh <introducer-ip>                 # box-a, FIRST - prints the token
./join-machine.sh <introducer-ip> <token>         # box-b, then box-c
```

### 4. Check, then rehearse

On box-a:

```bash
./demo/prep-1-check.sh        # must end in READY
./demo/prep-2-rehearse.sh     # 15-25 min: warms everything, then times each step
```

`prep-2` writes `demo/rehearsal-results.txt`:

```
step                   this machine   cluster        speedup
00_pool.py             41.2s          18.9s          2.18x
01_sklearn_rf.py       ...
```

**Use it to choose the steps.** Whether three machines beat one depends on how
much data a step ships and how fast the links between your networks are. A step
that ships little (step 2, 00_pool) wins almost anywhere. Steps that ship a lot
(sklearn sends ~18 MB per tree; pandas sends the whole CSV in 64 MB chunks) may
not win over home internet. Any step under ~1.2× goes to the "if asked" list
below — do not present a slowdown as a feature.

If pandas is slow over your links, make the file smaller and rehearse again:

```bash
rm pipedpeer-demo/03_pandas_ooc/data.csv
DEMO_CSV_ROWS=3000000 ./demo/prep-2-rehearse.sh    # ~330 MB instead of 1.1 GB
```

---

## On the day

**T−30 min** — on every machine: `pipedpeer status` (start it with
`pipedpeer start` if needed; it rejoins with no arguments). Then on box-a:

```bash
./demo/prep-1-check.sh        # READY, same hashes as before
```

**T−10 min** — open the screens above. Run `./demo/1-cluster.sh` once so you
have seen the table you are about to show. Close anything that uses the
network heavily (cloud sync, updates).

**Be honest about warm vs cold.** Everything was built and shipped during the
rehearsal, so on stage nothing waits on a Nix build. That is what a user sees
from their second run onward — say so if asked, and use the numbers in
[If asked](#if-asked) for the first-run cost.

---

## The demo

Each step pauses at `[enter to continue]` so you can talk. Yellow `>` lines are
notes for you, on screen; read them or paraphrase them.

| # | Run | Time | Optional? |
|---|---|---|---|
| 0 | (talk) | 1 min | |
| 1 | `./demo/1-cluster.sh` | 2 min | |
| 2 | `./demo/2-headline.sh` | 3 min | |
| 3 | `./demo/3-lambda.sh` | 2 min | |
| 4 | `./demo/4-sklearn.sh` | 4 min | only if it won in rehearsal |
| 5 | `./demo/5-numpy.sh` | 2 min | only if it won in rehearsal |
| 6 | `./demo/6-pandas.sh` | 2 min | |
| 7 | `./demo/7-gpu.sh` | 2 min | needs a GPU machine |
| 8 | `./demo/8-files.sh` | 1 min | |
| 9 | `./demo/9-failover.sh` | 2 min | |
| 10 | `./demo/10-proof.sh` | 1 min | |

Running short on time: drop 4 and 5 first, then 7. Steps 2, 3, 9 and 10 carry
the argument.

### 0 — The problem (talk, no commands)

> "Most of us have more than one computer — a laptop, a desktop, a lab
> machine — and when a script is slow it uses exactly one of them. The tools
> that fix that (Ray, Dask, Spark, Kubernetes) want you to rewrite your code
> for them, put the machines on one network, and run a cluster manager.
>
> Pipedpeer takes an ordinary Python script, unmodified, and runs it across
> whatever machines you have — on different networks, behind ordinary home
> routers, with no VPN and no port forwarding."

### 1 — The cluster · `./demo/1-cluster.sh`

**Show:** three rows, all `healthy`; `PATH` = `punched` / `punched-in`; the GPU column.

> "These are three machines on three different networks. `punched` means they
> made a direct connection through both routers — hole-punching, the way video
> calls do. The introducer only told them about each other; none of our
> traffic goes through it."

**If a row is missing or `PATH` is `-`:** carry on with the machines you have,
and say so. Fix later with `pipedpeer net-check` (DEMO.md Troubleshooting).

### 2 — The headline · `./demo/2-headline.sh`

**Show:** the file (plain `multiprocessing.Pool`), then two timings and the speedup.

> "This is ordinary Python — `multiprocessing.Pool`, nothing about pipedpeer in
> it. First with `python3`, this machine only. Now the same file with
> `pipedpeer run`." *(point at the dashboard as the other machines light up)*
> "Same file, not edited between the runs, and both times are printed by the
> script itself, not by us."

**If the cluster half is not faster:** check `pipedpeer nodes` — a peer has
probably dropped. Say what the table shows.

### 3 — What stock Python can't do · `./demo/3-lambda.sh`

**Show:** `python3` fails with a pickling error; `pipedpeer run` succeeds.

> "`multiprocessing` can't send a lambda to another process — it sends
> functions by name, and a lambda has none. People hit this and rewrite their
> code. Pipedpeer sends the function itself, so it runs — on all three
> machines. The point here is that it runs at all, not the speed."

### 4 — scikit-learn · `./demo/4-sklearn.sh` *(optional)*

> "`n_jobs=-1` normally means every core on this machine. Here it means every
> core in the cluster, and the script doesn't know. Each tree is a task the
> cluster schedules."

It is the longest step. Talk about how it works while it runs (next section).

### 5 — NumPy · `./demo/5-numpy.sh` *(optional)*

**Show:** the `matmul decision` line.

> "Not everything should be shipped. A matrix multiply on local BLAS is very
> fast, so the cost model may keep it here and send only the SVD. The promise
> is not 'everything is distributed' — it's 'never slower than doing it here'."

### 6 — pandas, bigger than one machine · `./demo/6-pandas.sh`

**Show:** `streaming … MB out-of-core`, `combining N chunk partials from the cluster`, the result table.

> "Plain `pd.read_csv` and `groupby().mean()`. The file is treated as too big
> for one machine — on a small laptop it would be. Pipedpeer streams it in
> 64 MB chunks; each machine parses its chunks and reduces them to per-group
> partial sums, and only those tiny partials come back. No machine ever holds
> the whole file, and the means are exact, not approximate."

**If it prints the red "fell back" line:** a machine has an old binary (check
the hashes). The answer is still correct — say it ran on one machine this time.

### 7 — GPU training · `./demo/7-gpu.sh`

**Show:** `nvidia-smi` on the GPU machines lighting up; no `DistributedDataParallel` in the file.

> "A plain PyTorch training loop — no `DistributedDataParallel`, no `torchrun`.
> `--ddp 2` asks for two ranks; pipedpeer places them on the two GPU machines,
> sets up the process group and synchronises gradients. The laptop has no GPU,
> so it is never given a rank — filtered, not tried and failed."

That flag is the one place you have to say something: how many ranks is a
decision about your training run, not something to guess.

### 8 — Files come back · `./demo/8-files.sh`

**Show:** before/after listing — one file created, one changed, one deleted.

> "That job ran on another machine. It created a file, changed one and deleted
> one, and all three changes are here now. Deletion is limited to files the job
> was sent, so nothing else on this machine can be touched."

**If it says "no eligible node found":** it has already waited 15 s for a peer
and printed why each node was turned down. Read those lines: usually a peer is
still busy, or not reporting healthy.

### 9 — Kill a machine · `./demo/9-failover.sh`

**Do:** when the `remote ok` lines start, on box-c: `pipedpeer stop`.

**Show:** the same `checksum 4319999744` as step 2.

> "I've just stopped one of the machines in the middle of the job. Same answer.
> Work it was holding is redone by the machines still up — losing a worker costs
> time, never correctness."

**Afterwards, on box-c:** `pipedpeer start` — it rejoins on its own.

### 10 — The receipts · `./demo/10-proof.sh`

**Show:** `pipedpeer traffic` **on box-b and box-c**.

> "Every claim that work ran somewhere else can be checked on the machine that
> ran it. This is box-b's own ledger of work it did for box-a — the laptop
> can't fake it."

### Close

> "Unmodified scripts, three machines on three networks, nothing forwarded, and
> every claim checkable on the machine that did the work."

---

## While a long step runs: how it works

Use these while step 4 or the cluster half of step 2 is running.

- **Environment from imports.** It reads the script's imports and builds an
  exact environment with Nix. The same imports always give the same build, so
  it is built once, cached, and shipped as a content-addressed closure — the
  other machines need nothing pre-installed.
- **Interception, not rewriting.** A small shim, loaded before the script,
  replaces `multiprocessing.Pool`, joblib's backend (scikit-learn),
  some NumPy/pandas calls and the PyTorch optimizer step with versions that
  can use the cluster. The script is never edited.
- **Scheduling.** Each machine reports cores, free memory and GPUs. A cost
  model splits work by measured speed and keeps work local when shipping it
  would be slower.
- **Connectivity.** The introducer swaps public addresses, then both sides send
  UDP at each other at once to open both routers (hole punching). After that it
  is a direct link between the two machines; the introducer can go away.
- **Isolation.** Where `crun` is installed (`pipedpeer setup` installs it), jobs
  run in a sandbox; without it they run unisolated, and the log says so.
- **Trust.** A shared cluster secret; closures are signed, and a machine only
  accepts closures signed by a key it trusts.

---

## If asked

### "What does the first run cost?"

Measured on the docker bench rig (4 containers on one 12-core host, 3 cores
each), first run on fresh machines vs later runs, median of 5:

| Script | First run | Later runs | Faster by | Of which: the script itself |
|---|---|---|---|---|
| 00_pool | 72 s | 35 s | 2.1× | ~24 s both |
| 01_sklearn_rf | 294 s | 233 s | 1.3× | ~190–220 s both |
| 02_numpy_heavy | 118 s | 68 s | 1.7× | ~56 s both |
| 03_pandas_ooc | 85 s | 24 s | 3.5× | ~12 s both |
| 06_pool_lambda | 67 s | 16 s | 4.1× | ~4 s both |

The cache removes 35–90 s of setup (building and shipping the environment) and
leaves the computation unchanged, so short jobs gain most. Every run still has
~10–14 s of fixed overhead outside the script — the next thing to optimise.

Caveats to state with it: one host, so the "network" is a local bridge; all
containers shared one Nix store, which makes first runs look cheaper than they
are on separate machines; the pandas rows were measured before the
out-of-core fix, so they are the single-machine fallback.

### "Isn't this just Ray / Dask?"

Those are excellent if you write for them. Pipedpeer's bet is the opposite end:
the code stays as it is, and the machines can be anywhere — different networks,
no shared filesystem, no cluster manager.

### Honest limitations

- Speedups depend on the links. Work that ships a lot of data per unit of
  compute can be slower across home internet than on one machine; that is why
  the cost model exists, and why some steps are optional.
- Two symmetric NATs with no port mapping (often a phone hotspot) cannot be
  punched through.
- Running jobs needs Linux (namespaces + crun) on the machines doing the work.

---

## Something went wrong

| Symptom | Do this |
|---|---|
| A step prints a traceback | Say what it says, move to the next step. Every step stands alone. |
| `pipedpeer nodes` lost a machine | On that machine: `pipedpeer stop && pipedpeer start`. Continue meanwhile. |
| A step hangs over a minute past its rehearsal time | Ctrl+C, then `pipedpeer tasks` to see where it is; move on. |
| `retrying in …` repeating | Ctrl+C. A machine has an old binary — check the hashes after the demo. |
| The network dies entirely | Run step 2's local half (`python3 00_pool.py`) and step 3's failure, then talk through the rest from this sheet. |

## After the demo

```bash
pipedpeer stop     # on each machine
```

The cluster is remembered: `pipedpeer start` later rejoins with no arguments.
To start again from truly clean machines, see "Resetting a machine" in DEMO.md.
