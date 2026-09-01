# Pipedpeer demo — three machines, three networks

Three ordinary machines, each behind its own router, on three unrelated
networks. No VPN, no port forwarding, no cluster manager. You run plain Python
scripts on one of them and the work spreads across all three.

Two of the machines have an NVIDIA GPU; one does not. That is deliberate — the
scheduler treats the GPU as a hard filter, so you can watch a training job land
only on the machines that can run it while CPU work uses all three.

Everything below is copy-pasteable. Commands take no flags unless the flag *is*
the point.

---

## What you need

| Role | What it is | Needs a GPU? |
|---|---|---|
| **introducer** | Any machine with a public IP. A free-tier VM is plenty — it does not touch your data or carry your traffic. | no |
| **box-a** | Your "laptop": where you type the commands | no |
| **box-b** | A worker | yes |
| **box-c** | A worker | yes |

The three boxes need Linux to *run* jobs (namespaces and a `crun` sandbox). They
do **not** need to reach each other by hostname, be on a VPN, or have any port
open. They punch a hole out to each other through their own routers.

**These instructions assume the three boxes start with nothing installed** — no
pipedpeer, no Nix, no identity, no caches. `pipedpeer setup` installs what it
needs. If a machine has been used for a previous run, reset it first
(see [Resetting a machine](#resetting-a-machine-to-a-clean-state) at the end),
or the first job will be much faster than a real cold start and the demo will
overstate itself.

> The introducer is only an address book. It tells two machines where to find
> each other and then stops being involved — the data path is machine to
> machine. If it goes down after everyone has met, running jobs carry on.

---

## Part 0 — the introducer (once, ~1 minute)

On the public machine:

```bash
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --channel nightly
pipedpeer rendezvous
```

That's it. It listens on UDP **38445** and prints each machine as it checks in.
Leave it running in a terminal (or `nohup pipedpeer rendezvous &`).

**Open UDP 38445 to the internet** in your cloud firewall. On GCP:

```bash
gcloud compute firewall-rules create pipedpeer-introducer \
  --allow udp:38443-38446 --source-ranges 0.0.0.0/0
```

Note the machine's public IP — every other machine needs it, and nothing else.

---

## Part 1 — install, on each of the three boxes

Identical on all three:

```bash
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --channel nightly
pipedpeer setup -y
```

`setup` checks for what it needs, installs what is missing (Nix, `crun`), makes
this machine's identity, opens the one UDP port in the host firewall if there is
one, and starts the daemon. It prints what it changed and how to undo it.

The `-y` is "don't ask me before installing". Drop it if you'd rather approve
each step.

---

## Part 2 — one cluster, one command per machine

A cluster is defined by a shared secret. Machines with different secrets never
see each other, even through the same introducer.

**On box-a**, make one and read it back:

```bash
pipedpeer auth set
pipedpeer auth show
```

**On box-b and box-c**, use that same value:

```bash
pipedpeer auth set <the-token-from-box-a>
```

Then, **on all three**, join — replacing the IP with your introducer's:

```bash
pipedpeer join 35.234.222.177
```

That is the whole join. It remembers the address, restarts the daemon into the
cluster, waits for the others to appear, and prints the node table. Afterwards a
plain `pipedpeer start` rejoins the same cluster with no arguments — including
after a reboot.

Run it on box-a first; it will say "no peers yet", which is correct for the
first machine. As b and c join, they find each other.

### Confirm it worked

On any machine:

```bash
pipedpeer nodes
```

```
NODE_ID    HOST         STATE      JOBS   CORES  MEM AVAIL  GPU            PATH       SOURCE
62d15a1f   box-a        healthy    0      8      12 GiB     -              self       self
92039b77   box-b        healthy    0      16     6.7 GiB    NVIDIA GeFo... punched    manual
b5969c13   box-c        healthy    0      16     14 GiB     NVIDIA RTX ... punched-in manual
```

What to check:

- **three rows**, all `healthy`
- **GPU** column: the card on box-b and box-c, `-` on box-a
- **PATH** column: `punched` or `punched-in`. That is a direct machine-to-machine
  connection, made by hole-punching through both routers. `lan` means they found
  each other locally. A `-` means no path — see Troubleshooting.

---

## Part 3 — what to watch during a demo

Put these on screen. One terminal each, ideally one machine each.

| Where | Command | What it shows |
|---|---|---|
| box-a | `pipedpeer dashboard` | live view: every node, its load, and recent tasks. The one to project. |
| box-b, box-c | `pipedpeer dashboard` | the same cluster seen from a worker — proves it is peer-to-peer, not a hub |
| any | `pipedpeer tasks` | what is running right now and which machine has it |
| any | `pipedpeer traffic` | the pool ledger: each batch of work received, from whom, how many items |
| any | `pipedpeer nodes` | membership and the route to each peer |
| box-b, box-c | `watch -n1 nvidia-smi` | the GPUs lighting up during the training demo |
| box-a | `htop` | box-a's own cores — mostly idle while the cluster works |

`traffic` is the honest one. It is the receiving daemon's own record of work it
executed for somebody else, so it cannot be faked by the machine making the
claim.

---

## Part 4 — the demos

All of these run **from box-a**, in this directory:

```bash
cd pipedpeer-demo
```

Nothing in these scripts mentions pipedpeer. They are ordinary Python, and they
run under plain `python3` exactly as they always did.

### 4.1 The headline: same script, one machine vs three

```bash
./compare.sh 00_pool.py
```

This runs `00_pool.py` twice — first with plain `python3` (this machine's cores
only), then with `pipedpeer run` (the cluster) — and prints both times and the
speedup. Both numbers come from the script's own output, not from pipedpeer.

While the second half runs, watch `pipedpeer tasks` on another screen.
Afterwards, `pipedpeer traffic` on box-b and box-c shows the batches they ran.

**What the audience should take away:** the file was not edited between the two
runs.

> `00_pool.py` imports only the standard library, which is why it is the one to
> lead with: the local baseline runs anywhere, with nothing installed. The
> library demos below (sklearn, pandas, torch) need those packages installed on
> box-a for the *local* half to run at all — `compare.sh` says so and names the
> package if they are missing. The cluster half never needs them: it builds the
> environment from the script's imports. That gap is worth pointing at.

### 4.2 The one stock Python refuses

```bash
python3 06_pool_lambda.py        # fails
pipedpeer run 06_pool_lambda.py  # works
```

The first raises `PicklingError: Can't pickle <function <lambda>>`.
`multiprocessing` sends a function to a worker by *name*, and a lambda has none
— so stock Python cannot run this at all, on any number of cores.

Under pipedpeer it runs, on every machine in the cluster: the function is sent
by value instead. This is the case people hit, rewrite around, and assume is
their fault.

> The local half of this one runs on threads rather than processes, because
> nothing can hand a lambda to a worker process. So the *speed* is not the point
> here — that it runs at all is.

### 4.3 scikit-learn, untouched

```bash
./compare.sh 01_sklearn_rf.py
```

A `RandomForestClassifier(n_jobs=-1)`. `n_jobs=-1` normally means "all cores on
this machine"; here it means all cores in the cluster, without the script
knowing.

### 4.4 NumPy

```bash
./compare.sh 02_numpy_heavy.py
```

A 4096×4096 `matmul` and an `svd`. Worth showing because the cost model
sometimes decides to keep the matmul local — local BLAS is very fast — and ships
the SVD. The interesting claim is not "everything is distributed", it is "the
choice is never slower than doing it here".

### 4.5 pandas, bigger than memory

```bash
cd 03_pandas_ooc
python3 -c "
import numpy as np, pandas as pd
n = 20_000_000
pd.DataFrame({'cat': np.random.randint(0, 50, n), 'a': np.random.rand(n), 'b': np.random.rand(n)}).to_csv('data.csv', index=False)
"          # ~1 GB, once
cd ..
pipedpeer run 03_pandas_ooc/03_pandas_ooc.py
```

`read_csv` in chunks across the cluster, then `groupby().mean()` as a hash
shuffle: each machine reduces its own share of the keys and the partials are
combined exactly at the end.

The first run uploads the CSV; do it once before the audience arrives.

### 4.6 GPU training on the two GPU boxes

```bash
pipedpeer run 04_torch_ddp.py --ddp 2
```

A plain PyTorch training loop — no `DistributedDataParallel`, no `torchrun`, no
`init_process_group` in the file. `--ddp 2` places two ranks; the shim wraps the
model, sets up the process group and synchronises gradients.

Watch `nvidia-smi` on box-b and box-c: both light up. Box-a has no GPU and is
never given a rank — the scheduler filters on it rather than trying and failing.

This is the one place a flag is unavoidable: "how many ranks" is a decision
about your training run, not something to guess.

### 4.7 Files come back

```bash
pipedpeer run 05_file_sync.py
cat created_by_job.txt
```

The job runs on another machine, creates a file, modifies another and deletes a
third. All three changes appear in this directory when it finishes.

### 4.8 Kill a machine mid-job

Start `./compare.sh 00_pool.py` and, while the cluster half is running, on box-c:

```bash
pipedpeer stop
```

The run finishes with the correct answer. Work that was out on box-c is re-run
by whoever is still up — the local side never stops pulling, so a lost worker
costs time, not correctness. `pipedpeer traffic` afterwards shows the batches
that landed elsewhere.

---

## Between two runs of the demo

Nothing needs re-joining — the cluster address is remembered, so a restart is
enough:

```bash
pipedpeer stop && pipedpeer start     # on each machine
```

The second run will be **much faster than the first**, because the environments
are built and the closures are already on every machine. That is honest and it
is what a real user experiences from their second job onward — but it is not a
cold start, so do not present it as one.

## Resetting a machine to a clean state

To rehearse the whole thing from scratch, including the install and the
environment builds. Everything below is derived data or was installed by
pipedpeer; none of it is your work.

```bash
pipedpeer stop 2>/dev/null

# pipedpeer itself: binary, identity, cluster secret, node database, caches
rm -f  ~/.local/bin/pipedpeer
chmod -R u+w ~/.local/share/pipedpeer ~/.local/state/pipedpeer ~/.cache/pipedpeer 2>/dev/null
rm -rf ~/.local/share/pipedpeer ~/.local/state/pipedpeer ~/.cache/pipedpeer
```

That alone leaves Nix and its store in place, so the next run reinstalls
pipedpeer but reuses every environment it has built before. For a **true** cold
start, remove Nix as well:

```bash
# Determinate installer (what `pipedpeer setup` uses)
sudo /nix/nix-installer uninstall --no-confirm
sudo rm -rf /etc/nix          # removes the cluster's trusted keys with it
```

`chmod -R u+w` first is not optional: Nix store paths are read-only by design,
and `rm -rf` stops on the first one.

**Leave `crun`, `tar`, `bash` and `curl` alone.** Those are ordinary system
tools; pipedpeer checks for `crun` and installs it only if it is missing, and
never touches the other three.

**Check your firewall before "cleaning" it.** On both machines here the high
ports were already open for unrelated reasons, so `pipedpeer setup` added no
rule at all — removing 38447/udp would have punched a hole in a range somebody
else opened, leaving pipedpeer's port specifically blocked while everything
around it stayed open. Look at `firewall-cmd --list-ports` before changing
anything.

---

## Troubleshooting

**`pipedpeer nodes` shows only this machine.**
Check the introducer's terminal — every machine that checks in prints there. If
a machine is missing, its `pipedpeer join` did not reach the introducer: the
cloud firewall probably does not allow UDP 38445 from anywhere.

**Machines appear, but `PATH` shows `-`.**
They found each other but could not open a direct connection. Run:

```bash
pipedpeer net-check
```

It reports what this machine's router does with outbound UDP. Two symmetric NATs
with no port mapping is the hard case; a phone hotspot is usually the culprit.

**A peer shows `unreachable` after working earlier.**
`pipedpeer nodes` on the peer itself — if the daemon is up there, the link
dropped and will be rebuilt on the next poll (about 20s). If it does not come
back, `pipedpeer stop && pipedpeer start` on that machine.

**The first job takes minutes.**
It is building the Nix environment for your script's imports and shipping it.
That happens once per unique set of imports; later runs reuse it. Rehearse each
script once before the audience.

**A job fails with "lacks a signature by a trusted key".**
The receiving machine is enforcing signatures and does not yet trust this
cluster's keys. Re-run `pipedpeer setup -y` there — it adds them.

**`pipedpeer run` says no eligible node for a GPU script.**
The GPU machines have not joined, or their driver is not visible. Check the GPU
column in `pipedpeer nodes`; a machine with a working driver advertises the card
by name.

---

## The claim, stated plainly

- The scripts in this directory are ordinary Python and were not modified.
- The three machines are on three different networks with nothing forwarded.
- The introducer sees who is in the cluster and nothing else — no job data, no
  results, no traffic.
- Every "it ran over there" claim is checkable in `pipedpeer traffic` on the
  machine that ran it, which is a different machine from the one making the
  claim.
