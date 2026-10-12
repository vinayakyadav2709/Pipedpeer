# Measurements recorded in commit messages (2026-08-26 to 2026-08-29)

Between the previous documentation pass (c44e486, 2026-08-25) and the current
tree, the multi-machine measurements were taken by hand and recorded in the
commit that the measurement justified. No results file was committed for them.
This file collects them so that every number the report quotes from that period
can be traced. Each excerpt below is copied verbatim from the commit message
(`git show -s <hash>`), and nothing has been re-measured or rounded here.

The machines are named only as the messages themselves describe them:
- "this machine", "this node" and "this project's own laptop" are the
  development machine the commit was made on.
  - fca0107 calls it "a 20-core machine".
  - 2da0857 names an RTX 3060 in it.
  - ba14ac0 says "a 14 GB machine".
  - The messages do not establish that every one of these refers to the same
    physical machine.
- "yeet" is a second machine. 15b98b2 gives it 13.49 GiB of RAM.
- "gcp1" is a public GCP host, used as the introducer. Before 95e0712 it was
  also the relay.
- "the lab" is containers on one host (`scripts/lab-up.sh`,
  `scripts/lab-hetero.sh`).

None of these numbers was re-measured for the report. The report labels each
one with its commit hash.

---

## Pool distribution

### fca0107 (2026-08-26) - 20-core machine plus a 16-core peer over Tailscale
> Measured against a hand-tuned local pool on a 20-core machine
> with a 16-core peer, the untouched script ran 8.5s where plain Python took
> 6.4s. Correct, distributed, and slower than not bothering.

> Same benchmark after: 4.9-5.4s against 5.8-6.4s plain, over Tailscale, with
> 116 of 256 items executing on the peer. The remaining gap to the 1.8x the
> core counts allow is link latency, the serial measurement batch, and chunk
> ownership that is still a fixed alternation rather than proportional to what
> each node can do.

### 8c9284c (2026-08-27) - lab, 8:2:1 CPU quotas (`scripts/lab-hetero.sh`)
> Measured on an 8:2:1 cluster, an even split ran
> the benchmark 1.6x slower than not distributing at all.

> On the same cluster the one-CPU worker is declined
> outright - "cannot start before the rest of the cluster finishes" - and the
> rest is split 3:1 rather than 1:1. Five runs each, median 3.61s by headcount
> against 2.87s by throughput.

## Distributed training (DDP)

### 145db30 (2026-08-27) - two machines, before daemon-side averaging
> The
> same demo took 2.9s on one machine and 62.9s across two, correct to four
> decimal places and useless.

> Instrumenting the shim said where it went: 88 syncs, 677ms each, 1.5 MiB sent
> and 3.0 MiB received per rank per step.

> Auto-tuning to every ~20 steps takes the run from 54s
> to 5.4s - and moves the final loss from 0.0603 to 0.0756, because an 87-step
> run averaged 7 times is barely distributed training.
>
> So it is off by default.

### 819ebca (2026-08-26) - fp16 gradient transport
> A two-rank run produced losses of 0.6140 and 0.5895 - identical to four
> decimals with the earlier float32 run on the same seed and shards.

### 364b3d9 (2026-08-27) - two machines, home link, `scripts/bench-ddp.sh`
This supersedes the single-scale int8 table in 994a0ee, which that commit's own
author withdrew as "luck".
> I reported that encoding as costing "about 5% of the final loss", from one
> sample. Running it again as a repeatable benchmark gave 0.127 against a 0.087
> baseline - 45% worse, not 5%.

> With a scale per tensor the same run comes out at 0.0877 against 0.0875.
>
>   one machine        54.9s   loss 0.087495
>   two ranks, fp16    69.4s   loss 0.087497   sync 36.2s
>   two ranks, int8    56.1s   loss 0.087688   sync 22.6s

The link is described in 994a0ee as "two machines on a 54 Mbps link".

### 2da0857 (2026-08-27) - break-even verdict, GPU against CPU
> fp16, 32.0s of sync against
> 33.4s of compute, ring slower (66.0s against 55.0s on one machine); int8, 20.4s
> of sync, ring faster (54.0s).

> the same job takes 1.5s on this machine's GPU
> against 55.0s on its CPUs.

> CUDA
> available inside the sandbox, RTX 3060 detected, 20 matmuls of 4096 squared in
> 0.55s.

### 7049eb5 (2026-08-27)
> the same model and batch takes
> 1.5s on this GPU and 56.2s on this CPU

### 2fc3914 (2026-08-27) - yeet, 16/8/2-core ring
> a three-rank ring spent
> 47-59% of itself moving gradients and took 36.4s, against 6.9s for
> the one machine k*rate_k picks.

> Measured on yeet, a 16/8/2-core ring: admission takes the 16 and the 8,
> shares them 66/34, both run 58 steps, shards disjoint with 608 of 60000
> dropped (under one global batch). Equal batches 25.4s, measured shares
> 23.6s.

### 83c59aa (2026-08-27) - yeet, co-located ranks
> 18.9s for two
> co-located CPU ranks against 2.8s for the same script on one.

### 1c76f51 (2026-08-28) - yeet
> Two ranks on yeet: 191.1s, against 92.8s for the same machine doing the whole
> dataset on its own. The run reported "the ring paid for itself".

(The commit fixes that verdict, which was wrong.)

### ac19310 (2026-08-27) - yeet, three ranks, one worker throttled 8 CPUs to 1 and back
>   settled at          rank 0 64%, rank 1 36%
>   after the throttle  rank 0 84%, rank 1 16%
>   after recovery      rank 0 70%, rank 1 30%

### 97e3478 / 2fc8766 - not verified on hardware
- 97e3478 (mid-run refit): "The live test - throttling a rank mid-run and
  measuring the ring recover - is still to run". The live run was later made in
  ac19310, above.
- 2fc8766 (one rank per accelerator): "NOT VERIFIED on real multi-GPU hardware -
  neither machine here has a second usable GPU."

## Closure transfer and caches

### 819ebca (2026-08-26) - lab
> a closure differing from the peer's by one package sent
> 3 of 39 store paths.

### 193e1bf (2026-08-27) - yeet against a cold container peer
> 7 of 43 paths, 206.0 MB down to
> 28.6 MB, 86% less across the wire.

### 837f9c2 (2026-08-28) - adding cloudpickle to every environment
> a peer that
> already holds the environment fetches 3 paths of 39, 0.43 MiB, against a
> 444 MiB closure.

### b19f7fc (2026-08-29) - LRU prune at startup
> Measured on this node with the cap set to 50 MiB: the publishing cache went
> from 109 MiB to 28 MiB and the archive cache from 768 MiB to 4 KiB, and the
> next job still ran 14 items on the peer

## Networking

### fe43933 / e06c534 (2026-08-28) - why hole punching needed a fallback
> on a phone tether it was seen
> as :38471 by one probe and :27486 by the next, and a simultaneous punch to a
> peer behind an ordinary home router moved 400 packets in each direction and
> received nothing.

> The symmetric side opens
> 256 sockets toward the peer, burning 256 external ports at once; the other
> sprays 600 probes across the port space. The chance of no collision is
> (1-256/64000)^600, under a tenth of a percent.

### a6e0723 (2026-08-27) - relay (since removed from the data path, 95e0712)
> yeet reached this machine's daemon through the relay on GCP and got
> its /health back, 200 with the real body. First request 4.5s while the
> connection is established, then 16-17ms, which is the round trip.

### 42c2426 (2026-08-28) - `scripts/net-verify.sh`
> peer reached in 0.03s WITH
> the introducer suspended on its own machine.

> over the whole
> verification window gcp1's interface moved 0.2 MB outbound - the square app
> included, so an upper bound - while some thirty megabytes crossed between the
> peers.

## Memory

### 96c7f35 (2026-08-27)
> a 3 GiB allocation under a 1 GiB MemoryHigh sat at
> 998 MiB with 7381 throttle events, zero kills and 88% memory pressure, for
> minutes.

> Verified on yeet: the daemon's scope comes up with MemoryMax at half of RAM
> and MemorySwapMax=0, and a 3 GiB allocation under a 1 GiB ceiling is killed in
> seconds with oom_kill recorded against that scope, while the machine stays at
> 11 GiB free.

### 15b98b2 (2026-08-27)
> Measured on yeet, one host daemon and one container worker:
> before, 7.24 GiB capped plus one unbounded daemon on a 13.49 GiB machine;
> after, 6.74 GiB and 1.97 GiB, leaving 4.8 GiB for everything that is not
> pipedpeer.

## Cost model

### ec5d9fc (2026-08-27) - yeet against a 4-core container peer
> all four shapes:
> local wins by 25x to 100x and the model chose local every time. That is
> one direction of the test only

### 2fecfe1 (2026-08-27)
> Measured on yeet: paid 0.6s
> against 0.7s local.

### ba14ac0 (2026-08-27)
> forcing an 8192-cube product on a 14 GB machine drove it to 194 MB free
> and the kernel killed the desktop shell.

## Signatures

### cf2e3ae (2026-08-29)
> Verified on yeet under require-sigs = true: a closure signed by a trusted
> peer imports and its work runs, and the same closure is refused with
> "lacks a signature by a trusted key" when the sending node's key is taken
> out of the trust list.
