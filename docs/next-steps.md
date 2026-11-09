# Next steps: coming back to Pipedpeer

Written for after the address-book phase (October 2026), which followed the
college demo of 2026-10-14. It assumes a fresh clone and nothing else.
File:line references drift; re-check them with `grep -n` before you edit.
Figures marked **TODO(verify)** are filled in by the verification pass of that
phase; if one is still a TODO, the measurement was not run.

## 1. Where it stands

**What the phase changed** (details: [system report 4.15](system-report.md#415-after-the-demo-setup-output-and-speed),
[Appendix B](system-report.md#appendix-b--what-changed)):

- **Joining.** `pipedpeer invite` prints one `pipedpeer connect <code>` line;
  `connect` says connected (exit 0), partly (exit 2) or failed (exit 1) and
  why. Every daemon is an introducer for its own cluster; a public server can
  be one that takes no jobs ([hosted-introducer.md](hosted-introducer.md)).
- **Addresses.** One identity (node id = node key fingerprint, TLS from the
  same key, pins by node id); an address book with every path per node id,
  switching only to verified paths; signed address gossip; QUIC migration and
  link replacement when a machine changes network
  ([ADR-0019](adr/0019-address-book.md), [ADR-0008](adr/0008-pinned-tls-and-key-addressed-nodes.md)).
- **Ports.** 4242/tcp API, 4242/udp discovery, 4243/udp direct, 4244/udp
  introducer; one hard cut ([ADR-0020](adr/0020-ports-block-hard-cut.md)).
- **Commands.** setup, invite, connect, run, status, dashboard, ui, jobs,
  attach, start, stop, doctor, uninstall (`map`, `events` hidden). One config
  file, `cluster.json`.
- **Runs like Python.** Clean output (`-v` for details), script flags pass
  through, the script's exit code, a one-line simple view of the run record.
- **Training.** Batch split to the script's batch; int8 with error feedback
  by default; one GPU or a split decided from measured steps; GPU lead;
  parameter manifest check; failed script or lost lead stops every rank;
  gloo/nccl removed; `--ddp-sync fast` = DiLoCo (opt-in until its loss
  benchmark passes).
- **Speed.** Per-node in-flight depth, cancelled race losers, chunk sizing
  from the measured link, the O(n²) request body fixed, globals once per peer,
  zstd by link speed, pickle-5 buffers, workspace delta, nodes fetching from
  cache.nixos.org, stats across runs, overlap and butterfly reduce in training.
- **Setup.** Installer by exact version, autostart user service, background
  warm-up, `doctor`, `uninstall`. Phones as Termux pool-only workers
  ([phone.md](phone.md)).
- **Measurement.** One run record (`internal/runrec`), emulated lab
  (`scripts/lab-emu.sh`), scaling map, `jobs --export-all`.

**Checks at the end of the phase.**

| Check | Result |
|---|---|
| `go test -count=1 -p 1 ./...` (src/, capped scope below) | TODO(verify) |
| `scripts/check-skips.sh` | must say "all 15 distribution tests ran"; TODO(verify) |
| `scripts/shim-check.sh` (ruff + pytest on the shim) | TODO(verify) |
| `make test-net` (E1-E5 network scenarios) | TODO(verify) |
| Benchmarks: `docs/benchmarks/{ddp-switch,w10,diloco,scaling,scaling-train}.md` | TODO(verify) |

## 2. Not verified on real hardware

The emulated lab measures overhead growth and emulated speedup; these need
real machines:

- **The two laptops over Tailscale** running `cpu.py`, `gpu.py`, `both.py`, and
  `--ddp 1` against automatic mode (the manual checklist in
  [pipedpeer-demo/TESTING.md](../pipedpeer-demo/TESTING.md)). TODO(verify).
- **The lab harness on real PCs**: `scripts/campaign/` has run only on Docker
  containers. CPU-busy thresholds (the E19 sampler against `top`), real network
  transfer, nix store mode, `pipedpeer-bare`, E14's 20 MB payloads against the
  pool body limit, torchrun at N=2 and a real join are checked by the two-PC
  check at the start of Session 1 in `scripts/campaign/OPERATOR.md`. Two arms
  have not completed even in containers, so the first real session is their
  first run. Ray forms a 2-node cluster with a 3 GB head (ready in 13 s, no
  out-of-memory kill), but E19's Ray run then held its container at the 3 GB
  limit until it stopped answering: Ray counts the host's 16 CPUs there, and
  no Ray workload finished. On the first real PC, watch Ray's memory on the
  first run. E17 needs three PCs.
- **The phone** (Samsung S20 FE 5G, Snapdragon 865): run on 2026-10-16 with
  Play Termux, Debian + Nix in proot: pool and torch chunks from laptop A
  verified (numbers in the report's results table). Still open: a run
  submitted from the phone fails, because its node list goes stale (Android
  refuses Go's interface listing, `netlinkrib: invalid argument`, so LAN
  discovery and address listing fail on the phone); and the numpy kernel
  against one laptop core.
- **The college network**: per-lab subnets joined by routers; PC #2 on
  10.10.220.x could not reach 10.10.70.x at the demo. `pipedpeer doctor` names
  the case; the fix is a different network or Tailscale, never a relay.
- **Hybrid-laptop dGPU sleep**: the sysfs enumeration should not wake a
  suspended NVIDIA GPU; confirm `runtime_status` stays `suspended` on an idle
  laptop.
- **Demo-day recording**: every run, exported with
  `pipedpeer jobs --export-all <dir> --since <day>` on each machine, goes into
  the paper.
- **E19 cold-joiner purge**: no container run has purged a real closure from a
  joiner, because at n=1 the joiner never held it (`joiner_warm=0` is correct
  there, and the byte columns are 0). On the first real session, confirm
  `purged_bytes` is close to `closure_bytes` on a joiner that ran the job
  before, and that `nix-store --delete` refusals are reported.

## 3. Known gaps, left by choice

Lab campaign limits, to account for when its results are written up:

- E19 join times: before quoting a mean or median, count the rows whose
  `join_status` is not `ok`. Timed-out joins have an empty `join_s`, so an
  average over the filled cells leaves out the slowest joiners.
- E19 "cold" joins remove the workload's top store path only. Its dependencies
  stay on the joiner, so `seed_s` is a lower bound on a truly empty machine.
- E19 Ray and Dask join times include starting Python and connecting to the
  cluster; Pipedpeer's do not. A small constant bias against Ray and Dask
  remains.
- E19 `join_status=partial` is confirmed by the cluster's healthy-node count,
  because the status output has no per-node join marker.
- A forced small map sent to a peer that hangs can block for up to 600 s.
  Forced maps over 4 items still race their tail on the local machine.
- `lab-pc.sh undo` leaves the lab user's `known_hosts` entries.

Parked by the user on 2026-10-15:

- Removing a member's trust (untrust); expiring or single-use invites.
- Memory escalation on out-of-memory (allocator setting, gradient
  accumulation, checkpointing, bf16), and a model too big for one GPU.
- AMD and Intel GPU support (detection fixes are in; no ROCm torch, no
  `/dev/kfd`).
- Training that survives losing its lead machine (DDP-LEAD).
- Automatic batch sizing (Pollux/AdaScale).
- Tuning levels (`--ddp-tune`) and defaults that change batch, learning
  rate, sync interval or epochs, so fast sync stays accurate past 4
  ranks. The simulated results are in
  [benchmarks/diloco.md](benchmarks/diloco.md); the one candidate that
  passed everywhere is exact sync with PowerSGD rank 16 plus error
  feedback for 5+ ranks.
- A single machine never crashing on huge numpy.
- Sleep handling (no sleep inhibitor).
- Web page parity with the CLI (the page is read-only by design).
- Phones beyond the Termux worker (an app without Termux, the Android Linux
  VM).
- IPv6 (the connection code looks IPv4-only; unverified).
- Whole jobs on phones: the job sandbox nests Termux's proot inside the
  proot-distro session, and on Play Termux proot 5.1.107.80 (loader embedded,
  started from memfd) the inner proot starts nothing. Phones advertise
  `chunks-only` meanwhile. Tried: none yet. Options: an un-nested per-job proot
  started at Termux level; an inner proot with a loader file (PROOT_LOADER).
  F-Droid Termux untested.
- A whole job started on a phone does not run on an x86_64 laptop: the CLI
  builds the job's closure for its own system and placement refuses it ("arch
  mismatch: closure aarch64-linux, node x86_64-linux"). Fix: the run request
  carries the job's flake + lock (additive field) and the receiving daemon
  builds it for its own system, as pool chunks to a phone already do; the CLI
  skips its local build when the target's system differs. ~200-400 lines.
- Two homes that both lack a reachable introducer (no UPnP or port forward,
  double NAT) cannot meet without an overlay such as Tailscale; there is no
  relay by design. A hosted introducer on any public machine removes this.

Parked by the user on 2026-10-16 (found by the external paper review; the
paper and report describe both as they are):

- The numpy offload cost gate assumes every node runs at six times this
  machine's speed (R' = 6R for all K nodes, the submitter included) when any
  peer reports a GPU, although matmul row blocks run on the peers' CPU NumPy
  (`numpy_ops.py:33`, `:350-352`, `:404`). Fix: sum per-node CPU rates.
- ~~The placement health factor h is always 1.0.~~ Fixed 2026-11-03 (23137d0): halved on an
  unreachable mark or failed job (floor 0.1), +0.1 per healthy poll.
- E15 (lab campaign): the pipedpeer arm splits the work into chunks (product
  behaviour); the baselines do not. The paper states this. Environment
  initialisation sits outside the timing window.

Dropped: Optuna interception, NPU support.

- **Login prompts in the NAT lab image.** `lab/nat/compose.yml:116` runs a privileged Debian container with `/sbin/init`. If that image starts getty, it opens the host's real terminals, as the Ubuntu test image did before its gettys were masked. Check it and mask them the same way before the next run of that lab.

## 4. Future work, in priority order

1. Whatever the verification pass and the real-hardware runs (section 2)
   found.
   - **Workspace selection.** Today `pipedpeer run` tars the whole folder up
     to the nearest `.git` or `.pipedpeerignore`, which can be far bigger
     than the script needs.
     - The upper bound becomes the cwd, never above `$HOME`; for a script
       outside the cwd, its own folder. For an import from above the cwd,
       decide between an error and including it automatically, depending
       on how sure the trace is.
     - Send only traced files: local imports (`pythondeps/deps.go` already
       finds them) plus literal `open`/`read_csv` paths. First measure how
       complete the trace is: dynamic paths, `importlib`, package data,
       paths relative to the cwd.
     - Fetch on demand: a job that opens a missing file fetches it from the
       submitter and caches it for peers, so only nodes that use a dataset
       receive it. Possibly with a confirm prompt.
     - Keep the 2 GiB cap and `.pipedpeerignore`.
   - **Cross-arch placement.** The closure is built for the submitter's
     architecture, so `coordinator/find.go` turns away nodes of another one
     (`arch mismatch`); a phone's whole job never goes to a laptop. Fix: the
     target builds its own closure.
   - **NAT hole-punch test with a public introducer.** Punching needs an
     introducer that sees both peers' public addresses; it has only been
     run in the lab, not between two real home networks.
   - ~~**Upgrade restart on real machines.**~~ Verified 2026-11-04 on two
     Fedora laptops (systemd unit) and two Termux phones (SM-G781B,
     CPH2569): installer or `pipedpeer setup -y` printed `restarting the
     daemon: <old> -> <new>` and `status` showed the new version on each.
   - **`pipedpeer status` prints the CLI's version** in its `daemon running
     (… version=…)` line (`src/main.go:159`), not the daemon's, so it hides
     a daemon left on an old build. Print the version from the daemon's
     `/health`.
   - **Stale `unreachable` rows** for an old LAN address in `pipedpeer
     status` after a network change.
   - **Docs** for `PIPEDPEER_CONNECT_FROM_INSTALLER` and
     `PIPEDPEER_WARMUP_RETRY`.
   - **Force-mode tests for every shim interceptor.** `--distribute force`
     and `--offload` make every gate say yes, so any distributed or fallback
     path that calls a patched function distributes again without end (the
     groupby shuffle ran out of memory; the numpy matmul fallback sent 33
     blocks for one). Only `tests/test_groupby_force.py` and
     `tests/test_numpy_force.py` run with `_FORCE` on; merge/join,
     dot/tensordot/svd/eig, the partitioned frame, out-of-core reads,
     joblib, `Pool` and torch were checked by reading only. Add one
     parametrised test that runs each under force and offload and asserts
     one distribution and a result equal to the plain library. Urgent since
     4ffffa8: before it, `--offload` never turned force on (the shim compared
     the machine key with the raw boot id and never matched), so this path
     was dead for `--offload`; now it is live. `cpu.py` uses only the fixed
     interceptors; anything else under `--offload` is untested.
   - **Pool chunks on a private nix store** run through crun. The daemon's
     environment is not forwarded into the container, so
     `PIPEDPEER_WORKER_PROCS` and `PIPEDPEER_CACHE_BUDGET` set on the daemon
     do not reach chunks there. A chunk executing inside crun is covered by
     argv/bundle tests only; run it end to end on a machine without `/nix`.
   - **Daemon crashes on Termux are not restarted.** The boot script starts
     it once; a crash leaves the phone off the cluster until `pipedpeer
     start` is run by hand. Add a supervisor loop or restart on the next
     `pipedpeer` command.
   - **Phone setup does not keep Termux alive.** The boot script and
     `termux-wake-lock` stop neither Android's battery killer nor a closed
     Termux; on an Oppo phone (CPH2569) Termux was closed mid-session and
     the node went unreachable with its SSH server gone. `pipedpeer setup` on
     Termux should: check the Termux:Boot app is installed and was opened
     once, and say so if not; open the battery-optimisation exemption prompt
     for Termux (`am start -a
     android.settings.REQUEST_IGNORE_BATTERY_OPTIMIZATIONS -d
     package:com.termux`) and print the vendor path (Oppo/OnePlus: Settings →
     Battery → Termux → allow background activity); acquire the wake lock
     now, not only at boot; and optionally start `sshd` from the boot script
     so the phone can be reached remotely. `pipedpeer doctor` should report
     each of these. It happened again on the CPH2569 (2026-11-04): the
     network was up, port 4242 closed, and the wake lock was gone with
     Termux. Also needed:
     - Restart the daemon when Android kills it, not only at boot: a
       Termux job or watchdog that brings back the wake lock, `sshd` and
       `pipedpeer start`.
     - Setup on Oppo/OnePlus (ColorOS) also says to lock Termux in recent
       apps (long-press the card → lock).
   - **A dead phone vanishes from `pipedpeer status`** instead of showing as
     `unreachable`, as a dead laptop does, so a node missing from the table
     is easy to overlook. Keep it listed as unreachable with its last-seen
     time.
   - **`pipedpeer start` on a phone prints "gpu cannot list: permission
     denied"** before "daemon running". A phone has no usable GPU, so skip the
     GPU probe on Termux or log it at debug level.
   - **`scripts/install-pipedpeer.sh` needs sudo for an upgrade** when the
     binary is in `/usr/local/bin`, so an unattended upgrade fails with
     "a terminal is required". Install to `~/.local/bin` when it is on PATH,
     or say so before downloading.

   Found on 2026-11-04 (fedora + yeet + two phones, test sheet
   `pipedpeer-demo/TESTING.md`); all four affect the demo:

   - **A phone's pool worker is killed before it ever warms up.** Run
     88202885 (`phone_train.py`, third run, environment already built):
     the phone got 4-5 items, logged `spawning warm worker`, and 27.6 s
     later `warm worker pid … stopped for a cancelled request: signal:
     killed`; the origin had expected 5.9 s, run its local backup copy,
     won, and cancelled. Every run starts the worker cold again, so the
     phone never lands in `ran on` for torch work, and the races cost
     50-55 s of wasted peer time per run (1m7s against 34 s). Cause: the
     worker imports only the stdlib (`daemonapi/poolscripts.go:272`), so
     `import torch` under proot happens inside the first task, and a
     cancel kills the process group (`daemonapi/poolworker.go:142`,
     added in 14028de for a killed script). Fix: (1) start the worker
     and import the job's modules (`phone.Need` has the list) when
     "job environment built" fires; (2) on cancel, let a worker that is
     still on its first task finish, drop the result and keep it; kill
     only a warm worker mid-chunk; (3) optionally a soft cancel (SIGINT,
     KeyboardInterrupt, ack, SIGKILL after a grace period). Cost: at most
     one unwanted chunk of phone CPU; idle workers are still reaped.
     This is why item 8's "from the second run on it joins" did not hold
     for `phone_train.py` (runs 2 and 3 were `ran on fedora 100%`): a
     `prepare` step that only builds the closure would not fix it; it
     must also start the worker and import the job's modules.
   - **~7.5 s fixed cost on every run.** `jobs last --timeline` on a warm
     run: `nix build 0ms · export closure 5.2s · upload 2.1s`, with
     `Closure already on the node; nothing to send` and 0 of 47 workspace
     files sent. `cpu.py` plain 19.4 s, `pipedpeer run` 25.1 s although
     the script part was faster (17.4 s): on demo-sized scripts pipedpeer
     looks slower than Python. The closure skip already exists
     (`daemonctl/upload.go:72`); the 5.2 s stage timed workspace packing,
     not the export (stage renamed "pack workspace"). Re-measure in the
     lab's fixed-cost runs; fix only if it stays over ~1 s.
   - **No progress while a run builds its environment.** `pipedpeer run
     cpu.py` on a phone printed only the GPU warning for 1.5+ min while
     it built for aarch64; nothing reached either daemon. Without `-v`
     print one line per slow stage (building, exporting, uploading).
   - **A copied node key is not detected.** The node id is the key's
     fingerprint (`identity/key.go:39`), so two machines sharing
     `~/.local/share/pipedpeer/node.key` (cloned lab disk, VM template,
     restored home) are one node to the cluster: both prove the id, the
     address book merges their paths, and "this machine" checks match
     both. Hostnames (F17) and machine keys (`heartbeat/machine.go`)
     already handle clones; the node key does not. Fix: each daemon
     announces a per-start random nonce; one node id seen with two nonces
     at two addresses at once is a clone, and `status`/`doctor` say
     "delete node.key on one of them and run setup".
   - **An unidentified flaky test in `internal/coordinator`.** It failed
     once in a full `go test ./...` after merging 74be7b8 and passed on
     every rerun (three alone, one full suite); the `--- FAIL` line was not
     captured. Run `go test -count=20 ./internal/coordinator/` under load
     to find it.
2. **DiLoCo in automatic mode**, only if its benchmark passes (validation loss
   within 1% of one GPU on every test model, mean of 3 seeds, spread
   reported), then **Streaming DiLoCo** if it is cheap on top of the overlap
   thread.
3. Superlinear steps the scaling map flagged (`docs/benchmarks/scaling.md`).
4. DDP-LEAD, then the memory ladder.
5. AMD through ROCm.
6. Locality- and topology-aware placement.
7. **`pipedpeer explain <script>`**, like SQL's EXPLAIN: before running, print
   the plan: the environment it would build (imports found, CPU or CUDA
   torch), the machines considered and the chosen one, each Pool / joblib /
   pandas / torch call the shim will take over (file:line, from a static
   scan), the DDP choice (one node or split, sync mode, H) and, from the
   job history, expected chunk counts and wall time. `pipedpeer jobs <id>
   --explain` is already the EXPLAIN ANALYZE half (after a run, why each
   machine won); chunk sizes are decided at run time from measured item
   cost, so before a first run they can only be estimates.
8. **Prepare a script on every machine before its first run.** A machine of
   another architecture (an ARM phone next to x86 laptops) cannot use the
   submitter's closure; it is sent the recipe and joins the pool once its own
   build is done. The job does not wait for it, so on a script's first run a
   new phone usually finishes building after the job ends and takes no
   share; from the second run on it joins. A `pipedpeer prepare <script>`
   (or the same step in `pipedpeer explain`) would send the recipe to every
   node and wait until each reports the closure runnable, showing per-node
   progress, so the first run in front of people already uses every machine.

## 5. Decisions already made (do not reopen)

By the user on 2026-10-12 and 2026-10-15 unless marked. The ADRs hold the
reasoning.

**Product**
- Thirteen commands (section 1); the `python` alias and `join` are gone.
- `invite` prints exactly one line, `pipedpeer connect <code>`; no install
  one-liner or short URL in the product. Every build is published under its own
  version (`nightly-<version>`) and the installer takes `--version <v>`, so
  every machine of a cluster runs the same build.
- The web page gets the token from the URL fragment that `pipedpeer ui` opens;
  no loopback exemption (internet peers arrive through loopback forwarders).
- Autostart is optional, asked during setup, default on; `start --boot` /
  `stop --boot` change it.
- `uninstall` removes the binary, data, service, firewall rules and root bits,
  and keeps Nix with a printed hint.
- No relay, ever. Where no direct path exists, use Tailscale.
- No sleep blocking; no NPU; Intel integrated graphics are never a GPU.
- No binary changes on other people's machines mid-event.

**Training**
- Training keeps the user's maths: the batch is split across ranks (sum = the
  script's batch); no learning-rate scaling; no added epochs.
- Automatic mode picks one GPU or an exact split; DiLoCo stays behind
  `--ddp-sync fast` until promoted on evidence. The old "auto interval gave a
  worse loss" result was measured under fp16 averaging, no outer optimizer
  and full-batch ranks; it does not judge DiLoCo.
- A rank whose script fails stops every rank; a lost machine is dropped and
  the ring continues; a lost lead stops the survivors (exit 76).

**Measurement**
- The plain lab (three containers on one host) proves work crosses nodes,
  never speedup. The quota-plus-netem lab (`scripts/lab-emu.sh`) is the
  emulation method; its results are always labelled emulated. Real points are
  the Tailscale runs and the demo-day recording.
- Every recorded timing run takes `flock ~/.cache/pipedpeer-bench.lock`.
- Lab containers get a hard `--memory` with `--memory-swap` equal to it, and
  the running total stays at least 5 GB below host RAM.

**Kept from 2026-10-12**
- `run --offload`: the script runs here; every distributed piece goes to other
  machines.
- `--remote` waits up to 10 min for room.
- Ctrl+C or closing the terminal stops the job everywhere; a vanished
  submitter leaves a `--remote` job running and attachable; `--detach`, Ctrl+Z
  and `attach <id>`.
- `--cold` re-sends the environment; it does not rebuild.
- `\r` progress updates are forwarded so tqdm bars animate.
- No result cache; common packages are pre-built (now the background warm-up).
- `--gpu prefer` never blends GPU and CPU ranks.

## 6. Open questions for the user

Assumptions the phase built on, waiting for confirmation:

| # | Assumption | Recommend |
|---|---|---|
| 1 | `start --boot` / `stop --boot` are the autostart switches | keep |
| 2 | `map` and `events` stay as hidden commands | keep: working features with tests |
| 3 | Plain `run` prints the simple view only when work left this machine; `-v` adds the detailed view | keep |
| 4 | int8 with error feedback replaces fp16 on the wire by default | keep unless the benchmarks show a loss gap |
| 5 | No gradient accumulation for the default batch split | keep: OOM handling is parked |
| 6 | numpy matmul/svd offload and the pandas hash-shuffle stay | keep |
| 7 | The GPU "quick speed test" is a static per-device score cached by PCI address | keep; DDP's step-time refit is the real per-model measure |
| 8 | Pool(4) is still widened automatically; placement load signals not wired | revisit with placement work |
| 9 | From 2026-10-12, kept as built: no HTTP timeout on a raced remote copy; late local wins not counted in speed; untried-node-first only for smart placement; `--offload` ships every interceptable op; no usage lines for attach/collect; the lead's BatchNorm buffers win; DDP start wait 5 min | keep |

## 7. Resuming on a new machine

### Tools

| Tool | Notes |
|---|---|
| Go | the version in `src/go.mod` |
| Nix | multi-user install; put `/nix/var/nix/profiles/default/bin` first on PATH in non-login shells, or the 4 signed-closure tests skip |
| docker | the labs (`lab/`, `lab/net`, the emulated lab). The GPU lab needs the NVIDIA container toolkit with a CDI spec |
| Python deps | pandas, scikit-learn, joblib, torch for the shim tests; otherwise they skip |
| uv | `uvx ruff`, `uvx pytest` for `scripts/shim-check.sh` |
| qemu-user-static | arm64 binfmt, for the Termux phone image |
| tectonic | to rebuild the PDFs |

### Commands

```sh
cd src && go build -o ../bin/pipedpeer .          # never a bare `go build` (stray binary)
cd src && gofmt -l . ; go vet ./...
cd src && PATH=/nix/var/nix/profiles/default/bin:$PATH go test -count=1 -p 1 ./...
scripts/check-skips.sh                            # must say "all 15 distribution tests ran"
scripts/shim-check.sh                             # ruff + pytest on src/internal/nixgen/shim
make test-net                                     # E1-E5 network scenarios in containers
tectonic --outdir <out> docs/report/main.tex
tectonic --outdir <out> docs/paper/paper.tex
```

Benchmarks (emulated, one timed run at a time under the bench lock):
`scripts/bench-ddp-switch.sh`, `scripts/bench-w10.sh`,
`scripts/bench-diloco.sh`, `scripts/scaling/sweep.sh`,
`scripts/scaling/train.sh`, all on top of `scripts/lab-emu.sh` and
`scripts/bench-lib.sh`.

### Lab facts and traps

- Containers share the host kernel's boot_id, so they count as one physical
  machine: `--offload` can't be shown in a lab without per-container boot_id
  bind-mounts.
- Docker bridges for network tests use
  `-o com.docker.network.bridge.inhibit_ipv4=true` so the host is invisible.
- `PIPEDPEER_NO_LAN_DISCOVERY=1` tests the connect/introducer path alone.
- Restart an introducer between test cases: it remembers peers for 2 minutes.
- Never touch the host's own daemon from a lab.

### Safety

Heavy work (labs, full tests, big builds) can crash a 13-16 GB laptop. Run Go
builds and tests in a memory-capped scope:

```sh
cd src && systemd-run --user --scope -q -p MemoryMax=3G -p MemorySwapMax=1G -p CPUQuota=400% \
  --setenv=PATH=/nix/var/nix/profiles/default/bin:$PATH \
  sh -c 'gofmt -l . ; go vet ./... && go test -count=1 -p 1 ./...'
```

## 8. How to work on it

1. Cut a new branch from `dev` and open a PR back into `dev` when it is
   verified; `main` is for tagged releases ([CONTRIBUTING.md](../CONTRIBUTING.md)).
2. Read [CONTEXT-MAP.md](../CONTEXT-MAP.md) and the matching ADR before
   changing anything that looks odd.
3. Security-relevant wire changes are hard cuts
   ([ADR-0006](adr/0006-hard-cuts-for-security-wire-changes.md)); everything
   else stays additive.
4. Update this file when an item lands or a decision changes.
