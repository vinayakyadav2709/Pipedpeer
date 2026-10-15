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
  `--ddp 1` against automatic mode (the manual checklist in the worktree's
  `testing-log.md`). TODO(verify).
- **The phone** (Samsung S20 FE 5G, Snapdragon 865): Termux, the arm64 binary,
  a Pool chunk on Termux Python, a numpy kernel timed against one laptop core
  (kill criterion: under 25%). Developed under qemu only, whose timings are not
  performance numbers.
- **The college network**: per-lab subnets joined by routers; PC #2 on
  10.10.220.x could not reach 10.10.70.x at the demo. `pipedpeer doctor` names
  the case; the fix is a different network or Tailscale, never a relay.
- **Hybrid-laptop dGPU sleep**: the sysfs enumeration should not wake a
  suspended NVIDIA GPU; confirm `runtime_status` stays `suspended` on an idle
  laptop.
- **Demo-day recording**: every run, exported with
  `pipedpeer jobs --export-all <dir> --since <day>` on each machine, goes into
  the paper.

## 3. Known gaps, left by choice

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
  [benchmarks/diloco.md](benchmarks/diloco.md).
- A single machine never crashing on huge numpy.
- Sleep handling (no sleep inhibitor).
- Web page parity with the CLI (the page is read-only by design).
- Phones beyond the Termux worker (an app without Termux, the Android Linux
  VM).
- IPv6 (the connection code looks IPv4-only; unverified).

Dropped: Optuna interception, NPU support.

## 4. Future work, in priority order

1. Whatever the verification pass and the real-hardware runs (section 2)
   found.
2. **DiLoCo in automatic mode**, only if its benchmark passes (validation loss
   within 1% of one GPU on every test model, mean of 3 seeds, spread
   reported), then **Streaming DiLoCo** if it is cheap on top of the overlap
   thread.
3. Superlinear steps the scaling map flagged (`docs/benchmarks/scaling.md`).
4. DDP-LEAD, then the memory ladder.
5. AMD through ROCm.
6. Locality- and topology-aware placement.

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
