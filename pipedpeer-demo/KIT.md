# Demo kit

One file for demo day. Part A is for whoever (person or agent) hands out commands, Part B has the copy-paste
blocks for each layout, Part C is the flow on stage with what you should see, Part D is the troubleshooting table.

Scripts in this folder (each also runs under plain `python3`, prints its own `TOTAL <s>` line):

| Script | What it does | Shows |
|---|---|---|
| `cpu.py` | Pool (pure Python), joblib (numpy Mandelbrot), numpy SVD, pandas groupby | CPU work spreads; the phone can take Pool/joblib work. `--items N` makes it longer |
| `gpu.py` | Small MLP, one DataLoader, same model on GPU and CPU, prints validation loss | Automatic one-GPU-or-split choice; `--epochs N` passes through |
| `both.py` | Pool feature step, then torch training on the result | CPU phase spreads, GPU phase goes where the GPU is |
| `00_pool.py` … `06_pool_lambda.py`, `04_torch_ddp.py`, `students/` | Older single-topic scripts | Spare material if someone asks "what about sklearn / files / lambdas" |

## Part A: before you hand out commands

1. Pick the cluster's build once, on the user's laptop: `pipedpeer --version` (example: `0.1.4-nightly.20261020-abc1234`).
   Every machine installs exactly that version (`--version`), never "latest": nightly republishes on every push.
2. The first machine's `pipedpeer setup` prints "cluster created" and a `pipedpeer connect pp1-…` line. Later, on any
   member, `pipedpeer invite` prints the same line again. That one line is all another machine needs.
3. Check live state before answering "what do I run": `ip -4 route get 1.1.1.1` (this machine's address, network),
   `pipedpeer status` (members, health), `pipedpeer doctor` (what is broken and the fix).
4. Fill `<VERSION>` and `<CODE>` in the blocks below and send one complete message: the user's own steps first, then
   one block per other machine.

Ports (all opened by `setup` when a firewall is active): 4242/tcp daemon API, 4242/udp LAN discovery, 4243/udp direct
links, 4244/udp introducer.

## Part B: copy-paste blocks per layout

Every layout uses the same two blocks; a layout is just which machines get which block.

### Block FIRST: the user's laptop (RTX 3060, Fedora)

```bash
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
# The installer runs `pipedpeer setup`; answer yes to "start at boot".
# Expect at the end: "cluster created" and a line  pipedpeer connect pp1-...
pipedpeer invite          # prints that line again whenever you need it
```

### Block JOIN: every other Linux machine (friend's laptop, college PCs)

```bash
# Skip the curl line if `pipedpeer --version` already prints <VERSION>.
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
pipedpeer connect <CODE>
```

`connect` ends with one of three results:

| Result | Exit | Means | Do |
|---|---|---|---|
| `connected: N machines (names)` | 0 | done | nothing |
| `partly connected` + one line per missing machine with reason and fix | 2 | some machines unreachable | the printed fix; then Part D |
| `failed:` damaged code / nobody answered / secret rejected | 1 | not in the cluster | re-copy the code (damaged), check the network (nobody answered), ask for a fresh `pipedpeer invite` (rejected) |

If `sudo` was not available, the installer puts the binary in `~/.local/bin` and prints an ACTION NEEDED line: run
the `export PATH=…` line it shows (or open a new terminal).

### Layout 1: two laptops

- User's laptop: Block FIRST.
- Friend's laptop: Block JOIN. Also turn off Wi-Fi power saving (Part D row "drops off Wi-Fi").

Check: `pipedpeer status` on either shows 2 machines, both healthy.

### Layout 2: two laptops + 1 college PC

- Layout 1, then the PC: Block JOIN.
- The PC must share a subnet with at least one laptop, or reach it through the router on UDP and TCP. Before
  connecting, from the PC: `ping -c 3 <laptop ip>`. If that fails, put everyone on the phone hotspot (Part D row
  "split subnets").

Check: 3 machines healthy. The PC has no NVIDIA GPU, so it takes CPU work only; that is expected.

### Layout 3: two laptops + 2 college PCs

- Layout 2, then the second PC: Block JOIN.
- The cloned PCs share a hostname (`psipl-OptiPlex-SFF-7010`); `status` and the web page tell them apart by node id.
  Both being listed with the same name is expected, not a duplicate row.

Check: 4 machines healthy.

### Layout +phone (add to any layout)

Samsung S20 FE 5G, Termux from F-Droid (not the Play Store). Once, from a laptop with the phone on USB debugging:

```bash
adb shell device_config put activity_manager max_phantom_processes 2147483647
adb shell settings put global settings_enable_monitor_phantom_procs false
```

Then in Termux on the phone:

```bash
pkg install -y python python-numpy curl
termux-wake-lock
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
pipedpeer connect <CODE>
```

The phone joins as a pool-only worker: it takes Pool and joblib chunks (`cpu.py`, `both.py`), never whole jobs or
GPU work. Keep Termux in the foreground or its notification pinned. More detail: `docs/phone.md`.

## Part C: the flow on stage

Run everything from this folder (`pipedpeer-demo/`) on the user's laptop unless a step says otherwise. Output is
the script's own prints, then a short summary when another machine helped: total time, the script's time vs
pipedpeer's overhead, and where it ran. `-v` shows every detail.

0. **Talk.** "Python, but on more machines": the scripts here are plain Python and run under `python3` too.
1. **Live setup.** On a fresh machine: Block JOIN.
   Expect: `connected: N machines (…)`, exit 0. `pipedpeer status` lists every machine healthy.
2. **CPU spreads.**
   ```bash
   python3 cpu.py              # baseline: note TOTAL
   pipedpeer run cpu.py
   ```
   Expect: the same four result lines (`Pool: 128 items, checksum 3839999232`, `joblib: … points inside`, …), a lower
   TOTAL, and the summary naming the other machines. The dashboard (`pipedpeer dashboard` in a second terminal)
   shows every machine busy during the Pool and joblib steps.
3. **GPU, decided automatically.**
   ```bash
   pipedpeer run gpu.py
   pipedpeer run --ddp 2 gpu.py               # force a split, to compare
   pipedpeer run --ddp 2 --ddp-sync fast gpu.py   # split, syncing less often
   ```
   Expect: the first run says it chose one GPU (gradients cross Wi-Fi slower than the GPU computes) and prints
   `epoch 0..2` lines plus `validation loss …`. The `--ddp 2` run is clearly slower, which is the point: the
   automatic choice was right. The `--ddp-sync fast` run closes most of that gap; its validation loss should be close
   to the one-GPU run's.
4. **Both.** `pipedpeer run both.py`
   Expect: `CPU phase: 256,000 rows …` (spread over every machine, phone included), then `GPU phase on: NVIDIA …`.
5. **Remote.** `pipedpeer run --remote gpu.py`
   Expect: the job runs on the friend's laptop (summary says where), output streams back as if local.
   Flags pass through: `pipedpeer run --remote gpu.py --epochs 5`.
6. **Phone takes CPU work.** `pipedpeer run cpu.py` again with the phone connected.
   Expect: the summary lists the phone among the machines that ran Pool/joblib chunks.
7. **Web page.** `pipedpeer ui` opens `http://127.0.0.1:4242/ui` already signed in (no token to type).
   Show nodes, recent runs from every machine, and the run view of step 2's run.
8. **Failover drill.**
   ```bash
   pipedpeer run cpu.py --items 600
   ```
   About 10 s in, switch the friend's laptop's Wi-Fi off. Expect: the run keeps going, the checksum line is the same
   as a run with nobody leaving would print (work that was out on the lost machine is re-run elsewhere), TOTAL is
   longer. Switch Wi-Fi back on; within ~30 s `pipedpeer status` shows it healthy again with no command typed.

Between rehearsals nothing needs reconnecting: the cluster is remembered and the daemon starts at boot. The second run
of a script is much faster than the first (environment already built and shipped). Say so; do not present it as a
cold start. To clean a college PC afterwards: `pipedpeer uninstall --yes`.

## Part D: troubleshooting

Run `pipedpeer doctor` on the machine that misbehaves first; each line is `✓/!/✗ what — fix: …`.

| Symptom | `doctor` says | Fix |
|---|---|---|
| `connect`: failed, nobody answered | ✗ introducer `<addr>` not answering | The machines cannot reach each other at all. Same network? `ping <other ip>`. If ping fails: split subnets, row below |
| PC on another lab subnet never appears (ping 100% loss) | ✗ introducer not answering / peer unreachable | Split subnets joined by routers that drop traffic between labs. Put every machine on one network (the phone hotspot) and run `pipedpeer connect <CODE>` again |
| Machine listed but `unreachable`, "could not prove it holds the cluster token" | ! peer `<name>`: UDP fine, TCP blocked | The router passes UDP but not TCP (college routers). Same fix as split subnets: one shared network |
| Machine shows up, then drops every few minutes (friend's laptop) | ! peer `<name>` unreachable: no route to host | Wi-Fi power saving. `nmcli connection modify "<wifi name>" 802-11-wireless.powersave 2` then reconnect Wi-Fi; plug in the charger; stop the laptop from suspending during the demo |
| `connect` partly connected, one machine "firewall" | ✗ port 4242/tcp (or 4243/udp) closed in firewalld/ufw | Run the `fix:` command doctor prints (it is the `firewall-cmd`/`ufw allow` line) |
| Phone connected but takes no chunks, or Termux dies mid-run | ! (on the phone) pool worker stopped | Phantom process killer: run the two `adb shell` lines in Layout +phone, then `termux-wake-lock`, keep Termux open |
| GPU script says "no eligible node" | ✗ NVIDIA device present but driver/nvidia-smi missing | Install or fix the NVIDIA driver on that laptop, reboot, `pipedpeer doctor` again |
| First run of a script takes minutes | ✓ everything | Normal: the environment for its imports is being built and shipped once. Run each demo script once before the audience |
| A machine got a different build | ! version differs from the cluster's | Re-run the curl line with the cluster's `--version <VERSION>` on that machine |
