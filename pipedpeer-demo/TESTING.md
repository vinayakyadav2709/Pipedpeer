# Testing log

## Steps to run

```bash
# Laptop A (inviter). <VERSION> = the build under test (pipedpeer --version prints "pipedpeer version <VERSION>")
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
pipedpeer invite                      # prints: pipedpeer connect <CODE>

# Laptop B, the college PC (installer runs `pipedpeer setup` itself)
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
pipedpeer connect <CODE>

# Phone, once from a laptop with USB debugging on
adb shell device_config put activity_manager max_phantom_processes 2147483647
adb shell settings put global settings_enable_monitor_phantom_procs false
# then in Termux (from F-Droid)
pkg install -y python python-numpy curl
termux-wake-lock
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
pipedpeer connect <CODE>

# Demo runs, from pipedpeer-demo/ on laptop A
python3 cpu.py
pipedpeer run cpu.py
pipedpeer run gpu.py
pipedpeer run --ddp 1 gpu.py
pipedpeer run --ddp 2 gpu.py
pipedpeer run --ddp 2 --ddp-sync fast gpu.py
pipedpeer run both.py
pipedpeer run --remote gpu.py --epochs 5
pipedpeer run cpu.py --items 600
pipedpeer status
pipedpeer ui
pipedpeer doctor
pipedpeer jobs --export-all ~/pp-runs
```

## Accounts

The invite code carries the cluster secret; there are no passwords.

| Role | Login | Password | What it sees |
|---|---|---|---|
| Laptop A (inviter, RTX 3060, Fedora) | the user's laptop; creates the secret at `pipedpeer setup` | — | every machine in `pipedpeer status`, `pipedpeer ui`, `pipedpeer dashboard` |
| Laptop B (joiner, RTX 3050, Fedora) | the friend's laptop; secret from laptop A's `pipedpeer connect <CODE>` line | — | the same cluster; its own runs in `pipedpeer jobs`, every machine's runs in the web page |
| College PC (optional, Ubuntu, Intel graphics) | joins with the same `<CODE>` | — | the cluster; takes CPU work only |
| Phone (optional, S20 FE 5G, Termux) | joins with the same `<CODE>` | — | pool-only worker: takes Pool/joblib chunks, never whole jobs |

## Pass 1 — check these

### `pipedpeer setup` / `pipedpeer invite` / `pipedpeer connect` — laptop A, laptop B
new · worker: U22 demo kit
1. [ ] Laptop A: run the install line → expect the installer to end with `✓ cluster created. On each other machine, run:`, the line `pipedpeer connect pp1-…`, and `Next: run 'pipedpeer invite' on the first machine; …`.
2. [ ] Laptop A: `pipedpeer invite` → expect exactly one line `pipedpeer connect pp1-…`.
3. [ ] Laptop B: install, then `pipedpeer connect <CODE>` → expect `✓ joining through <addresses>`, `✓ connected: 1 other machine(s): <laptop A>`, `✓ building the common environment in the background …`; `echo $?` → `0`.
4. [ ] Laptop B: `pipedpeer connect pp1-garbage` → expect `✗ not connected: invite code is damaged (copied wrongly or cut short)` and `ACTION NEEDED: copy the whole \`pipedpeer connect ...\` line from \`pipedpeer invite\` again`; `echo $?` → `1`.
5. [ ] Either: `pipedpeer status` → expect both laptops listed `healthy`, laptop B's GPU column shows the RTX 3050.

### Autostart after reboot — laptop A, laptop B
new · worker: U22 demo kit
1. [ ] Answer yes to "start at boot" during setup (or run `pipedpeer start --boot`) → expect no error.
2. [ ] Reboot laptop B, log in, open a terminal → `pipedpeer status` shows `daemon running` and both laptops `healthy` without any command typed.
3. [ ] `pipedpeer stop --boot`, reboot → expect `pipedpeer status` to say the daemon is not running; `pipedpeer start --boot` turns it back on.

### `pipedpeer run cpu.py` — laptop A (+ laptop B working)
changed: plain output by default, honest exit code, work spreads · worker: U22 demo kit
1. [ ] `python3 cpu.py` → expect four result lines and `TOTAL <s>`; note it.
2. [ ] `pipedpeer run cpu.py` → expect the same lines (`Pool: 128 items, checksum 3839999232`, `joblib: 64 Mandelbrot tiles, 217451260 points inside`, …), no `[1/7]`/`[coordinator]`/`[pipedpeer]` lines, a lower TOTAL, then `pipedpeer: <s> s total — script <s> s, pipedpeer <s> s — ran on <laptop A> N%, <laptop B> M%`.
3. [ ] `echo $?` → `0`. A script ending in `raise SystemExit(3)` run through `pipedpeer run` → `echo $?` gives `3`.
4. [ ] `pipedpeer jobs last --steps` → expect chunk steps on both machines.

### `pipedpeer run gpu.py` — automatic one GPU vs split, then `--ddp 1` — laptop A, laptop B
changed: the ring falls back to one GPU when it measures slower · worker: U22 demo kit
1. [ ] `pipedpeer run gpu.py` → expect `pipedpeer: one machine is faster alone: <machine> takes about N ms a step by itself against M ms split across 2 machines, so it trains on alone from the next epoch. To keep the split: --ddp 2` (or no line at all if no ring could win from the start), then `epoch 0..2` and `validation loss …`.
2. [ ] `pipedpeer run --ddp 1 gpu.py` → expect a plain single-GPU run; TOTAL close to step 1's.
3. [ ] `pipedpeer run --ddp 2 gpu.py` → expect it to stay split (no "faster alone" line) and be clearly slower than step 2.
4. [ ] Validation loss of all three within a few percent of each other.

### `pipedpeer run --ddp 2 --ddp-sync fast gpu.py` — laptop A, laptop B
new · pending a decision on outer optimizer defaults · worker: U22 demo kit
1. [ ] Run it → expect faster than `--ddp 2` without `--ddp-sync fast` from the previous item.
2. [ ] Expect `validation loss` within ~1% of the `--ddp 1` run.
3. [ ] `pipedpeer run --ddp-sync wrong gpu.py` → expect `--ddp-sync must be exact or fast, got "wrong"`.

### `pipedpeer run both.py` — laptop A, laptop B
new · worker: U22 demo kit
1. [ ] Run it → expect `CPU phase: 256,000 rows x 48 features, …` faster than under `python3 both.py`.
2. [ ] Expect `GPU phase on: NVIDIA GeForce RTX 3060 …` and `GPU phase: validation loss …`.
3. [ ] Expect the `ran on …` line to name both laptops.

### `pipedpeer run --remote gpu.py --epochs 5` — laptop A → laptop B
changed: script flags pass through; a missing script fails before placement · worker: U22 demo kit
1. [ ] Run it → expect `using GPU: NVIDIA GeForce RTX 3050 …` and `epoch 0..4` (the `--epochs 5` reached the script).
2. [ ] `pipedpeer run --remote --gpu force gpu.y` → expect an immediate "file not found"-style error, no placement on any machine, non-zero exit.

### Failover: Wi-Fi off mid-run — laptop A, laptop B
changed: lost machine's work re-runs elsewhere · worker: U22 demo kit
1. [ ] Laptop A: `pipedpeer run cpu.py --items 600`; ~10 s in, switch laptop B's Wi-Fi off → expect the run to keep going.
2. [ ] Expect the Pool checksum to equal a run where nobody left (run `python3 cpu.py --items 600` once to know it); exit `0`.
3. [ ] Switch Wi-Fi back on → within ~30 s `pipedpeer status` on laptop A shows laptop B again, no command typed.

### IP change: switch networks — laptop A, laptop B
changed: peers keep every known address and switch to the one that works · worker: U22 demo kit
1. [ ] Both on the same Wi-Fi, `pipedpeer status` healthy. Move laptop B to the phone hotspot (laptop A stays) → expect laptop B back in laptop A's `pipedpeer status` within ~30 s with no command, one row only.
2. [ ] Move laptop B back → expect the same, still one row, no `unknown`/`unreachable` leftovers.
3. [ ] `pipedpeer run cpu.py` after the move → expect work on both machines.

### `pipedpeer ui` — laptop A (runs from both machines)
changed: no token prompt; runs from every machine; run steps view · worker: U22 demo kit
1. [ ] `pipedpeer ui` → browser opens `http://127.0.0.1:4242/ui…` and shows the nodes, no token form; the address bar no longer shows `#token=`.
2. [ ] Expect recent runs from laptop A and from laptop B (run anything on B first), each with its origin.
3. [ ] Open the `cpu.py` run → expect per-machine step bars and the slowest chunks.
4. [ ] `pipedpeer ui --print` → prints the address only, opens nothing.

### `pipedpeer doctor` — laptop A, laptop B, PC
new · worker: U22 demo kit
1. [ ] On a healthy laptop → every line `✓` (daemon running, cluster secret set, introducer answering, peers healthy, firewall, user namespaces, crun, nix, `NVIDIA GPU and driver present`, `<N> GiB free at /nix`); `echo $?` → `0`.
2. [ ] `pipedpeer stop`, `pipedpeer doctor` → `✗ daemon not running — fix: pipedpeer start`, last line `Error: doctor found problems (the ✗ lines above, each with its fix)`; exit `1`. Then `pipedpeer start`.
3. [ ] PC on a different lab subnet than the laptops (if available) → a `✗` line naming the unreachable peer or `TCP blocked but UDP passes`.

### `pipedpeer uninstall` on a college PC — PC
new · worker: U22 demo kit
1. [ ] `pipedpeer uninstall --dry-run` → `would stop the daemon and remove its user service`, `would close pipedpeer's firewall ports`, `would remove the root parts (…)`, `would delete …` for the three data dirs and the binary; removes nothing.
2. [ ] `pipedpeer uninstall --yes` → expect it to end with `Nix was kept: other software may use it. To remove it too, follow https://nix.dev/…`; `command -v pipedpeer` prints nothing.
3. [ ] `ls ~/.local/share/pipedpeer ~/.local/state/pipedpeer ~/.cache/pipedpeer` → all missing; `systemctl --user status pipedpeer` → unit not found.
4. [ ] Laptop A `pipedpeer status` → the PC stops showing within a few minutes.

### Phone worker (Termux) — phone, laptop A
new · worker: phone worker (Termux)
1. [ ] Run the two `adb shell` lines → no output, no error.
2. [ ] In Termux: `pkg install -y python python-numpy curl && termux-wake-lock`, then the install line (takes a long while: Debian, Nix and ~1.5 GB of Python packages; prints `✓ Debian with Nix ready` and `✓ pool python ready`), then `pipedpeer connect <CODE>` → `✓ connected: …`.
3. [ ] Laptop A: `pipedpeer status` → the phone is listed as healthy.
4. [ ] Laptop A: `pipedpeer run cpu.py` → the checksum matches `python3 cpu.py`, and the phone shows in the `ran on …` line. If it doesn't, on the phone run `grep refusing ~/.local/state/pipedpeer/daemon.log` and note the output.
5. [ ] Laptop A: `pipedpeer run --remote gpu.py` → never placed on the phone.
6. [ ] Laptop A, a small training chunk per seed:
   ```bash
   cat > phone_train.py <<'EOF'
   import multiprocessing, torch
   def train(seed):
       torch.manual_seed(seed)
       m = torch.nn.Sequential(torch.nn.Linear(8, 16), torch.nn.ReLU(), torch.nn.Linear(16, 1))
       opt = torch.optim.SGD(m.parameters(), lr=0.05)
       x = torch.randn(256, 8); y = x.sum(1, keepdim=True)
       for _ in range(300):
           opt.zero_grad(); loss = torch.nn.functional.mse_loss(m(x), y); loss.backward(); opt.step()
       return round(float(loss), 4)
   if __name__ == "__main__":
       with multiprocessing.Pool() as p:
           print(p.map(train, range(32)))
   EOF
   pipedpeer run phone_train.py
   ```
   → 32 small losses (all below 0.5), and the phone shows in the `ran on …` line.

### Phone as a full node — phone
new · numbers go to the paper · worker: phone full node (proot + Nix)
1. [ ] Phone, in Termux: `pipedpeer doctor` → a `✓ phone: full node (Debian in proot with Nix)` line and the chunks-only reason line (see Pass 3, "A phone takes chunks, never whole jobs").
2. [ ] Phone, in Termux: `cd ~ && cp ~/pipedpeer-demo/cpu.py . 2>/dev/null || curl -fsSLO https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/pipedpeer-demo/cpu.py`, then `pipedpeer run cpu.py` → superseded: whole jobs from the phone are not run on it; see Pass 3 (it goes to a laptop, or fails with `arch mismatch`).
3. [ ] Phone, same file, plain Termux Python: `pkg install -y python-numpy` if needed, then `time python cpu.py` three times → write down the three `real` times.
4. [ ] Phone, same file inside proot with Nix's Python: `time proot-distro login debian -- /root/.local/share/pipedpeer/phone-python/bin/python3 $PWD/cpu.py` three times → write down the three `real` times.
5. [ ] Phone, in Termux: `pipedpeer stop && pipedpeer start && pipedpeer status` → the daemon is running again and the command prompt came back after each. (The `view.py` whole-job sandbox view no longer applies: phones take chunks only.)

### Tailscale worst case + `pipedpeer jobs --export-all` — laptop A, laptop B on different networks
new · numbers go to the paper · worker: U22 demo kit
1. [ ] Laptop A on home Wi-Fi, laptop B on the phone hotspot, both `tailscale up`; laptop A: `pipedpeer invite` (after Tailscale is up), laptop B: `pipedpeer connect <CODE>` → `✓ connected: 1 other machine(s): …`.
2. [ ] Laptop A: `pipedpeer run cpu.py`, `pipedpeer run both.py`, `pipedpeer run --ddp 1 gpu.py`, `pipedpeer run --ddp 2 gpu.py`, `pipedpeer run --ddp 2 --ddp-sync fast gpu.py` → each ends with exit `0`; write down every TOTAL.
3. [ ] `pipedpeer jobs --export-all ~/pp-runs` on both laptops → CSV and JSON per run in `~/pp-runs`; send both folders to the user.

### Kept checks from earlier rounds — laptop A, laptop B (PC for the busy case)
changed: requeue is bounded; Ctrl+C during a build · worker: U22 demo kit
1. [ ] Slow helper raced locally: throttle laptop B (`sudo tc qdisc add dev <wifi> root netem delay 300ms`), `pipedpeer run cpu.py` → result correct, first result wins; remove with `sudo tc qdisc del dev <wifi> root`. A script killed by a signal (`import os, signal; os.kill(os.getpid(), signal.SIGKILL)`) through `pipedpeer run --remote` → fails after 3 attempts, non-zero exit.
2. [ ] DDP rejoin: `pipedpeer run --ddp 2 gpu.py --epochs 30`, stop laptop B's Wi-Fi for one epoch, turn it back on → no NameError, B rejoins at a later epoch; `--ddp 2` prints one usage line per rank's machine with `-v`.
3. [ ] `--remote` with every other machine busy (`pipedpeer run --detach --remote gpu.py --epochs 200` twice first): `pipedpeer run -v --remote gpu.py` → `No node has room right now (…) — queued (attempt N)` lines; places when room frees, or after 10 min fails with `no node took this job within 10m0s (…)`, exit `1` (no endless retry).
4. [ ] With two machines, `pipedpeer run --remote gpu.py --epochs 50` and stop laptop B's daemon mid-run → the CLI reruns it elsewhere or says the machine is gone, never hangs. `pipedpeer run --offload cpu.py` → laptop A's share in the `ran on …` line is 0%.
5. [ ] Ctrl+C during a cold environment build (`pipedpeer run -v` a script with a new import, Ctrl+C at the build step) → `cancelled`, `echo $?` → `130`, and `pgrep -af 'nix build|nix-store|narpack'` prints nothing. With `-v`, the `--ddp 2` start barrier shows `waiting for rank 1 on <machine> to be ready …` and later `ddp: waited N s at the start … (not counted as sync)`.

## Pass 2 — check these

### Phone as a full node: setup on a new kernel — phone
changed: builds its own proot on kernels 6.6+; scripts for the phantom-process killer · worker: phone full node (proot + Nix)
1. [ ] Phone, Termux: `curl -fsSLO https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/phone-phantom-off.sh && bash phone-phantom-off.sh` (Termux and Settings in split screen; Wireless debugging: pairing port, code, then connect port) → ends with `max_phantom_processes = 2147483647` and `phantom process killer off; Wireless debugging can be turned off again`. Or the two `adb shell` lines from Steps to run.
2. [ ] Phone: install line, then `pipedpeer connect <CODE>` → on a 6.6+ kernel (`uname -r`) the output says it builds a newer proot, then `✓ Debian with Nix ready`; no `cp: ... Function not implemented`.
3. [ ] Phone: `pipedpeer doctor` → `✓ phone: full node (Debian in proot with Nix)` and `! phone: takes pool and torch chunks only; whole jobs and DDP ranks need a nested proot sandbox, which this Termux's proot can't start`.
4. [ ] Laptop A: `pipedpeer status` → the phone is `healthy`.

### Phone takes torch chunks of a laptop's job — phone, laptop A
new · numbers go to the paper · worker: phone full node (proot + Nix)
1. [ ] Laptop A: run the `phone_train.py` from Pass 1 (`pipedpeer run phone_train.py`) the first time → still correct, finishes without the phone (it is building the job's environment).
2. [ ] Phone: `grep -i "not runnable" ~/.local/state/pipedpeer/daemon.log` → a `phone: not runnable here yet` line, not repeated per chunk.
3. [ ] When the phone's build finishes (minutes), laptop A: `pipedpeer run phone_train.py` again → 32 losses all below 0.5, and the phone is in the `ran on …` line.
4. [ ] `pipedpeer run -v phone_train.py` → the summary block at the end (`environment`, `ran on`, `this machine`, `on <machine>`, `total` rows) appears once.

### CPU-only machine gets CPU torch — college PC (or any machine without an NVIDIA GPU)
changed: no CUDA download · worker: CPU torch environment
1. [ ] PC: `pipedpeer run gpu.py` (any torch script) on a fresh cache → the download is hundreds of MB, not ~4 GB; `du -sh /nix/store` grew by about 1.3 GiB, not 6.
2. [ ] Script prints a CPU device (no `using GPU`) and finishes with exit `0`.
3. [ ] Laptop A: `pipedpeer run cpu.py` still spreads to the PC and the laptops (smoke).

### A slow peer stops slowing runs — laptop A, laptop B throttled
changed: a slow machine's share moves to a faster one · worker: slow-peer learning
1. [ ] Throttle laptop B (`sudo tc qdisc add dev <wifi> root netem delay 300ms`), run `pipedpeer run cpu.py` four times → TOTAL of runs 2 to 4 is no worse than run 1 and close to laptop A alone; checksum equal each time.
2. [ ] Remove it: `sudo tc qdisc del dev <wifi> root`; `pipedpeer run cpu.py` → work on both laptops again.

### A run on a slow link starts quickly — laptop A, laptop B on the hotspot
changed: start within ~60 s on a slow link · worker: slow-link start
1. [ ] Laptop B on the phone hotspot, a script with a new import on laptop A: `pipedpeer run -v --remote gpu.py` (or `run cpu.py`) → the first epoch or result line shows within about a minute of the start (note the seconds).
2. [ ] A long environment build (new import, `-v`) → no `could not renew lease … for 2 min (<error>); the node frees this run's slot after 5 min without one` warning, and the run is not re-placed; `pipedpeer jobs last` shows one placement.

### `--ddp-sync` auto, fast and exact — laptop A, laptop B
changed: fast sync is chosen only when measured faster; exact pins · worker: DDP fast sync
1. [ ] `pipedpeer run --ddp 2 gpu.py` → at most one line `ddp: training with DiLoCo, sync every <N> steps: about <X> ms a step against <Y> ms for the exact split and <Z> ms on one rank alone (--ddp-sync exact for per-step sync)`; validation loss within ~1% of the `--ddp 1` run.
2. [ ] `pipedpeer run --ddp 2 --ddp-sync exact gpu.py` → no switch line, same result as Pass 1.
3. [ ] `pipedpeer run --ddp 2 --ddp-sync fast gpu.py` → faster than step 2; loss within ~1%.
4. [ ] `pipedpeer run --ddp-sync wrong gpu.py` → `--ddp-sync must be exact or fast, got "wrong"` (smoke).

### Real 2-laptop run (for the paper) — laptop A (yours), laptop B (your friend's)
new · numbers go to the paper · worker: real-run instructions
Both laptops: `tailscale up`, same build `<VERSION>`.

Laptop A (yours), paste:
```bash
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
pipedpeer invite        # send the printed `pipedpeer connect pp1-…` line to your friend
```
Laptop B (your friend's), paste after receiving the line:
```bash
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <VERSION>
pipedpeer connect <CODE>
pipedpeer status
```
Laptop A, in `pipedpeer-demo/` once `pipedpeer status` lists both:
```bash
pipedpeer run cpu.py
pipedpeer run --ddp 1 gpu.py
pipedpeer run --ddp 2 --ddp-sync exact gpu.py
pipedpeer run --ddp 2 --ddp-sync fast gpu.py
pipedpeer run -v both.py        # a new import on a cold environment: do this one first on a fresh machine
pipedpeer jobs --export-all ~/pp-real-run
```
1. [ ] Both: `pipedpeer status` → both `healthy`.
2. [ ] Laptop A: every command above ends with exit `0`; write down each TOTAL.
3. [ ] Laptop A: `ls ~/pp-real-run` → a CSV and JSON per run.
4. [ ] Send the folder `~/pp-real-run` to the user (main session).

## Pass 3 — check these

### Play-Store Termux installs and runs — phone (Termux from the Play Store)
changed: an Android build of the binary runs through linker64 · worker: Play-Store Termux
1. [ ] Phone, Termux from the Play Store: run the install line from Steps to run (`--version <VERSION>`) → it picks the Android asset and ends without `unexpected e_type`.
2. [ ] `pipedpeer connect <CODE>` → `✓ connected: …`; `pipedpeer status` on laptop A lists the phone `healthy`.
3. [ ] Smoke: F-Droid Termux (if you have it) still installs the same way.

### A phone takes chunks, never whole jobs — phone, laptop A
changed: phone refuses whole jobs and DDP ranks, says why · worker: phone chunks only
1. [ ] Phone, Termux: `pipedpeer doctor` → `phone: takes pool and torch chunks only; whole jobs and DDP ranks need a nested proot sandbox, which this Termux's proot can't start` (shown as a `!` line).
2. [ ] Phone, Termux: `pipedpeer run cpu.py` (from `pipedpeer-demo/`) → either `[coordinator] this machine is a … node and takes no jobs; placing the run on another node...` and a run on a laptop with the checksum of `python3 cpu.py`, or a plain failure naming `arch mismatch: closure aarch64-linux, node x86_64-linux` (whole jobs from the phone are parked); never a run on the phone.
3. [ ] Laptop A: `pipedpeer run phone_train.py` (Pass 1 file) → the phone is in the `ran on …` line.

### `--ddp 2` across two networks — laptop A, friend's laptop B over Tailscale
changed: rank 1 reaches the lead through its own forwarder · worker: internet-peer DDP
1. [ ] Laptop A and B on different networks, connected (Real 2-laptop run steps): `pipedpeer run --ddp 2 --ddp-sync exact gpu.py` → both ranks train (no `No route to host`), exit `0`.
2. [ ] `pipedpeer jobs last --steps` → steps on both machines.
3. [ ] A script that calls `init_process_group` itself with `--ddp 2` → `native torch.distributed needs a direct network path; <host> is reached over the internet link` and it runs on one node.

### Auto sync under `--ddp 2` — laptop A, laptop B
changed: auto switches to fast under a fixed ring too · worker: auto DiLoCo
1. [ ] `pipedpeer run --ddp 2 gpu.py` over the two networks → one line `ddp: training with DiLoCo, sync every <N> steps: about <X> ms a step against <Y> ms for the exact split and <Z> ms on one rank alone (--ddp-sync exact for per-step sync)`; TOTAL clearly below the `--ddp 2 --ddp-sync exact` run.
2. [ ] `pipedpeer run --ddp 2 --ddp-sync exact gpu.py` → no such line.
3. [ ] Validation loss of step 1 within ~1% of `--ddp 1`.

### An auto run that stays on one node says why — laptop A
new · worker: auto single-node reason
1. [ ] With laptop B stopped (`pipedpeer stop` on B) or too small: `pipedpeer run gpu.py` → `pipedpeer: DDP: <N> rank(s) fit, running on one node; turned down: <reason per machine>` (or `[coordinator] DDP: no other node can take a rank — running single-node` with `-v`).
2. [ ] Start B again → the same command spreads or prints its other reason; never silent.

### A Pool job over its memory — laptop A
changed: plain out-of-memory message; torch Pools size to the cap · worker: memory cap
1. [ ] `pipedpeer run phone_train.py` on laptop A alone → 32 losses below 0.5, exit `0` (it died with 137 before).
2. [ ] A Pool script holding ~2 GB per worker with `pipedpeer run --mem 1G big.py` → `[pipedpeer] out of memory: this node's daemon and its jobs used <X> of its <Y> ceiling, so its memory watchdog killed pid <N> (<name>, <size>)`, then `job ran out of memory: its node killed it for exceeding its <limit> memory limit. Raise it with --mem …`; exit non-zero.
3. [ ] Same script with `--mem 8G` → completes.

### Cross-network status and doctor — laptop A, friend's laptop B (different networks)
changed: a learnt path shows as gossip; a stranger's LAN address is no longer an introducer · worker: two-NAT fixes
1. [ ] Laptop B: `pipedpeer connect <CODE>` from a different network than A, then `pipedpeer doctor` → no `✗` lines about an unreachable introducer; `echo $?` → `0`.
2. [ ] Laptop A: `pipedpeer status` → B's path column says `gossip` where it was learnt from another node, not `lan` (`lan` only for a machine on your own network).
3. [ ] Both: `pipedpeer run cpu.py` → work on both laptops (smoke).

### A `--ddp 2` run prints one "ran on" line — laptop A, laptop B
changed: the ranks' records merge · worker: two-NAT fixes
1. [ ] `pipedpeer run --ddp 2 gpu.py` → exactly one `ran on` line at the end, naming both machines.
2. [ ] `pipedpeer jobs last` → the machine A ran on is shown as `this machine` (by node id), B by its hostname, nowhere `127.0.0.1`.

### Drift line after an auto switch — laptop A, laptop B
changed: the drift reference is kept when auto switches after warm-up · worker: auto DiLoCo drift
1. [ ] `pipedpeer run -v --ddp 2 gpu.py` (auto sync) → after the switch line, `ddp: exact sync warm-up done after <N> steps (drift reference <X>); training locally, syncing every <H> steps`.
2. [ ] Expect no line containing `older build` or `reports no drift between ranks`.

### Uninstall on a phone — phone
changed: removes everything setup added, keeps your own packages · worker: phone uninstall
1. [ ] Termux: `pipedpeer uninstall --dry-run` → lists the `pipedpeer` container, the proot build, recorded pip and Termux packages, data/state/cache and the binary, and `would print how to turn Android's phantom-process killer back on (if you turned it off for pipedpeer), from a computer with adb:` with the adb lines; removes nothing.
2. [ ] `pipedpeer uninstall -y` → ends with `adb shell device_config delete activity_manager max_phantom_processes` and `adb shell settings put global settings_enable_monitor_phantom_procs true` (each shown as `  adb …`); then `proot-distro list` shows no `pipedpeer` container and `ls ~/.local/share/pipedpeer` is missing; your own packages and a `debian` container of yours, if any, remain.
3. [ ] Install again, run `pipedpeer setup` twice, then `pipedpeer uninstall -y` → still removes everything (container, proot, packages setup added).
4. [ ] Laptop A: `pipedpeer status` → the phone stops showing within a few minutes.

## Passed

## Remaining
- The `--ddp-sync fast` (DiLoCo) outer optimizer defaults await the user's decision.
