# Card: LAPTOP-C (laptop without a GPU, layout 1: plays PC-1, PC-2 and PC-3)

Card URL: https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/cards/LAPTOP-C.md

Commands only, ready to copy: [LAPTOP-C.txt](LAPTOP-C.txt)
(`./demo/card.sh --commands LAPTOP-C`, or `curl -fsSLO https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/pipedpeer-demo/cards/LAPTOP-C.txt`).

Only this machine's actions, in order. The say/expect detail for each step is in
[DEMO-SCRIPT.md](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/DEMO-SCRIPT.md#the-flow).
Layout 1 (three laptops on Wi-Fi). In a lab, this laptop counts as one more PC:
use [PC.md](PC.md) instead.

## 1. Install and prep (the day before)

This laptop joins **live** in step 1, so it stays unjoined until then.

By hand: the sudo password when setup asks (Nix, firewall).

1. Get the demo folder (no git needed):
   `curl -L https://github.com/vinayakyadav2709/Pipedpeer/archive/refs/heads/dev.tar.gz | tar xz`
   then `cd Pipedpeer-dev/pipedpeer-demo`
2. `./demo/prep-0-install.sh` (installs the nightly, runs `setup -y`).
   Its `sha256:` line must match LAPTOP-A's.
3. `./demo/prep-4-common.sh` **only** (slow the first time: 1150–1261 s per
   machine in rehearsal).
4. **Do not** run `setup-machine.sh` or `pipedpeer join` (either joins, and step 1
   loses its first join). Do not run
   `prep-3-gpu.sh` (with no GPU it needs the GPU laptops as peers). Do not join
   and then `stop`: a stopped machine rejoins with no countdown.
5. Check this laptop reaches LAPTOP-A:
   `timeout 2 bash -c ': </dev/tcp/<LAPTOP-A IP>/38080' && echo reachable`. No
   `reachable`: [SETUP.md, "Can't reach LAPTOP-A"](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/SETUP.md#cant-reach-laptop-a).
6. Keep LAPTOP-A's `join-command.txt` (sent to yourself) at hand: its `0-join.sh` line is your
   step 1 command, token included. Or type the values.

## 2. On the day

- **T−30 min:** `pipedpeer status`, expect `daemon running (pid=… node-id=… port=38080)`.
  Not running: `pipedpeer start`. Font large (Ctrl+Shift+=), ~100 columns.
- Before step 6: run `pipedpeer nodes` after step 1 and read this laptop's
  MEM FREE. Under about 3.1 GiB, use the `--remote` line in step 6.

| Step | You type | Expect (one line) | Then |
|---|---|---|---|
| 1 live join | `./demo/0-join.sh <LAPTOP-A IP> <token>` (the last line of LAPTOP-A's `join-command.txt`; or the token from `pipedpeer auth show` there) | `waiting for peers · 90 s left · 0 found`, then `connected (N peer(s))` and a node table | hand back to LAPTOP-A |
| 4 cold vs cached | `./demo/2b-cold.sh` | second half: `Closure already on the node; nothing to send`; last line `first run (cold): … every run after: …` | hand back |
| 5 GPU from no GPU | `./demo/7-gpu.sh` | `using GPU: NVIDIA …`, and `ran on` names a GPU laptop | point at the two usage lines: `this machine` stays near idle |
| 6 everything | `./demo/12-everything.sh` (MEM FREE under 3.1 GiB: `pipedpeer run 07_everything.py --remote`) | `section pool …` through `section torch …`, then `wrote results/everything.txt` | hand back |
| 7 teacher's script | `pipedpeer run 00_pool.py --remote --check-only` | `CHECK 00_pool.py — nothing will run` … `checks complete` | then their script on any machine: `./demo/11-teacher.sh their.py [data files]` |
| 8 failover | when LAPTOP-A shows the first `[pipedpeer] remote ok:` line: `pipedpeer stop` | LAPTOP-A still prints `checksum 4319999744` | afterwards: `pipedpeer start` (rejoins with no arguments) |

Step 9's `pipedpeer traffic` runs on LAPTOP-B in this layout, not here.

Backup, only if asked: **B2** the sender loses its network (pull the Wi-Fi
mid-run), **B8** `--offload`. See DEMO-SCRIPT.md, "Backup".
