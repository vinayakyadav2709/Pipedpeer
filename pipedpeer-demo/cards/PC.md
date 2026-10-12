# Card: PC (college Ubuntu PC, layout 2: plays PC-1, PC-2 or PC-3)

Card URL: https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/cards/PC.md

Commands only, ready to copy: [PC.txt](PC.txt)
(`./demo/card.sh --commands PC`, or `curl -fsSLO https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/pipedpeer-demo/cards/PC.txt`).

Only this machine's actions, in order. The say/expect detail for each step is in
[DEMO-SCRIPT.md](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/DEMO-SCRIPT.md#the-flow).
Layout 2 (a college lab, PCs on ethernet, our laptops on Wi-Fi).

**Which role this PC plays:**

| PCs in the lab | first PC | second PC | third PC |
|---|---|---|---|
| 3 | PC-1 | PC-2 | PC-3 |
| 2 | PC-1 and PC-2 | PC-3 | — |
| 1 | PC-1, PC-2 and PC-3 | — | — |

**PC-3 (the last PC) joins live in step 1.** It is prepped differently: see 1b.

## 1a. Install and prep: a PC that is not PC-3 (the setup visit)

By hand: the sudo password when setup asks (Nix, crun, firewall, key helper).

1. Get the demo folder (no git needed):
   `curl -L https://github.com/vinayakyadav2709/Pipedpeer/archive/refs/heads/dev.tar.gz | tar xz`
   then `cd Pipedpeer-dev/pipedpeer-demo`
2. `./demo/prep-0-install.sh` (installs the nightly, runs `setup -y`).
   Its `sha256:` line must match LAPTOP-A's.
3. After LAPTOP-A has set up: copy its `join-command.txt` here by pen drive and
   run the `ubuntu-pc` line from it, or type the values:
   `./setup-machine.sh ubuntu-pc --token <secret> --introducer <LAPTOP-A IP> --prep`
   It first connects to LAPTOP-A; if it can't, it says how to fix it and stops
   without joining. Then it joins (expect `connected (N peer(s))`), then runs prep-4 (slow the first
   time: 1150–1261 s per machine in rehearsal). Join by IP: Wi-Fi and ethernet
   are different subnets, which discovery never crosses.
4. `./demo/prep-3-gpu.sh` (this PC sends the GPU or "everything" step).
   On a PC with 8–16 GB expect `ok` for both halves and no `GPU PREP FAILED`.
5. **Ubuntu 24.04 and later (AppArmor):** `pipedpeer setup` applies the AppArmor
   fix itself. Check: `pipedpeer run 00_pool.py 2>&1 | grep unisolated || echo "sandboxed: ok"`.
   If a job still prints `this node cannot create a user namespace, so jobs run unisolated`,
   run the fallback block in [PC.txt](PC.txt) (the one-file AppArmor profile, then
   `pipedpeer stop; pipedpeer start`; the machine-wide sysctl only if that fails). See
   [SETUP.md, "AppArmor"](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/SETUP.md#apparmor-ubuntu-2404-and-later).
6. Peers missing: `sudo ufw status` should list 38080/tcp, 38099/udp, 38447/udp
   (SETUP.md, "Firewall").

## 1b. Install and prep: PC-3 (or a lone PC)

By hand: the sudo password when setup asks.

1. Get the demo folder, as in 1a step 1.
2. `./demo/prep-0-install.sh`, as in 1a step 2 (`sha256:` must match).
3. `./demo/prep-4-common.sh` **only**.
4. **Do not** run `setup-machine.sh` or `pipedpeer join` (either joins, and step 1
   loses its first join), and not `prep-3-gpu.sh`. Do not join and then `stop`:
   a stopped machine rejoins with no countdown.
5. AppArmor and firewall as in 1a steps 5–6. Check this PC reaches LAPTOP-A:
   `timeout 2 bash -c ': </dev/tcp/<LAPTOP-A IP>/38080' && echo reachable`. No
   `reachable`: [SETUP.md, "Can't reach LAPTOP-A"](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/SETUP.md#cant-reach-laptop-a).
6. Keep LAPTOP-A's `join-command.txt` (pen drive) at hand: its `0-join.sh` line is your step 1
   command, token included. Or type the values.

## 2. On the day

- **T−30 min:** `pipedpeer status`, expect `daemon running (pid=… node-id=… port=38080)`.
  Not running: `pipedpeer start` (it rejoins by itself; PC-3 is not joined yet).
  Font large (Ctrl+Shift+=), ~100 columns.
- **PC-2, before step 6:** read this PC's MEM FREE in `pipedpeer nodes`. Under
  about 3.1 GiB, use the `--remote` line in step 6.

Do only the rows for your role(s):

| Step | Role | You type | Expect (one line) | Then |
|---|---|---|---|---|
| 1 live join | PC-3 | `./demo/0-join.sh <LAPTOP-A IP> <token>` (the last line of LAPTOP-A's `join-command.txt`; or the token from `pipedpeer auth show` there) | `waiting for peers · 90 s left · 0 found`, then `connected (N peer(s))` and a node table | hand back to LAPTOP-A |
| 4 cold vs cached | PC-2 | `./demo/2b-cold.sh` | second half: `Closure already on the node; nothing to send`; last line `first run (cold): … every run after: …` | hand back |
| 5 GPU from no GPU | PC-1 | `./demo/7-gpu.sh` | `using GPU: NVIDIA …`, and `ran on` names a GPU laptop | point at the two usage lines: `this machine` stays near idle |
| 6 everything | PC-2 | `./demo/12-everything.sh` (MEM FREE under 3.1 GiB: `pipedpeer run 07_everything.py --remote`) | `section pool …` through `section torch …`, then `wrote results/everything.txt` | hand back |
| 7 teacher's script | PC-1 | `pipedpeer run 00_pool.py --remote --check-only` | `CHECK 00_pool.py — nothing will run` … `checks complete` | then their script on any machine: `./demo/11-teacher.sh their.py [data files]` |
| 8 failover | PC-3 | when LAPTOP-A shows the first `[pipedpeer] remote ok:` line: `pipedpeer stop` | LAPTOP-A still prints `checksum 4319999744` | afterwards: `pipedpeer start` (rejoins with no arguments) |
| 9 receipts | PC-2 | after LAPTOP-A's `./demo/10-proof.sh`: `pipedpeer traffic` | `[pool] received pool/map from <LAPTOP-A IP>:…: items=…` lines; empty means this PC took no pool work: use another PC | hand back to LAPTOP-A |

Backup, only if asked (PC-1): **B2** the sender loses its network (pull the
cable mid-run), **B8** `--offload`. See DEMO-SCRIPT.md, "Backup".
