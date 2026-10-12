# Card: LAPTOP-B (the friend's GPU laptop: events screen, second GPU)

Card URL: https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/cards/LAPTOP-B.md

Commands only, ready to copy: [LAPTOP-B.txt](LAPTOP-B.txt)
(`./demo/card.sh --commands LAPTOP-B`, or `curl -fsSLO https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/pipedpeer-demo/cards/LAPTOP-B.txt`).

Only this machine's actions, in order. The say/expect detail for each step is in
[DEMO-SCRIPT.md](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/DEMO-SCRIPT.md#the-flow).
Same in both layouts (three laptops on Wi-Fi, or a lab).

## 1. Install and prep (the day before)

0. If this laptop still holds the old rehearsal setup, put it back to normal
   first: [TODO-BEFORE-DEMO.md, section 2](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/TODO-BEFORE-DEMO.md).
   By hand: the NVIDIA driver should already be installed (`nvidia-smi` works);
   the sudo password when setup asks (Nix, firewall, key helper).
1. Get the demo folder (no git needed):
   `curl -L https://github.com/vinayakyadav2709/Pipedpeer/archive/refs/heads/dev.tar.gz | tar xz`
   then `cd Pipedpeer-dev/pipedpeer-demo`
2. `./demo/prep-0-install.sh` (installs the nightly over any older build, runs
   `setup -y`). Its `sha256:` line must match LAPTOP-A's.
3. After LAPTOP-A has set up: open its `join-command.txt` here (you sent it to yourself) and
   run the `gpu-laptop` line from it, or type the values:
   `./setup-machine.sh gpu-laptop --token <secret> --introducer <LAPTOP-A IP> --prep`
   It first connects to LAPTOP-A; if it can't, it says why (most likely the Wi-Fi
   blocks laptops from each other), how to fix it, and stops without joining. Then it
   joins (expect `connected (N peer(s))`) and runs prep-4 and prep-3 on this GPU (slow
   the first time: 1150–1261 s per machine in rehearsal). With no working NVIDIA
   driver it warns, says how to install one, and goes on: the laptop joins as CPU only.

## 2. On the day

- **T−30 min:** `pipedpeer status`, expect `daemon running (pid=… node-id=… port=38080)`.
  Not running: `pipedpeer start` (it rejoins by itself).
- **T−10 min:** second terminal: `pipedpeer events -f`. **Keep it up the whole demo.**
  Font large (Ctrl+Shift+=), ~100 columns.

| Step | You type | Expect (one line) | Then |
|---|---|---|---|
| 1 live join | nothing | events feed: `JOIN   <PC-3> is up (…)` | turn the screen to the room |
| 5 GPU | before it starts: `watch -n1 nvidia-smi` in a spare terminal | GPU-Util rises while the job trains, if the `ran on` line names this laptop | turn the screen round; close `watch` after step 5 |
| 8 failover | nothing; **never `pipedpeer stop` here** (only the presenter would be left) | events feed: `LEAVE  <PC-3> is gone`, later `JOIN   <PC-3> is up` | — |
| 9 receipts, layout 1 only | `pipedpeer traffic` | `[pool] received pool/map from <LAPTOP-A IP>:…: items=…` lines | hand back to LAPTOP-A |

In a lab (layout 2) PC-2 runs step 9's `traffic`, not this laptop.

Backup, only if asked: **B1** background a job, then attach; **B6** pandas
out-of-core `./demo/6-pandas.sh`; **B4a** two GPUs at once (this laptop is the
second GPU; never run on two real laptops yet). See DEMO-SCRIPT.md, "Backup".
