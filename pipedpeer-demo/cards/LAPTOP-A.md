# Card: LAPTOP-A (your laptop: presents, GPU, runs the rendezvous)

Card URL: https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/cards/LAPTOP-A.md

Commands only, ready to copy: [LAPTOP-A.txt](LAPTOP-A.txt)
(`./demo/card.sh --commands LAPTOP-A`, or `curl -fsSLO https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/pipedpeer-demo/cards/LAPTOP-A.txt`).

Only this machine's actions, in order. The say/expect detail for each step is in
[DEMO-SCRIPT.md](https://github.com/vinayakyadav2709/Pipedpeer/blob/dev/pipedpeer-demo/DEMO-SCRIPT.md#the-flow).
Same in both layouts (three laptops on Wi-Fi, or a lab).

## 1. Install and prep (the day before)

By hand: the sudo password when setup asks (Nix, firewall, key helper); the
NVIDIA driver must already be installed (`nvidia-smi` works). This machine
goes **first**, before every other.

1. Get the demo folder (no git needed):
   `curl -L https://github.com/vinayakyadav2709/Pipedpeer/archive/refs/heads/dev.tar.gz | tar xz`
   then `cd Pipedpeer-dev/pipedpeer-demo`
2. `./demo/prep-0-install.sh` (installs the nightly, runs `setup -y`).
   Write down its `sha256:` line; every machine must print the same one.
3. `./setup-machine.sh fedora-laptop --rendezvous` (no `--token`: this is the first
   machine). It adds `~/.local/bin` to PATH in `~/.bashrc`/`~/.zshrc` if missing, makes
   the secret, and starts `pipedpeer rendezvous` as the systemd user service
   `pipedpeer-rendezvous`: no terminal to keep open, and re-running starts no second one.
   It writes `join-command.txt` (mode 600, holds the secret: never commit it) with the
   other machines' exact lines. Paste it into a **secret gist** (gist.github.com) and open its
   Raw link on the others (`curl -fsSL <raw link>`); delete the gist and run `pipedpeer auth clear`
   after the demo (full steps: SETUP.md, "The first machine writes join-command.txt"). Or type the values
   (`pipedpeer auth show`, and your IP from `ip -4 route get 1.1.1.1`, after `src`).
   If it notes that logging out stops the service: stay logged in (locking the screen
   is fine), or run `loginctl enable-linger`. Stop it after the demo with
   `systemctl --user disable --now pipedpeer-rendezvous`.
4. `pipedpeer join <your IP>`. Expect `no peers found` and "This is normal for
   the first machine": the others join after you.
5. `./demo/prep-4-common.sh`, then `./demo/prep-3-gpu.sh` (slow the first time:
   the common environment took 1150–1261 s per machine in rehearsal).
6. When LAPTOP-B (and in a lab, every PC except the last) has joined:
   `./demo/prep-2-rehearse.sh`. It writes `demo/rehearsal-results.txt`: the
   only speed numbers to quote. A step under 1.2× goes to "if asked".
7. `./demo/prep-1-check.sh` (after prep-2, never before). Expect `READY`. It also
   connects to every peer; a peer it can't reach prints how to fix that.
   Layout 1 (only LAPTOP-B joined): expect
   `warn  only 1 peer - the demo works, but say 'two machines'`.

## 2. On the day

- **T−30 min:** `pipedpeer status`, expect `daemon running (pid=… node-id=… port=38080)`.
  Not running: `pipedpeer start`. Check `systemctl --user status pipedpeer-rendezvous` says `active (running)`.
- **T−30 min:** `./demo/prep-1-check.sh`, expect `READY` with the same hash as at prep.
- **T−10 min:** second terminal: `pipedpeer dashboard`. **Keep it up the whole demo.**
  Then `./demo/1-cluster.sh` once. Font large (Ctrl+Shift+=), ~100 columns.

| Step | You type | Expect (one line) | Then |
|---|---|---|---|
| 0 the problem | nothing: talk | — | — |
| 1 live join | `pipedpeer auth show`; read the token and your IP out to PC-3 | after PC-3 joins, the dashboard gets a new row and `JOIN   <PC-3> is up (…)` under EVENTS | turn the dashboard to the room |
| 2 the cluster | `./demo/1-cluster.sh` | one row per machine, all `healthy`; the `*` row is this laptop | — |
| 3 headline | `./demo/2-headline.sh` | `checksum 4319999744` in both halves, then the `this machine only / across the cluster / speedup` box | point at the dashboard: every CPU column rises |
| 4 cold vs cached | nothing (PC-2 types) | — | keep the dashboard up |
| 5 GPU | before it starts: `watch -n1 nvidia-smi` in a spare terminal | GPU-Util rises while the job trains, if the `ran on` line names this laptop | turn the screen round; close `watch` after step 5 |
| 6 everything | nothing (PC-2 types) | — | — |
| 7 teacher's script | nothing, or run `./demo/11-teacher.sh their.py [data files]` here if handed the script | `CHECK their.py — nothing will run` … `checks complete`, then the run | — |
| 8 failover | `./demo/9-failover.sh` | when the first `[pipedpeer] remote ok:` line appears, tell PC-3 "stop now"; expect the same `checksum 4319999744` | PC-3 runs `pipedpeer start` afterwards |
| 9 receipts | `./demo/10-proof.sh` | `== Timeline ==` with `script` the longest bar, then `== Receipt ==` | hand to PC-2 (layout 1: LAPTOP-B) for `pipedpeer traffic` |

Backup, only if asked (all on this laptop): **B3** Ctrl+C stops everything,
**B5** finale `./demo/13-finale.sh`, **B7** capstone `./demo/14-capstone.sh`.
See DEMO-SCRIPT.md, "Backup".
