# Setting up the machines

Draft. `<!-- V1 8e14b9b: <file> -->` marks output copied from rehearsal pass 1
(containers on one laptop; evidence in `~/Downloads/pipedpeer-work/evidence/v1/`).
`<!-- V2 31b5364: <file> -->` / `<!-- V2 cc56753: <file> -->` marks output from rehearsal
pass 2 (evidence in `~/Downloads/pipedpeer-work/evidence/v2/`).
`<!-- V3 9949ad0: <file> -->` marks output from the final re-check (same lab, evidence in
`~/Downloads/pipedpeer-work/evidence/v3/`).
`<!-- from rehearsal -->` marks a spot only the lab visit can fill.
Lines that a fix made after V3 changed were re-run in V4 (`<!-- V4 c567369: <file> -->`, in
`~/Downloads/pipedpeer-work/evidence/v4/`).

**The rehearsal lab is not the venue.** The lab "PCs" were containers with no `python3`,
so the demo scripts that call `python3` (`demo/7-gpu.sh`, `demo/prep-3-gpu.sh`) skip
there unless you give them one (V3 ran prep-3 inside `nix shell …#python3`); the college
PCs and our laptops have `python3`. The friend's real 2-GPU ring (RTX 3060 + RTX 3050)
is for later, when both laptops are together (user, 2026-10-12): in V3 the ring was placed on both laptops, then
the friend's laptop dropped off the network mid-ship and the run finished on one rank.
Until it is checked, the live GPU step is single-GPU and `--ddp 2` is backup only.

**Two venue layouts** (user, 2026-10-12): layout 1, three laptops on Wi-Fi and no
lab (likely; "Layout 1" below); layout 2, a college lab with 1–3 Ubuntu PCs on ethernet
plus our laptops ("Lab visit"). Which machine plays which role in each is in
DEMO-SCRIPT.md, "Machines and screens".

One script does each machine: `setup-machine.sh <role>` (install, `pipedpeer setup -y`,
`auth set`, join; it can be re-run). It reuses `demo/prep-*.sh` instead of copying them.
Each machine's commands alone, ready to copy, are in `cards/<ROLE>.txt`
(`./demo/card.sh --commands <ROLE>`).

**PATH is handled.** `prep-0-install.sh` and `setup-machine.sh` add
`export PATH="$HOME/.local/bin:$PATH"` to `~/.bashrc` and `~/.zshrc` (those that exist,
never twice) when `~/.local/bin` is not on PATH, and print `PATH:` lines saying so. The
`NOTE: add ~/.local/bin to PATH` in the V3 output below is from before that change.
In the terminal you ran them from, run that export once, or open a new terminal.

**The first machine writes `join-command.txt`.** `setup-machine.sh` with no `--token`
writes `pipedpeer-demo/join-command.txt` (mode 600, git-ignored): the other machines'
exact lines, with the real secret and `--introducer <this machine's LAN IP>` (the `src`
of `ip -4 route get 1.1.1.1`), and the live-join `./demo/0-join.sh` line. It holds the
cluster secret: copy it by pen drive, never commit it. Or type the values.

**Every problem message has one shape:** what is wrong, numbered steps to fix it, how to
check, then "re-run this same command".

**Same binary everywhere.** `setup-machine.sh` installs the nightly only when no
`pipedpeer` is on PATH. The demo needs the current `dev` build, which the nightly channel
serves, so on each machine run `./demo/prep-0-install.sh` (no argument installs the
nightly; give it a path to install a binary you copied instead) (installs to
`~/.local/bin`, runs `setup -y`, prints a `sha256`; the hashes must match), then
`setup-machine.sh`. V3 ran prep-0 on pc1 and pc2 and `setup-machine.sh` on pc1, pc2
and pc4 (4–5 s each; Nix and crun were already there). prep-0 on pc1, then
`setup-machine.sh` on pc4
(<!-- V3 9949ad0: docs-SETUP-24-prep0-pc1.txt, docs-SETUP-24-setupmachine-pc4-fixed.txt -->):
```
$ ./demo/prep-0-install.sh /usr/local/bin/pipedpeer
==> installing /usr/local/bin/pipedpeer -> /root/.local/bin/pipedpeer
NOTE: add ~/.local/bin to PATH:  export PATH="$HOME/.local/bin:$PATH"
==> setup (nix, crun, identity, firewall, daemon)
…
  Setup complete.

binary: /root/.local/bin/pipedpeer
sha256: 7d4f26f15865e20c   <- must be identical on every machine

Next: ./join-machine.sh <introducer-ip> [token]   (box-a first, without a token)

$ ./setup-machine.sh ubuntu-pc --token <token>
==> [ubuntu-pc] install
already installed: /usr/local/bin/pipedpeer
==> [ubuntu-pc] setup (nix, crun, identity, firewall, daemon)
…
    firewall     ✓  none running; 38080/tcp, 38099/udp, 38447/udp, 38445/udp, 29500-29509/tcp reachable
…
  Setup complete.
==> [ubuntu-pc] cluster secret
<token>

Stored in /home/pp/.local/share/pipedpeer/auth_token (mode 0600).
Set the same token on every node:
  pipedpeer auth set <token>
Then restart each daemon. Until you do, this node still
answers unauthenticated requests.
==> [ubuntu-pc] restarting the daemon so it uses the secret
started daemon (node-id=cf072c2b, port=38080)
==> [ubuntu-pc] no --introducer: waiting on LAN auto-discovery (see 'pipedpeer nodes' in ~30 s)
Done. Check: pipedpeer status; pipedpeer nodes
```
The `restarting the daemon so it uses the secret` line is from 50bae3e (pc4 above). On
pc1 and pc2, run before that fix, the daemon kept the old secret and the laptop listed
them `unreachable` until `pipedpeer stop; pipedpeer start` there. FX-CLI2 makes
`pipedpeer auth set` restart the running daemon itself, as `join` does, so the
`Then restart each daemon` advice is gone. V4 ran it on pc2 and pc4 with no manual
restart; the output ended (<!-- V4 c567369: docs-SETUP-69-auth-set-restart.txt -->):
```
$ (pc4) pipedpeer auth set <token>
…
Set the same token on every node:
  pipedpeer auth set <token>
restarting the daemon so it uses the new secret
…
$ (host) pipedpeer nodes   # 40 s later
06b9a112*  yeet        healthy      0     15c 2%  6.7 GiB   RTX 3060 0% 41M/6.0G  self  self
4dc474f4   pc2         healthy      0     16c 1%  1.2 GiB   -                     lan   discovery
870179d7   pc4         healthy      0     16c 1%  1.1 GiB   -                     lan   discovery
```

## Per machine

| Machine | Role | Notes |
|---|---|---|
| College Ubuntu PC | `ubuntu-pc` | sudo is available (DECIDED), so the normal path: system Nix + crun. See "Firewall" and "AppArmor" below. |
| Your Fedora laptop | `fedora-laptop` | First machine: run with no `--token`, then read the secret with `pipedpeer auth show`. Add `--rendezvous`: runs `pipedpeer rendezvous` as the systemd user service `pipedpeer-rendezvous` (see "Rendezvous service"). |
| Friend's GPU laptop | `gpu-laptop` | Needs the NVIDIA driver (`nvidia-smi` works). Without one, `setup-machine.sh` prints how to install it (Fedora: RPM Fusion `akmod-nvidia`; Ubuntu: `ubuntu-drivers install`) and goes on: the laptop joins as CPU only. Runs `prep-3-gpu.sh` for the CUDA torch environment. The real 2-GPU ring with your laptop was never run in a rehearsal: check it in your own run (`./demo/7-gpu.sh` from a PC, both `nvidia-smi` screens). |

What `pipedpeer setup -y` printed on the Ubuntu stand-in (Debian + systemd, Determinate
multi-user Nix, user `pp` with sudo) (<!-- V1 8e14b9b: lab-up-logs/pc4-setup.log -->):
```
    nix-imports  ✗  missing
    → Installing pipedpeer's nix key helper, so the daemon can trust
      new peers' keys without a prompt:
    nix-imports  ✓  installed
  Daemon starting on :38080...  ✓
    firewall     ✓  none running; port 38447/udp is reachable
    router       ~  no port mapping offered
  Setup complete.
```
"no port mapping offered" is normal and irrelevant on one LAN.

### Firewall

`setup -y` opens every port the demo uses (`internal/setup/network.go` `openFirewall`, with
`ufw allow` or `firewall-cmd --permanent --add-port`, then prints the undo command):

| Port | Used for | Needed on |
|---|---|---|
| 38080/tcp | the daemon: jobs, pool work, `nodes` | every machine |
| 38099/udp | LAN auto-discovery broadcast | every machine |
| 38447/udp | direct links | every machine |
| 38445/udp | rendezvous | LAPTOP-A (opened everywhere) |
| 29500-29509/tcp | DDP | the GPU laptops (opened everywhere) |

V2 ran it on the Ubuntu stand-in with ufw active and default deny, as user `pp` with a
plain PATH (no `/usr/sbin`) (<!-- V2 cc56753: 96-c-ufw-plain-path.txt -->):
```
    firewall     ✓  opened 38080/tcp, 38099/udp, 38447/udp, 38445/udp, 29500-29509/tcp in ufw
                    Daemon API (token-authenticated), discovery, and signed-identity QUIC only.
                    Undo with: sudo ufw delete allow 38080/tcp && … && sudo ufw delete allow 29500:29509/tcp
…
  Setup complete.
$ ufw status
Status: active
…
38080/tcp                  ALLOW       Anywhere
38099/udp                  ALLOW       Anywhere
38447/udp                  ALLOW       Anywhere
38445/udp                  ALLOW       Anywhere
29500:29509/tcp            ALLOW       Anywhere
…
```
(Before dad7e42 the same run said `firewall ✓ none running` while ufw was active, because
`ufw` lives in `/usr/sbin`; 51-fxports-pc4-ufw.txt.) The firewalld path (Fedora) was not run
in a lab.

Check: `sudo ufw status` (Ubuntu) / `sudo firewall-cmd --list-ports` and `--get-active-zones`
(Fedora). If a firewall is active and peers still do not appear:
```bash
sudo ufw allow 38080/tcp && sudo ufw allow 38099/udp     # Ubuntu PCs (LAPTOP-A on ufw: also 38445/udp)
sudo ufw disable                                          # fallback (DECIDED), re-enable after the demo
sudo firewall-cmd --add-port=38080/tcp --add-port=38099/udp --add-port=38445/udp   # Fedora, until reboot
```
V1 did show that a join still succeeds with the rendezvous port blocked, through LAN
discovery (<!-- V1 8e14b9b: 70-firewall-join.txt -->: `connected (4 peer(s))`).

### Rendezvous service

`./setup-machine.sh fedora-laptop --rendezvous` runs `pipedpeer rendezvous` as the systemd
user service `pipedpeer-rendezvous` (unit file `~/.config/systemd/user/pipedpeer-rendezvous.service`,
`enable --now`). It needs no terminal of its own, survives closing the terminal and comes
back at the next login after a reboot. Re-running finds it active and starts no second one;
a rendezvous already running by hand in a terminal is refused with a message (stop it first).
Logging out stops it unless lingering is on: the script says so; stay logged in (a locked
screen is fine) or run `loginctl enable-linger`.
```
systemctl --user status pipedpeer-rendezvous         # is it up?
journalctl --user -u pipedpeer-rendezvous -n 20      # why not?
systemctl --user disable --now pipedpeer-rendezvous  # after the demo
```
<!-- not yet run on a real machine -->

### Can't reach LAPTOP-A

`setup-machine.sh … --introducer <IP>` first opens a TCP connection to `<IP>:38080`
(bash `/dev/tcp`, 2 s); `prep-1-check.sh` does the same to every peer. On failure it prints
what is wrong (most likely the Wi-Fi's client isolation), the fixes in order (on LAPTOP-A
`pipedpeer status`/`pipedpeer start`; re-run `./demo/prep-0-install.sh` there for the
firewall; move every machine to a phone hotspot or your own router and use LAPTOP-A's new IP
from `ip -4 addr`), the check, and stops without joining (prep-1-check: `NOT READY`).
`ping` is no longer the check: ICMP and TCP are filtered separately. By hand, from a
machine that does not run `setup-machine.sh` (LAPTOP-C, PC-3):
```
timeout 2 bash -c ': </dev/tcp/<LAPTOP-A IP>/38080' && echo reachable
```

### AppArmor (Ubuntu 24.04 and later)

Ubuntu 24.04+ restricts unprivileged user namespaces, which crun's sandbox needs. With the
system Nix that `setup -y` installs, jobs still **run, unisolated**, and every job prints
(`internal/daemonapi/execution.go`, message from `internal/userns`):
```
[pipedpeer] this node cannot create a user namespace, so jobs run unisolated.
    this system restricts unprivileged user namespaces via AppArmor (Ubuntu 24.04+; kernel.apparmor_restrict_unprivileged_userns = 1).
```
followed by the exact fix for that machine: a one-file AppArmor profile for the pipedpeer
binary (preferred), or `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`
(machine-wide, weaker). `pipedpeer setup` now applies the profile itself on Ubuntu 24.04+;
at the lab visit check that a job prints no `unisolated` line, and if one still does, run
the fallback block in `cards/PC.txt` (profile, daemon restart, sysctl last). The
daemon log says `no user namespace: jobs run unisolated`. Only a private no-root store
(no sudo) makes this fatal; with sudo available that path is not used.
Not reproducible in containers. <!-- from rehearsal: lab visit -->

## Layout 1: three laptops on Wi-Fi

LAPTOP-A and LAPTOP-B (Fedora, NVIDIA GPU) and LAPTOP-C (no GPU), on one Wi-Fi. Do this
the day before, on the venue's Wi-Fi if you can. Nothing here was run in a rehearsal as a
whole; VERIFY.md runs the same layout in Docker.

1. Same binary on all three: `./demo/prep-0-install.sh <path-to-binary>` on each, and
   compare the `sha256` lines.
2. LAPTOP-A: `./setup-machine.sh fedora-laptop --rendezvous` (makes the secret, starts
   the rendezvous service, writes `join-command.txt`), then `pipedpeer join <LAPTOP-A IP>`
   on LAPTOP-A itself (it prints `no peers found`, as in "Lab visit" step 4: normal for the first
   machine).
3. LAPTOP-B: `./setup-machine.sh gpu-laptop --token <secret> --introducer <LAPTOP-A IP> --prep`
   (the line from `join-command.txt`; checks LAPTOP-A is reachable, joins, then runs
   prep-4 and prep-3 on its own GPU). Join by IP, never by discovery:
   Wi-Fi access points often drop broadcast (client isolation).
4. LAPTOP-A: `./demo/prep-4-common.sh`, then `./demo/prep-3-gpu.sh` (its own GPU).
5. LAPTOP-C: `./demo/prep-4-common.sh` **only** (prep-0 already ran `setup -y`). It is
   PC-3, the live join, so:
   - not `setup-machine.sh`: with a token it joins, through discovery if no
     `--introducer` is given, and step 1 loses its first join;
   - not `prep-3-gpu.sh`: on a machine without a GPU it ships to the GPU laptops, so it
     needs them as peers.

   Steps 5 and 6 from LAPTOP-C then rely on prep-4's environment (CUDA torch included)
   and on what prep-3 left on the GPU laptops. VERIFY.md, step 5, layout 1, checks that
   in Docker. <!-- not yet run in this layout -->
6. LAPTOP-A: `./demo/prep-2-rehearse.sh`, as in "Lab visit" step 6. These are the real
   speed-ups for this layout, measured on two machines (LAPTOP-C joins only on stage).
   Drop any live step under 1.2×.
7. LAPTOP-A: `./demo/prep-1-check.sh`. Expect READY, with
   `warn  only 1 peer - the demo works, but say 'two machines'` (only LAPTOP-B is joined).
   <!-- not yet run in this layout -->
8. If a laptop can't reach LAPTOP-A (setup-machine.sh or prep-1-check says so, or the
   `/dev/tcp` check above fails), that Wi-Fi blocks laptops from each other outright.
   Nothing is relayed, so follow "Can't reach LAPTOP-A": a network you control (a phone
   hotspot or your own router), then join again by IP.

## Lab visit (layout 2: only if we get a lab, 1–3 PCs)

1. First machine (Fedora laptop): `./setup-machine.sh fedora-laptop --rendezvous`. It
   writes `join-command.txt` with the secret.
2. Each other machine except PC-3 (it joins live on the day; give it `setup -y` only):
   `./setup-machine.sh <role> --token <secret>`.
3. Wait ~30 s and run `pipedpeer nodes` on the laptop. LAN auto-discovery should list every
   machine, `healthy`, with `discovery` in SOURCE. V3 joined pc1, pc2 and pc4 this way, with
   no `--introducer` (<!-- V3 9949ad0: docs-SETUP-115-discovery-pc4-fixed.txt -->):
   ```
   NODE       HOST        STATE        JOBS  CPU       MEM FREE  GPU                   PATH  SOURCE
   06b9a112*  yeet        healthy      0     15c 3%    6.7 GiB   RTX 3060 0% 41M/6.0G  self  self
   77dd3fd6   10.88.0.13  unreachable  0     0%        0 B       -                     lan   discovery
   8a64951e   pc2         healthy      0     16c 3%    1.2 GiB   -                     lan   discovery
   cf072c2b   pc4         healthy      0     16c 3%    1.1 GiB   -                     lan   discovery
   f657a7d0   pc1         healthy      0     16c 3%    1.2 GiB   -                     lan   discovery
   ```
   The `unreachable` row is PC-3 (only `setup -y`, not joined yet): expected until step 1 of
   the demo. A machine that stays `unreachable` with `0 B` after setup holds an old secret:
   run `pipedpeer stop; pipedpeer start` on it (V3's pc1 and pc2, before 50bae3e:
   docs-SETUP-115-discovery-nodes.txt, docs-SETUP-115-discovery-after-restart.txt).
4. If Wi-Fi and wired do not see each other: the rendezvous already runs on the laptop
   (step 1's `--rendezvous`), so on each machine `./setup-machine.sh <role> --token <secret> --introducer <laptop-ip>`.
   Our laptops on Wi-Fi and the PCs on ethernet are different subnets, which broadcast
   discovery never crosses, so in this layout expect to need it. Expected on each machine (<!-- V1 8e14b9b: 05-join-pcs.txt -->):
   ```
   restarting the daemon to join through 10.88.0.1:38445
   this node is aa6e1138
   connected (3 peer(s))

   NODE       HOST           STATE        JOBS  CPU     MEM FREE  GPU  PATH  SOURCE
   043e7463   pc4            healthy      0     16c 3%  6.7 GiB   -    lan   discovery
   ```
   On the first machine `join` prints (<!-- V2 31b5364: 53-host-join.txt -->):
   ```
   restarting the daemon to join through 10.88.0.1:38445
   this node is 06b9a112
   no peers found
     same network? ping 10.88.0.1
     firewall: sudo firewall-cmd --list-ports (Fedora) / sudo ufw status (Ubuntu)
     details: pipedpeer net-check

   This is normal for the first machine in a
   cluster - run the same command on the others and they will find
   each other. Check with: pipedpeer nodes
   ```
   On a multi-user Nix it may ask for sudo once to install the Nix key helper; answer it in
   the terminal (without a terminal V1 printed `could not make system nix trust the cluster's
   keys`; then run `pipedpeer trust`) (<!-- V1 8e14b9b: 04-host-join.txt -->). In V2 the
   laptop's `pipedpeer events` showed, as each PC joined
   (<!-- V2 31b5364: 56-join-host-events.txt -->):
   ```
   06:54:03  JOIN   pc1 is up (10.88.0.11:38080)
   06:54:04  TRUST  system nix now trusts these cluster keys (added through the pipedpeer key helper)
   ```
5. Pre-builds, slow the first time, seconds after: `./setup-machine.sh <role> --token <secret> --prep`
   (runs `demo/prep-4-common.sh` everywhere and `demo/prep-3-gpu.sh` on the GPU laptop).
   Also run `./demo/prep-3-gpu.sh` on every PC that will SEND the GPU or everything step.
   Time it: the common environment (CUDA torch included) took **1150–1261 s per machine** in
   V2, four PCs building in parallel (1269–1315 s in V1) (<!-- V2 31b5364: 55-prep4-pc1.txt, 55-prep4-pc4.txt -->):
   ```
   $ ./demo/prep-4-common.sh
   ================================================================
    Common packages on pc4
   ================================================================
     > Building the common environment (first time: minutes; torch is GBs).
     environment  building (first build of a new env takes minutes)
     environment  built in 1150.5 s
   checks complete
     common environment ready in 1151s
   ```
   In the 1.2 GB containers (pc1–pc3) it printed this after building, then still
   `common environment ready in 1262s`:
   ```
   Error: NOT READY: refused: job rejected by remote daemon: insufficient memory: need 3930062848 bytes, available 1239543992 bytes
     NOT READY reported for prep/common.py (see above); the prep only needs the environment
   ```
   The environment is what counts; on an 8–16 GB PC expect `checks complete` as on pc4.
   `prep-3-gpu.sh` needs `python3` (the lab containers had none; the venue machines do). In V2's
   `./setup.sh` pass it printed `HTTP Error 401: Unauthorized; counted 0` and `no healthy GPU
   machine among the peers` (70-setup-firstpass.txt); c6dc71a sends the token. V3 ran it on
   pc1 inside `nix shell …#python3` (21.5 s, exit 1)
   (<!-- V3 9949ad0: docs-SETUP-176-prep3-gpu-pc1.txt -->):
   ```
    GPU prep on pc1
   ================================================================
     > No GPU here: this machine is a sender. Building here, shipping to the GPU laptops.

   --- 04_torch_ddp.py --remote --gpu force
   …
     ran on       yeet (192.168.0.200)
     environment  built in 0.8 s
     closure      already on the node
     script       4.5 s · peak memory 1.2 GB
     files back   none
     total        5.4 s
   [coordinator] Task completed successfully on node 06b9a112
     ok in 6 s
     > 07_everything.py imports torch too: building its environment here.

   --- 07_everything.py
   [coordinator] GPU-accelerated libraries detected — preferring GPU nodes
   [coordinator] Resource estimate: 3.1 GiB (tier=import)
   [coordinator] job rejected by remote daemon: insufficient memory: need 3300917248 bytes, available 2093244552 bytes; waiting up to 15s for memory to free...
   Error: job rejected by remote daemon: insufficient memory: need 3300917248 bytes, available 2093051888 bytes
     FAILED after 15 s - read the lines above

   GPU PREP FAILED - fix the FAILED lines above before the demo
   ```
   (Captured before 1e07d56. The wait is now up to 10 min, `PIPEDPEER_REMOTE_WAIT`, with a
   line every 30 s saying why each machine is busy; a job no machine could ever hold fails at once.)
   The GPU half passed. The `07_everything.py` half runs on the PC itself and needs about
   3.1 GiB free; pc1 had 2.1 GiB. On an 8–16 GB PC expect `ok` for both and no
   `GPU PREP FAILED`.
   Since ca70da4 the GPU run trains for about 13 s (V3's script took 4.5 s in all), and a
   `--remote` summary has two more lines, this machine's load and the node's. V5,
   `00_pool.py --remote` from pc2 (<!-- V5 ef68f02: docs-new-usage-remote-pool-pc2.txt -->):
   ```
     this machine CPU 22% avg · RAM +69 MB (peak +249 MB) · no GPU
     on pc3       CPU 31% avg
   ```
   On a run without `--remote` both lines name the same machine.
6. On the laptop: `./demo/prep-2-rehearse.sh` → `demo/rehearsal-results.txt` (cold, warm, speedup;
   the only speed numbers to quote). It generates `data.csv`, which prep-1 checks for, so run
   it **before** prep-1. V3 took 1390 s for the whole script
   (<!-- V3 9949ad0: docs-SETUP-179-prep2-rehearse.txt -->):
   ```
   Rehearsal on 2026-10-12 10:11, 4 peer(s), 1 GPU machine(s)

   step               cold     warm     python3   cluster   speedup / note
   00_pool.py         17s      18s      10.0s     18.5s     0.54x
   06_pool_lambda.py  5s       4s       did not finish 3.5s      -
   01_sklearn_rf.py   23s      18s      10.5s     16.8s     0.62x
   02_numpy_heavy.py  240s     243s     30.8s     241.6s    0.13x
   03_pandas_ooc.py   35s      35s      -         25.3s     out-of-core across cluster
   07_capstone.py     -        -        5.9s      15.1s     0.39x
   04_torch_ddp.py    19s      5s       -         2.3s      --gpu force using GPU: NVIDIA GeForce RTX 3060 Laptop GPU
   07_everything.py   41s      28s      -         23.1s     
   teacher check      2s       2s       /home/yeet/Downloads/pipedpeer-work/tmp/v3src/pipedpeer-demo/students/grid_search.py (before / after common prep, yeet)
   finale (all)       -        38s      -         -         SOME FAILED - see demo/finale-logs/
   ```
   Containers share one CPU, so the speedups are not speed numbers; take those from the lab
   visit. `06_pool_lambda.py` "did not finish" under plain `python3` is expected (a lambda
   cannot be pickled; that is the point of the script). The finale inside prep-2 ran 5 of 8:
   numpy, pandas and everything failed in the 1.2 GB containers. With the containers resized
   to 6.0 GB in total and 50bae3e's `PIPEDPEER_REMOTE_WAIT=10m` in `demo/13-finale.sh`, the
   finale on its own then passed 8 of 8 (VERIFY.md B5).
7. On the laptop: `./demo/prep-1-check.sh`. Expect READY. V3, after prep-2
   (<!-- V3 9949ad0: docs-SETUP-177-prep1-check-after-prep2.txt -->, 2.0 s):
   ```
    Is the cluster ready for the demo?
   ================================================================
     ok    daemon running here
   …
     ok    4 healthy peers besides this machine
     ok    every peer has a direct path (punched / punched-in / lan)
     warn  one GPU machine - the GPU step runs on one GPU, without --ddp 2
     ok    03_pandas_ooc/data.csv present (1.1G)
     ok    07_everything.py present
     ok    python3 here has numpy (needed for the local baseline only)
     ok    python3 here has pandas (needed for the local baseline only)
     ok    python3 here has sklearn (needed for the local baseline only)

   this binary: 7d4f26f15865e20c  - compare with prep-0 output on every machine

   READY
   ```
   Run before prep-2, the same check printed `FAIL  no data.csv - run ./demo/prep-2-rehearse.sh`
   and `NOT READY - fix the FAIL lines above` (docs-SETUP-177-prep1-check.txt). With both
   GPU laptops joined the `warn` line should go away. The node table it prints also lists old
   `unreachable` rows of past test nodes (`manual` in SOURCE) if any are left; they do not
   fail the check.

## Demo day

1. Power and network on; on each machine `pipedpeer status` shows running
   (<!-- V1 8e14b9b: 03-host-start.txt -->):
   ```
   daemon running (pid=433799 node-id=06b9a112-9739-4e3d-9728-103872ac5f67 port=38080)
   ```
   Not running → `pipedpeer start` (rejoins by itself).
2. On the laptop `./demo/prep-1-check.sh` (READY; data.csv is there from prep-2), then follow `DEMO-SCRIPT.md` step by step (`demo/0-join.sh` onward).
3. If a machine is missing: `pipedpeer join <laptop-ip>` on it, or re-run `setup-machine.sh` (safe).
