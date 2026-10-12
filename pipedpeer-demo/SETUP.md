# Setting up the machines

Draft. `<!-- V1 8e14b9b: <file> -->` marks output copied from rehearsal pass 1
(containers on one laptop; evidence in `~/Downloads/pipedpeer-work/evidence/v1/`).
`<!-- V2 31b5364: <file> -->` / `<!-- V2 cc56753: <file> -->` marks output from rehearsal
pass 2 (evidence in `~/Downloads/pipedpeer-work/evidence/v2/`).
`<!-- from rehearsal -->` marks a spot only the lab visit can fill.
`<!-- confirm in final re-check -->` marks a line that was not run in V1 or V2, or whose
behaviour changed after V2 ran: the line shown is the expected one.

**The rehearsal lab is not the venue.** The lab "PCs" were containers with no `python3`,
so the demo scripts that call `python3` (`demo/7-gpu.sh`, `demo/prep-3-gpu.sh`) skipped
there; the college PCs and our laptops have `python3`. The friend's real 2-GPU ring
(RTX 3060 + RTX 3050) is still to be checked in your own run: the friend's laptop went
offline before V2 could run it.

One script does each machine: `setup-machine.sh <role>` (install, `pipedpeer setup -y`,
`auth set`, join; it can be re-run). It reuses `demo/prep-*.sh` instead of copying them.

**Same binary everywhere.** `setup-machine.sh` installs the nightly only when no
`pipedpeer` is on PATH. The demo needs the `nightly-fixes` build, so first copy that
binary to each machine and run `./demo/prep-0-install.sh <path-to-binary>` (installs to
`~/.local/bin`, runs `setup -y`, prints a `sha256`; the hashes must match), then
`setup-machine.sh`. <!-- confirm in final re-check: prep-0-install.sh and setup-machine.sh were not run in V1 or V2; both labs mounted the binary -->

## Per machine

| Machine | Role | Notes |
|---|---|---|
| College Ubuntu PC | `ubuntu-pc` | sudo is available (DECIDED), so the normal path: system Nix + crun. See "Firewall" and "AppArmor" below. |
| Your Fedora laptop | `fedora-laptop` | First machine: run with no `--token`, then read the secret with `pipedpeer auth show`. Runs `pipedpeer rendezvous` (keep it in its own terminal). |
| Friend's GPU laptop | `gpu-laptop` | Needs the NVIDIA driver (`nvidia-smi` works). Runs `prep-3-gpu.sh` for the CUDA torch environment. The real 2-GPU ring with your laptop was never run in a rehearsal: check it in your own run (`./demo/7-gpu.sh` from a PC, both `nvidia-smi` screens). |

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
(machine-wide, weaker). Apply the profile at the lab visit so the demo runs sandboxed. The
daemon log says `no user namespace: jobs run unisolated`. Only a private no-root store
(no sudo) makes this fatal; with sudo available that path is not used.
Not reproducible in containers. <!-- from rehearsal: lab visit -->

## Lab visit

1. First machine (Fedora laptop): `./setup-machine.sh fedora-laptop`. Note the secret.
2. Each other machine except PC-3 (it joins live on the day; give it `setup -y` only):
   `./setup-machine.sh <role> --token <secret>`.
3. Wait ~30 s and run `pipedpeer nodes` on the laptop. LAN auto-discovery should list every machine.
   <!-- confirm in final re-check: V1 and V2 always joined through the rendezvous; a discovery-only join was not run -->
   Once joined, rows that came by broadcast say `discovery` in SOURCE.
4. Fallback if Wi-Fi and wired do not see each other: on the laptop run `pipedpeer rendezvous`
   (leave it open), then on each machine `./setup-machine.sh <role> --token <secret> --introducer <laptop-ip>`.
   Expected on each machine (<!-- V1 8e14b9b: 05-join-pcs.txt -->):
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
   machine among the peers` (70-setup-firstpass.txt); c6dc71a sends the token, so expect it to
   count the GPU laptops and build. <!-- confirm in final re-check -->
6. On the laptop: `./demo/prep-1-check.sh`. Expect READY. <!-- confirm in final re-check: not run in V1 or V2 -->
7. On the laptop: `./demo/prep-2-rehearse.sh` → `demo/rehearsal-results.txt` (cold, warm, speedup;
   the only speed numbers to quote). <!-- confirm in final re-check: not run in V1 or V2 -->
   The nearest V2 run is `./setup.sh` (one pass over every script, 246 s), whose summaries
   gave these cached totals on the laptop (<!-- V2 31b5364: 70-setup-firstpass.txt -->):
   00_pool 18.9 s, 06_pool_lambda 4.2 s, 01_sklearn_rf 19.6 s, 02_numpy_heavy 37.2 s,
   03_pandas_ooc 34.0 s, 05_file_sync 0.8 s, 07_capstone 101.7 s. Containers share one CPU,
   so these are not speed numbers.

## Demo day

1. Power and network on; on each machine `pipedpeer status` shows running
   (<!-- V1 8e14b9b: 03-host-start.txt -->):
   ```
   daemon running (pid=433799 node-id=06b9a112-9739-4e3d-9728-103872ac5f67 port=38080)
   ```
   Not running → `pipedpeer start` (rejoins by itself).
2. On the laptop `./demo/prep-1-check.sh`, then follow `DEMO-SCRIPT.md` step by step (`demo/0-join.sh` onward).
3. If a machine is missing: `pipedpeer join <laptop-ip>` on it, or re-run `setup-machine.sh` (safe).
