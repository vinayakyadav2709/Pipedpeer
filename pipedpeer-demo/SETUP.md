# Setting up the machines

Draft. `<!-- V1 8e14b9b: <file> -->` marks output copied from rehearsal pass 1
(containers on one laptop; evidence in `~/Downloads/pipedpeer-work/evidence/v1/`).
`<!-- from rehearsal -->` / `<!-- refresh from V2 -->` marks a spot still waiting for a real run.

One script does each machine: `setup-machine.sh <role>` (install, `pipedpeer setup -y`,
`auth set`, join; it can be re-run). It reuses `demo/prep-*.sh` instead of copying them.

**Same binary everywhere.** `setup-machine.sh` installs the nightly only when no
`pipedpeer` is on PATH. The demo needs the `nightly-fixes` build, so first copy that
binary to each machine and run `./demo/prep-0-install.sh <path-to-binary>` (installs to
`~/.local/bin`, runs `setup -y`, prints a `sha256`; the hashes must match), then
`setup-machine.sh`. <!-- refresh from V2: prep-0-install.sh / setup-machine.sh not run in V1; the lab mounted the binary -->

## Per machine

| Machine | Role | Notes |
|---|---|---|
| College Ubuntu PC | `ubuntu-pc` | sudo is available (DECIDED), so the normal path: system Nix + crun. See "Firewall" and "AppArmor" below. |
| Your Fedora laptop | `fedora-laptop` | First machine: run with no `--token`, then read the secret with `pipedpeer auth show`. Runs `pipedpeer rendezvous` (keep it in its own terminal). |
| Friend's GPU laptop | `gpu-laptop` | Needs the NVIDIA driver (`nvidia-smi` works). Runs `prep-3-gpu.sh` for the CUDA torch environment. |

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

`setup -y` opens exactly one port: **38447/udp** (direct links, `internal/setup/network.go`
`openFirewall`, with `firewall-cmd --add-port` or `ufw allow`). It does **not** open the
ports the LAN demo uses:

| Port | Used for | Needed on |
|---|---|---|
| 38080/tcp | the daemon: jobs, pool work, `nodes` | every machine |
| 38099/udp | LAN auto-discovery broadcast | every machine |
| 38445/udp | rendezvous | LAPTOP-A only |

Check: `sudo ufw status` (Ubuntu) / `sudo firewall-cmd --list-ports` and `--get-active-zones`
(Fedora). If a firewall is active and peers do not appear:
```bash
sudo ufw allow 38080/tcp && sudo ufw allow 38099/udp     # Ubuntu PCs (LAPTOP-A on ufw: also 38445/udp)
sudo ufw disable                                          # fallback (DECIDED), re-enable after the demo
sudo firewall-cmd --add-port=38080/tcp --add-port=38099/udp --add-port=38445/udp   # Fedora, until reboot
```
The ufw path itself was not exercised in V1 (no container ran ufw). <!-- refresh from V2 -->
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
   <!-- refresh from V2: V1 always joined through the rendezvous; discovery-only not run -->
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
   On the first machine `join` prints `no peers found … This is normal for the first machine`
   and asks for sudo once to install the Nix key helper; answer it in the terminal (without a
   terminal V1 printed `could not make system nix trust the cluster's keys`; then run
   `pipedpeer trust`) (<!-- V1 8e14b9b: 04-host-join.txt -->).
5. Pre-builds, slow the first time, seconds after: `./setup-machine.sh <role> --token <secret> --prep`
   (runs `demo/prep-4-common.sh` everywhere and `demo/prep-3-gpu.sh` on the GPU laptop).
   Also run `./demo/prep-3-gpu.sh` on every PC that will SEND the GPU or everything step.
   Time it: the CUDA torch environment took 1269–1315 s per machine in V1 (three building in
   parallel; `environment  built in 1269.1 s`) (<!-- V1 8e14b9b: 10-prep4-pc1.txt, 11-pc1-gpu-checkonly.txt -->).
   <!-- refresh from V2: in V1's 1.2 GB containers prep-4-common printed
   `Error: NOT READY: refused: job rejected by remote daemon: insufficient memory: need 3930062848 bytes`
   after building (then `common environment ready`), and failed its nix build on pc4. -->
6. On the laptop: `./demo/prep-1-check.sh`. Expect READY. <!-- refresh from V2: not run in V1 -->
7. On the laptop: `./demo/prep-2-rehearse.sh` → `demo/rehearsal-results.txt` (cold, warm, speedup;
   the only speed numbers to quote). <!-- refresh from V2 -->

## Demo day

1. Power and network on; on each machine `pipedpeer status` shows running
   (<!-- V1 8e14b9b: 03-host-start.txt -->):
   ```
   daemon running (pid=433799 node-id=06b9a112-9739-4e3d-9728-103872ac5f67 port=38080)
   ```
   Not running → `pipedpeer start` (rejoins by itself).
2. On the laptop `./demo/prep-1-check.sh`, then follow `DEMO-SCRIPT.md` step by step (`demo/0-join.sh` onward).
3. If a machine is missing: `pipedpeer join <laptop-ip>` on it, or re-run `setup-machine.sh` (safe).
