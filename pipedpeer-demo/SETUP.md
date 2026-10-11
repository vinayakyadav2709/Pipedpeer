# Setting up the machines

Draft. Every expected-output line is a placeholder until the rehearsal fills it in.
One script does each machine: `setup-machine.sh <role>` (install, `pipedpeer setup -y`,
`auth set`, join; it can be re-run). It reuses `demo/prep-*.sh` instead of copying them.

## Per machine

| Machine | Role | Notes |
|---|---|---|
| College Ubuntu PC | `ubuntu-pc` | Ships ufw and AppArmor. `setup -y` opens the ports with sudo. If peers still cannot connect: `sudo ufw disable` (or allow the ports). AppArmor may restrict the sandbox; check `pipedpeer setup` output. |
| Your Fedora laptop | `fedora-laptop` | First machine: run with no `--token`, then read the secret with `pipedpeer auth show`. Runs `pipedpeer rendezvous` if the network needs it. |
| Friend's GPU laptop | `gpu-laptop` | Needs the NVIDIA driver (`nvidia-smi` works). Runs `prep-3-gpu.sh` for the CUDA torch environment. |

## Lab visit

1. First machine (Fedora laptop): `./setup-machine.sh fedora-laptop`. Note the secret.
2. Each other machine: `./setup-machine.sh <role> --token <secret>`.
3. Wait ~30 s and run `pipedpeer nodes` on the laptop. LAN auto-discovery should list every machine.
   <!-- from rehearsal -->
4. Fallback if Wi-Fi and wired do not see each other: on the laptop run `pipedpeer rendezvous`
   (leave it open), then on each machine `./setup-machine.sh <role> --token <secret> --introducer <laptop-ip>`.
   <!-- from rehearsal -->
5. Pre-builds, slow the first time, seconds after: `./setup-machine.sh <role> --token <secret> --prep`
   (runs `demo/prep-4-common.sh` everywhere and `demo/prep-3-gpu.sh` on the GPU laptop).
   <!-- from rehearsal -->
6. On the laptop: `./demo/prep-1-check.sh`. Expect every line `ok`. <!-- from rehearsal -->

## Demo day

1. Power and Wi-Fi on; on each machine `pipedpeer status` shows running. <!-- from rehearsal -->
2. On the laptop `./demo/prep-1-check.sh`, then follow `DEMO-SCRIPT.md` step by step (`demo/0-join.sh` onward).
3. If a machine is missing: `pipedpeer join <laptop-ip>` on it, or re-run `setup-machine.sh` (safe).
