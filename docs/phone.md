# A phone as a node (Termux)

An Android phone joins the cluster from Termux, with no root. `pipedpeer
setup` tries the full-node route first and falls back to a pool-only worker.

| Route | What it takes | How |
|---|---|---|
| **Full node** (main) | whole jobs from aarch64 machines (other phones, itself), Pool/joblib chunks from every machine | Debian in `proot-distro` with single-user Nix; pipedpeer runs inside it |
| **Pool-only worker** (fallback) | Pool/joblib chunks only | Termux's own Python |

CPU only on both. The pool-only worker is tested in
`termux/termux-docker:aarch64` under qemu. The full node is not: proot needs
`ptrace`, which qemu-user lacks, and on this project's x86_64 host (kernel
7.x) proot does not translate `fchmodat2`, so the Nix install fails there;
Android kernels before 6.6 lack that syscall. Its pieces (forwarding, the
detached daemon, Termux's proot nested in the Debian) were checked in
`termux/termux-docker:x86_64`; the whole route needs a real phone. Timings
from containers are not performance numbers.

## One-time phone setup

1. Install **Termux from F-Droid** (or its GitHub releases), not from the Play
   Store. The Play build is outdated.
2. Turn off Android's phantom process killer. On Android 12 and later it kills
   anything past 32 child processes, which kills pool workers. Any one of:
   - **Android 14+:** Developer options, "Disable child process restrictions".
   - **From the phone itself** (Android 11+, no laptop): turn on Developer
     options, Wireless debugging, tap "Pair device with pairing code", put
     Termux and Settings in split screen, and in Termux run
     ```sh
     curl -fsSLO https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/phone-phantom-off.sh
     bash phone-phantom-off.sh     # asks for the pairing port, code, then the connect port
     ```
   - **From a laptop** with USB debugging on:
     ```sh
     adb shell device_config put activity_manager max_phantom_processes 2147483647
     adb shell settings put global settings_enable_monitor_phantom_procs false
     ```
3. Nothing else to install by hand: `pipedpeer setup` (the installer runs it)
   does the rest.

## Install and connect

In Termux:

```sh
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash
pipedpeer connect <code>      # the line `pipedpeer invite` printed on another machine
termux-wake-lock              # keeps Android from sleeping the worker
```

Setup installs `proot-distro`, a Debian (~100 MB), Nix inside it, and a
pinned Python with numpy, pandas, scipy, scikit-learn, torch, joblib and
cloudpickle from the public Nix cache (about 1.5 GB) for other machines' pool
work (short of storage? `PIPEDPEER_PHONE_PYTHON_PKGS="numpy pandas joblib cloudpickle" pipedpeer setup`
leaves torch out). From then on every `pipedpeer` command typed in Termux runs inside the
Debian; the files under Termux's home keep their paths there. `pipedpeer
doctor` names the route.

If any of that fails, setup says why and makes the phone a pool-only worker
instead (`"pool_only": true` in `cluster.json`): it installs Termux's `python`
and `python-numpy`, then `python-torch` and `python-scipy` (about 350 MB, a
~55 MB download; optional), then `cloudpickle` with pip.

Keep the phone **plugged in**. Charging keeps Android out of Doze, and a
synchronous Pool waits for its slowest chunk, so a throttled phone slows
everyone down.

## What runs where

- A laptop's job never runs whole on a phone: its environment is built for
  x86_64. A job submitted on the phone builds its environment there with Nix
  (aarch64, mostly fetched from cache.nixos.org) and runs on aarch64 nodes.
- Pool chunks from a laptop run, on a full node, in the job's own environment
  built on the phone: the laptop's node sends the job's package list, the
  phone builds it for aarch64 with Nix (cached, by flake hash, under
  `~/.local/share/pipedpeer/foreign-envs`) and takes the job's chunks once the
  build is done; until then its log says "not runnable here yet". A laptop on
  an older build sends no package list, and then the phone's pinned Python
  runs the chunks. A pool-only phone runs them on Termux's Python. Either
  way the phone takes a chunk only when its Python can import every module
  the chunk uses; otherwise the chunk goes to another machine and the
  phone's daemon log names the module.
- A function sent as source runs on any Python version. A function pickled by
  value (lambdas, closures) needs the submitter's minor version. The full
  node's Python is the jobs' own pin, so it matches the laptops'.
- Closures are only sent between machines of the same architecture.

## Sandbox

Android gives apps no user namespaces or cgroups, so crun cannot run. A full
node runs each job under a second proot whose view holds only the job's
directory and `/nix` (plus `/dev`, `/proc`, `/sys` and DNS files), with an
address-space limit of the job's memory plus 2 GiB and `nice 10`. That is
weaker than crun: no PID or network isolation and no exact memory cap. The
daemon log and `pipedpeer doctor` say so. Android's app boundary still keeps
jobs away from the rest of the phone.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Workers die after a few minutes | Phantom process killer is still on: step 2 again |
| Phone shows but never gets chunks | `grep refusing ~/.local/state/pipedpeer/daemon.log` (full node: `proot-distro login debian -- grep refusing /root/.local/state/pipedpeer/daemon.log`) names the missing module or Python version |
| Phone drops off when the screen locks | `termux-wake-lock`, keep it charging |
| LAN discovery doesn't find the phone | Use `pipedpeer connect <code>`; Android filters multicast |
