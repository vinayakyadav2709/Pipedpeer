# A phone as a worker (Termux)

An Android phone can take Pool and joblib work from the cluster. It runs the
pipedpeer daemon inside Termux and computes chunks with Termux's own Python.
It never takes whole jobs: a phone has no Nix and no crun sandbox, and an arm64
phone can't run the laptops' x86 environments anyway. CPU only.

Tested against the `termux/termux-docker:aarch64` image under qemu so far.
Timings from qemu are not performance numbers.

## One-time phone setup

1. Install **Termux from F-Droid** (or its GitHub releases), not from the Play
   Store. The Play build is outdated.
2. Turn off Android's phantom process killer. On Android 12 and later it kills
   anything past 32 child processes, which kills pool workers. With USB
   debugging on, run from a computer:

   ```sh
   adb shell device_config put activity_manager max_phantom_processes 2147483647
   adb shell settings put global settings_enable_monitor_phantom_procs false
   ```

   On Android 14 and later, Developer options also has a "Disable child process
   restrictions" toggle that does the same thing.
3. Nothing else to install by hand: `pipedpeer setup` (the installer runs it)
   installs Termux's `python` and `python-numpy`, then `python-torch` and
   `python-scipy` for ML chunks, then `cloudpickle` with pip. The ML packages
   are optional and add about 350 MB of storage (a ~55 MB download); if they fail to install, the
   phone still takes numpy work.

## Install and connect

In Termux:

```sh
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash
pipedpeer connect <code>      # the line `pipedpeer invite` printed on another machine
termux-wake-lock              # keeps Android from sleeping the worker
```

`setup` sees Termux and makes the phone pool-only (`"pool_only": true` in
`cluster.json`), so it skips the Nix, crun and firewall steps.

Keep the phone **plugged in**. Charging keeps Android out of Doze, and a
synchronous Pool waits for its slowest chunk, so a throttled phone slows
everyone down.

## What runs where

- The phone takes a chunk only when its Python can import every module the
  chunk uses (numpy, torch, scipy, or anything you `pip install` in Termux).
  A chunk that needs a missing module goes to another machine, and the phone's
  daemon log names the module.
- A function sent as source runs on any Python version. A function pickled by
  value (lambdas, closures) needs the same minor version as the submitter's
  (for example, 3.12), because bytecode is tied to it.
- Small ML training fits: a Pool chunk can build a torch model, train it for a
  few steps and return the loss. Termux has no scikit-learn or pandas packages
  (pip would build them from source), so chunks that need those skip the phone.
- The phone has no kernel memory limit (Android gives apps no cgroups). The
  daemon's memory watchdog still holds it to half the phone's RAM.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Workers die after a few minutes | Phantom process killer is still on: rerun the two `adb` lines |
| Phone shows but never gets chunks | `grep refusing ~/.local/state/pipedpeer/daemon.log` on the phone names the missing module or Python version |
| Phone drops off when the screen locks | `termux-wake-lock`, keep it charging |
| LAN discovery doesn't find the phone | Use `pipedpeer connect <code>`; Android filters multicast |
