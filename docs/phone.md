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
3. In Termux, install Python and numpy:

   ```sh
   pkg install python python-numpy
   ```

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

- The phone answers "runnable" for a pool chunk only when its Python has the
  same minor version as the submitter's (for example, 3.12). Chunks are
  pickled, and pickles are tied to the minor version.
- A chunk runs as `python3 pool_runner.py`, which needs only the standard
  library. Your function can use what Termux has installed (numpy). If it
  imports something missing, the chunk fails and the submitter reruns it
  locally.
- The phone has no kernel memory limit (Android gives apps no cgroups). The
  daemon's memory watchdog still holds it to half the phone's RAM.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Workers die after a few minutes | Phantom process killer is still on: rerun the two `adb` lines |
| Phone shows but never gets chunks | Python minor differs from the laptops': `python3 -V` on both |
| Phone drops off when the screen locks | `termux-wake-lock`, keep it charging |
| LAN discovery doesn't find the phone | Use `pipedpeer connect <code>`; Android filters multicast |
