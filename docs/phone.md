# A phone as a node (Termux)

An Android phone joins the cluster from Termux, with no root. `pipedpeer
setup` tries the full-node route first and falls back to a pool-only worker.

| Route | What it takes | How |
|---|---|---|
| **Full node** (main) | Pool/joblib and torch chunks from every machine, each in the job's own environment built on the phone | Debian in `proot-distro` with single-user Nix; pipedpeer runs inside it |
| **Pool-only worker** (fallback) | Pool/joblib chunks only | Termux's own Python |

Both routes are **chunks-only**: the phone takes no whole jobs and no DDP
ranks. Those need the job sandbox, a second proot nested inside the
`proot-distro` session, and on Play-Store Termux's proot the inner one starts
nothing (`docs/next-steps.md` §3). `pipedpeer doctor` and the daemon log say so.

CPU only on both. The pool-only worker is tested in
`termux/termux-docker:aarch64` under qemu. The full node is not: proot needs
`ptrace`, which qemu-user lacks, and on this project's x86_64 host (kernel
7.x) proot does not translate `fchmodat2`, so the Nix install fails there;
Android kernels before 6.6 lack that syscall. Its pieces (forwarding, the
detached daemon, Termux's proot nested in the Debian) were checked in
`termux/termux-docker:x86_64`; the whole route needs a real phone. Timings
from containers are not performance numbers.

## Install

The only prerequisite is Termux (the Play Store build is the one tested; the
F-Droid build is untested). In Termux, on a phone, run one command, with the
invite line `pipedpeer invite` printed on another machine:

```sh
curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh | bash -s -- --version <version> --connect <code>
```

The installer runs `pipedpeer setup --connect <code>`: setup, then the same
join as `pipedpeer connect <code>`. `--connect` is optional; without it, run
`pipedpeer connect <code>` later (it also moves a phone to another network).
Because stdin is the script under `curl | bash`, setup's prompts (the pairing
code, the Enter that confirms the developer toggle) are read from
`/dev/tty`; with no terminal setup skips them and says which step is left.

## What setup does

Setup checks each step's state, skips what is already done, guides the rest,
and can be run again at any point (`pipedpeer setup`). On a phone, besides
the install below, it:

1. Holds a wake lock (`termux-wake-lock`).
2. Turns off Android's phantom process killer. On Android 12 and later it
   kills anything past 32 child processes, which kills pool workers. The
   route depends on the Android version (`internal/phone/prep.go`,
   `phantom.go`):
   - **Android 14 and later:** you turn on Developer options, "Disable child
     process restrictions", and press Enter. Setup reads the property back;
     if this phone does not expose it, setup takes your word and doctor
     reports "unknown".
   - **Android 12 and 13 (SDK 31-33):** setup sets the three phantom
     settings itself, through Termux's own `adb` connected to the phone over
     Wireless debugging on 127.0.0.1. It finds the pairing and connect ports
     over mDNS (`internal/adbmdns`; Termux's adb has no mdns command), so you
     only turn on Wireless debugging, tap "Pair device with pairing code",
     and type the 6-digit code. If mDNS finds nothing, it asks for the
     ports. When the phone is already paired, no code is asked. If Wireless
     debugging was off when setup started, setup switches it off again at the
     end and confirms it.
   - Below Android 12 there is no killer.
3. Writes `~/.termux/boot/pipedpeer`, which starts the daemon at boot. The
   Play Store Termux runs it by itself; the F-Droid and GitHub builds also
   need the Termux:Boot app, opened once (`internal/phone/boot.go`). Uninstall
   removes the script.
4. Starts the common-environment warm-up once the daemon is up, as on every
   machine.

Setup installs `proot-distro`, a Debian (~100 MB), Nix inside it, and a
pinned Python with numpy, pandas, scipy, scikit-learn, torch, joblib and
cloudpickle from the public Nix cache (about 1.5 GB) for other machines' pool
work (short of storage? `PIPEDPEER_PHONE_PYTHON_PKGS="numpy pandas joblib cloudpickle" pipedpeer setup`
leaves torch out of that pinned Python). The warm-up in step 4 is separate: it
builds the common environment (torch, scipy, matplotlib; several GB) and the
variable does not change it. From then on every `pipedpeer` command typed in Termux runs inside the
Debian; the files under Termux's home keep their paths there. `pipedpeer
doctor` names the route. The phone is a node like any other for the peer
Nix cache ([ADR-0021](adr/0021-peers-as-nix-binary-cache.md)): it serves and
asks same-architecture peers (aarch64 only), so an x86_64 laptop never asks it.
Setup's first Debian and Nix build runs before any daemon exists and uses
cache.nixos.org only.

If any of that fails, setup says why and makes the phone a pool-only worker
instead (`"pool_only": true` in `cluster.json`): it installs Termux's `python`
and `python-numpy`, then `python-torch` and `python-scipy` (about 350 MB, a
~55 MB download; optional), then `cloudpickle` with pip.

Keep the phone **plugged in**. Charging keeps Android out of Doze, and a
synchronous Pool waits for its slowest chunk, so a throttled phone slows
everyone down.

## What runs where

- No job runs whole on a phone (chunks-only, above). A job submitted on the
  phone builds its environment there for aarch64 and is placed like
  `--remote`, but an x86_64 laptop refuses it ("arch mismatch"), so today it
  needs another aarch64 machine that is not a phone.
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
jobs away from the rest of the phone. This second proot is the one that does
not start on Play-Store Termux, which is why phones are chunks-only today.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Workers die after a few minutes | The phantom process killer is on. `pipedpeer doctor` reports it as off, on or unknown and names the fix; run `pipedpeer setup` again. On Android 12-13 doctor asks you to switch Wireless debugging on for a moment to read it (no new code), then switches it off again |
| Setup cannot find the pairing port | Open "Pair device with pairing code" and keep that screen up while setup listens (it searches for 60 s); if mDNS stays silent it asks for the port shown there |
| Setup says the pairing was refused | Wrong or expired code, three times in a row (setup asks again after each failure): run `pipedpeer setup` again and enter the new one |
| Phone shows but never gets chunks | `grep refusing ~/.local/state/pipedpeer/daemon.log` (full node: `proot-distro login debian -- grep refusing /root/.local/state/pipedpeer/daemon.log`) names the missing module or Python version |
| Phone drops off when the screen locks | `termux-wake-lock`, keep it charging |
| LAN discovery doesn't find the phone | Use `pipedpeer connect <code>`; Android filters multicast |
