# To do before the demo

> **Delete this file once every box below is ticked.** It is a one-off
> checklist for the demo round (October 2026), not project documentation.
> Longer-term work is in [docs/next-steps.md](docs/next-steps.md).

Everything here uses the `nightly-fixes` branch. The guides it points to hold
the exact commands and the output to expect:
[pipedpeer-demo/SETUP.md](pipedpeer-demo/SETUP.md) (machines and lab visit),
[pipedpeer-demo/DEMO-SCRIPT.md](pipedpeer-demo/DEMO-SCRIPT.md) (the live
demo) and [pipedpeer-demo/VERIFY.md](pipedpeer-demo/VERIFY.md) (checks).

## 1. Put the second GPU laptop back to normal (the friend's laptop)

The last rehearsal used this laptop as the second GPU machine and lost it
mid-run, so it was left in the test state:

- `~/.local/bin/pipedpeer` is the rehearsal build. Your original is
  `~/.local/bin/pipedpeer.bak`.
- Your own dev daemon (started with `cd ~/Projects/Pipedpeer && ./bin/pipedpeer start`)
  was stopped.
- A test rendezvous user service, `pp-v3-rendezvous`, may still be installed.
- `pipedpeer setup` installed its Nix key helper (plus its sudoers rule and
  the nix.conf include), and wrote `~/.local/share/pipedpeer/auth_token` and
  `cluster.json` for the test cluster.

If the laptop that ran the rehearsal is around, its owner can restore
everything from their backup in one go (the restore script there,
`evidence/v3/61-friend-restore-pending.sh`). Otherwise, on this laptop:

- [ ] `systemctl --user stop pp-v3-rendezvous; systemctl --user disable pp-v3-rendezvous`
      (ignore "not loaded")
- [ ] `~/.local/bin/pipedpeer stop`
- [ ] `sudo ~/.local/bin/pipedpeer setup --uninstall` (removes the key helper,
      the sudoers rule and the nix include)
- [ ] `rm -f ~/.local/share/pipedpeer/auth_token ~/.local/share/pipedpeer/cluster.json`
- [ ] Either keep the new build (`nightly-fixes`, needed for the demo) or put
      yours back: `mv ~/.local/bin/pipedpeer.bak ~/.local/bin/pipedpeer`
- [ ] Start your own daemon again if you want it: `cd ~/Projects/Pipedpeer && ./bin/pipedpeer start`

For the demo itself, every machine needs the same `nightly-fixes` build:
SETUP.md, "Per machine".

## 2. The two-laptop GPU run (never done on real hardware)

Two GPUs on two real laptops is the one demo feature that never ran for real:
the rehearsal used two containers sharing one GPU. Until it passes, it stays
out of the live demo (DEMO-SCRIPT backup **B4a**; `7-gpu.sh` skips it unless
`DEMO_DDP=1`).

- [ ] Both GPU laptops and one machine without a GPU on the **same Wi-Fi or
      LAN**, all on the same `nightly-fixes` build, joined to one cluster
      (SETUP.md).
- [ ] Prepare the CUDA environment on each laptop once first (it is ~3.6 GB):
      `./demo/prep-3-gpu.sh` (SETUP.md, lab visit).
- [ ] From the machine without a GPU, run what DEMO-SCRIPT B4a shows
      (`pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force 1`) and compare with
      the expected output there: both `rank` lines, `epoch` lines, usage lines
      for both laptops, no `SAME gradients` warning.
- [ ] Optional: stop the daemon on the laptop that is **not** in the `rank 0:`
      line mid-run; the run must continue on one rank.
- [ ] If it passes, it may move into the live GPU step (`DEMO_DDP=1 ./demo/7-gpu.sh`).
      If it doesn't, leave it as backup and note what failed in docs/next-steps.md.

## 3. The lab visit (before each demo)

SETUP.md, "Lab visit", step by step. In short:

- [ ] Same build on every machine (`demo/prep-0-install.sh` prints a sha256 to compare).
- [ ] `pipedpeer rendezvous` on the presenting laptop; every machine except
      the one kept for the live join runs `auth set` + `join`.
- [ ] `./demo/prep-4-common.sh` on **every** PC, all at the same time (about
      20 minutes each; it builds the shared environment, CUDA torch included).
- [ ] `./demo/prep-3-gpu.sh` on every machine that sends or runs GPU work.
- [ ] `./demo/prep-2-rehearse.sh` on the presenting laptop: the real
      cold / warm / speed-up table. **Drop any live step under 1.2×.**
- [ ] `./demo/prep-1-check.sh` (after prep-2) ends in `READY`.
- [ ] Time one full run-through of the live flow (DEMO-SCRIPT, "The flow").

## 4. Optional checks (not needed for the demo)

[VERIFY.md](pipedpeer-demo/VERIFY.md), "Not re-checked on real machines yet":
features that pass their tests but were never run on real hardware. The live
demo does not use them.
