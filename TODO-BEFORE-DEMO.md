# To do before the demo

> **Delete this file once every box below is ticked.** It is a one-off
> checklist for the demo round (October 2026), not project documentation.
> Longer-term work is in [docs/next-steps.md](docs/next-steps.md).

Everything here uses the `dev` branch (the demo build; install it with the
README's `--channel nightly` line once the nightly has rebuilt). The guides it points to hold
the exact commands and the output to expect:
[pipedpeer-demo/SETUP.md](pipedpeer-demo/SETUP.md) (machines and lab visit),
[pipedpeer-demo/DEMO-SCRIPT.md](pipedpeer-demo/DEMO-SCRIPT.md) (the live
demo) and [pipedpeer-demo/VERIFY.md](pipedpeer-demo/VERIFY.md) (checks).
Each machine has a card with only its own steps, in order:
[LAPTOP-A](pipedpeer-demo/cards/LAPTOP-A.md),
[LAPTOP-B](pipedpeer-demo/cards/LAPTOP-B.md),
[LAPTOP-C](pipedpeer-demo/cards/LAPTOP-C.md),
[PC](pipedpeer-demo/cards/PC.md) (or `./demo/card.sh <ROLE>` in
`pipedpeer-demo`).

## 1. Run the demo in Docker first (the friend, now)

[VERIFY.md](pipedpeer-demo/VERIFY.md) runs the live flow, steps 0–9 with the
same commands in the same order, on one laptop plus four containers, then the
backup steps B1–B8. Each step shows the output a rehearsal produced.

- [ ] "Bring the lab up", L1–L7 (L7's builds take about 21 minutes, in the
      background).
- [ ] Steps 0–9 for **layout 1** (three laptops on Wi-Fi; pc1 plays LAPTOP-C,
      pc2 plays LAPTOP-B). Tick each step whose output matches; for each
      mismatch, note the step, the command and the first line that differed.
      Step 5 in layout 1 matters most: it shows whether LAPTOP-C's prep (prep-4
      only) is enough.
- [ ] Steps 0–9 again for **layout 2** (join pc4; pc3 joins live).
- [ ] B1–B8, if there is time (B4a and B4 last: they need the GPU lab).
- [ ] "Afterwards": take the lab down and put your `nodes.db` back.
- [ ] Send the mismatch notes back; lines marked "not yet run in this layout"
      are guesses until your run.

## 2. Put the second GPU laptop back to normal (the friend's laptop)

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

The rehearsal laptop's backup was deleted in its cleanup, so restore by hand,
on this laptop:

- [ ] `systemctl --user stop pp-v3-rendezvous; systemctl --user disable pp-v3-rendezvous`
      (ignore "not loaded")
- [ ] `~/.local/bin/pipedpeer stop`
- [ ] `sudo ~/.local/bin/pipedpeer setup --uninstall` (removes the key helper,
      the sudoers rule and the nix include)
- [ ] `rm -f ~/.local/share/pipedpeer/auth_token ~/.local/share/pipedpeer/cluster.json`
- [ ] Either keep the new build (the `dev` nightly, needed for the demo) or put
      yours back: `mv ~/.local/bin/pipedpeer.bak ~/.local/bin/pipedpeer`
- [ ] Start your own daemon again if you want it: `cd ~/Projects/Pipedpeer && ./bin/pipedpeer start`

For the demo itself, every machine needs the same `dev` nightly build:
SETUP.md, "Per machine".

## 3. LATER — not now (user, 2026-10-12): the two-laptop GPU run

Kept for reference; do it later, when both GPU laptops are together. Two GPUs
on two real laptops is the one demo feature that never ran for real: the
rehearsal used two containers sharing one GPU. Until it passes, it stays out
of the live demo (DEMO-SCRIPT backup **B4a**; `7-gpu.sh` skips it unless
`DEMO_DDP=1`).

- Both GPU laptops and one machine without a GPU on the **same Wi-Fi or
  LAN**, all on the same `dev` nightly build, joined to one cluster
  (SETUP.md).
- Prepare the CUDA environment on each laptop once first (it is ~3.6 GB):
  `./demo/prep-3-gpu.sh` (SETUP.md, lab visit).
- From the machine without a GPU, run what DEMO-SCRIPT B4a shows
  (`pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force 1`) and compare with
  the expected output there: both `rank` lines, `epoch` lines, usage lines
  for both laptops, no `SAME gradients` warning.
- Optional: stop the daemon on the laptop that is **not** in the `rank 0:`
  line mid-run; the run must continue on one rank.
- If it passes, it may move into the live GPU step (`DEMO_DDP=1 ./demo/7-gpu.sh`).
  If it doesn't, leave it as backup and note what failed in docs/next-steps.md.

## 4. Layout 1 prep: three laptops on Wi-Fi (likely venue)

SETUP.md, "Layout 1", step by step. In short:

- [ ] Same build on all three laptops (`demo/prep-0-install.sh` prints a sha256 to compare).
- [ ] `pipedpeer rendezvous` on LAPTOP-A; LAPTOP-A and LAPTOP-B join by IP
      (`pipedpeer join <LAPTOP-A IP>`). LAPTOP-C stays unjoined: it is the
      live join.
- [ ] `./demo/prep-4-common.sh` on LAPTOP-C (about 20 minutes; nothing else
      there).
- [ ] `./demo/prep-3-gpu.sh` (after prep-4) on both GPU laptops.
- [ ] `./demo/prep-2-rehearse.sh` on LAPTOP-A: the real speed-ups on these
      laptops. **Drop any live step under 1.2×**; with three machines expect
      smaller speed-ups than a lab would give.
- [ ] `./demo/prep-1-check.sh` (after prep-2) ends in `READY`
      (`only 1 peer` is a warning, not a failure).
- [ ] Time one full run-through of the live flow (DEMO-SCRIPT, "The flow").

## 5. The lab visit: only if we get a lab (1–3 PCs)

Layout 2. SETUP.md, "Lab visit", step by step. In short:

- [ ] Same build on every machine (`demo/prep-0-install.sh` prints a sha256 to compare).
- [ ] `pipedpeer rendezvous` on the presenting laptop; every machine except
      the one kept for the live join runs `auth set` + `join`, by IP (laptops
      on Wi-Fi and PCs on ethernet are different subnets).
- [ ] `./demo/prep-4-common.sh` on **every** PC, all at the same time (about
      20 minutes each; it builds the shared environment, CUDA torch included).
- [ ] `./demo/prep-3-gpu.sh` on every machine that sends or runs GPU work.
- [ ] `./demo/prep-2-rehearse.sh` on the presenting laptop: the real
      cold / warm / speed-up table. **Drop any live step under 1.2×.**
- [ ] `./demo/prep-1-check.sh` (after prep-2) ends in `READY`.
- [ ] Time one full run-through of the live flow (DEMO-SCRIPT, "The flow").

## 6. Optional checks (not needed for the demo)

[VERIFY.md](pipedpeer-demo/VERIFY.md), "Not re-checked on real machines yet":
features that pass their tests but were never run on real hardware. The live
demo does not use them.
