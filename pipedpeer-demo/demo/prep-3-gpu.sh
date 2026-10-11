#!/usr/bin/env bash
# Build the multi-GB torch environment once, so nothing transfers on stage.
# Run at the setup visit, after every machine has joined:
#   - on each GPU laptop (LAPTOP-A, LAPTOP-B): runs the GPU script on its own GPU;
#   - on every PC that will submit the GPU or everything step (the sender):
#     builds the CUDA torch environment here and ships it to the GPU laptops.
#
#   ./demo/prep-3-gpu.sh
#
# Slow the first time (10-30 min: CUDA torch is several GB). A second run
# should take seconds; if it does not, something is being rebuilt or resent.
source "$(dirname "$0")/_lib.sh"

fail=0
step() {
	echo
	echo "${B}--- $*${N}"
	local t0=$SECONDS
	if "$PIPE" run "$@" 2>&1 | tail -12; [[ "${PIPESTATUS[0]}" -eq 0 ]]; then
		echo "${G}  ok${N} in $((SECONDS - t0)) s"
	else
		echo "${R}  FAILED${N} after $((SECONDS - t0)) s - read the lines above"
		fail=1
	fi
}

banner "GPU prep on $(uname -n)"
if [[ "$(node_count gpu-self)" -gt 0 ]]; then
	note "This machine has a GPU: building and running the GPU script here, on its own GPU."
	step 04_torch_ddp.py --gpu force
else
	g=$(node_count gpu-peers)
	if [[ "$g" -eq 0 ]]; then
		echo "${R}  no healthy GPU machine among the peers - start/join the GPU laptops first${N}"
		exit 1
	fi
	note "No GPU here: this machine is a sender. Building here, shipping to the GPU laptops."
	step 04_torch_ddp.py --remote --gpu force
	if [[ "$g" -ge 2 ]]; then
		note "Two GPU laptops: --ddp 2 so the environment lands on both."
		step 04_torch_ddp.py --ddp 2 --gpu force
	fi
fi
if [[ -f 07_everything.py ]]; then
	note "07_everything.py imports torch too: building its environment here."
	step 07_everything.py
else
	echo "${Y}  07_everything.py not found - skipped${N}"
fi

echo
if [[ "$fail" -eq 0 ]]; then
	echo "${G}${B}GPU PREP DONE${N} - run it once more: every step should now take seconds."
else
	echo "${R}${B}GPU PREP FAILED${N} - fix the FAILED lines above before the demo"
	exit 1
fi
