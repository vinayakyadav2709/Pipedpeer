#!/usr/bin/env bash
# GPU highlight - a PC with no GPU trains on a laptop's GPU (run on a PC).
#
#   ./demo/7-gpu.sh
#
# The torch environment is several GB. It must already be built here and on
# the GPU laptops (demo/prep-3-gpu.sh, at the setup visit). Never cold on stage.
source "$(dirname "$0")/_lib.sh"

banner "GPU training from a machine without one"
g=$(node_count gpu-peers)
if [[ "$g" -eq 0 ]]; then
	note "No GPU machine among the peers - skipping. Say: 'GPU is a hard filter; with none, it would refuse rather than run slowly.'"
	exit 0
fi
if [[ "$(node_count gpu-self)" -gt 0 ]]; then
	note "This machine HAS a GPU. The point lands better from a college PC - run it there."
fi
show grep -n "cuda\|DistributedDataParallel\|init_process_group" 04_torch_ddp.py
note "Plain PyTorch: 'cuda if available'. Nothing about other machines in it."
note "On the GPU laptops' screens now:   watch -n1 nvidia-smi"
pause

# grep keeps the screen short; the summary lines say where it ran.
# "this machine" and "on <node>" are the summary's usage lines: this PC idle,
# the laptop's GPU busy.
keep='using GPU|using CPU|rank [0-9]|epoch [0-9]+:|training done|final loss|TOTAL|ran on|closure|this machine|^  on |^ *total|cancelled'

note "One GPU: --remote sends it off this machine, --gpu force allows only GPU machines."
typed pipedpeer run 04_torch_ddp.py --remote --gpu force
"$PIPE" run 04_torch_ddp.py --remote --gpu force 2>&1 | grep -E "$keep"
echo
note "'using GPU: ...' printed by a machine that has no GPU. The script did not change."
note "'this machine' is this PC during the ~15 s of training: idle. The 'on ...' line is the laptop's GPU."

if [[ "$g" -ge 2 ]]; then
	pause
	# One epoch: each step syncs ~34 MB of gradients, and Wi-Fi makes three
	# epochs of that a long wait.
	note "Two GPUs: --ddp 2 = one rank on each laptop's GPU; gradients synced after every step."
	typed pipedpeer run 04_torch_ddp.py --ddp 2 --gpu force 1
	"$PIPE" run 04_torch_ddp.py --ddp 2 --gpu force 1 2>&1 | grep -E "$keep"
	echo
	note "Both nvidia-smi screens jumped. The PCs were never given a rank - filtered, not tried and failed."
fi
