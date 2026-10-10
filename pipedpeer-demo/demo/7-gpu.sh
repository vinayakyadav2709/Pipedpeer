#!/usr/bin/env bash
# Step 7 - plain PyTorch training, placed only on machines with a GPU.
source "$(dirname "$0")/_lib.sh"

banner "Step 7 - GPU training"
g=$(gpu_nodes)
if [[ "$g" -eq 0 ]]; then
	note "No GPU machine in the cluster - skipping. Say: 'GPU is a hard filter; with none, it would refuse rather than run slowly.'"
	exit 0
fi
show grep -n "DistributedDataParallel\|init_process_group\|torchrun" 04_torch_ddp.py || note "^ none of those appear in the file."
note "Put 'watch -n1 nvidia-smi' on the GPU machines' screens now."
pause
if [[ "$g" -ge 2 ]]; then
	note "--ddp 2: two ranks on the two GPU machines. The shim sets up the process group and syncs gradients."
	typed pipedpeer run 04_torch_ddp.py --ddp 2
	"$PIPE" run 04_torch_ddp.py --ddp 2 2>&1 | grep -E 'using GPU|rank|epoch [0-9] step +0 |training done|final loss|TOTAL|Placed|completed'
else
	note "One GPU machine: the job is placed there because the script imports torch and uses CUDA."
	typed pipedpeer run 04_torch_ddp.py
	"$PIPE" run 04_torch_ddp.py 2>&1 | grep -E 'using GPU|epoch [0-9] step +0 |training done|final loss|TOTAL|Placed|completed'
fi
note "The machine without a GPU was never given a rank - filtered, not tried and failed."
