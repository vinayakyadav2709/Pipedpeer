#!/usr/bin/env bash
# Port of t1_mlp.py to torchrun (gloo). Starts one rank per PC over ssh,
# rank 0 on pc00; waits for all and prints rank 0's output.
# Usage: t1_torchrun.sh <n>
set -euo pipefail

n=${1:?usage: t1_torchrun.sh <n>}
: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
user=${LAB_USER:-$(id -un)}
workdir=${LAB_WORKDIR:-$HOME/pipedpeer-lab/work}
py=${LAB_PY:-$HOME/pipedpeer-lab/venv/bin/python}
master=$(sed -n '1p' "$LAB_HOSTS_FILE")

tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/t1-torchrun.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

i=0
pids=()
while IFS= read -r ip; do
  [ "$i" -lt "$n" ] || break
  ssh -n -F "${SSH_CONFIG:-$HOME/.ssh/config}" "$user@$ip" \
    "cd $workdir && BENCH_EXPECT_WORLD=$n BENCH_SEED=${BENCH_SEED:-11} BENCH_EPOCHS=${BENCH_EPOCHS:-30} \
     $py -m torch.distributed.run --nnodes=$n --nproc-per-node=1 --node-rank=$i \
       --master-addr=$master --master-port=29500 baselines/t1_torchrun.py" \
    >"$tmpd/rank$i.out" 2>"$tmpd/rank$i.err" &
  pids+=("$!")
  i=$((i + 1))
done <"$LAB_HOSTS_FILE"

# `wait` alone returns 0 whatever the ranks did: collect every rank's status.
bad=0
for r in "${!pids[@]}"; do
  if ! wait "${pids[$r]}"; then
    echo "t1_torchrun: rank $r failed:" >&2
    tail -n 5 "$tmpd/rank$r.err" >&2
    bad=1
  fi
done
[ "$bad" = 0 ] || exit 1
cat "$tmpd/rank0.out"
