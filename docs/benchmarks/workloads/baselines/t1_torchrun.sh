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

mkdir -p "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/t1-torchrun.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

# The lab PCs keep no ~/.ssh/config: pc00's own key and a first-contact host
# key are named here. A rank that hangs is cut by `timeout` on its own PC.
ssh_opts=(-n -i "${LAB_SSH_KEY:-$HOME/.ssh/pipedpeer-lab-pc00}" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
rank_timeout=${BENCH_RANK_TIMEOUT_S:-3000}

i=0
pids=()
while IFS= read -r ip; do
  [ "$i" -lt "$n" ] || break
  ssh "${ssh_opts[@]}" "$user@$ip" \
    "cd $workdir && BENCH_EXPECT_WORLD=$n BENCH_SEED=${BENCH_SEED:-11} BENCH_EPOCHS=${BENCH_EPOCHS:-30} \
     timeout $rank_timeout $py -m torch.distributed.run --nnodes=$n --nproc-per-node=1 --node-rank=$i \
       --master-addr=$master --master-port=29500 baselines/t1_torchrun.py" \
    >"$tmpd/rank$i.out" 2>"$tmpd/rank$i.err" &
  pids+=("$!")
  i=$((i + 1))
done <"$LAB_HOSTS_FILE"

# Wait for any rank. One that fails ends the run at once: a surviving rank
# would otherwise wait for it in the rendezvous until its timeout.
remaining=${#pids[@]}
while [ "$remaining" -gt 0 ]; do
  rc=0
  wait -n -p done_pid "${pids[@]}" || rc=$?
  if [ "$rc" != 0 ]; then
    for r in "${!pids[@]}"; do
      if [ "${pids[$r]}" = "${done_pid:-}" ]; then
        echo "t1_torchrun: rank $r failed (exit $rc):" >&2
        tail -n 5 "$tmpd/rank$r.err" >&2
      fi
    done
    for p in "${pids[@]}"; do
      [ "$p" = "${done_pid:-}" ] || kill "$p" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    exit 1
  fi
  remaining=$((remaining - 1))
done
cat "$tmpd/rank0.out"
