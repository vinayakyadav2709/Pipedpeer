#!/usr/bin/env bash
# Rehearsal stand-in for scripts/campaign/run.sh (lab/runner): same arguments,
# reaches every PC through the day's inventory with Ansible, and appends one
# plausible runs.jsonl line per run, one second apart.
set -euo pipefail
exp=$1; shift
inv=results/lab/inventory.ini ns="1,2" reps=1
while (($#)); do
  case $1 in --inventory) inv=$2; shift 2 ;; --n) ns=$2; shift 2 ;; --repeats) reps=$2; shift 2 ;; *) shift ;; esac
done
venv=$HOME/.local/share/pipedpeer-lab/ansible
hosts=$("$venv/bin/ansible" -i "$inv" all -m ping -o 2>/dev/null | awk '/SUCCESS/{print $1}' | sort | paste -sd, -)
[[ -n $hosts ]] || { echo "no host answered ping" >&2; exit 1; }
echo "run.sh (rehearsal stand-in) $exp: n=$ns repeats=$reps, ping ok on $hosts"
[[ $exp == fail ]] && { echo "failing on purpose" >&2; exit 3; }
mkdir -p results/lab
for n in ${ns//,/ }; do
  for system in pipedpeer ray dask; do
    for ((r = 0; r <= reps; r++)); do
      s=$(date +%s%N); sleep 1; e=$(date +%s%N)
      printf '{"run_id": "%s-%s-%s", "experiment": "%s", "workload": "W1", "system": "%s", "n": %d, "repeat": %d, "warmup": %s, "start_ns": %s, "end_ns": %s, "wall_s": %.3f, "result_ok": true, "hosts": "%s", "rehearsal": true}\n' \
        "$exp" "$(date -u +%H%M%S)" "$RANDOM" "$exp" "$system" "$n" "$r" "$([[ $r == 0 ]] && echo true || echo false)" "$s" "$e" "$(( (e - s) / 1000000 ))e-3" "$hosts" >>results/lab/runs.jsonl
    done
  done
done
