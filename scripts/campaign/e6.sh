#!/usr/bin/env bash
# e6.sh (brief S14): faults — SIGKILL a PC's agent mid-run; Ctrl-C stops
# the run and its cleanup within a measured latency.
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"
# shellcheck source=/dev/null
. "$D/run.sh"

usage() { echo "usage: e6.sh [--n <N>] [--dry-run] [-h]"; }
DRY=0
N=16
while (($#)); do
  case "$1" in
    --dry-run) DRY=1; shift ;;
    --n) N="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; echo "e6.sh: unknown arg $1" >&2; exit 2 ;;
  esac
done

mkdir -p "$LAB/analysis-input" 2>/dev/null || true

if ((DRY)); then
  echo "E6-DRY-RUN n=$N systems=pipedpeer,ray,dask kills=3 ctrlc=3"
  exit 0
fi

out="$LAB/analysis-input/e6.csv"
printf 'system,kind,i,kill_at_s,wall_s,result_ok,stop_latency_s\n' >"$out"

# median wall of W1 at N from runs.jsonl, else a fixed 60s fallback.
med_wall() {
  local sys="$1"
  python3 - "$LAB/runs.jsonl" "$sys" "$N" <<'PY'
import json, sys
runs, sys_, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
vals = []
try:
    for ln in open(runs):
        d = json.loads(ln)
        if d["experiment"] == "E1" and d["workload"] == "W1" and d["system"] == sys_ and d["n"] == n and not d["warmup"] and d["result_ok"]:
            vals.append(d["wall_s"])
except FileNotFoundError:
    pass
vals.sort()
print(vals[len(vals)//2] if vals else 60)
PY
}

declare -A unit=( [pipedpeer]=pipedpeer.service [ray]=lab-ray [dask]=lab-dask-worker )
kill_pc="pc05"
[[ $N -lt 6 ]] && kill_pc="pc01"

for sys in pipedpeer ray dask; do
  med="$(med_wall "$sys")"
  kill_at="$(python3 -c "print(0.3 * $med)")"
  for i in 1 2 3; do
    run_id="E6-W1-$sys-n$N-k$i"
    if _run_id_in_runs "$run_id"; then
      echo "skipped $run_id"
      continue
    fi
    lab_detach pc00 "$run_id" "$(sys_cmd "$sys" W1 "$N" "$run_id")" || continue
    sleep "$kill_at"
    lab_ssh "$kill_pc" "systemctl --user kill -s SIGKILL ${unit[$sys]}" || true
    kill_elapsed="$(python3 -c "print($kill_at)")"
    lab_wait pc00 "$run_id" 600 || true
    wall="$(grep "\"run_id\":\"$run_id\"" "$LAB/runs.jsonl" | tail -1 | python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["wall_s"])' 2>/dev/null || echo 0)"
    ok="$(grep "\"run_id\":\"$run_id\"" "$LAB/runs.jsonl" | tail -1 | python3 -c 'import json,sys; print(1 if json.loads(sys.stdin.read())["result_ok"] else 0)' 2>/dev/null || echo 0)"
    printf '%s,kill,%s,%s,%s,%s,\n' "$sys" "$i" "$kill_elapsed" "$wall" "$ok" >>"$out"
    "$D/cluster.sh" up "$N" || true
  done
done

# Ctrl-C arm: SIGINT pipedpeer run on pc00, watch JOBS go to 0.
for i in 1 2 3; do
  run_id="E6-W1-pipedpeer-n$N-c$i"
  lab_detach pc00 "$run_id" 'cd ~/pipedpeer-lab/work && pipedpeer run pool_map.py' || continue
  sleep 10
  t0="$(date +%s%N)"
  lab_ssh pc00 'pkill -INT -f "pipedpeer run pool_map.py"' || true
  lab_wait pc00 "$run_id" 120 || true
  t1="$(date +%s%N)"
  stop="$(python3 -c "print(($t1-$t0)/1e9)")"
  printf 'pipedpeer,ctrlc,%s,,%s,1,%s\n' "$i" 0 "$stop" >>"$out"
done

"$D/upload.sh" || true
echo "E6-DONE csv=$out"
