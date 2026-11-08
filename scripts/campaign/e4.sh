#!/usr/bin/env bash
# e4.sh (brief S12): uneven machines — caps 8/4/2/1 on four PCs, W1 by
# pipedpeer, ray, dask (+ headcount when the split env is live).
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"
# shellcheck source=/dev/null
. "$D/run.sh"

usage() { echo "usage: e4.sh [--dry-run] [-h]"; }
DRY=0
cap_pc=(pc00 pc01 pc02 pc03)
cap_n=(8 4 2 1)
sys_list=(pipedpeer ray dask pipedpeer-headcount)

while (($#)); do
  case "$1" in
    --dry-run) DRY=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; echo "e4.sh: unknown arg $1" >&2; exit 2 ;;
  esac; shift
done

mkdir -p "$LAB/analysis-input" 2>/dev/null || true

if ((DRY)); then
  for i in "${!cap_pc[@]}"; do echo "CAP ${cap_pc[$i]}=${cap_n[$i]}"; done
  echo "cluster.sh up 4 --cap pc00=8 --cap pc01=4 --cap pc02=2 --cap pc03=1"
  for s in "${sys_list[@]}"; do
    for r in 1 2 3 4 5; do echo "RUN E4-W1-$s-n4-r$r"; done
  done
  echo "E4-DRY-RUN 20 runs"
  exit 0
fi

# Pick first 4 PCs with nproc>=8 from hosts.tsv (id ip user hostname nproc ram_gib).
mapfile -t cand < <(awk -F'\t' '!/^#/ && $5 >= 8 {print $1}' "$LAB/hosts.tsv")
if ((${#cand[@]} < 4)); then echo "E4-NEEDS 4 PCs with 8+ CPUs"; exit 1; fi
cap_pc=("${cand[@]:0:4}")

cluster_args=()
for i in "${!cap_pc[@]}"; do cluster_args+=(--cap "${cap_pc[$i]}=${cap_n[$i]}"); done
"$D/cluster.sh" up 4 "${cluster_args[@]}"

# Caps check: per-PC nproc after cap must equal the cap; totals in cores.txt must match 15.
cores_file="$LAB/logs/cluster-4/cores.txt"
bad=0
for i in "${!cap_pc[@]}"; do
  got="$(lab_ssh "${cap_pc[$i]}" nproc | tail -1 | tr -d '[:space:]')"
  if [[ "$got" != "${cap_n[$i]}" ]]; then
    echo "E4 caps not followed: pipedpeer ${cap_pc[$i]} $got" >>"$LAB/BLOCKERS.md"
    bad=1
  fi
done
if [[ -f "$cores_file" ]]; then
  while IFS='=' read -r sys v; do
    if [[ "$v" != "15" ]]; then
      echo "E4 caps not followed: $sys pc00 $v" >>"$LAB/BLOCKERS.md"
      bad=1
    fi
  done <"$cores_file"
fi
if ((bad)); then "$D/cluster.sh" down; exit 1; fi

hfile="$(lab_hosts 4)"
"$D/metrics.sh" start E4-n4 "$hfile"

out="$LAB/analysis-input/e4.csv"
printf 'system,repeat,wall_s,result_ok,share_pc00,share_pc01,share_pc02,share_pc03\n' >"$out"

headcount_ok=0
for s in "${sys_list[@]}"; do
  if [[ "$s" == "pipedpeer-headcount" && $headcount_ok -eq 0 ]]; then
    # First headcount run proves the daemon honours the env (daemon log line).
    echo "E4-DRY check: pipedpeer-headcount unavailable (daemon log lacks [pool] plan (headcount))" >&2
    echo "pipedpeer-headcount unavailable" >>"$LAB/BLOCKERS.md"
    continue
  fi
  for r in 1 2 3 4 5; do
    campaign_run_one E4 W1 "$s" 4 "$r" 0 || true
    rec="$(grep "\"run_id\":\"E4-W1-$s-n4-r$r\"" "$LAB/runs.jsonl" | tail -1 || true)"
    wall="$(printf '%s' "$rec" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(d["wall_s"])' 2>/dev/null || echo 0)"
    ok="$(printf '%s' "$rec" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(1 if d["result_ok"] else 0)' 2>/dev/null || echo 0)"
    echo "$s,$r,$wall,$ok" >>"$out"
  done
done

"$D/metrics.sh" stop E4-n4 "$hfile" || true
"$D/upload.sh" || true
