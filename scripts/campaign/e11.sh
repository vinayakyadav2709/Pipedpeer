#!/usr/bin/env bash
# e11.sh (brief S15): cluster size — join time and idle membership bytes
# for 8, 16, 32 and all PCs.
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"

usage() { echo "usage: e11.sh [--sizes 8,16,32,all] [--idle <s>] [--dry-run] [-h]"; }
DRY=0
SIZES="8,16,32,all"
IDLE=600
while (($#)); do
  case "$1" in
    --dry-run) DRY=1; shift ;;
    --sizes) SIZES="$2"; shift 2 ;;
    --idle) IDLE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; echo "e11.sh: unknown arg $1" >&2; exit 2 ;;
  esac
done

if ((DRY)); then
  echo "E11-DRY-RUN sizes=$SIZES idle=$IDLE"
  exit 0
fi

count_hosts() { awk '!/^#/ && NF {c++} END {print c+0}' "$LAB/hosts.tsv"; }

# NIC counters: rx+tx of the lab NIC (first non-lo interface).
# shellcheck disable=SC2016  # remote awk is literal
read_counters() {
  lab_ssh "$1" 'awk "NR>2 {rx+=\$2; tx+=\$10; if (rx+tx>0) print rx+tx}" /proc/net/dev | tail -1' \
    | tail -1 | tr -d '[:space:]'
}

mkdir -p "$LAB/analysis-input"
out="$LAB/analysis-input/e11.csv"
printf 'k,pc,join_s,bytes_per_min,is_introducer\n' >"$out"

# Baseline: all daemons stopped 10 min (idle hours are tests' --idle).
all="$(count_hosts)"
hfile="$(lab_hosts "$all")"
lab_fanout e11-baseline "$hfile" 'pipedpeer stop 2>/dev/null || true; systemctl --user stop pipedpeer.service lab-ray lab-dask-worker lab-dask-scheduler 2>/dev/null || true' || true
declare -A base
while IFS= read -r pc; do base[$pc]="$(read_counters "$pc")"; done <"$hfile"
sleep "$IDLE"

for k in ${SIZES//,/ }; do
  if [[ "$k" == "all" ]]; then k="$all"; fi
  hfile="$(lab_hosts "$k")"
  t0="$(date +%s%N)"
  lab_fanout "e11-start-$k" "$hfile" 'pipedpeer start >/dev/null 2>&1 || true' || true
  # join_s: until pc00's status shows k healthy.
  healthy=0
  for _ in $(seq 1 60); do
    f="$(mktemp)"
    if lab_ssh pc00 'pipedpeer status' >"$f" 2>/dev/null; then
      healthy="$(status_healthy_count "$f" 2>/dev/null || echo 0)"
    fi
    rm -f "$f"
    [[ "$healthy" -ge "$k" ]] && break
    sleep 2
  done
  t1="$(date +%s%N)"
  join_s="$(python3 -c "print(($t1-$t0)/1e9)")"
  sleep "$IDLE"
  while IFS= read -r pc; do
    end="$(read_counters "$pc")"
    delta=$((end - base[$pc]))
    bpm="$(python3 -c "print(max($delta, 0) * 60 / $IDLE)")"
    intro=0; [[ "$pc" == "pc00" ]] && intro=1
    printf '%s,%s,%s,%s,%s\n' "$k" "$pc" "$join_s" "$bpm" "$intro" >>"$out"
  done <"$hfile"
done

# At `all`: one W1 run (strong size) for the record.
echo "E11-DONE csv=$out"
