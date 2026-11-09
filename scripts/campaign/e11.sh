#!/usr/bin/env bash
# e11.sh (brief S15): cluster size. For each size k (default 8, 16, 32, all):
# how long until k daemons show healthy on pc00 (join_s) and the idle
# membership traffic per PC (bytes_per_min). Writes analysis-input/e11.csv:
#   k,pc,join_s,bytes_per_min,is_introducer
#
#   e11.sh [--sizes 8,16,32,all] [--idle <s>] [--dry-run] [-h]
#
# Traffic is rx+tx of the default-route NIC (never lo). A baseline pass with
# every daemon stopped measures what the machine does on its own over the
# same idle window; it is subtracted from each size's delta. join_s is empty
# (and the reason logged) when k daemons never became healthy within
# E11_JOIN_S (default 120 s). A counter that cannot be read leaves
# bytes_per_min empty, never 0. At `all` one W1 run goes through
# campaign_run_one.
#
# Sourceable: e5.sh uses read_counters from here. E11_IDLE_SLEEP replaces
# `sleep` for the idle windows (tests advance counter files with it).
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"
# shellcheck source=/dev/null
. "$D/run.sh"
# shellcheck source=/dev/null
. "$D/cluster.sh"

e11_usage() { echo "usage: e11.sh [--sizes 8,16,32,all] [--idle <s>] [--dry-run] [-h]"; }

# read_counters <pc> [rx]: rx+tx bytes (rx only with the second argument) of
# the default-route NIC, one integer. A
# failure (no default route, no such interface, ssh error) logs and returns 1.
# LAB_NET_DEV points the counters at another file (tests).
# shellcheck disable=SC2016  # the remote script is literal
read_counters() {
  local pc="$1" mode="${2:-rxtx}" v
  if ! v="$(lab_ssh "$pc" "m=$mode; "'nic=$(ip route show default | awk '"'"'{for(i=1;i<NF;i++) if($i=="dev"){print $(i+1); exit}}'"'"'); f=${LAB_NET_DEV:-/proc/net/dev}; f=${f/#\~/$HOME}; awk -v n="$nic" -v m="$m" '"'"'{sub(":"," ")} $1==n {printf "%.0f\n", $2+(m=="rx"?0:$10)}'"'"' "$f"' | tail -1)"; then
    lab_log "e11 $pc: could not read the NIC counters"
    return 1
  fi
  v="${v//[[:space:]]/}"
  if [[ ! "$v" =~ ^[0-9]+$ ]]; then
    lab_log "e11 $pc: no counter for the default-route NIC (got '$v')"
    return 1
  fi
  printf '%s\n' "$v"
}

# e11_result_ok <run_id>: "true" or "false" as recorded in runs.jsonl; empty
# (with the reason logged) when the record has no value or there is no record.
# Never the text "null".
e11_result_ok() {
  local v
  v="$(python3 - "$LAB/runs.jsonl" "$1" <<'PY'
import json, sys
try:
    for ln in open(sys.argv[1]):
        d = json.loads(ln)
        if d.get("run_id") == sys.argv[2]:
            v = d.get("result_ok")
            print("" if v is None else str(bool(v)).lower())
            sys.exit(0)
except FileNotFoundError:
    pass
sys.exit(3)
PY
)" || { lab_log "e11 $1: no record in runs.jsonl; result_ok left empty"; return 0; }
  [[ -n "$v" ]] || lab_log "e11 $1: the record has no result_ok (no reference to check against); left empty"
  printf '%s\n' "$v"
}

e11_idle() { "${E11_IDLE_SLEEP:-sleep}" "$1"; }

e11_main() {
  local DRY=0 SIZES="8,16,32,all" IDLE=600
  while (($#)); do
    case "$1" in
      --dry-run) DRY=1; shift ;;
      --sizes) SIZES="${2:?--sizes needs a list}"; shift 2 ;;
      --idle) IDLE="${2:?--idle needs seconds}"; shift 2 ;;
      -h | --help) e11_usage; return 0 ;;
      *) e11_usage >&2; echo "e11.sh: unknown arg $1" >&2; return 2 ;;
    esac
  done

  if ((DRY)); then
    echo "E11-DRY-RUN sizes=$SIZES idle=$IDLE"
    return 0
  fi

  local all hfile allfile k pc out limit
  all="$(lab_available_count)"
  limit="${E11_JOIN_S:-120}"
  _load_sizes || return 1
  mkdir -p "$LAB/analysis-input"
  out="$LAB/analysis-input/e11.csv"
  printf 'k,pc,join_s,bytes_per_min,is_introducer\n' >"$out"

  allfile="$(lab_hosts "$all")"
  e11_stop_all "$allfile"

  # Baseline: the same idle window with every daemon stopped.
  local -A base=() cstart=()
  while IFS= read -r pc; do cstart[$pc]="$(read_counters "$pc" || true)"; done <"$allfile"
  e11_idle "$IDLE"
  local end
  while IFS= read -r pc; do
    end="$(read_counters "$pc" || true)"
    if [[ -n "${cstart[$pc]}" && -n "$end" ]]; then
      base[$pc]=$((end - cstart[$pc]))
    else
      lab_log "e11 $pc: baseline counters unreadable; its bytes_per_min stays empty"
    fi
  done <"$allfile"

  for k in ${SIZES//,/ }; do
    [[ "$k" == all ]] && k="$all"
    hfile="$(lab_hosts "$k")" || { lab_log "e11: cannot get $k PCs; size skipped"; continue; }
    e11_stop_all "$allfile"
    local st="$LAB/logs/e11-status.txt" t0 t1 got=0 join_s="" waited_ns=0
    mkdir -p "$LAB/logs"
    t0="$(date +%s%N)"
    lab_fanout "e11-start-$k" "$hfile" 'pipedpeer start' >/dev/null ||
      lab_log "e11 k=$k: pipedpeer start failed on every PC"
    while :; do
      if lab_ssh pc00 pipedpeer status >"$st" 2>/dev/null; then
        got="$(status_healthy_count "$st" 2>/dev/null || echo 0)"
      fi
      t1="$(date +%s%N)"
      if ((got >= k)); then
        join_s="$(python3 -c "print(round(($t1 - $t0) / 1e9, 3))")"
        break
      fi
      waited_ns=$((t1 - t0))
      if ((waited_ns >= limit * 1000000000)); then
        lab_log "e11 k=$k: $got of $k daemons healthy after ${limit}s; did not reach $k healthy, join_s left empty"
        break
      fi
      sleep 2
    done

    local -A kstart=()
    while IFS= read -r pc; do kstart[$pc]="$(read_counters "$pc" || true)"; done <"$hfile"
    e11_idle "$IDLE"
    local bpm intro first
    first="$(head -1 "$hfile")"
    while IFS= read -r pc; do
      end="$(read_counters "$pc" || true)"
      bpm=""
      if [[ -n "${kstart[$pc]}" && -n "$end" && -n "${base[$pc]:-}" ]]; then
        bpm="$(python3 -c "
d = $end - ${kstart[$pc]} - ${base[$pc]}
if d < 0:
    import sys
    sys.stderr.write('e11 $pc: delta below the baseline (%d); counted as 0\n' % d)
    d = 0
v = d * 60 / $IDLE
print(int(v) if v == int(v) else round(v, 2))
")"
      else
        lab_log "e11 k=$k $pc: counters unreadable; bytes_per_min left empty"
      fi
      intro=0
      [[ "$pc" == "$first" ]] && intro=1
      printf '%s,%s,%s,%s,%s\n' "$k" "$pc" "$join_s" "$bpm" "$intro" >>"$out"
    done <"$hfile"

    if [[ "$k" == "$all" ]]; then
      campaign_run_one E11 W1 pipedpeer "$k" 1 0 || lab_log "e11: the W1 run at all=$k did not finish cleanly"
      lab_log "e11: W1 run at all=$k result_ok=$(e11_result_ok "E11-W1-pipedpeer-n$k-r1")"
    fi
    unset kstart
  done
  echo "E11-DONE csv=$out"
}

e11_stop_all() {
  # shellcheck disable=SC2016  # expanded on the PC, not here
  lab_fanout e11-stop "$1" 'pipedpeer stop || echo "pipedpeer stop: not running"; systemctl --user stop lab-ray lab-dask-worker lab-dask-scheduler 2>/dev/null; for u in lab-ray lab-dask-worker lab-dask-scheduler; do if [ "$(systemctl --user show -p ActiveState --value $u 2>/dev/null)" = active ]; then echo "$u is still active" >&2; exit 1; fi; done; exit 0' >/dev/null ||
    lab_log "e11: stopping the daemons failed on every PC"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  e11_main "$@"
fi
