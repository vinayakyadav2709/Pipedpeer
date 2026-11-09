#!/usr/bin/env bash
# e11.sh (brief S15): cluster size. For each size k (default 8, 16, 32, all):
# how long until k daemons show healthy on pc00 (join_s) and the idle
# membership traffic per PC (bytes_per_min). Writes analysis-input/e11.csv:
#   k,pc,join_s,bytes_per_min,is_introducer,joined
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

# e11_read_counters_fanout <step> <hostsfile>
# Reads default-route NIC counters and timestamps for all PCs in <hostsfile>
# in parallel using lab_fanout.
# Writes $LAB/logs/<step>/counters.tsv (pc<TAB>bytes<TAB>ts)
# and $LAB/logs/<step>/timestamps.tsv (pc<TAB>ts).
# shellcheck disable=SC2016  # the remote script is literal
e11_read_counters_fanout() {
  local step="${1:?usage: e11_read_counters_fanout <step> <hostsfile>}"
  local hostsfile="${2:?usage: e11_read_counters_fanout <step> <hostsfile>}"
  local probe_cmd outdir log out pc val ts

  probe_cmd='m=rxtx; nic=$(ip route show default 2>/dev/null | awk '"'"'{for(i=1;i<NF;i++) if($i=="dev"){print $(i+1); exit}}'"'"'); f=${LAB_NET_DEV:-/proc/net/dev}; f=${f/#\~/$HOME}; v=""; if [ -f "$f" ]; then v=$(awk -v n="$nic" -v m="$m" '"'"'{sub(":"," ")} $1==n {printf "%.0f\n", $2+(m=="rx"?0:$10)}'"'"' "$f" 2>/dev/null | tail -1); fi; ts=$(date +%s%N); printf "%s\t%s\n" "${v:-NONE}" "$ts"'

  lab_fanout "$step" "$hostsfile" "$probe_cmd" >/dev/null 2>&1 || true

  outdir="$LAB/logs/$step"
  mkdir -p "$outdir"
  : >"$outdir/counters.tsv"
  : >"$outdir/timestamps.tsv"

  while IFS= read -r pc; do
    [[ -z "$pc" || "$pc" == \#* ]] && continue
    log="$outdir/$pc"
    val=""
    ts=""
    if [[ -f "$log" ]]; then
      out="$(cat "$log" 2>/dev/null || true)"
      out="${out//[[:space:]]/ }"
      read -r val ts <<<"$out"
    fi

    if [[ ! "$val" =~ ^[0-9]+$ || ! "$ts" =~ ^[0-9]+$ ]]; then
      val=""
      ts=""
      lab_log "e11 $pc: counter unreadable ($step)"
    fi

    printf '%s\t%s\t%s\n' "$pc" "${val:--}" "${ts:--}" >>"$outdir/counters.tsv"
    printf '%s\t%s\n' "$pc" "${ts:--}" >>"$outdir/timestamps.tsv"
  done <"$hostsfile"
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
  printf 'k,pc,join_s,bytes_per_min,is_introducer,joined\n' >"$out"

  allfile="$(lab_hosts "$all")"
  e11_stop_all "$allfile"

  # Baseline: the same idle window with every daemon stopped.
  local -A base=() base_ns=() cstart=() cstart_ts=()
  e11_read_counters_fanout "e11-counters-base-start" "$allfile"
  while IFS=$'\t' read -r pc val ts; do
    [[ "$val" == "-" ]] && val=""
    cstart[$pc]="$val"
    cstart_ts[$pc]="$ts"
  done <"$LAB/logs/e11-counters-base-start/counters.tsv"

  e11_idle "$IDLE"

  e11_read_counters_fanout "e11-counters-base-end" "$allfile"
  while IFS=$'\t' read -r pc val ts; do
    [[ "$val" == "-" ]] && val=""
    if [[ -n "${cstart[$pc]:-}" && -n "$val" ]] && ((ts > cstart_ts[$pc])); then
      base[$pc]=$((val - cstart[$pc]))
      base_ns[$pc]=$((ts - cstart_ts[$pc]))
    else
      lab_log "e11 $pc: baseline counters unreadable; its bytes_per_min stays empty"
    fi
  done <"$LAB/logs/e11-counters-base-end/counters.tsv"

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

    local -A kstart=() kstart_ts=()
    e11_read_counters_fanout "e11-counters-$k-start" "$hfile"
    while IFS=$'\t' read -r pc val ts; do
      [[ "$val" == "-" ]] && val=""
      kstart[$pc]="$val"
      kstart_ts[$pc]="$ts"
    done <"$LAB/logs/e11-counters-$k-start/counters.tsv"

    e11_idle "$IDLE"

    local -A kend=() kend_ts=()
    e11_read_counters_fanout "e11-counters-$k-end" "$hfile"
    while IFS=$'\t' read -r pc val ts; do
      [[ "$val" == "-" ]] && val=""
      kend[$pc]="$val"
      kend_ts[$pc]="$ts"
    done <"$LAB/logs/e11-counters-$k-end/counters.tsv"

    local bpm intro first joined end s_ts e_ts
    first="$(head -1 "$hfile")"
    joined="${got}/${k}"
    while IFS= read -r pc; do
      end="${kend[$pc]:-}"
      bpm=""
      s_ts="${kstart_ts[$pc]:-}"
      e_ts="${kend_ts[$pc]:-}"
      if [[ -n "${kstart[$pc]:-}" && -n "$end" && -n "${base[$pc]:-}" ]]; then
        bpm="$(python3 -c "
import sys
interval_ns = $e_ts - $s_ts
if interval_ns <= 0:
    sys.stderr.write('e11 $pc: end read is not after the start read; rate left empty\n')
    sys.exit(0)
# The baseline window has its own measured length: scale its bytes to this one.
d = $end - ${kstart[$pc]} - ${base[$pc]} * interval_ns / ${base_ns[$pc]}
if d < 0:
    sys.stderr.write('e11 $pc: delta below the baseline (%d); counted as 0\n' % d)
    d = 0
v = round(d * 60e9 / interval_ns, 2)
print(int(v) if v == int(v) else v)
")"
      else
        lab_log "e11 k=$k $pc: counters unreadable; bytes_per_min left empty"
      fi
      intro=0
      [[ "$pc" == "$first" ]] && intro=1
      printf '%s,%s,%s,%s,%s,%s\n' "$k" "$pc" "$join_s" "$bpm" "$intro" "$joined" >>"$out"
    done <"$hfile"

    if [[ "$k" == "$all" ]]; then
      campaign_run_one E11 W1 pipedpeer "$k" 1 0 || lab_log "e11: the W1 run at all=$k did not finish cleanly"
      lab_log "e11: W1 run at all=$k result_ok=$(e11_result_ok "E11-W1-pipedpeer-n$k-r1")"
    fi
    unset kstart kstart_ts kend kend_ts
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
