#!/usr/bin/env bash
# e6.sh (brief S14): faults. Kill arm: a PC's agent gets SIGKILL 30% of the way
# into a W1 run; Ctrl-C arm: SIGINT stops a pipedpeer run and the time until
# every PC reports 0 JOBS is the stop latency. Writes analysis-input/e6.csv:
#   system,kind,i,kill_at_s,wall_s,result_ok,stop_latency_s
#
#   e6.sh [--n N] [--systems a,b] [--kills K] [--ctrlc K] [--ctrlc-after S]
#         [--only kill|ctrlc] [--dry-run] [-h]
#
# Runs go through campaign_run_one (ids E6-W1-<system>-n<N>-r<i> and
# E6c-W1-pipedpeer-n<N>-r<i>), so wall_s and result_ok come from the run's
# runs.jsonl record. A crashed or cut run has result_ok 0; a run with no
# record has an empty wall_s and result_ok (logged). kill_at_s is the time from the run's
# own t0 to the moment the kill was issued, after the run was seen running.
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"
# shellcheck source=/dev/null
. "$D/run.sh"

e6_usage() { echo "usage: e6.sh [--n N] [--systems a,b] [--kills K] [--ctrlc K] [--ctrlc-after S] [--only kill|ctrlc] [--dry-run] [-h]"; }

# e6_median_wall <system>: median wall of the E1 W1 runs at N, else 60.
e6_median_wall() {
  python3 - "$LAB/runs.jsonl" "$1" "$N" <<'PY'
import json, sys
runs, system, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
vals = []
try:
    for ln in open(runs):
        d = json.loads(ln)
        if (d.get("experiment") == "E1" and d.get("workload") == "W1" and d.get("system") == system
                and d.get("n") == n and not d.get("warmup") and d.get("result_ok")):
            vals.append(d["wall_s"])
except FileNotFoundError:
    sys.stderr.write("e6: no runs.jsonl; median wall taken as 60 s\n")
vals.sort()
if not vals:
    sys.stderr.write("e6: no E1 W1 runs for %s at n=%d; median wall taken as 60 s\n" % (system, n))
print(vals[len(vals) // 2] if vals else 60)
PY
}

# e6_wait_running <run_id>: until the run on pc00 has a t0 and no exit file.
# Sets RUN_ELAPSED_NS (pc00's clock now minus t0, both read on pc00) and
# POLL_NS (the driver's clock at that moment).
e6_running_now() {
  local id="$1" out
  # shellcheck disable=SC2016  # remote script
  out="$(lab_ssh pc00 'R=~/pipedpeer-lab/runs/'"$id"'; t=$(cat $R/t0 2>/dev/null); if [ -n "$t" ] && [ ! -e $R/exit ]; then echo "$t $(date +%s%N)"; fi' 2>/dev/null)" || out=""
  if [[ "$out" =~ ^([0-9]+)\ ([0-9]+)$ ]]; then
    RUN_ELAPSED_NS=$((BASH_REMATCH[2] - BASH_REMATCH[1]))
    POLL_NS="$(date +%s%N)"
    return 0
  fi
  return 1
}
e6_wait_running() {
  local id="$1" deadline=$((SECONDS + ${E6_START_WAIT_S:-120}))
  while ((SECONDS < deadline)); do
    e6_running_now "$id" && return 0
    sleep 0.2
  done
  lab_log "e6 $id: never seen running within ${E6_START_WAIT_S:-120}s"
  return 1
}

# e6_record <run_id>: "<wall_s>,<result_ok>" from the run's runs.jsonl line;
# result_ok is 1 only for a true, 0 for a recorded failure; wall_s is empty
# when unknown. A run with no record prints "," (both empty: a measurement
# that failed is never 0) and logs the reason.
e6_record() {
  local id="$1" rec
  rec="$(grep -F -- "\"run_id\":\"$id\"" "$LAB/runs.jsonl" 2>/dev/null | tail -1 || true)"
  if [[ -z "$rec" ]]; then
    lab_log "e6 $id: no record in runs.jsonl; wall_s and result_ok left empty"
    echo ","
    return 0
  fi
  # stdin, not the environment: a long run's record is over the 128 KB limit of one env string
  printf '%s' "$rec" | python3 -c '
import json, sys
d = json.load(sys.stdin)
w = d.get("wall_s")
print("%s,%d" % ("" if not w else w, 1 if d.get("result_ok") is True else 0))
'
}


# e6_interrupt_run <id>: SIGINT to the `pipedpeer run` client on pc00 (the
# process whose argv is <...>/pipedpeer run ..., not the shell wrapper that
# started it, which would ignore or outlive the signal). Returns 1 and logs
# when no such process exists.
# shellcheck disable=SC2016  # the remote script is literal
E6_INT_SCRIPT='found=0
for p in $(pgrep -f "pipedpeer run"); do
  [ "$p" = "$$" ] && continue
  mapfile -d "" a </proc/$p/cmdline 2>/dev/null || continue
  for i in "${!a[@]}"; do
    if [ "${a[i]##*/}" = pipedpeer ] && [ "${a[i+1]:-}" = run ]; then
      kill -INT "$p" && found=1
      break
    fi
  done
done
[ "$found" = 1 ]'
e6_interrupt_run() {
  if ! lab_ssh pc00 "bash -c $(printf '%q' "$E6_INT_SCRIPT")"; then
    lab_log "e6 $1: no pipedpeer run process found on pc00 to interrupt; stop latency left empty"
    return 1
  fi
}

# e6_jobs_total <pc>: JOBS summed over the rows of that PC's `pipedpeer
# status` table; the JOBS column is found by the header's position (the table
# is tab-aligned). Prints nothing and returns 1 when the table is unreadable.
e6_jobs_total() {
  local f="$LAB/logs/e6-status-$1.txt"
  mkdir -p "$LAB/logs"
  lab_ssh "$1" pipedpeer status >"$f" 2>/dev/null || { lab_log "e6: pipedpeer status failed on $1"; return 1; }
  awk '
    /^NAME[ ]+STATE/ { col = index($0, "JOBS"); intable = 1; next }
    intable && NF == 0 { exit }
    intable && col { s = substr($0, col); split(s, a, " "); t += a[1] + 0; seen = 1 }
    END { if (!seen) exit 1; print t }
  ' "$f" || { lab_log "e6: no JOBS column in the status table of $1"; return 1; }
}

e6_main() {
  local DRY=0 N=16 KILLS=3 CTRLC=3 AFTER=10 ONLY=both
  local -a systems=(pipedpeer ray dask)
  while (($#)); do
    case "$1" in
      --dry-run) DRY=1; shift ;;
      --n) N="${2:?--n needs a number}"; shift 2 ;;
      --systems) IFS=',' read -r -a systems <<<"${2:?--systems needs a list}"; shift 2 ;;
      --kills) KILLS="${2:?}"; shift 2 ;;
      --ctrlc) CTRLC="${2:?}"; shift 2 ;;
      --ctrlc-after) AFTER="${2:?}"; shift 2 ;;
      --only) ONLY="${2:?}"; shift 2 ;;
      -h | --help) e6_usage; return 0 ;;
      *) e6_usage >&2; echo "e6.sh: unknown arg $1" >&2; return 2 ;;
    esac
  done

  mkdir -p "$LAB/analysis-input"
  if ((DRY)); then
    echo "E6-DRY-RUN n=$N systems=$(IFS=,; echo "${systems[*]}") kills=$KILLS ctrlc=$CTRLC"
    return 0
  fi

  _load_sizes || return 1
  local out="$LAB/analysis-input/e6.csv"
  [[ -f "$out" ]] || printf 'system,kind,i,kill_at_s,wall_s,result_ok,stop_latency_s\n' >"$out"

  local -A unit=([pipedpeer]=pipedpeer.service [ray]=lab-ray [dask]=lab-dask-worker)
  # the kill target is the 6th PC of the PCs in use (the 2nd on a small
  # cluster), never a name guessed from the list of all PCs
  local kill_pc p sys i id med remain kill_at pid rec now_ns
  local -a kill_hosts
  mapfile -t kill_hosts <"$(lab_hosts "$N")"
  kill_pc="${kill_hosts[5]:-${kill_hosts[1]:-}}"
  [[ -n "$kill_pc" ]] || { lab_log "e6: no PC to kill among $N"; return 1; }

  "$CLUSTER_SH" up "$N" || { lab_log "e6: cluster up failed"; return 1; }

  if [[ "$ONLY" != ctrlc ]]; then
    for sys in "${systems[@]}"; do
      med="$(e6_median_wall "$sys")"
      for ((i = 1; i <= KILLS; i++)); do
        id="E6-W1-$sys-n$N-r$i"
        if _run_id_in_runs "$id"; then
          echo "skipped $id"
          continue
        fi
        campaign_run_one E6 W1 "$sys" "$N" "$i" 0 600 &
        pid=$!
        kill_at=""
        if e6_wait_running "$id"; then
          remain="$(python3 -c "print(max(0.0, 0.3 * $med - $RUN_ELAPSED_NS / 1e9))")"
          sleep "$remain"
          # a run that ended while we waited was not disturbed: no kill, no kill_at_s
          if e6_running_now "$id"; then
            now_ns="$(date +%s%N)"
            kill_at="$(python3 -c "print(round(($RUN_ELAPSED_NS + $now_ns - $POLL_NS) / 1e9, 3))")"
            lab_ssh "$kill_pc" "systemctl --user kill -s SIGKILL ${unit[$sys]}" ||
              lab_log "e6 $id: SIGKILL of ${unit[$sys]} on $kill_pc failed; the run was not disturbed"
          else
            lab_log "e6 $id: run had already ended before the kill; no kill sent, kill_at_s left empty"
          fi
        else
          lab_log "e6 $id: no kill (run not seen running); kill_at_s left empty"
        fi
        wait "$pid" || lab_log "e6 $id: campaign_run_one returned $?"
        rec="$(e6_record "$id")"
        printf '%s,kill,%s,%s,%s,\n' "$sys" "$i" "$kill_at" "$rec" >>"$out"
        "$CLUSTER_SH" up "$N" || lab_log "e6: cluster up after the kill failed; later runs may be short of PCs"
      done
    done
  fi

  if [[ "$ONLY" != kill ]]; then
    local -a hosts
    mapfile -t hosts <"$(lab_hosts "$N")"
    local stop_s all_zero tot limit="${E6_STOP_LIMIT_S:-120}" t_int
    for ((i = 1; i <= CTRLC; i++)); do
      id="E6c-W1-pipedpeer-n$N-r$i"
      if _run_id_in_runs "$id"; then
        echo "skipped $id"
        continue
      fi
      campaign_run_one E6c W1 pipedpeer "$N" "$i" 0 600 &
      pid=$!
      stop_s=""
      if e6_wait_running "$id"; then
        sleep "$AFTER"
        t_int="$(date +%s%N)"
        if ! e6_interrupt_run "$id"; then
          wait "$pid" || lab_log "e6 $id: campaign_run_one returned $?"
          rec="$(e6_record "$id")"
          printf 'pipedpeer,ctrlc,%s,,%s,\n' "$i" "$rec" >>"$out"
          continue
        fi
        while :; do
          all_zero=1
          for p in "${hosts[@]}"; do
            if tot="$(e6_jobs_total "$p")" && [[ "$tot" == 0 ]]; then
              :
            else
              all_zero=0
            fi
          done
          now_ns="$(date +%s%N)"
          if ((all_zero)); then
            stop_s="$(python3 -c "print(round(($now_ns - $t_int) / 1e9, 3))")"
            break
          fi
          if ((now_ns - t_int > limit * 1000000000)); then
            lab_log "e6 $id: JOBS still not 0 on every PC ${limit}s after Ctrl-C; stop latency left empty"
            break
          fi
          sleep "${E6_POLL_S:-1}"
        done
      else
        lab_log "e6 $id: no Ctrl-C (run not seen running); stop latency left empty"
      fi
      wait "$pid" || lab_log "e6 $id: campaign_run_one returned $?"
      rec="$(e6_record "$id")"
      printf 'pipedpeer,ctrlc,%s,,%s,%s\n' "$i" "$rec" "$stop_s" >>"$out"
    done
  fi

  "$UPLOAD_SH" || lab_log "e6: upload failed; results stay in $LAB"
  echo "E6-DONE csv=$out"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  e6_main "$@"
fi
