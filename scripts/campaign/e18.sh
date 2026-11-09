#!/usr/bin/env bash
# e18.sh (brief X17): E18, failure detection. At N the cluster is up and idle;
# the target (the second PC) loses one system's daemon to SIGKILL and a stop.
# From pc00, once a second, that one machine's state is read until it is gone
# or the limit (120 s) passes. Only the target's row counts: another node's
# state is never a detection. Writes analysis-input/e18.csv:
#   system,i,pc,detect_s,timed_out
#
#   e18.sh [--n 8] [--repeats 3] [--systems pipedpeer,ray,dask] [--dry-run] [-h]
#
# A kill or stop that fails writes its row empty and logs; there is no wait.
# detect_s is the driver's measured seconds from the kill to the first poll
# that sees the target gone; timed_out=1 means the limit passed and detect_s
# stays empty. cluster.sh up N runs between repeats.
#
# Environment (tests; the campaign leaves them alone):
#   E18_POLL_S (1), E18_DETECT_TIMEOUT_S (120).
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"
# shellcheck source=/dev/null
. "$D/run.sh"

E18_PC00=""
E18_HOST=""
E18_IP=""
E18_SCHED_IP=""

e18_usage() {
  echo "usage: e18.sh [--n 8] [--repeats 3] [--systems pipedpeer,ray,dask] [--dry-run] [-h]"
}

e18_poll_s() { printf '%s\n' "${E18_POLL_S:-1}"; }

# e18_plan <n> <repeats> <systems...>: the dry-run's plan, read-only.
e18_plan() {
  local n="$1" repeats="$2"
  shift 2
  local systems="$*" i
  local -a all=()
  mapfile -t all < <(_lab_available_hosts 2>/dev/null || true)
  echo "E18-DRY-RUN n=$n repeats=$repeats systems=$systems"
  if ((${#all[@]} < 2)); then
    echo "PCs unknown or too few (need at least 2 for pc00 and a target)"
    return 0
  fi
  printf 'OBSERVER %s\n' "${all[0]}"
  printf 'TARGET %s\n' "${all[1]}"
  printf 'KILL %s\n' "$systems"
  printf 'LIMIT %ss, poll every %ss, cluster.sh up %s between repeats\n' "${E18_DETECT_TIMEOUT_S:-120}" "$(e18_poll_s)" "$n"
}

# e18_hostname <pc>: the hostname column of hosts.tsv.
e18_hostname() {
  awk -F'\t' -v pc="$1" '$1 == pc { print $4; exit }' "$LAB/hosts.tsv"
}

# e18_kill <pc> <system>: SIGKILL the system's unit on that PC, then stop it
# (pipedpeer's Restart=on-failure would bring it back otherwise). Returns
# non-zero with a log line when the kill or the stop fails.
e18_kill() {
  local pc="$1" sys="$2" cmd
  case "$sys" in
    pipedpeer) cmd='systemctl --user kill -s SIGKILL pipedpeer.service && systemctl --user stop pipedpeer.service' ;;
    ray) cmd='systemctl --user kill -s SIGKILL lab-ray && systemctl --user stop lab-ray' ;;
    dask) cmd='systemctl --user kill -s SIGKILL lab-dask-worker && systemctl --user stop lab-dask-worker' ;;
    *)
      lab_log "E18: unknown system $sys"
      return 2
      ;;
  esac
  lab_ssh "$pc" "$cmd" ||
    {
      lab_log "E18: SIGKILL+stop of $sys on $pc failed; no wait, the row is left empty"
      return 1
    }
}

# e18_row_state <status file> <hostname>: the STATE of that hostname's row, or
# "absent"; exits 3 when the file has no status table (never a detection).
e18_row_state() {
  awk -v h="$2" '
    /^[[:space:]]*$/ { in_table = 0 }
    /^NAME[[:space:]]+STATE/ { in_table = 1; seen = 1; next }
    in_table { if ($1 == h || index($0, h " (") == 1) { print $2; found = 1; exit } }
    END { if (!seen) exit 3; if (!found) print "absent" }
  ' "$1"
}

# e18_ray_query <ip>: a command that prints alive Ray nodes at that address.
e18_ray_query() {
  local ip="$1" q
  q="import ray
ray.init(address='auto')
print(sum(1 for n in ray.nodes() if n['Alive'] and n['NodeManagerAddress'].split(':')[0] == '$ip'))"
  printf 'timeout 10 ~/pipedpeer-lab/venv/bin/python -c %q' "$q"
}

# e18_dask_query <scheduler ip> <worker ip>: prints 1 while the worker's host
# is in scheduler_info()['workers'], 0 when it is gone.
e18_dask_query() {
  local sched="$1" ip="$2" q
  q="from dask.distributed import Client
c = Client('tcp://$sched:8786')
print(int('$ip' in {a.rsplit('://', 1)[-1].split(':')[0] for a in c.scheduler_info()['workers']}))"
  printf 'timeout 10 ~/pipedpeer-lab/venv/bin/python -c %q' "$q"
}

# e18_gone <system>: 0 when the target is gone, 1 while it is still there,
# 2 when its state cannot be read (logged by the caller).
e18_gone() {
  local sys="$1" f="$LAB/logs/e18-status.txt" state="" v=""
  case "$sys" in
    pipedpeer)
      if ! lab_ssh "$E18_PC00" pipedpeer status >"$f" 2>/dev/null; then
        return 2
      fi
      state="$(e18_row_state "$f" "$E18_HOST")" || return 2
      [[ "$state" != healthy ]] && return 0
      return 1
      ;;
    ray)
      if ! v="$(lab_ssh "$E18_PC00" "$(e18_ray_query "$E18_IP")" 2>/dev/null | tail -1)"; then
        return 2
      fi
      v="${v//[[:space:]]/}"
      [[ "$v" == 0 ]] && return 0
      [[ "$v" =~ ^[1-9][0-9]*$ ]] && return 1
      return 2
      ;;
    dask)
      if ! v="$(lab_ssh "$E18_PC00" "$(e18_dask_query "$E18_SCHED_IP" "$E18_IP")" 2>/dev/null | tail -1)"; then
        return 2
      fi
      v="${v//[[:space:]]/}"
      [[ "$v" == 0 ]] && return 0
      [[ "$v" =~ ^[1-9][0-9]*$ ]] && return 1
      return 2
      ;;
    *)
      lab_log "E18: unknown system $sys"
      return 2
      ;;
  esac
}

# e18_poll <system>: "detect_s,timed_out" for one measurement. A state that
# cannot be read is logged once and polled again; the limit is the answer then.
e18_poll() {
  local sys="$1" limit="${E18_DETECT_TIMEOUT_S:-120}" limit_ns t0 now rc warned=0 detect="" timed=0
  if ! limit_ns="$(python3 -c "print(int(float('$limit') * 1e9))" 2>/dev/null)"; then
    lab_log "E18 $sys: E18_DETECT_TIMEOUT_S [$limit] is not a number; using 120s"
    limit=120
    limit_ns=120000000000
  fi
  t0="$(date +%s%N)"
  while :; do
    rc=1
    e18_gone "$sys" && rc=0 || rc=$?
    if ((rc == 0)); then
      now="$(date +%s%N)"
      detect="$(python3 -c "print(round(($now - $t0) / 1e9, 3))")"
      break
    fi
    if ((rc == 2 && warned == 0)); then
      lab_log "E18 $sys on $E18_HOST: the state could not be read; still polling"
      warned=1
    fi
    now="$(date +%s%N)"
    if (((now - t0) >= limit_ns)); then
      timed=1
      lab_log "E18 $sys on $E18_HOST: not detected within ${limit}s; timed_out=1"
      break
    fi
    sleep "$(e18_poll_s)"
  done
  printf '%s,%s\n' "$detect" "$timed"
}

e18_main() {
  local DRY=0 N=8 REPEATS=3
  local -a systems=(pipedpeer ray dask)
  while (($#)); do
    case "$1" in
      --dry-run) DRY=1; shift ;;
      --n) N="${2:?--n needs a number}"; shift 2 ;;
      --repeats) REPEATS="${2:?--repeats needs a number}"; shift 2 ;;
      --systems) IFS=',' read -r -a systems <<<"${2:?--systems needs a list}"; shift 2 ;;
      -h | --help) e18_usage; return 0 ;;
      *)
        e18_usage >&2
        echo "e18.sh: unknown arg $1" >&2
        return 2
        ;;
    esac
  done
  [[ "$N" =~ ^[0-9]+$ && "$REPEATS" =~ ^[0-9]+$ ]] ||
    { e18_usage >&2; echo "e18.sh: --n and --repeats need numbers" >&2; return 2; }
  if ((N < 2)); then
    echo "e18.sh: --n must be at least 2 (pc00 and a target)" >&2
    return 2
  fi
  if ((REPEATS < 1)); then
    echo "e18.sh: --repeats must be at least 1" >&2
    return 2
  fi

  if ((DRY)); then
    e18_plan "$N" "$REPEATS" "${systems[@]}"
    return 0
  fi

  mkdir -p "$LAB/analysis-input" "$LAB/logs"
  local hfile
  hfile="$(lab_hosts "$N")" || {
    lab_log "E18: need $N PCs"
    echo "E18-NEEDS $N PCs" >&2
    return 1
  }
  local -a hosts=()
  mapfile -t hosts <"$hfile"
  local target="${hosts[1]}"
  E18_PC00="${hosts[0]}"
  E18_HOST="$(e18_hostname "$target")"
  E18_IP="$(lab_ip "$target")"
  E18_SCHED_IP="$(lab_ip "$E18_PC00")"
  if [[ -z "$E18_HOST" || -z "$E18_IP" ]]; then
    lab_log "E18: no hostname or ip for $target in hosts.tsv"
    echo "E18-NEEDS a row for $target in hosts.tsv" >&2
    return 1
  fi
  local node_id
  node_id="$(lab_ssh "$target" pipedpeer status 2>/dev/null | head -1 | sed -n 's/.*node-id=\([^ )]*\).*/\1/p')" || node_id=""
  lab_log "E18: observer $E18_PC00, target $target ($E18_HOST, ${node_id:-no node-id}), ip $E18_IP"

  local out="$LAB/analysis-input/e18.csv"
  [[ -s "$out" ]] || printf 'system,i,pc,detect_s,timed_out\n' >"$out"

  local sys i detect timed
  for sys in "${systems[@]}"; do
    case "$sys" in
      pipedpeer | ray | dask) : ;;
      *)
        lab_log "E18: unknown system $sys in --systems; skipped"
        continue
        ;;
    esac
    for ((i = 1; i <= REPEATS; i++)); do
      detect=""
      timed=""
      if ! "$CLUSTER_SH" up "$N"; then
        lab_log "E18 $sys r$i: cluster up failed; the row is left empty"
        printf '%s,%s,%s,,\n' "$sys" "$i" "$target" >>"$out"
        continue
      fi
      if ! e18_kill "$target" "$sys"; then
        printf '%s,%s,%s,,\n' "$sys" "$i" "$target" >>"$out"
        continue
      fi
      IFS=, read -r detect timed <<<"$(e18_poll "$sys")"
      printf '%s,%s,%s,%s,%s\n' "$sys" "$i" "$target" "$detect" "$timed" >>"$out"
    done
  done

  "$UPLOAD_SH" || lab_log "E18: upload failed; results stay in $LAB"
  echo "E18-DONE csv=$out"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  e18_main "$@"
fi
