#!/usr/bin/env bash
# e17.sh (brief X17): E17, a job that loses its introducer. pc00 introduces the
# cluster and every member daemon is restarted with
# PIPEDPEER_NO_LAN_DISCOVERY=1, so a PC that joins mid-job depends on the
# introducer. A W1 run is submitted on the second member; 30% of the measured
# W1 wall in, pc00 is SIGKILLed and its unit stopped. A new code from the
# surviving member then joins a fresh PC, and a small forced W1 job proves work
# still leaves it. Writes analysis-input/e17.csv:
#   i,kill_at_s,job_wall_s,job_result_ok,new_join_s,new_join_ok,new_remote_items,old_invite_joined
#
#   e17.sh [--n 4] [--joiners 2] [--repeats 3] [--dry-run] [-h]
#
# --n counts every member, pc00 included; the joiners are the next PCs in
# lab_hosts. Every wait is finite; a step that times out writes its row with
# empty fields and goes on. EXIT, INT and TERM restore the members' environment
# and daemons and every file moved aside on a joiner.
#
# Environment (tests; the campaign leaves them alone):
#   E17_START_WAIT_S (120), E17_JOIN_S (120), E17_POLL_S (1),
#   E17_CONNECT_TIMEOUT_S (120), E17_OLD_INVITE_TIMEOUT_S (60),
#   E17_JOB_TIMEOUT_S (600), E17_RESTART_WAIT_S (120), E17_MEDIAN_WALL_S (unset:
#   the E1 median, else the W1 calibration target; sets the kill point).
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"
# shellcheck source=/dev/null
. "$D/run.sh"

E17_MEMBERS=()
E17_MOVED=() # "pc<TAB>file<TAB>existed"
E17_JOB_PID=""
E17_CONNECT_PID=""
E17_ENV_SET=0
E17_RUN_ELAPSED_NS=0
E17_POLL_NS=0

e17_usage() {
  echo "usage: e17.sh [--n 4] [--joiners 2] [--repeats 3] [--dry-run] [-h]"
}

# e17_poll_s: the polling interval, in seconds.
e17_poll_s() { printf '%s\n' "${E17_POLL_S:-1}"; }

# e17_plan <n> <joiners> <repeats>: the dry-run's plan, read-only.
e17_plan() {
  local n="$1" joiners="$2" repeats="$3" need i
  local -a all=()
  need=$((n + joiners))
  mapfile -t all < <(_lab_available_hosts 2>/dev/null || true)
  echo "E17-DRY-RUN n=$n joiners=$joiners repeats=$repeats"
  if ((${#all[@]} < need)); then
    echo "PCs unknown or too few (need $need, hosts.tsv lists ${#all[@]})"
    return 0
  fi
  printf 'INTRODUCER %s\n' "${all[0]}"
  printf 'MEMBERS'
  for ((i = 0; i < n; i++)); do printf ' %s' "${all[$i]}"; done
  echo
  printf 'SUBMIT_PC %s\n' "${all[1]}"
  printf 'JOINERS'
  for ((i = n; i < n + joiners; i++)); do printf ' %s' "${all[$i]}"; done
  echo
  printf 'KILL_AT 30%% of the pipedpeer E1 W1 median wall: SIGKILL then stop on %s\n' "${all[0]}"
}

# e17_median_wall: the wall the kill point is 30% of. E17_MEDIAN_WALL_S when
# set; else the E1 W1 median on pipedpeer at this N; else (no E1 yet, as in
# the Session 1 check) the W1 calibration target in sizes.env; else 10 s.
# A fixed 10 s let a short calibrated job finish before the kill (emulated lab).
e17_median_wall() {
  if [[ -n "${E17_MEDIAN_WALL_S:-}" ]]; then
    printf '%s\n' "$E17_MEDIAN_WALL_S"
    return 0
  fi
  python3 - "$LAB/runs.jsonl" "$N" "$LAB/sizes.env" <<'PY'
import json, re, sys
runs, n, sizes = sys.argv[1], int(sys.argv[2]), sys.argv[3]
vals = []
try:
    for ln in open(runs):
        d = json.loads(ln)
        if (d.get("experiment") == "E1" and d.get("workload") == "W1"
                and d.get("system") == "pipedpeer" and d.get("n") == n
                and not d.get("warmup") and d.get("result_ok")):
            vals.append(d["wall_s"])
except FileNotFoundError:
    sys.stderr.write("e17: no runs.jsonl; median wall taken as 10 s\n")
vals.sort()
if vals:
    print(vals[len(vals) // 2])
    sys.exit(0)
target = None
try:
    for ln in open(sizes):
        m = re.search(r"plain W1=.*target ([0-9.]+)s", ln)
        if m:
            target = float(m.group(1))
except FileNotFoundError:
    pass
if target:
    sys.stderr.write("e17: no E1 W1 runs for pipedpeer at n=%d; median wall taken as the W1 calibration target, %g s\n" % (n, target))
    print("%g" % target)
else:
    sys.stderr.write("e17: no E1 W1 runs for pipedpeer at n=%d and no W1 target in sizes.env; median wall taken as 10 s\n" % n)
    print(10)
PY
}

# e17_invite <pc>: the "pipedpeer connect ..." code from that PC's invite.
# Empty and logged when the invite or its parse fails.
e17_invite() {
  local pc="$1" inv code
  if ! inv="$(lab_ssh "$pc" pipedpeer invite)"; then
    lab_log "E17: invite on $pc failed"
    return 1
  fi
  code="$(printf '%s\n' "$inv" | sed -n 's/^pipedpeer connect //p' | head -1)"
  if [[ -z "$code" ]]; then
    lab_log "E17: invite on $pc printed no 'pipedpeer connect' line"
    return 1
  fi
  printf '%s\n' "$code"
}

# e17_record <run_id>: "<job_wall_s>,<job_result_ok>" from runs.jsonl. Both
# empty (and logged) when there is no record; a record that cannot be parsed
# leaves them empty too.
e17_record() {
  local id="$1" rec out
  rec="$(grep -F -- "\"run_id\":\"$id\"" "$LAB/runs.jsonl" 2>/dev/null | tail -1 || true)"
  if [[ -z "$rec" ]]; then
    lab_log "E17 $id: no record in runs.jsonl; job_wall_s and job_result_ok left empty"
    printf ',\n'
    return 0
  fi
  # stdin, not the environment: a long record is over one env string's limit
  if ! out="$(printf '%s' "$rec" | python3 -c '
import json, sys
d = json.load(sys.stdin)
w = d.get("wall_s")
r = d.get("result_ok")
print("%s,%s" % ("" if not w else w, "" if r is None else (1 if r is True else 0)))
')"; then
    lab_log "E17 $id: the runs.jsonl record is not usable; wall and result left empty"
    printf ',\n'
    return 0
  fi
  if [[ "$out" == *, ]]; then
    lab_log "E17 $id: result_ok is null in runs.jsonl (no reference checksum: run REF first); job_result_ok left empty, not 0"
  fi
  printf '%s\n' "$out"
}
# e17_healthy_count_on <pc> <file>: healthy rows in that PC's status table
# (STATE is column 2, the table ends at the first blank line). Returns 1 when
# the status cannot be read or has no table; never prints a fake 0.
e17_healthy_count_on() {
  local pc="$1" f="$2"
  if ! lab_ssh "$pc" pipedpeer status >"$f" 2>/dev/null; then
    return 1
  fi
  awk '
    /^[[:space:]]*$/ { in_table = 0 }
    /^NAME[[:space:]]+STATE/ { in_table = 1; seen = 1; next }
    in_table && $2 == "healthy" { c++ }
    END { if (!seen) exit 3; print c + 0 }
  ' "$f"
}

# e17_wait_count <pc> <want> <limit_s>: until that PC's status shows want
# healthy rows, or the limit passes. Logs the last count on timeout.
e17_wait_count() {
  local pc="$1" want="$2" limit="$3" got="" deadline
  deadline=$(($(date +%s) + limit))
  while :; do
    if got="$(e17_healthy_count_on "$pc" "$LAB/logs/e17-status-$pc.txt" 2>/dev/null)"; then
      ((got >= want)) && return 0
    else
      got=""
    fi
    if (($(date +%s) >= deadline)); then
      lab_log "E17 $pc: ${got:-unknown} of $want daemons healthy after ${limit}s"
      return 1
    fi
    sleep "$(e17_poll_s)"
  done
}

# e17_wait_pc_healthy <observer> <hostname> <limit_s>: until that hostname's row
# is healthy on the observer.
e17_wait_pc_healthy() {
  local obs="$1" host="$2" limit="$3" f="$LAB/logs/e17-rejoin-status.txt" state="" deadline
  deadline=$(($(date +%s) + limit))
  while :; do
    if lab_ssh "$obs" pipedpeer status >"$f" 2>/dev/null; then
      state="$(awk -v h="$host" '
        /^[[:space:]]*$/ { in_table = 0 }
        /^NAME[[:space:]]+STATE/ { in_table = 1; next }
        in_table { if ($1 == h || index($0, h " (") == 1) { print $2; found = 1; exit } }
        END { if (!found) print "absent" }
      ' "$f")"
      [[ "$state" == healthy ]] && return 0
    fi
    if (($(date +%s) >= deadline)); then
      lab_log "E17 $obs: $host is ${state:-unknown} after ${limit}s, not healthy"
      return 1
    fi
    sleep "$(e17_poll_s)"
  done
}

# e17_wait_join <observer> <joiner> <want> <limit_s> <t0_ns> [connect pid]:
# prints the join's measured seconds when both views show want healthy rows;
# returns 1 (and logs both counts) on timeout, or one poll after the connect
# process ended without the join showing.
e17_wait_join() {
  local obs="$1" join="$2" want="$3" limit="$4" t0="$5" cpid="${6:-}" go gj now deadline last=0
  deadline=$(($(date +%s) + limit))
  while :; do
    if [[ -n "$cpid" ]] && ! kill -0 "$cpid" 2>/dev/null; then
      ((last)) && {
        lab_log "E17: connect on $join ended and $obs shows ${go:-unknown}, $join ${gj:-unknown} healthy; wanted $want on both"
        return 1
      }
      last=1
    fi
    go="$(e17_healthy_count_on "$obs" "$LAB/logs/e17-status-join-obs.txt" 2>/dev/null || echo 0)"
    gj="$(e17_healthy_count_on "$join" "$LAB/logs/e17-status-join-new.txt" 2>/dev/null || echo 0)"
    if ((go >= want && gj >= want)); then
      now="$(date +%s%N)"
      python3 -c "print(round(($now - $t0) / 1e9, 3))"
      return 0
    fi
    if (($(date +%s) >= deadline)); then
      lab_log "E17: after the join $obs shows ${go:-unknown} and $join ${gj:-unknown} healthy; wanted $want on both within ${limit}s"
      return 1
    fi
    sleep "$(e17_poll_s)"
  done
}

# e17_env_on <pc>: restart that daemon with LAN discovery off (hc_switch's
# pattern: the user manager's env and the start's own env).
e17_env_on() {
  local pc="$1"
  lab_ssh "$pc" "systemctl --user set-environment PIPEDPEER_NO_LAN_DISCOVERY=1; PIPEDPEER_NO_LAN_DISCOVERY=1 pipedpeer start --introducer-only=false" ||
    {
      lab_log "E17: could not restart $pc with PIPEDPEER_NO_LAN_DISCOVERY=1"
      return 1
    }
}

# e17_running_now <run_id> <pc>: the run has a t0 and no exit file. Sets
# E17_RUN_ELAPSED_NS (the PC's clock now minus t0) and E17_POLL_NS (the
# driver's clock at that moment).
e17_running_now() {
  local id="$1" pc="$2" out
  # shellcheck disable=SC2016  # the remote script is literal
  out="$(lab_ssh "$pc" 'R=~/pipedpeer-lab/runs/'"$id"'; t=$(cat $R/t0 2>/dev/null); if [ -n "$t" ] && [ ! -e $R/exit ]; then echo "$t $(date +%s%N)"; fi' 2>/dev/null)" || out=""
  if [[ "$out" =~ ^([0-9]+)\ ([0-9]+)$ ]]; then
    E17_RUN_ELAPSED_NS=$((BASH_REMATCH[2] - BASH_REMATCH[1]))
    E17_POLL_NS="$(date +%s%N)"
    return 0
  fi
  return 1
}

# e17_wait_running <run_id> <pc>: until the run is seen running.
e17_wait_running() {
  local id="$1" pc="$2" deadline=$((SECONDS + ${E17_START_WAIT_S:-120}))
  while ((SECONDS < deadline)); do
    e17_running_now "$id" "$pc" && return 0
    sleep 0.2
  done
  lab_log "E17 $id: never seen running on $pc within ${E17_START_WAIT_S:-120}s"
  return 1
}

# e17_kill <pc>: SIGKILL the daemon and stop its unit. Restart=on-failure
# would bring a plain SIGKILL back in 5 s; the stop is what keeps pc00 dead.
e17_kill() {
  local pc="$1"
  lab_ssh "$pc" 'systemctl --user kill -s SIGKILL pipedpeer.service && systemctl --user stop pipedpeer.service' ||
    {
      lab_log "E17: SIGKILL+stop of pipedpeer.service on $pc failed; the introducer was not killed"
      return 1
    }
}

# e17_move_aside <pc>: move nodes.db and cluster.json on a joiner out of the
# way (the campaign is their only writer). The manifest remembers whether each
# existed, so the restore can put absence back too.
e17_move_aside() {
  local pc="$1" out f existed
  # shellcheck disable=SC2016  # expanded on the joiner
  if ! out="$(lab_ssh "$pc" 'd="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer"; mkdir -p "$d"; for f in nodes.db cluster.json; do if [ -e "$d/$f" ]; then mv "$d/$f" "$d/$f.e17-backup" && echo "$f=1" || echo "$f=err"; else echo "$f=0"; fi; done')"; then
    lab_log "E17 $pc: could not move its nodes.db/cluster.json aside; the join is not isolated"
    return 1
  fi
  while IFS='=' read -r f existed; do
    [[ -n "$f" ]] || continue
    if [[ "$existed" == err ]]; then
      lab_log "E17 $pc: could not move $f aside"
      continue
    fi
    E17_MOVED+=("$pc"$'\t'"$f"$'\t'"$existed")
  done <<<"$out"
}

# e17_restore_files: put every moved file back (or remove one that was not
# there), then stop the joiner's daemon so it no longer takes part.
e17_restore_files() {
  local entry pc f existed
  for entry in "${E17_MOVED[@]}"; do
    IFS=$'\t' read -r pc f existed <<<"$entry"
    # shellcheck disable=SC2016  # expanded on the joiner
    if ! lab_ssh "$pc" "d=\"\${XDG_DATA_HOME:-\$HOME/.local/share}/pipedpeer\"; if [ -e \"\$d/$f.e17-backup\" ]; then mv -f \"\$d/$f.e17-backup\" \"\$d/$f\"; elif [ '$existed' = 0 ]; then rm -f \"\$d/$f\"; fi"; then
      lab_log "E17 $pc: could not restore $f; the backup may still be there"
      continue
    fi
    lab_ssh "$pc" 'pipedpeer stop 2>/dev/null || true' || lab_log "E17 $pc: could not stop the joiner's daemon"
  done
  E17_MOVED=()
}

# e17_small_job <joiner>: the 64-item forced W1 run from the joiner; prints
# remote_items from its receipt, or nothing (logged) when that cannot be read.
e17_small_job() {
  local joiner="$1" rc=0 jd txt out
  # shellcheck disable=SC2016  # expanded on the joiner
  lab_ssh "$joiner" "cd ~/pipedpeer-lab/work && timeout ${E17_JOB_TIMEOUT_S:-600} pipedpeer run --distribute force -e BENCH_ITEMS=64 -e BENCH_PROCS=0 pool_map.py" >/dev/null || rc=$?
  if ((rc != 0)); then
    lab_log "E17 $joiner: the 64-item forced job failed (exit $rc); new_remote_items left empty"
    return 1
  fi
  # shellcheck disable=SC2016  # expanded on the joiner
  jd="$(lab_ssh "$joiner" 'd="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/jobs"; ls -t "$d" 2>/dev/null | head -1')" || jd=""
  if [[ -z "$jd" ]]; then
    lab_log "E17 $joiner: no job dir after the forced job; new_remote_items left empty"
    return 1
  fi
  txt="$(lab_ssh "$joiner" "cat \"\${XDG_DATA_HOME:-\$HOME/.local/share}/pipedpeer/jobs/$jd/receipt.json\"" 2>/dev/null)" || txt=""
  if [[ -z "$txt" ]]; then
    lab_log "E17 $joiner: no receipt for the forced job; new_remote_items left empty"
    return 1
  fi
  if ! out="$(printf '%s' "$txt" | python3 -c 'import json,sys; d=json.load(sys.stdin); r=d.get("remote_items"); print("" if r is None else r)')"; then
    lab_log "E17 $joiner: the receipt is not usable JSON; new_remote_items left empty"
    return 1
  fi
  printf '%s\n' "$out"
}

# e17_hostname <pc>: the hostname column of hosts.tsv (the NAME of its status row).
e17_hostname() {
  awk -F'\t' -v pc="$1" '$1 == pc { print $4; exit }' "$LAB/hosts.tsv"
}
# e17_cleanup: put the environment, the daemons and the joiners' files back.
# Runs on every exit path, Ctrl-C and SIGTERM included.
e17_cleanup() {
  local rc=$? pc
  trap - EXIT
  if [[ -n "${E17_JOB_PID:-}" ]]; then
    kill "$E17_JOB_PID" 2>/dev/null || true
    wait "$E17_JOB_PID" 2>/dev/null || true
    E17_JOB_PID=""
  fi
  if [[ -n "${E17_CONNECT_PID:-}" ]]; then
    kill "$E17_CONNECT_PID" 2>/dev/null || true
    wait "$E17_CONNECT_PID" 2>/dev/null || true
    E17_CONNECT_PID=""
  fi
  e17_restore_files
  if ((E17_ENV_SET)); then
    for pc in "${E17_MEMBERS[@]}"; do
      lab_ssh "$pc" "systemctl --user unset-environment PIPEDPEER_NO_LAN_DISCOVERY; env -u PIPEDPEER_NO_LAN_DISCOVERY pipedpeer start --introducer-only=false" ||
        lab_log "E17 cleanup $pc: could not clear PIPEDPEER_NO_LAN_DISCOVERY and restart; clear it by hand"
    done
    E17_ENV_SET=0
  fi
  exit "$rc"
}

e17_main() {
  local DRY=0 N=4 JOINERS=2 REPEATS=3
  while (($#)); do
    case "$1" in
      --dry-run) DRY=1; shift ;;
      --n) N="${2:?--n needs a number}"; shift 2 ;;
      --joiners) JOINERS="${2:?--joiners needs a number}"; shift 2 ;;
      --repeats) REPEATS="${2:?--repeats needs a number}"; shift 2 ;;
      -h | --help) e17_usage; return 0 ;;
      *)
        e17_usage >&2
        echo "e17.sh: unknown arg $1" >&2
        return 2
        ;;
    esac
  done
  [[ "$N" =~ ^[0-9]+$ && "$JOINERS" =~ ^[0-9]+$ && "$REPEATS" =~ ^[0-9]+$ ]] ||
    { e17_usage >&2; echo "e17.sh: --n, --joiners and --repeats need numbers" >&2; return 2; }
  if ((N < 2)); then
    echo "e17.sh: --n must be at least 2 (pc00 and a submit member)" >&2
    return 2
  fi
  if ((JOINERS < 1)); then
    echo "e17.sh: --joiners must be at least 1" >&2
    return 2
  fi
  if ((REPEATS < 1)); then
    echo "e17.sh: --repeats must be at least 1" >&2
    return 2
  fi

  if ((DRY)); then
    e17_plan "$N" "$JOINERS" "$REPEATS"
    return 0
  fi

  _load_sizes || return 1
  mkdir -p "$LAB/analysis-input" "$LAB/logs"

  local need=$((N + JOINERS)) hfile
  hfile="$(lab_hosts "$need")" || {
    lab_log "E17: need $need PCs (--n $N includes pc00, plus $JOINERS joiners)"
    echo "E17-NEEDS $need PCs" >&2
    return 1
  }
  local -a all=()
  mapfile -t all <"$hfile"
  E17_MEMBERS=("${all[@]:0:N}")
  local -a joiners=("${all[@]:N:JOINERS}")
  local pc00="${E17_MEMBERS[0]}" submit="${E17_MEMBERS[1]}"
  if [[ "$pc00" != pc00 ]]; then
    lab_log "E17: the first available PC is $pc00, not pc00; the introducer is read from its own invite"
  fi

  trap e17_cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM

  E17_ENV_SET=1
  local pc rc=0
  for pc in "${E17_MEMBERS[@]}"; do
    e17_env_on "$pc" || rc=1
  done
  if ((rc != 0)); then
    echo "E17-NEEDS the member daemons restarted with PIPEDPEER_NO_LAN_DISCOVERY=1" >&2
    return 1
  fi
  e17_wait_count "$pc00" "$N" "${E17_JOIN_S:-120}" ||
    {
      lab_log "E17: the members did not all come back healthy; nothing measured"
      echo "E17-NEEDS $N healthy daemons on $pc00" >&2
      return 1
    }

  local out="$LAB/analysis-input/e17.csv"
  [[ -s "$out" ]] || printf 'i,kill_at_s,job_wall_s,job_result_ok,new_join_s,new_join_ok,new_remote_items,old_invite_joined\n' >"$out"

  local i id med remain now_ns kill_at wall okv join_s join_ok remote old_joined old_code code
  export CAMPAIGN_SUBMIT_PC="$submit"
  med="$(e17_median_wall)"
  lab_log "E17: introducer $pc00, submit $submit, members ${E17_MEMBERS[*]}, joiners ${joiners[*]}, median wall ${med}s"

  for ((i = 1; i <= REPEATS; i++)); do
    id="$(_run_id E17 W1 pipedpeer "$N" "$i" 0)"
    if _run_id_in_runs "$id"; then
      echo "skipped $id"
      continue
    fi
    kill_at="" wall="" okv="" join_s="" join_ok="" remote="" old_joined=""

    # the joiners leave the cluster before the job: a joiner still running
    # as a member took the job's work on the emulated lab (N+1 machines)
    local j
    for j in "${joiners[@]}"; do
      lab_ssh "$j" 'pipedpeer stop' || lab_log "E17 $id: could not stop $j's daemon before the job; it may take work (ssh or pipedpeer stop failed)"
      e17_move_aside "$j" || lab_log "E17 $id: $j's files were not moved aside; its state is not isolated"
    done

    # the old invite must be issued before the introducer dies
    old_code="$(e17_invite "$pc00" 2>/dev/null)" || old_code=""
    if [[ -z "$old_code" ]]; then
      lab_log "E17 $id: no invite from $pc00 before the kill; the old-invite arm is recorded empty"
    fi

    CAMPAIGN_SUBMIT_PC="$submit" campaign_run_one E17 W1 pipedpeer "$N" "$i" 0 1800 &
    E17_JOB_PID=$!
    if e17_wait_running "$id" "$submit"; then
      remain="$(python3 -c "print(max(0.0, 0.3 * $med - $E17_RUN_ELAPSED_NS / 1e9))")"
      sleep "$remain"
      if e17_running_now "$id" "$submit"; then
        now_ns="$(date +%s%N)"
        kill_at="$(python3 -c "print(round(($E17_RUN_ELAPSED_NS + $now_ns - $E17_POLL_NS) / 1e9, 3))")"
        e17_kill "$pc00" || kill_at=""
      else
        lab_log "E17 $id: the job had already ended before the kill; kill_at_s left empty"
      fi
    else
      lab_log "E17 $id: the job was not seen running; kill_at_s left empty"
    fi
    wait "$E17_JOB_PID" || lab_log "E17 $id: campaign_run_one returned $?"
    E17_JOB_PID=""
    IFS=, read -r wall okv <<<"$(e17_record "$id")"

    # a new code from the surviving introducer, on a fresh joiner
    join_ok=0
    code="$(e17_invite "$submit" 2>/dev/null)" || code=""
    if [[ -z "$code" ]]; then
      lab_log "E17 $id: no invite from $submit after the kill; new_join_s and new_join_ok left empty"
      join_ok=""
    else
      local t0 rc2 seen=0 js=""
      t0="$(date +%s%N)"
      rc2=0
      # connect runs while both views are polled: with the dead introducer in
      # the survivor's invite connect waits its full 60 s for it, long after
      # the join is visible, so timing connect would time that wait
      lab_ssh "${joiners[0]}" "cd ~/pipedpeer-lab/work && timeout ${E17_CONNECT_TIMEOUT_S:-120} pipedpeer connect $(printf '%q' "$code")" &
      E17_CONNECT_PID=$!
      if js="$(e17_wait_join "$submit" "${joiners[0]}" "$N" "${E17_JOIN_S:-120}" "$t0" "$E17_CONNECT_PID")"; then
        seen=1
      fi
      wait "$E17_CONNECT_PID" || rc2=$?
      E17_CONNECT_PID=""
      if ((rc2 == 2)); then
        # partly connected: the join counts when both views confirm it (install.sh's rule)
        lab_log "E17 $id: connect on ${joiners[0]} exited 2 (partly connected); the join counts only as both views confirm it"
      fi
      if ((seen)) && ((rc2 == 0 || rc2 == 2)); then
        join_ok=1
        join_s="$js"
      else
        join_ok=0
        join_s=""
        if ((rc2 != 0 && rc2 != 2)); then
          lab_log "E17 $id: connect on ${joiners[0]} failed (exit $rc2; a timeout is 124)"
        fi
      fi
      if [[ "$join_ok" == 1 ]]; then
        remote="$(e17_small_job "${joiners[0]}")" || remote=""
      fi
    fi

    # the old invite arm, on the second joiner. An arm that did not run
    # leaves the field empty: 0 would read as "tested, did not join".
    if [[ -z "$old_code" ]]; then
      old_joined=""
    elif ((${#joiners[@]} < 2)); then
      old_joined=""
      lab_log "E17 $id: only one joiner; the old-invite arm did not run (recorded empty)"
    else
      rc2=0
      lab_ssh "${joiners[1]}" "timeout ${E17_OLD_INVITE_TIMEOUT_S:-60} pipedpeer connect $(printf '%q' "$old_code")" || rc2=$?
      if ((rc2 == 0)); then
        old_joined=1
        lab_log "E17 $id: UNEXPECTED: the old invite from $pc00 still joined ${joiners[1]}"
      else
        old_joined=0
        lab_log "E17 $id: the old invite did not join ${joiners[1]} (exit $rc2), as expected"
      fi
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$i" "$kill_at" "$wall" "$okv" "$join_s" "$join_ok" "$remote" "$old_joined" >>"$out"

    # pc00 comes back with the same env, then the joiner is restored for the
    # next repeat
    if ! lab_ssh "$pc00" "PIPEDPEER_NO_LAN_DISCOVERY=1 pipedpeer start --introducer-only=false"; then
      lab_log "E17 $id: could not restart $pc00 with the env for the next repeat"
    else
      e17_wait_pc_healthy "$submit" "$(e17_hostname "$pc00")" "${E17_RESTART_WAIT_S:-120}" ||
        lab_log "E17 $id: $pc00 did not come back healthy on $submit"
    fi
    e17_restore_files
  done

  "$UPLOAD_SH" || lab_log "E17: upload failed; results stay in $LAB"
  echo "E17-DONE csv=$out"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  e17_main "$@"
fi


