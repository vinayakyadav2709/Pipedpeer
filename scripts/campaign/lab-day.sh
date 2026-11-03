#!/usr/bin/env bash
# lab-day.sh: the whole headless lab day, on pc00, as the lab user. lab.sh
# head starts it as the systemd unit pipedpeer-lab-day; nobody runs it by hand.
#
#   discover   scan pc00's subnet until LAB_PCS PCs (pc00 included) log in
#              with the campaign key, or LAB_DISCOVER_TIMEOUT (1800 s) passes
#   prepare    machines.csv, network.csv, clocks, pipedpeer + join, the first
#              distributed job (E14), the Nix env (pc00 builds it, the rest
#              pull it from pc00), telemetry
#   calibrate  W1-W3 sizes on pc01; then the weak-scaling plain references
#   run        the profile's experiments through run.sh, syncing R2 after each
#   analyse    analyse.py, then sync
#   finish     pull every PC's pcap, clean every worker and undo lab.sh there, final sync and count,
#              clean and undo pc00, print the summary
#
# Resumable: $DAY/done is a ledger of finished steps (day/lib.sh); rerunning
# lab.sh head restarts this script, which skips them. An experiment cut off
# midway runs again; its partial lines stay in runs.jsonl, marked in
# results/lab/attempts.jsonl as interrupted. A PC that stops answering is
# dropped (results/lab/dropped.csv) and the day goes on without it.
#
# Env (set by lab.sh): LAB_KEY, PIPEDPEER_LAB_R2_ENV, LAB_PCS, LAB_SITE,
# LAB_PIPEDPEER_VERSION; optional LAB_DISCOVER_TIMEOUT, LAB_PROFILE, LAB_SUBNET.
set -Euo pipefail
CAMP=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=day/lib.sh
source "$CAMP/day/lib.sh"
# shellcheck source=day/prepare.sh
source "$CAMP/day/prepare.sh"
# shellcheck source=day/finish.sh
source "$CAMP/day/finish.sh"
# shellcheck source=day/timeline.sh
source "$CAMP/day/timeline.sh"

LAB_PCS=${LAB_PCS:-17}
PROFILE=${LAB_PROFILE:-$CAMP/profile-minimal.tsv}
[[ -f ${LAB_KEY:-} ]] || { echo "lab-day: no campaign key (LAB_KEY); start the day with lab.sh head" >&2; exit 1; }
[[ -f $PROFILE ]] || { echo "lab-day: no profile $PROFILE" >&2; exit 1; }
mkdir -p "$RESULTS/logs/day"
exec > >(tee -a "$RESULTS/logs/day/lab-day.log") 2>&1

[[ -f $DAY/started ]] || date +%s >"$DAY/started"
DAY_START=$(<"$DAY/started")

# The profile's rows: "<experiment> <minutes> <run.sh args...>".
mapfile -t EXPS < <(grep -v '^[[:space:]]*\(#\|$\)' "$PROFILE")
STEPS="bootstrap 3
discover 5
prep.machines 1
prep.network 4
prep.clocks 1
prep.code 2
prep.pipedpeer 8
prep.join 2
prep.firstjob 3
prep.nixenv 20
prep.otel 3
calibrate 14
weakrefs 17
$(for r in "${EXPS[@]}"; do read -r id min _ <<<"$r"; echo "exp.$id $min"; done)
analyse 3
clocks.end 1
pull.pcap 2
clean.workers 10
undo.workers 3
final.sync 3
clean.pc00 3"

# step <name> <phase> <function> [args]: run it unless the ledger has it.
step() {
  local name=$1 phase=$2; shift 2
  if is_done "$name"; then say "$name: done before, skipped"; return 0; fi
  CUR_STEP=$name
  progress "$phase" "$name"
  say "== $name"
  if "$@"; then mark_done "$name"; return 0; fi
  return 1
}

# abort <why>: a step the day cannot do without failed. Record it, skip to
# the cleanup, so the PCs are still put back.
abort() {
  blocker "the day stops early: $*"
  mark_done "aborted: $*"
}
aborted() { grep -q '^aborted: ' "$LEDGER" 2>/dev/null; }

main() {
  say "lab day on $(uname -n): $LAB_PCS PCs expected, profile $(basename "$PROFILE")"
  if ! aborted; then
    step bootstrap setup bootstrap || abort "Ansible could not be installed on pc00"
  fi
  if ! aborted; then
    step discover discover discover || abort "no worker PC answered"
  fi
  if ! aborted; then
    local s
    for s in machines network clocks code pipedpeer join firstjob nixenv otel; do
      ensure_services
      step "prep.$s" prepare "prep_$s" || { abort "prepare $s failed on pc00"; break; }
    done
  fi
  if ! aborted; then
    ensure_services
    step calibrate calibrate calibrate || { blocker "calibration failed; the provisional sizes in workloads.json stay"; mark_done calibrate; }
    step weakrefs calibrate weakrefs || { blocker "weak-scaling references incomplete; run.sh falls back to the first result"; mark_done weakrefs; }
    local r id min args
    for r in "${EXPS[@]}"; do
      read -r id min args <<<"$r"
      # shellcheck disable=SC2086  # args are the profile's words
      run_exp "$id" $args
    done
    step analyse analyse analyse || { blocker "analyse.py failed"; mark_done analyse; }
  fi
  finish
}

main
