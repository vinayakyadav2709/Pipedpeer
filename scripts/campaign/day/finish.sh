# shellcheck shell=bash
# The lab day's last phase: clean and undo every worker, final R2 sync and
# count, clean pc00, the summary, and lab.sh undo on pc00 last. Sourced by
# lab-day.sh. Each PC's cleanup and undo has its own ledger line, so a resumed
# day never repeats one that finished.
# shellcheck disable=SC2016  # remote commands are single-quoted on purpose

PROBLEMS=$DAY/cleanup-problems   # "<pc> <what>" per line

# had <pc> nix|pipedpeer: 1 if the PC had it before the day. Without a record
# the answer is 1, so the day never removes what it cannot prove it installed.
had() {
  [[ -f $HOSTREC/$1 ]] || { echo 1; return; }
  grep -qx "had-$2" "$HOSTREC/$1" && echo 1 || echo 0
}

problem() { grep -qxF -- "$1 $2" "$PROBLEMS" 2>/dev/null || printf '%s %s\n' "$1" "$2" >>"$PROBLEMS"; }

clean_workers() {
  local h script; script=$(base64 -w0 <"$CAMP/day/cleanup-host.sh")
  for h in $(workers "$INV_ALL"); do
    is_done "clean.$h" && continue
    # raw sends no files, and Ansible templates its argument ({# would start a
    # Jinja comment): the script goes over as base64 and runs as the lab user.
    ans "clean.$h" "$h" "mkdir -p \$HOME/pipedpeer-lab; f=\$HOME/pipedpeer-lab/cleanup-host.sh; echo $script | base64 -d >\$f; bash \$f $(had "$h" nix) $(had "$h" pipedpeer)" "$INV_ALL" || true
    local o; o=$(out_of "clean.$h" "$h" | tail -n1)
    case $o in
      "CLEANUP ok") mark_done "clean.$h" ;;
      "CLEANUP left: "*) problem "$h" "${o#CLEANUP left: }"; mark_done "clean.$h" ;;
      *) say "cleanup of $h did not finish (rc $(rc_of "clean.$h" "$h")): $(out_of "clean.$h" "$h" | tail -n2 | tr '\n' ' ')" ;;
    esac
  done
  return 0
}

# undo_workers runs lab.sh undo on each worker in a transient system unit, so
# it finishes even when removing sshd ends the session that started it.
undo_workers() {
  local h
  for h in $(workers "$INV_ALL"); do
    is_done "undo.$h" && continue
    if ! is_done "clean.$h"; then
      ANSIBLE_TIMEOUT=10 ans "reach.$h" "$h" true "$INV_ALL" || { problem "$h" "unreachable at cleanup: not cleaned, lab.sh undo not run"; mark_done "undo.$h"; continue; }
      problem "$h" "cleanup did not finish: $(out_of "clean.$h" "$h" | tail -n1)"
    fi
    ans "undo.$h" "$h" "sudo -n systemd-run --quiet --wait --pipe --collect --unit=pipedpeer-lab-undo bash $LABSH undo" "$INV_ALL" || true
    if out_of "undo.$h" "$h" | grep -q 'undo complete'; then
      out_of "undo.$h" "$h" | grep -q WARNING && problem "$h" "lab.sh undo warned: $(out_of "undo.$h" "$h" | grep WARNING | head -n3 | tr '\n' ' ')"
      mark_done "undo.$h"
    elif ANSIBLE_TIMEOUT=10 ans "after.$h" "$h" "test ! -e $LABSH" "$INV_ALL"; then
      mark_done "undo.$h"
    elif [[ $(rc_of "after.$h" "$h") == 255 ]]; then
      # The key no longer logs in: undo removed it, so it ran.
      mark_done "undo.$h"
    else
      problem "$h" "lab.sh undo failed: $(out_of "undo.$h" "$h" | tail -n2 | tr '\n' ' ')"
      mark_done "undo.$h"
    fi
  done
  return 0
}

final_sync() {
  bash "$CAMP/telemetry.sh" down >/dev/null 2>&1 || true   # flush the last batches to R2
  dist_down
  local out loc rem
  for _ in 1 2 3; do
    bash "$CAMP/sync-r2.sh" final || true
    out=$(bash "$CAMP/sync-r2.sh" count 2>&1) || true
    echo "$out" | tee "$DAY/final-count.txt"
    loc=$(sed -n 's/^local: *\([0-9]*\) .*/\1/p' <<<"$out")
    rem=$(sed -n 's/^remote: *\([0-9]*\) .*/\1/p' <<<"$out")
    if [[ -n $loc && $loc == "$rem" ]]; then return 0; fi
    if grep -qE 'R2 not configured|stay local only|no ingest credentials' <<<"$out"; then blocker "R2 is not configured; nothing was uploaded"; return 1; fi
    say "R2 count differs (local ${loc:-?}, remote ${rem:-?}); syncing again"
    sleep 20
  done
  blocker "R2 still differs from local after 3 syncs (local ${loc:-?}, remote ${rem:-?}); pc00 keeps its results"
  return 1
}

clean_pc00() {
  local o
  o=$(bash "$CAMP/day/cleanup-host.sh" "$(had pc00 nix)" "$(had pc00 pipedpeer)" keep-day 2>&1 | tail -n1)
  [[ $o == "CLEANUP ok" ]] || problem pc00 "${o#CLEANUP left: }"
  return 0
}

summary() {
  python3 "$CAMP/day/tables.py" summary "$RESULTS" "$DAY" "$LAB_PCS" "${1:-}"
}

finish() {
  step clean.workers cleanup clean_workers
  step undo.workers cleanup undo_workers
  if ! step final.sync cleanup final_sync; then
    summary "pc00 is NOT restored yet: its results are not confirmed in R2. Fix the network, then type the head line again." | tee "$DAY/summary.txt"
    exit 1
  fi
  step clean.pc00 cleanup clean_pc00
  progress "done" "summary and undo of pc00"
  local text; text=$(summary)
  printf '%s\n' "$text" >"$DAY/summary.txt"
  # pc00's undo deletes $DAY; the summary and a finished mark go to its state
  # dir first, so a cut-off undo still ends with this summary on the next head.
  # shellcheck disable=SC2024  # the lab user reads it; root writes the copy
  sudo -n tee /var/lib/pipedpeer-lab/summary.txt <"$DAY/summary.txt" >/dev/null
  sudo -n touch /var/lib/pipedpeer-lab/day-finished
  touch "$DAY/.synced"
  say "undoing lab.sh on pc00 (this PC); results, secrets and the code go with it"
  local undo_ok=1
  cd / && sudo -n bash "$LABSH" undo || undo_ok=0
  printf '%s\n' "$text"
  ((undo_ok)) || echo "pc00: lab.sh undo failed; run: sudo bash $LABSH undo"
}
