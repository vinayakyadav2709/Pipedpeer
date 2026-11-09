#!/usr/bin/env bash
# Case table for lab-pc.sh undo / mark_nix and the step-8 host list, with stub
# commands in a private dir (no real systemctl, apt, ufw, sudo or /nix).
# LAB_PC_SCRIPT replaces the script under test (to see the rows fail on an old copy).
# shellcheck disable=SC2016,SC2034,SC2319  # stub bodies expand in the stub; the sourced script reads the variables
set -u
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
script=${LAB_PC_SCRIPT:-$here/../lab-pc.sh}
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
mkdir -p "$t/bin"
pass=0; total=0
row() { total=$((total + 1)); if [ "$2" = 0 ]; then pass=$((pass + 1)); echo "ok $1"; else echo "FAIL $1: $3"; fi; }

stub() { printf '#!/usr/bin/env bash\n%s\n' "$2" >"$t/bin/$1"; chmod +x "$t/bin/$1"; }
stub sudo 'shift 2; exec "$@"'
stub systemctl 'echo "systemctl $*" >>"$CALLS"
case "$*" in
  *" show "*|"show "*) cat "$T/cap" 2>/dev/null ;;
  *list-units*) cat "$T/units" 2>/dev/null ;;
  *" stop "*) if [ -d "$T/home/pipedpeer-lab" ]; then echo "venv present at stop" >>"$CALLS"; else echo "venv gone at stop" >>"$CALLS"; fi
              [ -e "$T/fail-stop" ] && exit 1 ;;
esac
exit 0'
stub pgrep 'exit 1'
stub pkill 'exit 0'
stub ufw 'exit 0'
stub loginctl 'exit 0'
stub apt-get 'echo "apt-get $*" >>"$CALLS"; [ -e "$T/fail-apt" ] && exit 100; exit 0'
export PATH="$t/bin:$PATH" T="$t" CALLS="$t/calls"

# fresh <state lines...>: a new fake PC with those state lines.
fresh() {
  rm -rf "${t:?}/state" "${t:?}/home" "$t/nix" "$t/units" "$t/cap" "$t/fail-"* "$CALLS"
  mkdir -p "$t/home/pipedpeer-lab" "$t/home/.ssh" "$t/rundir/$(id -u)"
  : >"$CALLS"
  if [ $# -gt 0 ]; then mkdir -p "$t/state"; printf '%s\n' "$@" >"$t/state/state"; fi
}
# run <fn>: the function in a subshell with the paths pointed at the fake PC.
run() {
  ( LAB_USER=$(id -un); export SUDO_USER=$LAB_USER
    # shellcheck source=/dev/null
    . "$script" || exit 99
    STATE=$t/state/state; LINE_FILE=$t/state/line; HOME_DIR=$t/home
    NIX_DIR=$t/nix; RUN_USER_DIR=$t/rundir; SUDOERS=$t/sudoers
    "$@" ) >"$t/out" 2>&1
}
has() { grep -qE -- "$1" "$2"; }
nixinst() { mkdir -p "$t/nix"; printf '#!/usr/bin/env bash\necho "nix-installer $*" >>"$CALLS"\nexit "${NIXFAIL:-0}"\n' >"$t/nix/nix-installer"; chmod +x "$t/nix/nix-installer"; }

# 1 capped PC: undo resets the cap
fresh "linger"; echo "0-1" >"$t/cap"; run undo
has 'set-property --runtime user-[0-9]+.slice AllowedCPUs=$' "$CALLS"; row "capped PC: undo resets AllowedCPUs" $? "$(cat "$CALLS")"
# 2 not capped: no set-property
fresh "linger"; run undo
! has set-property "$CALLS"; row "uncapped PC: no set-property" $? "$(cat "$CALLS")"
# 3 units running: stopped before the venv goes
fresh "linger"; printf 'lab-ray.service loaded active running x\nlab-dask-worker.service loaded active running x\n' >"$t/units"; run undo
has ' stop lab-ray.service' "$CALLS" && has ' stop lab-dask-worker.service' "$CALLS" && has 'venv present at stop' "$CALLS" && ! has 'venv gone' "$CALLS"
row "units running: stopped while the venv is still there" $? "$(cat "$CALLS")"
[ ! -d "$t/home/pipedpeer-lab" ]; row "units running: venv removed after" $? ""
# 4 no units: no stop
fresh "linger"; run undo
! has ' stop ' "$CALLS"; row "no units: nothing stopped" $? "$(cat "$CALLS")"
# 5 failed step keeps the state, rerun retries it
fresh "pkg sysstat" "linger"; touch "$t/fail-apt"; run undo; rc1=$?
[ "$rc1" != 0 ] && [ -f "$t/state/state" ] && has '^pkg sysstat$' "$t/state/state" && ! has '^linger$' "$t/state/state"
row "failed purge: state kept with only the failed line" $? "rc=$rc1 $(cat "$t/state/state" 2>&1)"
has 'incomplete' "$t/out"; row "failed purge: output says undo is incomplete" $? "$(cat "$t/out")"
rm -f "$t/fail-apt"; : >"$CALLS"; run undo; rc2=$?
[ "$rc2" = 0 ] && has 'apt-get purge .*sysstat' "$CALLS" && [ ! -e "$t/state" ]
row "rerun retries the purge and removes the state" $? "rc=$rc2 $(cat "$CALLS")"
# 5b unit that will not stop keeps the state (cleanup)
fresh "linger"; echo 'lab-ray.service loaded active running x' >"$t/units"; touch "$t/fail-stop"; run undo
[ -f "$t/state/state" ]; row "unit not stopped: state kept" $? "$(cat "$t/out")"
# 6 nothing recorded: no action
fresh; echo 0-1 >"$t/cap"; run undo
has 'nothing recorded' "$t/out" && [ ! -s "$CALLS" ]; row "nothing recorded: no action" $? "$(cat "$CALLS")"
# 7 clean run removes the state
fresh "linger"; run undo
[ ! -e "$t/state" ] && has 'undo complete' "$t/out"; row "clean run: state removed, undo complete" $? "$(cat "$t/out")"
# 8 Nix present before setup: never removed
fresh "linger"; nixinst; run undo
! has nix-installer "$CALLS"; row "Nix present before setup: not removed" $? "$(cat "$CALLS")"
# 9 campaign Nix (marker): removed
fresh "nix-absent" "nix-campaign determinate"; nixinst; run undo
has 'nix-installer uninstall --no-confirm' "$CALLS"; row "campaign Nix (marked): removed" $? "$(cat "$CALLS")"
# 9b removal fails: kept for a rerun
fresh "nix-absent" "nix-campaign determinate"; nixinst; NIXFAIL=1 run undo
[ -f "$t/state/state" ] && has '^nix-absent$' "$t/state/state" && has '^nix-campaign determinate$' "$t/state/state"
row "Nix removal fails: nix lines kept" $? "$(cat "$t/state/state" 2>&1)"
# 10 nix-absent but nobody's marker (someone else's Nix): left
fresh "nix-absent"; nixinst; run undo
! has nix-installer "$CALLS" && has 'Nix left in place' "$t/out"; row "Nix without the campaign marker: left, warned" $? "$(cat "$CALLS")"
# 11 stock Nix: warned with the manual step, undo still completes
fresh "nix-absent" "nix-campaign stock"; mkdir -p "$t/nix"; run undo
has 'stock installer.*by hand' "$t/out" && has 'undo complete' "$t/out"; row "stock Nix: manual step named, undo completes" $? "$(cat "$t/out")"
# 12 mark_nix
fresh "nix-absent"; nixinst; run mark_nix
has '^nix-campaign determinate$' "$t/state/state"; row "mark_nix: new Nix with installer recorded" $? ""
fresh "nix-absent"; mkdir -p "$t/nix"; run mark_nix
has '^nix-campaign stock$' "$t/state/state"; row "mark_nix: stock Nix recorded" $? ""
fresh "linger"; nixinst; run mark_nix
! has nix-campaign "$t/state/state"; row "mark_nix: Nix there at setup (no nix-absent): nothing recorded" $? ""
fresh "nix-absent"; run mark_nix
! has nix-campaign "$t/state/state"; row "mark_nix: no Nix: nothing recorded" $? ""
# 12b the real flow: setup (nix-absent), install brings Nix, mark, mark again, undo
fresh "nix-absent" "linger"; nixinst; run mark_nix; run mark_nix
[ "$(grep -c '^nix-campaign ' "$t/state/state")" = 1 ]; row "mark twice (install rerun): one marker line" $? "$(cat "$t/state/state")"
: >"$CALLS"; run undo
has 'nix-installer uninstall' "$CALLS"; row "install-then-undo flow: campaign Nix removed" $? "$(cat "$CALLS")"
# 12c setup keeps a copy of itself where install.sh runs mark from
grep -q 'install -m 0700 -o root "${BASH_SOURCE\[0\]}" "$SELF"' "$script"; row "setup copies itself to the state dir" $? ""
grep -q '^SELF=/var/lib/pipedpeer-lab/' "$script"; row "the copy lives in the state dir (undo removes it with the state)" $? ""
# 13 step 8 host list: the PCs lab_hosts hands out, not every row of hosts.tsv
grep -q 'lab_hosts' <(sed -n '/^step8()/,/^}/p' "$here/../test-ubuntu-lab.sh") \
  && ! grep -q 'hosts.tsv' <(sed -n '/^step8()/,/^}/p' "$here/../test-ubuntu-lab.sh")
row "step 8 leaves only lab_hosts N PCs (one down host cannot widen the fanout)" $? ""
echo "PASS $pass/$total"
[ "$pass" = "$total" ]
