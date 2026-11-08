#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317,SC2034
# S12-S15 dry-run rows (fake lab; nothing touches a real PC).
set -euo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
pass=0; failed=0; problems=(); dirs=()
cleanup() { for d in "${dirs[@]}"; do rm -rf "$d"; done; }
trap cleanup EXIT
ok() { pass=$((pass + 1)); echo "ok   $1"; }
bad() { failed=$((failed + 1)); echo "FAIL $1 -- $2"; }
pnote() { problems+=("$*"); }
verdict() { if ((${#problems[@]} == 0)); then ok "$1"; else bad "$1" "${problems[*]}"; fi; problems=(); }
newdir() { local d; d="$(mktemp -d "$STATE/e-exp-test.XXXXXX")"; mkdir -p "$d/home" "$d/bin"; dirs+=("$d"); echo "$d"; }

# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

setup_fake() {
  local dir="$1"
  fakelab_init "$dir" 3 || exit 90
  export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.2 LAB_POLL_S=1
  export PATH="$CAMP:$PATH"
}

# e4 dry-run: 20 runs and cap lines
d="$(newdir)"; setup_fake "$d"
run="$(cd "$REPO" && bash "$CAMP/e4.sh" --dry-run 2>&1)" || pnote "e4 dry exit $?"
echo "$run" | grep -q "CAP pc00=8" || pnote "no pc00 cap"
echo "$run" | grep -q "CAP pc03=1" || pnote "no pc03 cap"
n="$(echo "$run" | grep -c '^RUN ')"
[[ "$n" == "20" ]] || pnote "runs $n != 20"
verdict "e4 --dry-run: 20 runs + caps"

# e5 dry-run
run="$(cd "$REPO" && bash "$CAMP/e5.sh" --dry-run --n 2 2>&1)" || pnote "e5 dry exit $?"
echo "$run" | grep -q "E5-DRY-RUN" || pnote "no marker"
verdict "e5 --dry-run"

# e6 dry-run
run="$(cd "$REPO" && bash "$CAMP/e6.sh" --dry-run --n 2 2>&1)" || pnote "e6 dry exit $?"
echo "$run" | grep -q "E6-DRY-RUN n=2" || pnote "no marker"
verdict "e6 --dry-run"

# e11 dry-run with --idle 5
run="$(cd "$REPO" && bash "$CAMP/e11.sh" --dry-run --sizes 2 --idle 5 2>&1)" || pnote "e11 dry exit $?"
echo "$run" | grep -q "E11-DRY-RUN sizes=2 idle=5" || pnote "no marker"
verdict "e11 --dry-run"

total=$((pass + failed))
echo "PASS $pass/$total"
[[ $failed -eq 0 ]]
