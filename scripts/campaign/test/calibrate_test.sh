#!/usr/bin/env bash
# shellcheck disable=SC2317,SC2015,SC1090,SC2034  # rows run in subshells; cleanup from a trap
# The S9b case table for calibrate.sh on the fake lab. The "plain" venv python
# on pc00 is a stub that sleeps CAL_STUB_S seconds (or fails), so the measured
# seconds come from the real t0/t1 files of real detached runs, not from a
# hook in the script. One ok/FAIL line per row; ends "PASS <k>/<k>".

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAL="$REPO/scripts/campaign/calibrate.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

RESULTS="$(mktemp)"
problems=()
dirs=()
cleanup() {
	local d
	if [[ "${LAB_TEST_KEEP:-0}" != 1 ]]; then
		for d in "${dirs[@]}"; do rm -rf "$d"; done
	fi
	rm -f "$RESULTS"
}
trap cleanup EXIT
ok() { echo ok >>"$RESULTS"; printf 'ok   %s\n' "$1"; }
bad() { echo bad >>"$RESULTS"; printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then ok "$1"; else bad "$1" "${problems[*]}"; fi
	problems=()
}
newdir() {
	local d
	d="$(mktemp -d "$STATE/campaign-cal-test.XXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}
row_setup() { # row_setup <dir> <python stub body>
	local dir="$1"
	cd "$REPO"
	fakelab_init "$dir" 2 || exit 90
	export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.2 LAB_POLL_S=1
	mkdir -p "$dir/pc00/pipedpeer-lab/venv/bin"
	printf '#!/usr/bin/env bash\n%s\n' "$2" >"$dir/pc00/pipedpeer-lab/venv/bin/python"
	chmod +x "$dir/pc00/pipedpeer-lab/venv/bin/python"
}

# --- the arithmetic: pure rows -------------------------------------------------
(
source "$CAL"
set +e
got="$(calibrate_scale 48 30 120)"
[[ "$got" == 192 ]] || pnote "30 s at 48 items, target 120 -> [$got], want 192"
got="$(calibrate_scale 48 240 120)"
[[ "$got" == 24 ]] || pnote "240 s at 48, target 120 -> [$got], want 24"
if calibrate_scale 48 0 120 >/dev/null 2>&1; then pnote "measured 0 s was accepted"; fi
if calibrate_scale 48 "" 120 >/dev/null 2>&1; then pnote "empty measurement was accepted"; fi
if calibrate_scale 48 abc 120 >/dev/null 2>&1; then pnote "non-numeric measurement was accepted"; fi
verdict "calibrate_scale: 30 s at 48 items, target 120 -> 192; 0 s / empty / junk -> error"
)

# --- real detached runs on the fake lab -----------------------------------------
(d=$(newdir)
row_setup "$d" 'sleep 1; echo ran'
out="$(bash "$CAL" --target 2 2>"$d/err")" || pnote "calibrate failed: $(tail -3 "$d/err")"
[[ -s "$d/sizes.env" ]] || pnote "no sizes.env"
w1="$(sed -n 's/^W1_ITEMS=//p' "$d/sizes.env")"
# measured is about 1.0-1.9 s (ssh and process start add to the 1 s sleep) -> 48*2/m
[[ "$w1" =~ ^[0-9]+$ ]] && ((w1 >= 40 && w1 <= 100)) || pnote "W1_ITEMS=$w1 not near 48*2/1.x"
for k in BENCH_PROCS W1_WEAK_PER_PC W2_ITEMS W3_TREES T1_EPOCHS; do
	grep -q "^$k=[0-9]" "$d/sizes.env" || pnote "sizes.env lacks $k"
done
grep -q '^BENCH_PROCS=8$' "$d/sizes.env" || pnote "BENCH_PROCS not pc00's nproc 8"
[[ "$(grep -c '^# calibrated .* plain=\|^# calibrated .* plain [A-Z0-9]*=' "$d/sizes.env")" == 4 ]] || pnote "want 4 measured-seconds comments: $(grep '#' "$d/sizes.env")"
verdict "calibrate on the fake lab writes sizes.env with measured seconds as comments")

(d=$(newdir)
row_setup "$d" 'echo boom >&2; exit 1'
rc=0
bash "$CAL" --target 2 >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "a failed plain run exited 0"
[[ ! -e "$d/sizes.env" ]] || pnote "sizes.env written after a failed run"
grep -q 'exited 1' "$d/campaign.log" || pnote "failure not logged: $(tail -2 "$d/campaign.log")"
verdict "failed plain run -> error, no sizes.env")

(d=$(newdir)
row_setup "$d" 'sleep 1'
printf 'W1_ITEMS=7\n' >"$d/sizes.env"
rc=0
bash "$CAL" --target 2 >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "overwrite without --force exited 0"
[[ "$(cat "$d/sizes.env")" == "W1_ITEMS=7" ]] || pnote "sizes.env changed without --force"
[[ ! -s "$d/units.log" ]] || pnote "runs were started although the file exists"
rc=0
bash "$CAL" --target 2 --force >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "--force failed: $(tail -3 "$d/err")"
grep -q '^BENCH_PROCS=' "$d/sizes.env" && grep -q '^# calibrated' "$d/sizes.env" || pnote "--force did not rewrite sizes.env"
verdict "existing sizes.env refused unchanged; --force rewrites with comments")

nrows=$(wc -l <"$RESULTS")
nfail=$(grep -c '^bad$' "$RESULTS" || true)
if ((nfail == 0 && nrows > 0)); then printf 'PASS %d/%d\n' "$nrows" "$nrows"; exit 0; fi
printf 'FAIL %d of %d rows\n' "$nfail" "$nrows"
exit 1
