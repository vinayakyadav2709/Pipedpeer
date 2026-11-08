#!/usr/bin/env bash
# shellcheck disable=SC2317,SC2015  # rows run in subshells; cleanup from a trap
# The stage.sh case table on the fake lab. Every PC starts with an EMPTY work
# dir (FAKELAB_NO_STAMP=1), so a file that stage.sh forgets is missing here as
# it would be on a real PC. One ok/FAIL line per row; ends "PASS <k>/<k>".

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
STAGE="$CAMP/stage.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
# shellcheck source=/dev/null
source "$CAMP/test/fakelab.sh"

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
	d="$(mktemp -d "$STATE/campaign-stage-test.XXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}
row_setup() {
	local dir="$1"
	cd "$REPO"
	FAKELAB_NO_STAMP=1 fakelab_init "$dir" 3 || exit 90
	export HOME="$dir/home" LAB_RESULTS="$dir" LAB_RETRY_SLEEP=0.2 LAB_POLL_S=1
	rm -rf "$dir"/pc0*/pipedpeer-lab/work
}

work() { printf '%s/%s/pipedpeer-lab/work' "$1" "$2"; }

# --- empty work dirs: everything a run names is there afterwards ------------------
(d=$(newdir)
row_setup "$d"
for pc in pc00 pc01 pc02; do
	[[ ! -e "$(work "$d" $pc)/stamp.py" ]] || pnote "$pc work dir not empty at the start"
done
bash "$STAGE" >"$d/out" 2>"$d/err" || pnote "stage.sh failed: $(tail -3 "$d/err")"
grep -q 'STAGE ok=3 failed=0' "$d/out" || pnote "no STAGE ok=3 line: $(cat "$d/out")"
for pc in pc00 pc01 pc02; do
	w="$(work "$d" $pc)"
	for f in stamp.py pool_map.py cpu.py w3_rf.py t1_mlp.py e5_env.py; do
		[[ -f "$w/$f" ]] || pnote "$pc lacks $f"
	done
	[[ -d "$w/baselines" && -n "$(ls "$w/baselines" 2>/dev/null)" ]] || pnote "$pc lacks baselines/"
done
verdict "empty work dir: stamp.py, pool_map.py, cpu.py, w3_rf.py, t1_mlp.py, e5_env.py, baselines/ on each PC")

# --- one PC unreachable ---------------------------------------------------------------
(d=$(newdir)
row_setup "$d"
rc=0
FAKELAB_FAIL=pc01 bash "$STAGE" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "stage.sh exited 0 with pc01 unreachable"
grep -q 'stage FAILED pc01' "$d/err" || pnote "pc01 not reported: $(tail -3 "$d/err")"
grep -qx pc01 "$d/logs/stage/FAILED" || pnote "FAILED does not list pc01"
[[ -f "$(work "$d" pc00)/pool_map.py" && -f "$(work "$d" pc02)/pool_map.py" ]] || pnote "the other PCs were not staged"
grep -q 'STAGE ok=2 failed=1' "$d/out" || pnote "summary: $(cat "$d/out")"
verdict "one PC unreachable: reported failed, exit non-zero, others staged")

# --- every script a run names exists in the staged set ---------------------------------
(d=$(newdir)
row_setup "$d"
bash "$STAGE" >"$d/out" 2>"$d/err" || pnote "stage.sh failed: $(tail -3 "$d/err")"
w="$(work "$d" pc00)"
names="$( (grep -ohE '[A-Za-z0-9_]+\.py' "$CAMP/systems.sh" "$CAMP/e5.sh" "$CAMP/e6.sh" "$CAMP/lib.sh" | grep -v '^_\|^t1_torchrun'; \
	for s in w1 w2 w3 trivial; do echo "baselines/${s}_ray.py baselines/${s}_dask.py baselines/${s}_ssh.sh" | tr ' ' '\n'; done; \
	echo baselines/t1_torchrun.py; echo baselines/t1_torchrun.sh) | sort -u)"
n=0
for f in $names; do
	n=$((n + 1))
	[[ -e "$w/$f" ]] || pnote "named but not staged: $f"
done
((n > 15)) || pnote "only $n names found; the grep is broken"
verdict "every script named by systems.sh, e5.sh, e6.sh (and the ports) exists in the staged set")

nrows=$(wc -l <"$RESULTS")
nfail=$(grep -c '^bad$' "$RESULTS" || true)
if ((nfail == 0 && nrows > 0)); then printf 'PASS %d/%d\n' "$nrows" "$nrows"; exit 0; fi
printf 'FAIL %d of %d rows\n' "$nfail" "$nrows"
exit 1
