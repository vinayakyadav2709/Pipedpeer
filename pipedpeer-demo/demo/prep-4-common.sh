#!/usr/bin/env bash
# Pre-build the common packages on THIS machine, so a teacher's script starts
# fast. Run on every machine at the setup visit (setup.sh runs it on LAPTOP-A).
#
#   ./demo/prep-4-common.sh                     # build the common environment
#   ./demo/prep-4-common.sh students/primes.py  # also time a teacher-style
#                                               # script's check, before and after
#
# How: 'pipedpeer run prep/common.py --check-only' builds the environment of a
# script importing numpy, pandas, scikit-learn, scipy, matplotlib, seaborn,
# tqdm and torch (CUDA build, as any torch script gets). A later script that
# imports any subset reuses those store paths and builds only a wrapper.
# Before/after: "before" checks the script itself; "after" checks a copy with
# 'import seaborn' appended - a new environment (so nothing is reused from the
# before run) whose packages are all in the common set. "before" is honest only
# on a machine that never built that script (a fresh lab container).
teacher="${1:-}"
if [[ -n "$teacher" ]]; then
	[[ -f "$teacher" ]] || { echo "no such script: $teacher" >&2; exit 2; }
	teacher="$(realpath "$teacher")"   # _lib.sh changes directory
fi
source "$(dirname "$0")/_lib.sh"

# check <script>: wall seconds of a --check-only run (which builds the env).
check() {
	local t0=$SECONDS log
	log="$(mktemp)"
	"$PIPE" run "$1" --check-only 2>&1 | tee "$log" | grep -E 'environment|not packaged|checks complete|NOT READY' || true
	if grep -q 'NOT READY' "$log"; then
		echo "${R}  NOT READY reported for $1 (see above)${N}" >&2
		rm -f "$log"
		return 1
	fi
	if ! grep -q 'built in' "$log"; then
		echo "${R}  no 'built in' line for $1 - the environment was not built (see above)${N}" >&2
		rm -f "$log"
		return 1
	fi
	rm -f "$log"
	echo "$((SECONDS - t0))s" >"$tfile"
}
tfile="$(mktemp)"
trap 'rm -f "$tfile"' EXIT

banner "Common packages on $(uname -n)"
before=-
if [[ -n "$teacher" ]]; then
	note "Before: $teacher on a store without the common set."
	check "$teacher" && before=$(<"$tfile")
fi

note "Building the common environment (first time: minutes; torch is GBs)."
check prep/common.py || { echo "${R}${B}COMMON PREP FAILED${N}"; exit 1; }
echo "${G}  common environment ready${N} in $(<"$tfile")"

if [[ -n "$teacher" ]]; then
	note "After: a copy with one more common import, so its environment is new."
	copy="$(mktemp -d "${TMPDIR:-/tmp}/pipedpeer-prep.XXXXXX")"
	: >"$copy/.pipedpeerignore"
	{ cat "$teacher"; printf '\nimport seaborn  # prep-4: a new env from common packages\n'; } >"$copy/$(basename "$teacher")"
	after=-
	check "$copy/$(basename "$teacher")" && after=$(<"$tfile")
	rm -rf "$copy"
	line="$(printf '%-18s %-8s %-8s %s' "teacher check" "$before" "$after" "$teacher (before / after common prep, $(uname -n))")"
	echo "${B}  $line${N}"
	[[ -f demo/rehearsal-results.txt ]] && echo "$line" >>demo/rehearsal-results.txt
fi
