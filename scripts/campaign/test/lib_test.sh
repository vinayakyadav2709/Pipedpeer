#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317  # rows run in subshells; helpers are called via variables
# The S1 case table: one row per behaviour scripts/campaign/lib.sh promises
# (brief S1; docs/benchmarks/campaign-cases.md for the inputs behind them).
#
# Every row runs in its own subshell with its own fake lab (test/fakelab.sh),
# its own HOME and its own LAB_RESULTS, so the real results dir is never
# touched. One ok/FAIL line per row; the run ends with "PASS <k>/<k>" and
# exits 0 only when every row passed.
#
# Set LAB_TEST_KEEP=1 to keep the scratch dirs of a failing run.

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
LIB="$REPO/scripts/campaign/lib.sh"
FAKELAB_LIB="$REPO/scripts/campaign/test/fakelab.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"

# shellcheck source=/dev/null
source "$FAKELAB_LIB"

pass=0
failed=0
problems=()
dirs=()

cleanup() {
	local d
	if [[ "${LAB_TEST_KEEP:-0}" == 1 ]]; then
		printf 'kept: %s\n' "${dirs[*]}" >&2
		return
	fi
	for d in "${dirs[@]}"; do
		rm -rf "$d"
	done
}
trap cleanup EXIT

ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }

# verdict <row>: one line for the row, then the notes are gone.
verdict() {
	if ((${#problems[@]} == 0)); then
		ok "$1"
	else
		bad "$1" "${problems[*]}"
	fi
	problems=()
}

# newdir: a scratch dir for one row, with the driver's HOME inside it.
newdir() {
	local d
	d="$(mktemp -d "$STATE/campaign-test.XXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}

# case_setup <dir> [n]: fake lab, own HOME/LAB_RESULTS, lib.sh sourced, and
# -e off so a row can look at a function's exit code instead of dying on it.
case_setup() {
	local dir="$1" n="${2:-3}"
	cd "$REPO"
	fakelab_init "$dir" "$n" || exit 90
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=1
	export LAB_POLL_S=1
	# shellcheck source=/dev/null
	source "$LIB" || exit 91
	set +e
}

val() { # val <log> <key>: the last "<key>=<value>" line's value
	[[ -f "$1" ]] || return 0
	sed -n "s/^$2=//p" "$1" | tail -1
}

count() { # count <file> <pattern>
	if [[ -f "$1" ]]; then
		grep -c "$2" "$1" || true
	else
		echo 0
	fi
}

line() { # line <file> <exact line>
	[[ -f "$1" ]] || return 1
	grep -qxF -- "$2" "$1"
}

want_val() { # want_val <log> <key> <want> <what>
	local got
	got="$(val "$1" "$2")"
	[[ "$got" == "$3" ]] || pnote "$4: exit $got, want $3"
}

# --- the table ------------------------------------------------------------

# lab_ssh pc00 true -> 0, one call
t1() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_ssh pc00 true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 0 "lab_ssh pc00 true"
	local calls
	calls="$(count "$d/calls.log" '^ssh pc00$')"
	[[ "$calls" == 1 ]] || pnote "ssh called $calls times, want 1"
	verdict "lab_ssh pc00 true -> 0, one ssh call"
}

# lab_ssh pc00 'exit 3' -> 3 at once, no retry
t2() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_ssh pc00 'exit 3'
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 3 "remote exit 3"
	local calls
	calls="$(count "$d/calls.log" '^ssh pc00$')"
	[[ "$calls" == 1 ]] || pnote "ssh called $calls times, want 1 (no retry on 3)"
	verdict "lab_ssh pc00 'exit 3' -> 3 at once, no retry"
}

# FAKELAB_FAIL=pc01 lab_ssh pc01 true -> 3 attempts, 255, logged
t3() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		export FAKELAB_FAIL=pc01
		lab_ssh pc01 true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 255 "ssh to a dead host"
	local calls
	calls="$(count "$d/calls.log" '^ssh pc01$')"
	[[ "$calls" == 3 ]] || pnote "ssh called $calls times, want 3 attempts"
	line "$d/campaign.log" "" && pnote "campaign.log is empty"
	grep -q 'ssh pc01' "$d/campaign.log" 2>/dev/null || pnote "no ssh pc01 line in campaign.log"
	verdict "FAKELAB_FAIL=pc01 lab_ssh pc01 true -> 3 attempts, 255, logged"
}

# lab_fanout s1 hosts-3.txt true -> ok=3 failed=0, a file per PC
t4() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_hosts 3 >/dev/null
		lab_fanout s1 "$d/hosts-3.txt" true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	line "$d/log" "FANOUT step=s1 ok=3 failed=0" || pnote "no FANOUT ok=3 line"
	want_val "$d/log" rc 0 "fanout"
	local pc
	for pc in pc00 pc01 pc02; do
		[[ -f "$d/logs/s1/$pc" ]] || pnote "no logs/s1/$pc"
	done
	verdict "lab_fanout s1 hosts-3.txt true -> ok=3, logs/s1/pc00..pc02"
}

# one host failing -> ok=2 failed=1, FAILED lists it, exit 0
t5() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		export FAKELAB_FAIL=pc02
		lab_hosts 3 >/dev/null
		lab_fanout s1b "$d/hosts-3.txt" true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	line "$d/log" "FANOUT step=s1b ok=2 failed=1" || pnote "no FANOUT ok=2 line"
	want_val "$d/log" rc 0 "fanout with one dead host"
	[[ -f "$d/logs/s1b/FAILED" ]] || pnote "no logs/s1b/FAILED"
	line "$d/logs/s1b/FAILED" pc02 || pnote "FAILED does not list pc02"
	local calls
	calls="$(count "$d/calls.log" '^ssh pc02$')"
	[[ "$calls" == 3 ]] || pnote "pc02 tried $calls times, want 3"
	verdict "FAKELAB_FAIL=pc02 fanout -> ok=2 failed=1, FAILED=pc02, exit 0"
}

# every host failing -> exit 1
t6() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		export FAKELAB_FAIL=pc00,pc01,pc02
		lab_hosts 3 >/dev/null
		lab_fanout s1c "$d/hosts-3.txt" true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 1 "fanout where every host failed"
	line "$d/log" "FANOUT step=s1c ok=0 failed=3" || pnote "no FANOUT ok=0 line"
	verdict "every host failing -> exit 1"
}

# lab_hosts 2 with pc00 excluded -> pc01, pc02
t7() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		printf 'pc00\n' >"$d/excluded.txt"
		lab_hosts 2
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 0 "lab_hosts 2"
	line "$d/log" "$d/hosts-2.txt" || pnote "did not print $d/hosts-2.txt"
	line "$d/hosts-2.txt" pc01 || pnote "hosts-2.txt has no pc01"
	line "$d/hosts-2.txt" pc02 || pnote "hosts-2.txt has no pc02"
	line "$d/hosts-2.txt" pc00 && pnote "hosts-2.txt still has the excluded pc00"
	verdict "lab_hosts 2 with pc00 excluded -> pc01, pc02"
}

# lab_hosts 5 with 3 PCs -> exit 1 and a message
t8() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_hosts 5
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 1 "lab_hosts 5 with three PCs"
	grep -q 'only 3 PCs available, need 5' "$d/log" || pnote "no 'only 3 PCs available, need 5'"
	verdict "lab_hosts 5 with 3 PCs -> exit 1, 'only 3 PCs available, need 5'"
}

# detach, then wait -> 0; out, first_out_ns, t0 < t1
t9() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_detach pc00 r1 'echo hi; sleep 1'
		printf 'detach_rc=%s\n' "$?"
		lab_wait pc00 r1 30
		printf 'wait_rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" detach_rc 0 "lab_detach"
	want_val "$d/log" wait_rc 0 "lab_wait"
	line "$d/log" 0 || pnote "lab_wait did not print the exit code 0"
	local run="$d/pc00/pipedpeer-lab/runs/r1"
	line "$run/out" hi || pnote "out is not 'hi'"
	[[ -s "$run/first_out_ns" ]] || pnote "no first_out_ns"
	grep -Eq '^[0-9]+$' "$run/first_out_ns" 2>/dev/null || pnote "first_out_ns is not a nanosecond count"
	local t0 t1
	t0="$(cat "$run/t0" 2>/dev/null || echo 0)"
	t1="$(cat "$run/t1" 2>/dev/null || echo 0)"
	((t0 > 0 && t1 > t0)) || pnote "t0=$t0 t1=$t1, want 0 < t0 < t1"
	verdict "lab_detach pc00 r1 'echo hi; sleep 1' then lab_wait -> 0, out=hi, t0<t1"
}

# detach twice with the same id -> ALREADY-STARTED, one run
t10() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_detach pc00 r1 'echo hi'
		lab_wait pc00 r1 30 >/dev/null
		lab_detach pc00 r1 'echo hi'
		printf 'second_rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" second_rc 0 "second lab_detach"
	line "$d/log" "ALREADY-STARTED r1" || pnote "no ALREADY-STARTED r1 line"
	local lines
	lines="$(wc -l 2>/dev/null <"$d/pc00/pipedpeer-lab/runs/r1/out" || echo 0)"
	[[ "$lines" == 1 ]] || pnote "out has $lines lines, want 1"
	local starts
	starts="$(count "$d/units.log" 'r1')"
	[[ "$starts" == 1 ]] || pnote "the run was started $starts times, want 1"
	verdict "lab_detach twice with the same id -> ALREADY-STARTED r1, one run"
}

# lab_wait on a run that exits 7 -> prints 7, returns 0
t11() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_detach pc00 r7 'exit 7'
		lab_wait pc00 r7 30
		printf 'wait_rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" wait_rc 0 "lab_wait on exit 7"
	line "$d/log" 7 || pnote "lab_wait did not print 7"
	verdict "lab_wait on 'exit 7' -> prints 7, returns 0"
}

# lab_wait pc00 r2 3 on sleep 30 -> TIMEOUT r2, 124
t12() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_detach pc00 r2 'sleep 30'
		lab_wait pc00 r2 3
		printf 'wait_rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" wait_rc 124 "lab_wait timeout"
	line "$d/log" "TIMEOUT r2" || pnote "no TIMEOUT r2 line"
	[[ -f "$d/pc00/pipedpeer-lab/runs/r2/exit" ]] && pnote "the run was killed instead of left running"
	verdict "lab_wait pc00 r2 3 on 'sleep 30' -> TIMEOUT r2, 124"
}

# ssh dropping for two polls mid-wait -> the exit code, no restart
t13() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_detach pc00 r3 'sleep 6; echo done'
		printf 'pc00\n' >"$d/fail"
		(sleep 5 && rm -f "$d/fail") &
		lab_wait pc00 r3 60
		printf 'wait_rc=%s\n' "$?"
		wait
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" wait_rc 0 "lab_wait across an ssh drop"
	line "$d/log" 0 || pnote "lab_wait did not print the exit code 0"
	local run="$d/pc00/pipedpeer-lab/runs/r3"
	line "$run/out" 'done' || pnote "out is not 'done'"
	local lines starts
	lines="$(wc -l 2>/dev/null <"$run/out" || echo 0)"
	[[ "$lines" == 1 ]] || pnote "out has $lines lines, want 1"
	starts="$(count "$d/units.log" 'r3')"
	[[ "$starts" == 1 ]] || pnote "the run was started $starts times, want 1 (no restart)"
	verdict "ssh drop for 2 polls mid-wait -> exit code returned, no restart"
}

# pssh 2.3.1's real report lines: a lone SUCCESS line -> failed=0, no FAILED file
t14() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		cat >"$d/bin/pssh" <<'PSSH'
#!/usr/bin/env bash
echo "[1] 12:00:00 [SUCCESS] pc01"
exit 0
PSSH
		printf 'pc01\n' >"$d/h.txt"
		lab_fanout s14 "$d/h.txt" true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	line "$d/log" "FANOUT step=s14 ok=1 failed=0" || pnote "no ok=1 failed=0 line: $(tail -3 "$d/log")"
	[[ ! -e "$d/logs/s14/FAILED" ]] || pnote "FAILED file exists for an all-success run"
	verdict "pssh '[1] 12:00:00 [SUCCESS] pc01' -> failed=0, no FAILED file"
}

# the real FAILURE line: retried, then FAILED lists the host
t15() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		cat >"$d/bin/pssh" <<'PSSH'
#!/usr/bin/env bash
echo "x" >>"$FAKELAB_DIR/pssh.calls"
echo "[2] 12:00:00 [FAILURE] pc02 Exited with error code 255"
exit 1
PSSH
		export LAB_RETRY_SLEEP=0.1
		printf 'pc02\npc03\n' >"$d/h.txt"
		lab_fanout s15 "$d/h.txt" true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	line "$d/logs/s15/FAILED" pc02 || pnote "FAILED does not list pc02"
	[[ "$(count "$d/pssh.calls" x)" == 3 ]] || pnote "pssh called $(count "$d/pssh.calls" x) times, want 3 attempts"
	line "$d/log" "FANOUT step=s15 ok=1 failed=1" || pnote "no ok=1 failed=1 line"
	verdict "pssh '[2] 12:00:00 [FAILURE] pc02 Exited with error code 255' -> retried, FAILED=pc02"
}

# pssh failing with no parsable report line must not look like success
t16() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		cat >"$d/bin/pssh" <<'PSSH'
#!/usr/bin/env bash
echo "pssh: unrecognized option" >&2
exit 2
PSSH
		export LAB_RETRY_SLEEP=0.1
		printf 'pc01\n' >"$d/h.txt"
		lab_fanout s16 "$d/h.txt" true
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" rc 1 "fanout whose pssh printed no report"
	line "$d/log" "FANOUT step=s16 ok=0 failed=1" || pnote "no ok=0 failed=1 line"
	verdict "pssh exit 2 and no report line -> every host failed, exit 1"
}

# a $LAB with a space is refused with a clear error (pssh -x / rsync -e split it)
t17() {
	local d rc=0
	d="$(newdir)"
	(
		export LAB_RESULTS="$d/has space"
		# shellcheck source=/dev/null
		source "$LIB"
	) >"$d/log" 2>&1 || rc=$?
	((rc != 0)) || pnote "sourcing lib.sh with a space in LAB succeeded"
	grep -q 'space' "$d/log" || pnote "no clear error: $(cat "$d/log")"
	rc=0
	(
		export LAB_RESULTS="$d/nospace"
		# shellcheck source=/dev/null
		source "$LIB"
	) >"$d/log2" 2>&1 || rc=$?
	((rc == 0)) || pnote "a LAB without a space was refused: $(cat "$d/log2")"
	verdict "LAB with a space refused with an error; LAB without one accepted"
}

printf 'scripts/campaign/lib.sh case table\n\n'
t1
t2
t3
t4
t5
t6
t7
t8
# the run's exit marker appears only after the output reader finished: a unit
# that ends with the shell kills a reader still holding the last line (seen on a
# 24.04 container: out empty, exit 0)
t18() {
	local d rc=0 sh
	d="$(newdir)"
	sh="$d/slowbin"; mkdir -p "$sh"
	printf '#!/usr/bin/env bash\nsleep 2\nexec %s "$@"\n' "$(command -v python3)" >"$sh/python3"; chmod +x "$sh/python3"
	(
		case_setup "$d"
		PATH="$sh:$PATH"
		lab_detach pc00 r18 'sleep 1; echo done'
		lab_wait pc00 r18 30 >/dev/null
		sleep 0.2
		printf 'out=%s\n' "$(cat "$d/pc00/pipedpeer-lab/runs/r18/out" 2>/dev/null)"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	want_val "$d/log" out "done" "out read when the exit marker is there"
	verdict "slow output reader: out is complete once lab_wait returns"
}

t9
t10
t11
t12
t13
t14
t15
t16
t17
t18
printf '\n'

total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1
