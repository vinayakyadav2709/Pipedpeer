#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2034,SC2317
# install_warmup_test.sh: tests for WU1 (install.sh warm-up wait & timing).
#
# Cases:
#   1. all done -> exit 0, one row each in warmup-times.tsv
#   2. already done at first poll -> no wait, exit 0
#   3. one PC failed -> named with last log line, other PCs kept, exit 1
#   4. log stalls -> named before timeout with last log line, other PCs kept, exit 1
#   5. log missing -> named, other PCs kept, exit 1
#   6. timeout -> named with last log line, exit 1

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

verdict() {
	if ((${#problems[@]} == 0)); then
		ok "$1"
	else
		bad "$1" "${problems[*]}"
	fi
	problems=()
}

newdir() {
	local d
	d="$(mktemp -d "$STATE/campaign-warmup-test.XXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}

pipedpeer_stub() {
	local bin="$1"
	cat >"$bin/pipedpeer" <<'PP-STUB'
#!/usr/bin/env bash
set -uo pipefail
dir="${FAKELAB_DIR:?}"
pc="$(basename "$(cd "${HOME:-}" && pwd)")"
VERSION="${STUB_VERSION:-0.0.1-test.20261108-abc1234}"
case "${1:-}" in
	status)
		node_id="node-$pc"
		printf 'daemon running (pid=%s node-id=%s port=4242 version=%s)\n' "$$" "$node_id" "$VERSION"
		printf '\n'
		printf 'NAME  STATE    PATH  CPU     MEM FREE  GPU   JOBS  NOTE\n'
		for d in "$dir"/pc*; do
			[[ -d "$d" ]] || continue
			h="$(basename "$d").lab"
			printf '%s  healthy  peer  8c 1%%  31GiB  none 0  0  -\n' "$h"
		done
		;;
	invite)
		printf 'pipedpeer connect FAKE-CODE-123\n'
		echo "created a new cluster" >&2
		;;
	connect)
		exit 0
		;;
	--version | version)
		printf 'version=%s\n' "$VERSION"
		;;
	setup)
		exit 0
		;;
	*)
		echo "stub pipedpeer: unknown command ${1:-}" >&2
		exit 9
		;;
esac
PP-STUB
	chmod +x "$bin/pipedpeer"
}

case_setup() {
	local dir="$1" n="${2:-3}"
	cd "$REPO"
	fakelab_init "$dir" "$n" || exit 90
	pipedpeer_stub "$dir/bin"
	cat >"$dir/bin/INSTALL-STUB" <<'IS'
#!/usr/bin/env bash
exit 0
IS
	chmod +x "$dir/bin/INSTALL-STUB"
	export INSTALL_CMD="INSTALL-STUB {version}"
	mkdir -p "$dir/home/pipedpeer-lab/work" "$dir/home/pipedpeer-lab/runs"
	cp "$dir/pc00/pipedpeer-lab/work/stamp.py" "$dir/home/pipedpeer-lab/work/" 2>/dev/null || true
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=1
	export LAB_POLL_S=1
	unset XDG_STATE_HOME
	# shellcheck source=/dev/null
	source "$LIB" || exit 91
	set +e
}

set_warmup_log() {
	local pcdir="$1" start_ago="$2" last_line="$3"
	local logdir="$pcdir/.local/state/pipedpeer"
	mkdir -p "$logdir"
	local now start_iso
	now="$(date +%s)"
	start_iso="$(date -d "@$((now - start_ago))" -Is)"
	{
		printf '%s building the common environment (numpy, pandas, torch CPU) build=test torch=CPU\n' "$start_iso"
		printf 'compiling extensions\n'
		printf '%s\n' "$last_line"
	} >"$logdir/warmup.log"
}

count() {
	if [[ -f "$1" ]]; then
		grep -c "$2" "$1" || true
	else
		echo 0
	fi
}

line() {
	[[ -f "$1" ]] || return 1
	grep -qxF -- "$2" "$1"
}

# --- Case 1: all done -> exit as today, one row each ----------------------
t1_all_done() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d" 3
		set_warmup_log "$d/pc00" 12 "done"
		set_warmup_log "$d/pc01" 15 "done"
		set_warmup_log "$d/pc02" 20 "done"
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'INSTALL ok=3 failed=0' "$d/log" || pnote "no 'INSTALL ok=3 failed=0'"
	[[ -f "$d/warmup-times.tsv" ]] || pnote "warmup-times.tsv missing"
	grep -qxF $'pc\tseconds\tstate' "$d/warmup-times.tsv" || pnote "warmup-times.tsv missing header"
	local n_pc
	n_pc="$(count "$d/warmup-times.tsv" '^pc[0-9]')"
	[[ "$n_pc" == 3 ]] || pnote "warmup-times.tsv has $n_pc pc rows, want 3"
	grep -q $'pc00\t[0-9]\+\tdone' "$d/warmup-times.tsv" || pnote "pc00 row not done in warmup-times.tsv"
	grep -q $'pc01\t[0-9]\+\tdone' "$d/warmup-times.tsv" || pnote "pc01 row not done in warmup-times.tsv"
	grep -q $'pc02\t[0-9]\+\tdone' "$d/warmup-times.tsv" || pnote "pc02 row not done in warmup-times.tsv"
	[[ -f "$d/logs/install/FAILED" ]] && pnote "FAILED file created when all succeeded"
	verdict "all done -> exit as today, one row each in warmup-times.tsv"
}

# --- Case 2: already done at first poll -> no wait ------------------------
t2_already_done() {
	local d rc=0 t_start t_end dur
	d="$(newdir)"
	(
		case_setup "$d" 3
		set_warmup_log "$d/pc00" 30 "done"
		set_warmup_log "$d/pc01" 30 "done"
		set_warmup_log "$d/pc02" 30 "done"
		# A long poll sleep: if it slept, it would take >=10s
		export CAMPAIGN_WARMUP_POLL_S=10
		t_start="$(date +%s)"
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
		t_end="$(date +%s)"
		printf 'dur=%d\n' "$((t_end - t_start))"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'INSTALL ok=3 failed=0' "$d/log" || pnote "no 'INSTALL ok=3 failed=0'"
	dur="$(sed -n 's/^dur=//p' "$d/log" | tail -1 || echo 99)"
	((dur < 6)) || pnote "already done waited $dur s (poll was 10s); expected no wait"
	grep -q $'pc00\t[0-9]\+\tdone' "$d/warmup-times.tsv" || pnote "pc00 row wrong"
	verdict "already done at first poll -> no wait"
}

# --- Case 3: one PC failed -> named, the others kept ----------------------
t3_one_failed() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d" 3
		set_warmup_log "$d/pc00" 10 "done"
		set_warmup_log "$d/pc01" 10 "failed (exit 1); details above"
		set_warmup_log "$d/pc02" 10 "done"
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q '^rc=1$' "$d/log" || pnote "expected install.sh exit 1 on failure"
	grep -q 'INSTALL ok=2 failed=1' "$d/log" || pnote "no 'INSTALL ok=2 failed=1'"
	grep -q 'FAILED pc01:.*failed (exit 1)' "$d/log" || pnote "pc01 failure does not name last log line"
	line "$d/logs/install/FAILED" pc01 || pnote "pc01 not in logs/install/FAILED"
	! line "$d/logs/install/FAILED" pc00 || pnote "pc00 incorrectly in FAILED"
	! line "$d/logs/install/FAILED" pc02 || pnote "pc02 incorrectly in FAILED"
	[[ -f "$d/warmup-times.tsv" ]] || pnote "warmup-times.tsv missing"
	grep -q $'pc00\t[0-9]\+\tdone' "$d/warmup-times.tsv" || pnote "pc00 row missing or not done"
	grep -q $'pc02\t[0-9]\+\tdone' "$d/warmup-times.tsv" || pnote "pc02 row missing or not done"
	grep -q '^pc01' "$d/warmup-times.tsv" || pnote "pc01 row missing from warmup-times.tsv"
	verdict "one PC failed -> named with last log line, the others kept"
}

# --- Case 4: log stalls -> named before the timeout -----------------------
t4_log_stalls() {
	local d rc=0 t_start t_end dur
	d="$(newdir)"
	(
		case_setup "$d" 3
		set_warmup_log "$d/pc00" 5 "done"
		# pc01's log stops changing at this line
		set_warmup_log "$d/pc01" 10 "building wheels: torch 2.13.0"
		set_warmup_log "$d/pc02" 5 "done"
		export CAMPAIGN_WARMUP_STALL_S=2
		export CAMPAIGN_WARMUP_POLL_S=1
		export CAMPAIGN_WARMUP_TIMEOUT_S=30
		t_start="$(date +%s)"
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
		t_end="$(date +%s)"
		printf 'dur=%d\n' "$((t_end - t_start))"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q '^rc=1$' "$d/log" || pnote "expected install.sh exit 1 on stall"
	grep -q 'INSTALL ok=2 failed=1' "$d/log" || pnote "no 'INSTALL ok=2 failed=1'"
	dur="$(sed -n 's/^dur=//p' "$d/log" | tail -1 || echo 99)"
	((dur < 15)) || pnote "stall wait took $dur s, expected <15s before timeout (30s)"
	grep -q 'FAILED pc01:.*stalled.*building wheels: torch 2.13.0' "$d/log" || pnote "pc01 stall failure does not name last line"
	line "$d/logs/install/FAILED" pc01 || pnote "pc01 not in logs/install/FAILED"
	grep -q $'pc01\t.*\tstalled' "$d/warmup-times.tsv" || pnote "pc01 not recorded as stalled in warmup-times.tsv"
	verdict "log stalls -> named before timeout with last log line"
}

# --- Case 5: log missing -> named -----------------------------------------
t5_log_missing() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d" 3
		set_warmup_log "$d/pc00" 5 "done"
		# pc01 has no warmup.log
		set_warmup_log "$d/pc02" 5 "done"
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q '^rc=1$' "$d/log" || pnote "expected install.sh exit 1 on missing log"
	grep -q 'INSTALL ok=2 failed=1' "$d/log" || pnote "no 'INSTALL ok=2 failed=1'"
	grep -q 'FAILED pc01:.*missing' "$d/log" || pnote "pc01 missing log not reported"
	line "$d/logs/install/FAILED" pc01 || pnote "pc01 not in logs/install/FAILED"
	grep -q $'pc01\t.*\tmissing' "$d/warmup-times.tsv" || pnote "pc01 not recorded as missing in warmup-times.tsv"
	verdict "log missing -> named, the others kept"
}

# --- Case 6: timeout -> named with last log line -------------------------
t6_timeout() {
	local d rc=0 bg_pid
	d="$(newdir)"
	(
		case_setup "$d" 2
		set_warmup_log "$d/pc00" 5 "done"
		# pc01 keeps updating so it does not stall, but never says done
		set_warmup_log "$d/pc01" 5 "compiling C extension"
		(
			while true; do
				sleep 0.5
				printf 'building step %s\n' "$SECONDS" >>"$d/pc01/.local/state/pipedpeer/warmup.log" 2>/dev/null || break
			done
		) &
		bg_pid=$!
		export CAMPAIGN_WARMUP_TIMEOUT_S=3
		export CAMPAIGN_WARMUP_POLL_S=1
		export CAMPAIGN_WARMUP_STALL_S=60
		bash "$REPO/scripts/campaign/install.sh" 0.0.1-test.20261108-abc1234
		printf 'rc=%s\n' "$?"
		kill "$bg_pid" 2>/dev/null || true
		wait "$bg_pid" 2>/dev/null || true
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q '^rc=1$' "$d/log" || pnote "expected install.sh exit 1 on timeout"
	grep -q 'INSTALL ok=1 failed=1' "$d/log" || pnote "no 'INSTALL ok=1 failed=1'"
	grep -q 'FAILED pc01:.*timed out' "$d/log" || pnote "pc01 timeout not reported"
	line "$d/logs/install/FAILED" pc01 || pnote "pc01 not in logs/install/FAILED"
	grep -q $'pc01\t.*\ttimeout' "$d/warmup-times.tsv" || pnote "pc01 not recorded as timeout in warmup-times.tsv"
	verdict "timeout -> named with last log line"
}

t1_all_done
t2_already_done
t3_one_failed
t4_log_stalls
t5_log_missing
t6_timeout

printf 'PASS %d/%d\n' "$pass" "$((pass + failed))"
((failed == 0)) || exit 1
