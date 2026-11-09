#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2317  # rows run in subshells; cleanup runs from a trap
# The S10 case table for metrics.sh (brief S10): start is idempotent, stop
# warns about a PC with no sar data and still pulls it, and the vmstat/sar
# units are the real commands. PCs are the fake lab (test/fakelab.sh) plus
# stub binaries for vmstat, sar, systemd-run-time tools; nothing real is
# touched. One ok/FAIL line per row; ends with "PASS <k>/<k>".

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
METRICS="$REPO/scripts/campaign/metrics.sh"
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
	d="$(mktemp -d "$STATE/campaign-metrics-test.XXXXXX")"
	mkdir -p "$d/home"
	dirs+=("$d")
	printf '%s\n' "$d"
}

count() {
	if [[ -f "$1" ]]; then
		grep -c -- "$2" "$1" || true
	else
		echo 0
	fi
}

# case_setup <dir> [n]: fake lab PCs, own HOME/LAB, lib sourced, and stub
# binaries for the tools metrics.sh runs on each PC (vmstat, sar, systemctl,
# journalctl, sadf) in the fake lab's bin dir, which is first on PATH.
case_setup() {
	local dir="$1" n="${2:-3}"
	cd "$REPO"
	fakelab_init "$dir" "$n" || exit 90
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=1
	# shellcheck source=/dev/null
	source "$REPO/scripts/campaign/lib.sh" || exit 91
	set +e
	_metric_stubs "$dir"
}

_metric_stubs() {
	local dir="$1"
	cat >"$dir/bin/vmstat" <<'STUB'
#!/usr/bin/env bash
set -u
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "vmstat stub: FAKELAB_DIR unset" >&2; exit 97; fi
printf 'vmstat %s\n' "$*" >>"$FAKELAB_DIR/vmstat.log"
printf 'cpu line from stub\n'
STUB
	cat >"$dir/bin/sar" <<'STUB'
#!/usr/bin/env bash
set -u
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "sar stub: FAKELAB_DIR unset" >&2; exit 97; fi
printf 'sar %s\n' "$*" >>"$FAKELAB_DIR/sar.log"
out=""
while (( $# )); do
	case "$1" in
		-o) out="${2:-}"; shift 2 ;;
		*) shift ;;
	esac
done
if [[ -n "$out" ]]; then printf 'SADATA %s\n' "$HOME" >"$out"; fi
STUB
	cat >"$dir/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
set -u
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "systemctl stub: FAKELAB_DIR unset" >&2; exit 97; fi
printf 'systemctl %s\n' "$*" >>"$FAKELAB_DIR/systemctl.log"
STUB
	cat >"$dir/bin/journalctl" <<'STUB'
#!/usr/bin/env bash
set -u
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "journalctl stub: FAKELAB_DIR unset" >&2; exit 97; fi
printf 'journalctl %s\n' "$*" >>"$FAKELAB_DIR/journalctl.log"
printf 'pc daemon log line\n'
STUB
	cat >"$dir/bin/sadf" <<'STUB'
#!/usr/bin/env bash
set -u
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "sadf stub: FAKELAB_DIR unset" >&2; exit 97; fi
printf 'sadf %s\n' "$*" >>"$FAKELAB_DIR/sadf.log"
for f in "$@"; do
	case "$f" in *.sa) [[ -f "$f" ]] && cat -- "$f" ;; esac
done
STUB
	chmod +x "$dir/bin/vmstat" "$dir/bin/sar" "$dir/bin/systemctl" \
		"$dir/bin/journalctl" "$dir/bin/sadf"
}

# --- the table -------------------------------------------------------------

# start, then start again with the same tag -> second is a no-op + warning
t1() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_hosts 3 >/dev/null
		bash "$METRICS" start smoke-n1 "$d/hosts-3.txt"
		printf 'rc=%s\n' "$?"
		bash "$METRICS" start smoke-n1 "$d/hosts-3.txt"
		printf 'rc2=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'FANOUT step=metrics-start-smoke-n1 ok=3 failed=0' "$d/log" ||
		pnote "no FANOUT ok=3 line"
	[[ "$(count "$d/units.log" 'lab-vmstat-smoke-n1:')" == 3 ]] ||
		pnote "lab-vmstat-smoke-n1 started $(count "$d/units.log" 'lab-vmstat-smoke-n1:') times, want 3"
	[[ "$(count "$d/units.log" 'lab-sar-smoke-n1:')" == 3 ]] ||
		pnote "lab-sar-smoke-n1 started $(count "$d/units.log" 'lab-sar-smoke-n1:') times, want 3"
	local pc
	for pc in pc00 pc01 pc02; do
		[[ -s "$d/$pc/pipedpeer-lab/metrics/smoke-n1.start" ]] ||
			pnote "no .start for $pc"
		grep -qE '^[0-9]+$' "$d/$pc/pipedpeer-lab/metrics/smoke-n1.start" 2>/dev/null ||
			pnote "$pc .start is not an epoch"
		[[ -f "$d/$pc/pipedpeer-lab/metrics/smoke-n1.vmstat" ]] ||
			pnote "no .vmstat for $pc"
		grep -q 'cpu line from stub' "$d/$pc/pipedpeer-lab/metrics/smoke-n1.vmstat" 2>/dev/null ||
			pnote "$pc vmstat produced no data (script file not run?)"
		grep -q '^SADATA' "$d/$pc/pipedpeer-lab/metrics/smoke-n1.sa" 2>/dev/null ||
			pnote "no .sa for $pc"
	done
	grep -q 'ALREADY-METRICS smoke-n1' "$d/logs/metrics-start-smoke-n1/pc00" 2>/dev/null ||
		pnote "second start not reported as ALREADY-METRICS"
	grep -q 'already running on some PCs' "$d/campaign.log" 2>/dev/null ||
		pnote "no already-running warning in campaign.log"
	verdict "start twice, same tag -> units started once, second a warning"
}

# stop: a PC whose sar never started is a warning, the others still pulled
t2() {
	local d rc=0
	d="$(newdir)"
	(
		case_setup "$d"
		lab_hosts 3 >/dev/null
		bash "$METRICS" start smoke-n1 "$d/hosts-3.txt"
		rm -f "$d/pc01/pipedpeer-lab/metrics/smoke-n1.sa"
		bash "$METRICS" stop smoke-n1 "$d/hosts-3.txt"
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc"
	grep -q 'FANOUT step=metrics-stop-smoke-n1 ok=3 failed=0' "$d/log" ||
		pnote "stop fan-out did not reach all 3 PCs"
	[[ "$(count "$d/systemctl.log" 'stop lab-vmstat-smoke-n1')" == 3 ]] ||
		pnote "systemctl stop not called on all 3 PCs"
	local pc
	for pc in pc00 pc01 pc02; do
		[[ -f "$d/hosts/$pc/metrics/smoke-n1.daemon.log" ]] ||
			pnote "daemon.log not pulled for $pc"
	done
	grep -q '^SADATA .*pc00' "$d/hosts/pc00/metrics/smoke-n1.net.csv" 2>/dev/null ||
		pnote "pc00 net.csv missing or wrong content"
	[[ "$(count "$d/journalctl.log" '--user')" == 3 ]] ||
		pnote "journalctl --user not called on all 3 PCs"
	grep -q 'WARN no sar data for smoke-n1' "$d/logs/metrics-stop-smoke-n1/pc01" 2>/dev/null ||
		pnote "no sar-data warning for pc01"
	[[ ! -f "$d/hosts/pc01/metrics/smoke-n1.net.csv" ]] ||
		pnote "pc01 should have no net.csv"
	# workers have no runs/ dir (only pc00 does): that is not a failed pull
	! grep -q 'metrics stop: pull .* failed' "$d/log" "$d/logs"/lab*.log 2>/dev/null ||
		pnote "a worker pull without runs/ was logged as failed"
	verdict "stop with no sar on pc01 -> warning, all PCs pulled, no runs/ is not an error"
}

printf 'scripts/campaign/metrics.sh case table\n\n'
t1
t2

total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1
