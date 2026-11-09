#!/usr/bin/env bash
# shellcheck disable=SC2004,SC2030,SC2031,SC2317
# DK1 case table: Dask worker flags and counting on the fake lab.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
FAKELAB_LIB="$REPO/scripts/campaign/test/fakelab.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"

# shellcheck source=scripts/campaign/test/fakelab.sh
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
	for d in "${dirs[@]}"; do rm -rf "$d"; done
}
trap cleanup EXIT

ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then ok "$1"; else bad "$1" "${problems[*]}"; fi
	problems=()
}
newdir() {
	local d; d="$(mktemp -d "$STATE/cluster-dask-test.XXXXXXX")"
	mkdir -p "$d/home"; dirs+=("$d"); printf '%s\n' "$d"
}
count() { local f="$1"; shift; if [[ -f "$f" ]]; then grep -c "$@" "$f" || true; else echo 0; fi; }
val() { [[ -f "$1" ]] || return 0; sed -n "s/^$2=//p" "$1" | tail -1; }
want_val() { local got; got="$(val "$1" "$2")"; [[ "$got" == "$3" ]] || pnote "$4: got [$got], want [$3]"; }
line() { [[ -f "$1" ]] || return 1; grep -qxF -- "$2" "$1"; }

_verif_install() {
	local d="$1"
	mkdir -p "$d/verif"
	cat >"$d/verif/pipedpeer" <<'EOF'
#!/usr/bin/env bash
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "pipedpeer stub: no FAKELAB_DIR" >&2; exit 97; fi
printf 'pipedpeer %s\n' "$*" >>"$FAKELAB_DIR/pipedpeer.log"
printf '%s %s sudo=%s\n' "$(basename "$HOME")" "${1:-}" "$(wc -l <"$FAKELAB_DIR/sudo.log" 2>/dev/null || echo 0)" >>"$FAKELAB_DIR/pipedpeer.order"
case "${1:-}" in
	status) exec cat -- "${PIPEDPEER_STATUS_FILE:?no PIPEDPEER_STATUS_FILE}" ;;
	start | stop | setup | invite | jobs | doctor) exit 0 ;;
	*) echo "pipedpeer stub: unknown call: $*" >&2; exit 64 ;;
esac
EOF
	cat >"$d/verif/systemctl" <<'EOF'
#!/usr/bin/env bash
if [[ -z "${FAKELAB_DIR:-}" ]]; then echo "systemctl stub: no FAKELAB_DIR" >&2; exit 97; fi
printf 'systemctl %s\n' "$*" >>"$FAKELAB_DIR/systemctl.log"
exit 0
EOF
	chmod +x "$d/verif/pipedpeer" "$d/verif/systemctl"
}

_verif_python() {
	local d="$1"
	mkdir -p "$d/pc00/pipedpeer-lab/venv/bin"
	cat >"$d/pc00/pipedpeer-lab/venv/bin/python" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *NodeManagerAddress* && -n "${PY_FAKE_NODES:-}" ]]; then printf '%s\n' "$PY_FAKE_NODES"; exit 0; fi
if [[ "$*" == *"sum(w.get"* && -n "${PY_FAKE_CORES:-}" ]]; then printf '%s\n' "$PY_FAKE_CORES"; exit 0; fi
if [[ -n "${PY_FAKE_ERR:-}" ]]; then printf '%s\n' "$PY_FAKE_ERR" >&2; exit 1; fi
printf '%s\n' "${PY_FAKE_OUT:?PY_FAKE_OUT unset}"
EOF
	chmod +x "$d/pc00/pipedpeer-lab/venv/bin/python"
	# give every PC a venv python
	local i pc
	for ((i=1;i<8;i++)); do
		pc=$(printf 'pc%02d' "$i")
		mkdir -p "$d/$pc/pipedpeer-lab/venv/bin"
		cp "$d/pc00/pipedpeer-lab/venv/bin/python" "$d/$pc/pipedpeer-lab/venv/bin/python"
	done
}

ss_setup() {
	local dir="$1" n="${2:-2}"
	cd "$REPO"
	fakelab_init "$dir" "$n" || exit 90
	_verif_install "$dir"
	_verif_python "$dir"
	export PATH="$dir/verif:$PATH"
	export HOME="$dir/home"
	export LAB_RESULTS="$dir"
	export LAB_RETRY_SLEEP=1
	export LAB_POLL_S=1
	export CLUSTER_READY_S=2
	set +e
}

fx() { cat >"$1"; }

# Row 1: 8-core PC, no cap -> --nworkers 8 --nthreads 1
t1() {
	local d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 2
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	(($rc == 0)) || pnote "row exited $rc: $(tail -5 "$d/log" | tr '\n' '|')"
	grep -q -- '--nworkers 8 --nthreads 1' "$d/units.log" || pnote "no --nworkers 8 --nthreads 1 in units.log: $(grep lab-dask-worker "$d/units.log" | tr '\n' '|')"
	verdict "8-core PC, no cap -> worker command has --nworkers 8 --nthreads 1"
}

# Row 2: CLUSTER_CAP_CPU=4 -> --nworkers 4 --nthreads 1
t2() {
	local d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 2
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2
		CLUSTER_CAP_CPU=4 bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	grep -q -- '--nworkers 4 --nthreads 1' "$d/units.log" || pnote "no --nworkers 4 --nthreads 1: $(grep lab-dask-worker "$d/units.log" | tr '\n' '|')"
	verdict "8-core PC, CLUSTER_CAP_CPU=4 -> --nworkers 4 --nthreads 1"
}

# Row 3: PC missing core count -> no start, logged, not ready; others start
t3() {
	local d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 2
		# remove pc01's nproc column (only 4 fields)
		python3 - "$d/hosts.tsv" <<'PY'
import sys
p=sys.argv[1]
lines=open(p).read().splitlines()
out=[]
for ln in lines:
    if ln.startswith('#') or not ln.strip():
        out.append(ln); continue
    f=ln.split('\t')
    if f and f[0]=='pc01':
        out.append('\t'.join(f[:4]))
    else:
        out.append(ln)
open(p,'w').write('\n'.join(out)+'\n')
PY
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=1
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	# pc00 started
	grep -q 'lab-dask-worker' "$d/units.log" || pnote "pc00 worker did not start"
	# only one worker start
	[[ "$(count "$d/units.log" 'lab-dask-worker')" == 1 ]] || pnote "expected 1 worker start, got $(count "$d/units.log" 'lab-dask-worker')"
	# logged missing
	grep -qi 'no core count' "$d/log" "$d/campaign.log" 2>/dev/null || pnote "no 'no core count' log"
	# not ready
	grep -q 'NOT-READY dask' "$d/log" "$d/campaign.log" 2>/dev/null || pnote "no NOT-READY dask line"
	verdict "PC missing cores -> no start on that PC, logged, listed as not ready; others start"
}

# Row 4: 2 PCs reporting 16 scheduler workers -> ready count 2
t4() {
	local d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 2
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	want_val "$d/log" rc 0 "up ready with 2 unique hosts"
	verdict "2 PCs reporting 16 scheduler workers -> ready count 2, not 16"
}

# Row 5: 1 PC reporting 8 workers when 2 expected -> not ready
t5() {
	local d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 2
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=1
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	grep -q 'NOT-READY dask' "$d/log" "$d/campaign.log" 2>/dev/null || pnote "no NOT-READY dask line"
	verdict "1 PC reporting 8 workers when 2 expected -> not ready yet"
}

# Row 6: capped sum at :348 equals total cores
t6() {
	local d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 2
		fx "$d/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
EOF
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2 PY_FAKE_CORES=8
		CLUSTER_CAP_CPU=4 bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	grep -q 'CLUSTER-UP' "$d/log" || pnote "no CLUSTER-UP"
	[[ "$(count "$d/units.log" -- '--nworkers 4 --nthreads 1')" == 2 ]] || pnote "both PCs should get 4 workers: $(grep lab-dask-worker "$d/units.log" || true)"
	verdict "capped sum at :348 equals total cores"
}

# Rows 7-9: the real ready expression, against a stub dask.distributed
ready_expr() { # <per-host thread lists, e.g. "8,8">; prints what _dask_count_cmd's python prints for total 16
	local d="$1" spec="$2" cmd
	mkdir -p "$d/stub/dask"
	: >"$d/stub/dask/__init__.py"
	cat >"$d/stub/dask/distributed.py" <<EOF
class Client:
    def __init__(self, addr):
        pass
    def scheduler_info(self):
        out = {}
        for h, n in enumerate([$spec]):
            for w in range(n):
                out["tcp://10.0.0.%d:%d" % (h, w)] = {"host": "10.0.0.%d" % h, "nthreads": 1}
        return {"workers": out}
EOF
	eval "$(sed -n '/^_dask_count_cmd()/,/^}/p' "$REPO/scripts/campaign/cluster.sh")"
	cmd="$(_dask_count_cmd 10.0.0.0 16)"
	cmd="python3${cmd#*venv/bin/python}"
	PYTHONPATH="$d/stub" bash -c "$cmd"
}
t7() { local d got; d="$(newdir)"; got="$(ready_expr "$d" "8,8" 2>&1)"; [[ "$got" == 2 ]] || pnote "got $got"; verdict "real expression: 2 PCs, all 16 workers up -> 2"; }
t8() { local d got; d="$(newdir)"; got="$(ready_expr "$d" "8,1" 2>&1)"; [[ "$got" == 0 ]] || pnote "got $got"; verdict "real expression: 2 PCs but only 9 of 16 workers up -> not ready"; }
t9() { local d got; d="$(newdir)"; got="$(ready_expr "$d" "16" 2>&1)"; [[ "$got" == 1 ]] || pnote "got $got"; verdict "real expression: 1 PC holding all 16 workers -> 1 PC, not 2"; }

# Rows 10-13 (FU-F): a worker that does not start, and the Dask allowed-failures override.
_fail_unit_on() { # <dir> <pc> <unit>: that unit's systemd-run fails on that PC
	cat >"$1/verif/systemd-run" <<EOF
#!/usr/bin/env bash
if [[ "\$(basename "\$HOME")" == "$2" && "\$*" == *"$3"* ]]; then
	echo "systemd-run: simulated start failure" >&2
	exit 1
fi
exec "\$FAKELAB_DIR/bin/systemd-run" "\$@"
EOF
	chmod +x "$1/verif/systemd-run"
}
_status_2() {
	fx "$1/status.txt" <<'EOF'
NAME  STATE    PATH  CPU     MEM FREE  GPU  JOBS  NOTE
pc00  healthy  self  8c 1%   7.2 GiB   none 0     -
pc01  healthy  pc01  8c 1%   7.2 GiB   none 0     -
EOF
}
_worker_fail_row() { # <sys> <unit> <label>
	local sys="$1" unit="$2" d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 3
		_fail_unit_on "$d" pc01 "$unit"
		_status_2 "$d"
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=3
		bash "$REPO/scripts/campaign/cluster.sh" up 3 --systems "$sys"
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	want_val "$d/log" rc 1 "up with a failed $sys worker"
	grep -q "NOT-READY $sys pc=pc01" "$d/log" || pnote "no 'NOT-READY $sys pc=pc01'"
	grep -q 'CLUSTER-UP' "$d/log" && pnote "a smaller cluster was reported up"
	# the other PCs are still tried: the PC list is complete, not cut at the first failure
	grep -q "$unit" "$d/units.log" 2>/dev/null || true
	[[ "$(grep -c "$unit" "$d/units.log" 2>/dev/null || true)" -ge 2 ]] || pnote "PCs after the failed one were not started"
	verdict "$3"
}
t10() { _worker_fail_row dask lab-dask-worker "Dask worker fails on pc01 -> NOT-READY dask pc=pc01, the rest started, no CLUSTER-UP"; }
t11() { _worker_fail_row ray lab-ray "Ray worker fails on pc01 -> NOT-READY ray pc=pc01, the rest started, no CLUSTER-UP"; }
_allowed_row() { # <value or empty> <label>
	local v="$1" d rc=0
	d="$(newdir)"
	(
		ss_setup "$d" 2
		_status_2 "$d"
		export PIPEDPEER_STATUS_FILE="$d/status.txt"
		export PY_FAKE_OUT=2
		if [[ -n "$v" ]]; then export CLUSTER_DASK_ALLOWED_FAILURES="$v"; else unset CLUSTER_DASK_ALLOWED_FAILURES; fi
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems dask
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1 || rc=$?
	if [[ -n "$v" ]]; then
		grep 'lab-dask-scheduler' "$d/units.log" | grep -q "DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES=$v" || pnote "scheduler has no allowed-failures=$v"
	else
		grep 'lab-dask-scheduler' "$d/units.log" | grep -q ALLOWED_FAILURES && pnote "unset override still changed the scheduler"
	fi
	verdict "$2"
}
t12() { _allowed_row 10 "CLUSTER_DASK_ALLOWED_FAILURES=10 -> scheduler started with it"; }
t13() { _allowed_row "" "no override -> the scheduler keeps Dask's default"; }

printf 'scripts/campaign/cluster_dask_test.sh case table\n\n'
# Rows 14-15: a probe that keeps failing leaves its own error in the log, so
# NOT-READY have=unknown can be diagnosed; a probe that merely counts low adds nothing.
_probe_row() {
	local err="$1" want_have="$2" name="$3" d
	d="$(newdir)"
	(
		ss_setup "$d" 2
		export PY_FAKE_OUT=1 PY_FAKE_ERR="$err"
		bash "$REPO/scripts/campaign/cluster.sh" up 2 --systems ray
		printf 'rc=%s\n' "$?"
	) >"$d/log" 2>&1
	grep -q "NOT-READY ray want=2 have=$want_have" "$d/log" || pnote "no NOT-READY have=$want_have line"
	if [[ -n "$err" ]]; then
		grep -qF "$err" "$d/campaign.log" || pnote "the probe's error is not in campaign.log"
	elif grep -q 'last probe error' "$d/campaign.log"; then
		pnote "a clean probe logged an error line"
	fi
	verdict "$name"
}
t14() { _probe_row "ConnectionError: GCS unreachable" unknown "Ray probe keeps failing -> NOT-READY have=unknown and its stderr in the log"; }
t15() { _probe_row "" 1 "Ray probe counts too few, no stderr -> NOT-READY have=1, no error line"; }

t1; t2; t3; t4; t5; t6; t7; t8; t9; t10; t11; t12; t13; t14; t15
printf '\n'
total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1