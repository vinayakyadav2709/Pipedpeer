#!/usr/bin/env bash
# Runner for the port smoke rows (brief S8, done-when): the per-workload files
# test/ports_<w>.sh are sourced here; each adds the rows of its unit (S8a one
# file, S8b one, S8c one) via the row DSL the runner defines:
#
#   port_expect <name> <orig cmd> <port cmd> <result prefix>
#       runs both, takes the one line starting with <result prefix> from each
#       stdout, masks only the seconds field (timing may differ; items and the
#       checksum must not) and compares the rest byte for byte.
#   port_exact <name> <orig cmd> <port cmd> <line>
#       <line> must appear exactly once, standalone, in both stdouts.
#   port_fail <name> <cmd>
#       the port gets an unreachable address and must exit non-zero fast;
#       the 60 s timeout (CASE_TMO) firing is a FAIL (a hang is the bug).
#   port_ray_case / port_dask_case / port_ssh_case / port_exact_ssh_case /
#       port_fail_ray_case / port_fail_dask_case — the same, skipping the row
#       when the local cluster is (or the driver's ssh machinery partly) not
#       up; a skip says why and is a pass for the campaign (the real runs
#       happen in the lab), but it stays visible in the output.
#
# Where things run: this driver starts a local Ray head
# (RAY_ADDRESS=127.0.0.1:6379) and a local Dask scheduler + 2 workers
# (DASK_SCHEDULER=tcp://127.0.0.1:8786) and stops them at exit. A cluster
# already on that port is left running (row status says `already up`).
#
# Sizes are the case table's small ones (W1 items=48, idle items=16, T1 at 2
# ranks vs 1), never sizes.env: rows check formats and checksums, not speed.
#
# Usage: "$0" [-k <name substring>]   # subset run, uncounted rows skipped
# Output: one line per row (PASS/SKIP/FAIL) then a PORTS total. Exit 0 unless a
# row FAILED; skips are fine but named, so the report can carry them.
# shellcheck shell=bash disable=SC2317,SC1090
# (SC2317: the row DSL below is called only by the sourced ports_*.sh files.)
set -u
# The dev runner owns its own state dir; a stale XDG_STATE_HOME from a parent
# shell must not redirect mktemp into a missing tree.
unset XDG_STATE_HOME

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(git -C "$here/../.." rev-parse --show-toplevel)
PY=${PORTS_DEV_PY:-"${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer-lab-dev/venv/bin/python"}
BINDIR=$(dirname "$PY")
RAY_ADDR=${RAY_ADDR:-127.0.0.1:6379}
DASK_ADDR=${DASK_ADDR:-tcp://127.0.0.1:8786}
CASE_TMO=${CASE_TMO:-90}
# Cluster start waits: START_TRIES polls START_WAIT seconds apart (the selftest
# shrinks them).
START_TRIES=${START_TRIES:-30}
START_WAIT=${START_WAIT:-2}
# Ray's socket paths need a short dir: a private mktemp -d under /tmp, removed
# at exit (never a fixed shared name).
RAYROOT=${PORTS_RAYROOT:-$(mktemp -d /tmp/ports-ray-root.XXXXXX)}
RAYROOT_OWN=0
[ -n "${PORTS_RAYROOT:-}" ] || RAYROOT_OWN=1
RAY_PORT=${RAY_PORT:-6379}
DASK_PORT=${DASK_PORT:-8786}

FILTER=
while getopts k: opt; do
	case $opt in
	k) FILTER=$OPTARG ;;
	*) echo "usage: $0 [-k substring]" >&2; exit 1 ;;
	esac
done

# The runner's own rows (fake clusters, private HOME): run first, once.
if [ -z "${PORTS_IN_SELFTEST:-}" ] && [ -z "$FILTER" ]; then
	echo "ports_test: selftest of the runner ..."
	bash "$here/ports_selftest.sh" || { echo "ports_test: selftest failed" >&2; exit 1; }
fi

if [ ! -x "$PY" ]; then
	echo "SKIP all rows: dev venv missing ($PY)."
	echo "  uv venv --python 3.11 \${XDG_CACHE_HOME:-\$HOME/.cache}/pipedpeer-lab-dev/venv"
	echo "  uv pip install --python <venv>/bin/python -r scripts/campaign/requirements-dev.txt"
	exit 0
fi

# Own state dir (the mktemp pattern of scripts/bench-pool.sh, never /tmp by
# name), the relative work dir a PC-side run has (workload files top level,
# ports under baselines/), and per-row output/error captures inside it.
mkdir -p "$HOME/.local/state/pipedpeer" "$RAYROOT"
statedir=$(mktemp -d "$HOME/.local/state/pipedpeer/ports-test.XXXXXX")
mkdir -p "$statedir/logs" "$statedir/work"
logs="$statedir/logs"
workdir="$statedir/work"
mkdir -p "$statedir/state/pipedpeer" # the ssh baselines mktemp under it
export XDG_STATE_HOME="$statedir/state"
# Ray's AF_UNIX socket paths must stay under 107 bytes, so its temp dir lives
# in the short $RAYROOT tree rather than under the (long) state dir.
raydir=$(mktemp -d "$RAYROOT/ports-ray.XXXXXX")
(cd "$REPO/docs/benchmarks/workloads" && cp -r -- *.py baselines "$workdir/")
FILTERED() { [ -z "$FILTER" ] || case $1 in *"$FILTER"*) return 0 ;; *) return 1 ;; esac; }

# Teardown at exit, before any row runs: stop what we started, keep the state
# dir when something failed or PORTS_KEEP=1.
# Cluster state: 0 not up, 1 up and ours (stopped at exit), 2 already up.
# RAY_OURS is set before `ray start` runs, so a kill during startup still
# stops the head.
RAY_STATE=0
DASK_STATE=0
DASK_PIDS=
RAY_OURS=0
KEEP=0
cleanup() {
	trap '' INT TERM
	local p
	for p in $DASK_PIDS; do
		pkill -TERM -P "$p" >/dev/null 2>&1 # a worker's nanny and children
		kill "$p" >/dev/null 2>&1
	done
	if [ "$RAY_OURS" = 1 ]; then
		# `ray stop --force` kills every ray of this user. Our head is found by
		# its temp dir; the host-wide stop runs only when no ray process outside
		# ours exists (it also clears stragglers), else it is skipped and said.
		local mine foreign
		mine=$(pgrep -f -- "$raydir" 2>/dev/null | tr '\n' ' ')
		# shellcheck disable=SC2086
		[ -z "$mine" ] || kill -TERM $mine >/dev/null 2>&1
		foreign=$(pgrep -f '^[^ ]*/?(raylet|gcs_server)( |$)' 2>/dev/null | grep -vxF -f <(tr ' ' '\n' <<<"$mine") | grep -vx "$$" | head -1)
		if [ -z "$foreign" ]; then
			"$BINDIR/ray" stop --force >/dev/null 2>&1
		else
			echo "note: another ray (pid $foreign) is running; not running 'ray stop --force', stopped only the head in $raydir" >&2
			# shellcheck disable=SC2086
			[ -z "$mine" ] || { sleep 1; kill -KILL $mine >/dev/null 2>&1; }
		fi
	fi
	if [ "$KEEP" != 1 ] && [ "${PORTS_KEEP:-0}" != 1 ]; then
		rm -rf "$statedir" "$raydir"
		[ "$RAYROOT_OWN" != 1 ] || rmdir "$RAYROOT" 2>/dev/null
	else
		echo "kept for debugging: $statedir"
	fi
}
# A kill (timeout, Ctrl-C) must run the cleanup too: exit turns the signal
# into the EXIT trap.
trap 'exit 143' INT TERM
trap cleanup EXIT

# --- clusters ---------------------------------------------------------------

ray_ready() { timeout 15 "$PY" -c "import ray; ray.init(address='$RAY_ADDR')" >/dev/null 2>&1; }
port_ready() { timeout 3 bash -c "exec 3<>/dev/tcp/127.0.0.1/$1" 2>/dev/null; }

start_ray() { # sets RAY_STATE: 0 not up, 1 ours, 2 already up; notes on stderr
	if port_ready "$RAY_PORT"; then
		if ray_ready; then RAY_STATE=2; return 0; fi
		echo "note: port $RAY_PORT is taken but is not a ray head; ray rows skipped" >&2
		return 0
	fi
	mkdir -p "$raydir"
	RAY_OURS=1 # from here a kill must stop the head
	if ! "$BINDIR/ray" start --head --port="$RAY_PORT" --object-store-memory=200000000 \
		--temp-dir "$raydir" --include-dashboard=False >"$logs/ray-start.log" 2>&1; then
		echo "note: local ray head did not start (see $logs/ray-start.log); ray rows skipped" >&2
		return 0
	fi
	local i=0
	# Wait, bounded (~2 min): the head's first import is slow but not forever.
	until ray_ready || [ $i -ge "$START_TRIES" ]; do
		i=$((i + 1))
		sleep "$START_WAIT"
	done
	if ray_ready; then RAY_STATE=1; return 0; fi
	echo "note: local ray head did not become ready (see $logs/ray-start.log); ray rows skipped" >&2
}

dask_ready() { timeout 15 "$PY" -c "from dask.distributed import Client; Client('$DASK_ADDR').close()" >/dev/null 2>&1; }
dask_workers_ready() {
	"$PY" -c "
from dask.distributed import Client
c = Client('$DASK_ADDR')
c.wait_for_workers(2, timeout=75)
n = len(c.scheduler_info()['workers'])
assert n >= 2, n
" >"$logs/dask-ready.log" 2>&1
}

start_dask() {
	if port_ready "$DASK_PORT"; then
		if dask_ready; then DASK_STATE=2; return 0; fi
		echo "note: port $DASK_PORT is taken but is not a dask scheduler; dask rows skipped" >&2
		return 0
	fi
	"$BINDIR/dask" scheduler --host 127.0.0.1 --port "$DASK_PORT" --no-dashboard \
		>"$logs/dask-scheduler.log" 2>&1 &
	DASK_PIDS=$!
	local i=0
	until port_ready "$DASK_PORT" || [ $i -ge "$START_TRIES" ]; do
		i=$((i + 1))
		sleep "$START_WAIT"
	done
	if ! port_ready "$DASK_PORT"; then
		echo "note: local dask scheduler did not start (see $logs/dask-scheduler.log); dask rows skipped" >&2
		return 0
	fi
	"$BINDIR/dask" worker "$DASK_ADDR" --nthreads 2 --no-dashboard \
		--local-directory "$statedir/dw1" >"$logs/dask-worker1.log" 2>&1 &
	DASK_PIDS="$DASK_PIDS $!"
	"$BINDIR/dask" worker "$DASK_ADDR" --nthreads 2 --no-dashboard \
		--local-directory "$statedir/dw2" >"$logs/dask-worker2.log" 2>&1 &
	DASK_PIDS="$DASK_PIDS $!"
	if dask_workers_ready; then
		DASK_STATE=1
		return 0
	fi
	echo "note: local dask workers did not join (see $logs/dask-worker*.log); dask rows skipped" >&2
}

echo "ports_test: starting the local ray head ..."
start_ray
echo "ports_test: starting the local dask scheduler and workers ..."
start_dask
RAY_UP=0
[ "$RAY_STATE" != 0 ] && RAY_UP=1
DASK_UP=0
[ "$DASK_STATE" != 0 ] && DASK_UP=1

# ssh rows need GNU parallel (the lab PCs get it from lab-pc.sh; the driver may
# not) and a working key login to localhost for the 2-node split rows.
WORKS_SSH=1
SSH_SKIP_WHY=""
if ! command -v parallel >/dev/null 2>&1; then
	WORKS_SSH=0
	SSH_SKIP_WHY="parallel not installed (sudo dnf install parallel)"
elif ! ssh -o BatchMode=yes -o ConnectTimeout=3 localhost true >/dev/null 2>&1; then
	WORKS_SSH=0
	SSH_SKIP_WHY="no key login to localhost for the ssh rows"
fi

# Two localhost entries: the driver plays its own two PCs for the split rows,
# and the ports_*.sh row files get the shared bits as PORTS_* env.
hosts2="$workdir/hosts-2.txt"
printf '127.0.0.1\n127.0.0.1\n' >"$hosts2"
PORTS_PY=$PY
PORTS_HOSTS2=$hosts2
PORTS_WORKDIR=$workdir
PORTS_RAY_ADDR=$RAY_ADDR
PORTS_DASK_ADDR=$DASK_ADDR
export PORTS_PY PORTS_HOSTS2 PORTS_WORKDIR PORTS_RAY_ADDR PORTS_DASK_ADDR

# --- row DSL -----------------------------------------------------------------

ROWPASS=0
ROWSKIP=0
ROWFAIL=0
row_pass() { echo "PASS $1"; ROWPASS=$((ROWPASS + 1)); }
row_skip() { echo "SKIP $1: $2"; ROWSKIP=$((ROWSKIP + 1)); }
row_fail() {
	echo "FAIL $1: $2"
	ROWFAIL=$((ROWFAIL + 1))
}

# _errtail <name>: last three stderr lines of a row's port run, for a FAIL msg.
_errtail() {
	tail -3 "$logs/$1.err" 2>/dev/null | sed 's/^/       /'
}

_run() { # <name> <cmd>: stdout captured, stderr to <logs>/<name>.err, rc kept
	timeout "$CASE_TMO" bash -c "$2" 2>"$logs/$1.err"
}

_line() { # <stdout> <prefix>: the one result line
	printf '%s' "$1" | grep -m1 "^$2" || true
}

_norm() { printf '%s' "$1" | sed -E 's/ seconds=[0-9]+\.[0-9]+/ seconds=S/'; }

# Port rows only compare format and the checksum/items: mask the seconds field.
port_expect() {
	local name=$1 orig=$2 port=$3 prefix=$4 out_o out_p rc_o rc_p lo lp
	if ! FILTERED "$name"; then return 0; fi
	out_o=$(_run "$name-o" "$orig")
	rc_o=$?
	out_p=$(_run "$name-p" "$port")
	rc_p=$?
	if [ "$rc_o" != 0 ]; then
		row_fail "$name" "original exit $rc_o"
		_errtail "$name-o"
		return 0
	fi
	if [ "$rc_p" != 0 ]; then
		row_fail "$name" "port exit $rc_p"
		_errtail "$name-p"
		return 0
	fi
	lo=$(_line "$out_o" "$prefix")
	lp=$(_line "$out_p" "$prefix")
	if [ -z "$lo" ]; then
		row_fail "$name" "original prints no $prefix line"
		_errtail "$name-o"
		return 0
	fi
	if [ -z "$lp" ]; then
		row_fail "$name" "port prints no $prefix line"
		_errtail "$name-p"
		return 0
	fi
	lo=$(_norm "$lo")
	lp=$(_norm "$lp")
	if [ "$lo" = "$lp" ]; then
		row_pass "$name"
	elif [ -n "${PORT_TOL:-}" ] && awk -v a="${lo##* }" -v b="${lp##* }" -v t="$PORT_TOL" \
		'BEGIN { d = a - b; if (d < 0) d = -d; exit !(a != "" && b != "" && a + 0 == a && b + 0 == b && d <= t) }'; then
		# the number after the last space may differ by PORT_TOL (workloads.md)
		row_pass "$name"
	else
		row_fail "$name" "result lines differ: original [$lo] port [$lp]"
	fi
}

port_exact() {
	local name=$1 orig=$2 port=$3 line=$4 out_o out_p rc_o rc_p ko kp
	if ! FILTERED "$name"; then return 0; fi
	out_o=$(_run "$name-o" "$orig")
	rc_o=$?
	out_p=$(_run "$name-p" "$port")
	rc_p=$?
	ko=$(printf '%s' "$out_o" | grep -cx -- "$line" || true)
	kp=$(printf '%s' "$out_p" | grep -cx -- "$line" || true)
	if [ "$rc_o" = 0 ] && [ "$ko" = 1 ] && [ "$rc_p" = 0 ] && [ "$kp" = 1 ]; then
		row_pass "$name"
	else
		row_fail "$name" "line [$line] appears orig $ko x (exit $rc_o), port $kp x (exit $rc_p)"
	fi
}

port_fail() {
	local name=$1 cmd=$2 rc
	if ! FILTERED "$name"; then return 0; fi
	_=$(_run "$name-p" "$cmd")
	rc=$?
	if [ "$rc" = 124 ]; then
		row_fail "$name" "still running after ${CASE_TMO}s (hung instead of a fast error)"
		_errtail "$name-p"
	elif [ "$rc" = 0 ]; then
		row_fail "$name" "exited 0 on an unreachable address"
		_errtail "$name-p"
	else
		row_pass "$name"
	fi
}

# Subsystem-guarded wrappers: the signatures match their base helpers.
port_ray_case() {
	if [ "$RAY_UP" = 1 ]; then port_expect "$1" "$2" "$3" "$4"; else row_skip "$1" "local ray cluster not up"; fi
}
port_dask_case() {
	if [ "$DASK_UP" = 1 ]; then port_expect "$1" "$2" "$3" "$4"; else row_skip "$1" "local dask cluster not up"; fi
}
port_fail_ray_case() {
	if [ "$RAY_UP" = 1 ]; then port_fail "$1" "$2"; else row_skip "$1" "local ray cluster not up"; fi
}
port_fail_dask_case() {
	if [ "$DASK_UP" = 1 ]; then port_fail "$1" "$2"; else row_skip "$1" "local dask cluster not up"; fi
}
port_ssh_case() {
	if [ "$WORKS_SSH" = 1 ]; then port_expect "$1" "$2" "$3" "$4"; else row_skip "$1" "$SSH_SKIP_WHY"; fi
}
port_exact_ssh_case() {
	if [ "$WORKS_SSH" = 1 ]; then port_exact "$1" "$2" "$3" "$4"; else row_skip "$1" "$SSH_SKIP_WHY"; fi
}

# --- rows -------------------------------------------------------------------

cd "$workdir" || exit 1
shopt -s nullglob
# Each ports_*.sh is rows only, in the workload order of its unit; the runner
# owns the DSL above and the clusters.
for f in "$here"/ports_w[0-9]*.sh; do
	source "$f"
done
shopt -u nullglob

total=$((ROWPASS + ROWSKIP + ROWFAIL))
echo "PORTS rows=$total pass=$ROWPASS skip=$ROWSKIP fail=$ROWFAIL"
if [ "$ROWFAIL" -gt 0 ]; then
	KEEP=1
	exit 1
fi
exit 0
