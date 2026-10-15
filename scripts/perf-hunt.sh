#!/usr/bin/env bash
# PERF-HUNT profiler: runs one workload on an emulated lab (scripts/lab-emu.sh,
# LAB_NAME=perf by default) with py-spy on the job's Python processes (node 1
# submitter, node 2 pool workers) and a pprof CPU profile from both daemons.
#
#   scripts/perf-hunt.sh pprof-on                 restart every daemon with PIPEDPEER_PPROF=6060
#   scripts/perf-hunt.sh case <name> <script> [run flags...]
#       <script> is a path on the host; it is copied to /bench on node 1.
#       Output: $PH_OUT/<name>/{run.log,pyspy-n1.txt,pyspy-n2.txt,cpu-n1.pprof,cpu-n2.pprof}
#
# Knobs: LAB_NAME (perf), PH_OUT (~/.local/state/pipedpeer/perf-hunt),
#        PY_SPY (py-spy binary on the host, static; `uvx py-spy` fetches one),
#        PH_SECS (pprof seconds, default 20), PH_LOCK (1: take the bench lock).
# Timed numbers are only comparable under the bench lock (PH_LOCK=1).
set -euo pipefail

rt="${LAB_RT:-docker}"
prefix="pp${LAB_NAME:-perf}"
out_root="${PH_OUT:-${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/perf-hunt}"
secs="${PH_SECS:-20}"

ex() { local i="$1"; shift; "$rt" exec "$prefix-$i" sh -c "$*"; }

# Every node of this lab, by container name.
nodes() { "$rt" ps --format '{{.Names}}' | grep -E "^$prefix-[0-9]+$" | sed "s/^$prefix-//" | sort -n; }

# PIDs whose command line matches a pattern (no ps/pgrep in the image).
pids() {
	ex "$1" 'for p in /proc/[0-9]*; do c=$(tr "\0" " " 2>/dev/null < $p/cmdline); case "$c" in *"'"$2"'"*) case "$c" in *"for p in"*) ;; *) echo ${p#/proc/};; esac;; esac; done'
}

pprof_on() {
	for i in $(nodes); do
		for p in $(pids "$i" "pipedpeer __daemon__"); do ex "$i" "kill $p"; done
		sleep 1
		"$rt" exec -d -e PIPEDPEER_PPROF=6060 "$prefix-$i" sh -c \
			'exec /usr/local/bin/pipedpeer __daemon__ --port "${LAB_API_PORT:-4242}" >/var/log/pp-daemon.log 2>&1'
	done
	sleep 8
	for i in $(nodes); do
		echo "node $i pprof: $(ex "$i" 'curl -s -o /dev/null -w %{http_code} http://127.0.0.1:6060/debug/pprof/')"
	done
}

# spy <node> <pattern> <file>: wait for a Python process matching pattern, record it.
spy() {
	local i="$1" pat="$2" file="$3" done="$4" pid=""
	while [[ -z "$pid" && ! -e "$done" ]]; do
		pid="$(pids "$i" "$pat" 2>/dev/null | head -1)"
		[[ -n "$pid" ]] || sleep 0.2
	done
	[[ -n "$pid" ]] || { echo "perf-hunt: no process matching '$pat' on node $i" >&2; return 0; }
	ex "$i" "/usr/local/bin/py-spy record -p $pid --subprocesses --nonblocking -r 200 -f raw -o /tmp/pyspy.txt" \
		>"$file.log" 2>&1 || echo "perf-hunt: py-spy on node $i exited non-zero (see $file.log)" >&2
	"$rt" cp "$prefix-$i:/tmp/pyspy.txt" "$file" 2>/dev/null || echo "perf-hunt: no py-spy output from node $i" >&2
}

run_case() {
	local name="$1" script="$2"
	shift 2
	local d="$out_root/$name" base
	base="$(basename "$script")"
	mkdir -p "$d"
	rm -f "$d/.done"
	for i in $(nodes); do
		"$rt" cp "${PY_SPY:?set PY_SPY to a static py-spy binary}" "$prefix-$i:/usr/local/bin/py-spy" >/dev/null
	done
	ex 1 'mkdir -p /bench'
	"$rt" cp "$script" "$prefix-1:/bench/$base" >/dev/null
	spy 1 "/bin/python3 $base" "$d/pyspy-n1.txt" "$d/.done" &
	local s1=$!
	spy 2 "warm_worker.py" "$d/pyspy-n2.txt" "$d/.done" &
	local s2=$!
	for i in 1 2; do
		ex "$i" "curl -s -o /tmp/cpu.pprof 'http://127.0.0.1:6060/debug/pprof/profile?seconds=$secs'" &
	done
	local t0 t1
	t0="$(date +%s.%N)"
	"$rt" exec -w /bench "$prefix-1" pipedpeer run "$@" "$base" >"$d/run.log" 2>&1 || echo "perf-hunt: run failed (see $d/run.log)" >&2
	t1="$(date +%s.%N)"
	echo "wall $(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.3f", b - a }') s" | tee -a "$d/run.log"
	touch "$d/.done"
	# The warm worker on node 2 outlives the run: stop py-spy so it writes its file.
	for i in 1 2; do for p in $(pids "$i" "py-spy record"); do ex "$i" "kill -INT $p" || true; done; done
	wait
	for i in 1 2; do "$rt" cp "$prefix-$i:/tmp/cpu.pprof" "$d/cpu-n$i.pprof" 2>/dev/null || echo "perf-hunt: no pprof from node $i" >&2; done
	wait "$s1" "$s2" 2>/dev/null || true
}

case "${1:-}" in
pprof-on) pprof_on ;;
case)
	shift
	if [[ "${PH_LOCK:-0}" == 1 ]]; then
		exec 9>"$HOME/.cache/pipedpeer-bench.lock"
		echo "perf-hunt: waiting for the bench lock..."
		flock 9
	fi
	run_case "$@"
	;;
*)
	sed -n '2,15p' "$0"
	exit 2
	;;
esac
