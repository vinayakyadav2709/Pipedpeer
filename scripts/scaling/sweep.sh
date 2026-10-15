#!/usr/bin/env bash
# M2 scaling map, data path: one variable at a time with the rest fixed, on
# the emulated lab (scripts/lab-emu.sh via scripts/bench-lib.sh). Each run's
# steps.json (C24 run record) lands beside its log; scripts/scaling/fit.py
# fits log-log growth per step and flags superlinear steps.
#
# Every number is EMULATED: equal cgroup quotas plus tc netem on one host.
#
#   scripts/scaling/sweep.sh [variable...]   # default: all
#   scripts/scaling/sweep.sh table           # fit + tables (fit.py)
#
# Variables: payload items cost profile loss nodes spread cold
#
# Knobs: M2_BIN (default: built from `git archive HEAD`, CGO_ENABLED=0),
# BL_REPS (3), M2_MEM (1g per node), M2_NODES ("1 2 3 4"), M2_NODE_MEM (768m),
# M2_PAYLOADS_KB (total payload, "1 16 256 4096 65536 262144").
# Fits and flags use CPU time (cgroup usage of every container, cpu.csv), not
# wall time: parallel agents load the host. Wall time is kept with the host
# load beside it; paper wall times come from a quiet rerun of this script
# (all variables, fresh BL_OUT) on an idle host.
# Labs are LAB_NAME=m2 (containers ppm2-<i>); the bench lock is held throughout.
set -euo pipefail
export BL_LAB_NAME="${BL_LAB_NAME:-m2}"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/bench-lib.sh"

if [[ "${1:-}" == table ]]; then
	exec uvx -q --with numpy --with matplotlib python "$BL_ROOT/scripts/scaling/fit.py" "$BL_OUT/m2/results.csv" "$BL_ROOT/docs/benchmarks/scaling"
fi
vars=("$@")
((${#vars[@]})) || vars=(payload items cost profile loss nodes spread cold)

mem="${M2_MEM:-1g}"
# The fixed point every sweep varies away from.
# ~75 ms per item, 4.8 CPU-s in all: enough that auto mode distributes it
# (100000 was kept local, so links and peers went unmeasured).
BASE_ITEMS=64 BASE_SPIN=1000000 BASE_IN_KB=256

# m2_bin: the binary under test, from the object store (git archive), so other
# agents' uncommitted edits in this tree never leak into the numbers.
m2_bin() {
	BL_REV="$(git -C "$BL_ROOT" rev-parse --short HEAD)"
	BL_BIN="${M2_BIN:-$BL_BINDIR/pipedpeer-m2-$BL_REV}"
	# A pinned binary keeps its own rev in the rows (pipedpeer-m2-<rev>).
	[[ -n "${M2_BIN:-}" && "$(basename "$M2_BIN")" == pipedpeer-m2-* ]] && BL_REV="${M2_BIN##*pipedpeer-m2-}"
	BL_API_PORT=4242
	[[ -x "$BL_BIN" ]] && return 0
	local src="$BL_BINDIR/../src-m2-$BL_REV"
	mkdir -p "$BL_BINDIR"
	rm -rf "$src" && mkdir -p "$src"
	git -C "$BL_ROOT" archive HEAD src | tar -x -C "$src"
	bl_log "building $BL_BIN from HEAD $BL_REV"
	(cd "$src/src" && CGO_ENABLED=0 systemd-run --user --scope -q --slice=pipedpeer-agents.slice \
		go build -o "$BL_BIN" .) >&2 || bl_die "build failed"
}

# The kernel is a plain module-level def in __main__: the shim ships it as
# source (func_src), not by cloudpickle. Items carry their own payload, so the
# send path scales with BENCH_IN_KB; compute does not read the bytes.
m2_workload() {
	local d="$1"
	mkdir -p "$d"
	: > "$d/.pipedpeerignore"
	cat > "$d/m2pool.py" <<'PY'
import multiprocessing, os, time

ITEMS = int(os.environ.get("BENCH_ITEMS", "64"))
SPIN = int(os.environ.get("BENCH_SPIN", "100000"))
IN_B = int(os.environ.get("BENCH_IN_B", "0")) or 1024 * int(os.environ.get("BENCH_IN_KB", "0"))
OUT_KB = int(os.environ.get("BENCH_OUT_KB", "0"))


def work(item):
    x, payload = item
    total = len(payload)
    for i in range(SPIN):
        total += (i ^ x) % 7
    return total, os.urandom(OUT_KB * 1024) if OUT_KB else b""


if __name__ == "__main__":
    items = [(x, os.urandom(IN_B)) for x in range(ITEMS)]
    t0 = time.monotonic()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        out = pool.map(work, items)
    print("BENCH_ELAPSED %.3f" % (time.monotonic() - t0))
    print("BENCH_CHECK", sum(t for t, _ in out))
PY
}

# m2_lab_up <nodes> <mem> <cpus list>: bl_lab_up with a CPU list (bl_lab_up
# pins LAB_CPUS=2; the spread sweep needs uneven quotas). Waits for the memory
# cap like bl_lab_up does.
m2_lab_up() {
	bl_lab_down
	local out="$BL_OUT/$BL_SUITE/lab-up.log" waited=0
	bl_log "lab up: $1 x ($3 cpu, $2), lan, $(basename "$BL_BIN")"
	until LAB_MEM="$2" LAB_PROFILE=lan LAB_LOSS=0 LAB_CPUS="$3" PIPEDPEER="$BL_BIN" \
		LAB_API_PORT="$BL_API_PORT" LAB_NAME="$BL_LAB_NAME" "$BL_EMU" up "$1" > "$out" 2>&1; do
		cat "$out" >&2
		grep -Eq "refusing to start|gave up after|waiting for memory" "$out" || bl_die "lab-emu.sh up failed"
		((waited < ${BL_LAB_WAIT:-3600})) || bl_die "memory cap still full after ${waited}s"
		bl_log "memory cap full (other labs running); retrying in 60 s (waited ${waited}s)"
		sleep 60
		waited=$((waited + 60))
	done
	BL_NODES_UP="$1" BL_PROFILE=lan BL_LOSS=0
}

work="$BL_OUT/m2/work"
lab_n=0 lab_mem="" lab_cpus=""
# up <nodes> <mem> [cpus list]: (re)start the m2 lab only when its shape changes.
up() {
	local cpus="${3:-2}"
	if [[ "$lab_n" == "$1" && "$lab_mem" == "$2" && "$lab_cpus" == "$cpus" && "${4:-}" != fresh ]]; then
		bl_lab_shape lan 0
		return 0
	fi
	m2_lab_up "$1" "$2" "$cpus"
	lab_n="$1" lab_mem="$2" lab_cpus="$cpus"
	bl_put 1 "$work"
	[[ "${4:-}" == fresh ]] || warm

}
# warm: build and ship the environment untimed. A cold nix build on a fresh
# lab can take far longer than one run's timeout, so it gets M2_WARM_TIMEOUT.
warm() {
	local log="$BL_OUT/m2/logs/warm_$(date +%s).log"
	bl_log "warming the environment (up to ${M2_WARM_TIMEOUT:-2400} s)"
	timeout "${M2_WARM_TIMEOUT:-2400}" "$BL_RT" exec -w /bench "${BL_EMU_PREFIX}1" \
		pipedpeer run -e BENCH_ITEMS=4 -e BENCH_SPIN=10 m2pool.py > "$log" 2>&1 ||
		bl_log "warning: warm-up failed or timed out ($log); timed runs will show it"
}
# cell <var> <value> [flags...]: BL_REPS timed runs of m2pool.py.
# cpu_usec: summed cgroup CPU time (usage_usec) of every lab container, one
# per line, node 1 first. Covers daemon, shim and pool workers alike; unlike
# wall time it is not inflated by a loaded host.
cpu_usec() {
	local i
	for ((i = 1; i <= BL_NODES_UP; i++)); do
		# the image has no awk: plain sh
		bl_exec "$i" sh -c 'while read -r k v; do [ "$k" = usage_usec ] && echo "$v"; done < /sys/fs/cgroup/cpu.stat; true' ||
			{ bl_log "warning: no cgroup cpu.stat in node $i; its CPU counts as 0"; echo 0; }
	done
}
# run1 <var> <value> <rep> [flags...]: one bl_run plus a side row in cpu.csv
# (CPU seconds: total, submitter, peers; host 1-min load before and after),
# joined to results.csv by the log path.
run1() {
	local var="$1" val="$2" rep="$3" before after load0 rc=0
	shift 3
	# bl_run takes and drops the bench lock per run (fd 9). Holding fd 9
	# here first keeps the CPU reads inside the locked window: bl_run's
	# flock on the same descriptor succeeds at once; it unlocks on return,
	# so lock again for the after-read.
	flock 9
	mapfile -t before < <(cpu_usec)
	load0="$(cut -d' ' -f1 /proc/loadavg)"
	bl_run "$var" "$val" m2 "$rep" m2pool.py "$@" || rc=1
	flock 9
	mapfile -t after < <(cpu_usec)
	flock -u 9
	python3 - "$(tail -1 "$BL_CSV" | cut -d, -f20)" "$load0" "$(cut -d' ' -f1 /proc/loadavg)" \
		"${before[*]}" "${after[*]}" >> "$M2_CPU_CSV" <<'PY'
import sys
log, l0, l1, b, a = sys.argv[1:]
d = [(int(y) - int(x)) / 1e6 for x, y in zip(b.split(), a.split())]
print("%s,%.3f,%.3f,%.3f,%s,%s" % (log, sum(d), d[0] if d else 0, sum(d[1:]), l0, l1))
PY
	return "$rc"
}
# warm: build and ship the environment untimed. A cold nix build on a fresh
# lab can take far longer than one run's timeout, so it gets M2_WARM_TIMEOUT.
warm() {
	local log="$BL_OUT/m2/logs/warm_$(date +%s).log"
	bl_log "warming the environment (up to ${M2_WARM_TIMEOUT:-2400} s)"
	timeout "${M2_WARM_TIMEOUT:-2400}" "$BL_RT" exec -w /bench "${BL_EMU_PREFIX}1" \
		pipedpeer run -e BENCH_ITEMS=4 -e BENCH_SPIN=10 m2pool.py > "$log" 2>&1 ||
		bl_log "warning: warm-up failed or timed out ($log); timed runs will show it"
}
# cell <var> <value> [flags...]: BL_REPS timed runs of m2pool.py; the first
# failure ends the cell (as bl_reps does).
cell() {
	local var="$1" val="$2" r
	shift 2
	for r in $(seq 1 "$BL_REPS"); do
		run1 "$var" "$val" "$r" "$@" || { bl_log "$var=$val failed on $BL_REV; skipping its remaining reps"; return 0; }
	done
}
base() { echo "-e BENCH_ITEMS=${1:-$BASE_ITEMS} -e BENCH_SPIN=${2:-$BASE_SPIN} -e BENCH_IN_KB=${3:-$BASE_IN_KB}"; }

bl_init m2
M2_CPU_CSV="$BL_OUT/m2/cpu.csv"
[[ -f "$M2_CPU_CSV" ]] || echo "log,cpu_s,cpu_submitter_s,cpu_peers_s,load1_before,load1_after" > "$M2_CPU_CSV"
m2_bin
m2_workload "$work"
bl_log "M2 sweep: ${vars[*]} on $BL_BIN ($BL_REV), $BL_REPS reps, emulated"

# shellcheck disable=SC2046 # base() is a word list on purpose
for v in "${vars[@]}"; do
	case "$v" in
	payload) # total bytes in over 16 items; send/pickle/receive should grow ~1.
		# Forced across nodes: the compute is tiny on purpose, auto keeps it local.
		up 3 "$mem"
		for kb in ${M2_PAYLOADS_KB:-1 16 256 4096 65536 262144}; do
			cell payload "$((kb * 1024))" $(base 16 20000 0) -e BENCH_IN_B=$((kb * 64)) -e PIPEDPEER_DISTRIBUTE=force
		done ;;
	items) # item count, fixed per-item cost (~15 ms) and size
		up 3 "$mem"
		for n in 8 32 128 512 2048; do cell items "$n" $(base "$n" 200000 4); done ;;
	cost) # per-item cost (loop iterations), fixed items
		up 3 "$mem"
		for s in 1000 10000 100000 1000000 3000000; do cell cost "$s" $(base 32 "$s" 4); done ;;
	profile) # link profiles; categorical
		up 3 "$mem"
		for p in lan wifi 5g; do
			bl_lab_shape "$p" 0
			cell profile "$p" $(base)
		done
		bl_lab_shape lan 0 ;;
	loss) # packet loss on every link, lan profile
		up 3 "$mem"
		for l in 0 1 2 5; do
			bl_lab_shape lan "$l"
			cell loss "$l" $(base)
		done
		bl_lab_shape lan 0 ;;
	nodes) # equal containers; speedup = 1 vs N
		for n in ${M2_NODES:-1 2 3 4}; do
			up "$n" "${M2_NODE_MEM:-768m}"
			cell nodes "$n" $(base 96 300000 64)
		done ;;
	spread) # node speed spread (cgroup cpu quota), value = fastest/slowest
		for c in "2 2 2" "2 2 1" "2 2 0.5" "2 2 0.2"; do
			up 3 "$mem" "$c"
			cell spread "$(awk -v c="$c" 'BEGIN { n = split(c, a, " "); mx = a[1]; mn = a[1]; for (i = 2; i <= n; i++) { if (a[i] > mx) mx = a[i]; if (a[i] < mn) mn = a[i] }; printf "%g", mx / mn }')" $(base)
		done ;;
	cold) # cold = fresh lab, nothing built; warm = the run after it
		for r in $(seq 1 "$BL_REPS"); do
			up 3 "$mem" 2 fresh
			run1 cold cold "$r" $(base) || true
			run1 cold warm "$r" $(base) || true
		done
		lab_n=0 ;;
	*) bl_die "unknown variable $v" ;;
	esac
done
bl_lab_down
bl_log "done; rows in $BL_CSV; run: scripts/scaling/sweep.sh table"
