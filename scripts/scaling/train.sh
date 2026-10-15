#!/usr/bin/env bash
# Scaling map, training (M3): how the DDP sync cost and the per-step compute
# grow with one variable at a time, the rest held at the baseline.
#
#   scripts/scaling/train.sh run [sweep...]   sweeps: size batch interval int8 ranks (default: all)
#   scripts/scaling/train.sh raw              control: train.py with no pipedpeer in node 1 (lab left up by M3_KEEP=1)
#   scripts/scaling/train.sh gpu              single-GPU baselines on this machine's real GPU (BL_GPU_PYTHON)
#   scripts/scaling/train.sh fit [plot-dir]   log-log fits, superlinear flags, markdown tables (+ PNG plots)
#
# Baseline: CPU-training stand-in (bench-lib.sh train.py), width 512 depth 2,
# batch 1024, sync every step, int8 on, 1 rank per node, 2 nodes, netem wifi.
# Sweeps (each alone):
#   size      width 128..2048 (0.07M..8.9M params) under lan, wifi and 5g
#   batch     256..8192 rows per step (the script's batch; the ring splits it)
#   interval  PIPEDPEER_DDP_SYNC_EVERY 1..16
#   int8      PIPEDPEER_DDP_INT8=0 vs on, at width 512 and 2048
#   ranks     ranks per node 1..3 (GPUs per node emulated by ranks: each rank
#             gets 2 CPUs of its own, raised in place with `docker update`;
#             PIPEDPEER_DDP_RANKS_PER_NODE on the CLI)
# Per run it reads every steps.json the run wrote (C24 ddp_sync steps: rounds,
# seconds, bytes per round); DDP ranks save no run record today, so it falls
# back to each rank's "ddp: N sync(s)..." line (PIPEDPEER_VERBOSE=1) and says
# which in the row's source column. Compute per step = (script elapsed of the
# last rank to start - mean sync seconds) / steps. Each row also has the host's
# 1-minute load and cpu_s, the CPU seconds every node used (container cgroups):
# the growth fits use CPU per step, which other agents' load barely moves;
# wall times are kept with their load and only reported.
#
# Every number is EMULATED (equal cgroup quotas + netem on one host) except
# `gpu`, which times train.py on the real GPU with no pipedpeer at all.
#
# Knobs: M3_BIN (prebuilt binary; default: build this checkout), PROFILE (wifi),
# SIZE_PROFILES ("lan wifi 5g"), ONLY (globs over sweep:profile:x, e.g.
# "size:wifi:* raw-size:none:*": only those cells; the quiet rerun of the paper
# rows uses it with BL_OUT=<fresh dir>),
# STEPS (60), TRAIN_MEM (2g), BL_REPS (3), BL_LAB_NAME (m3), M3_KEEP=1 (leave
# the lab up at exit), M3_REUSE=1 (start on a running lab, environment built),
# M3_WARM_TIMEOUT (3600 s: a cold torch environment is a ~4 GB download).
set -euo pipefail
export BL_LAB_NAME="${BL_LAB_NAME:-m3}" LAB_MEM_WAIT="${LAB_MEM_WAIT:-3600}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$here/../bench-lib.sh"

profile="${PROFILE:-wifi}"
steps="${STEPS:-60}"
mem="${TRAIN_MEM:-2g}"
out="$BL_OUT/scaling-train"
csv="$out/runs.csv"

# node_cpu: CPU seconds every lab node has used so far (each container's own
# cgroup cpu.stat: daemon + ranks + CLI), space-separated. CPU time is what the
# fits use: other agents' load stretches wall time, not the CPU a run needs.
node_cpu() {
	local i
	for ((i = 1; i <= BL_NODES_UP; i++)); do
		bl_exec "$i" sh -c 'while read -r k v; do [ "$k" = usage_usec ] && echo "$v"; done </sys/fs/cgroup/cpu.stat; true'
	done | tr '\n' ' '
}

# m3_want <key>: whether ONLY (space-separated globs over sweep:profile:x, for
# the quiet rerun of the paper rows) lets this cell run. Empty ONLY = all.
m3_want() {
	[[ -z "${ONLY:-}" ]] && return 0
	local pat
	for pat in $ONLY; do
		# shellcheck disable=SC2053 # glob match on purpose
		[[ "$1" == $pat ]] && return 0
	done
	return 1
}

# m3_run <sweep> <x> <rep> <cli-env> [run flags...] - one timed run on node 1,
# one CSV row. <cli-env> is "K=V ..." for the pipedpeer CLI itself (not the job).
m3_run() {
	local sweep="$1" x="$2" rep="$3" cenv="$4" rc=0 status=ok t0 t1
	shift 4
	local log="$out/logs/${sweep}_${x}_${BL_PROFILE}_r${rep}.log"
	local -a eargs=()
	for kv in $cenv; do eargs+=(-e "$kv"); done
	bl_exec 1 touch /bench/.mark
	flock 9 # the bench lock, per timed run (bench-lib opened fd 9)
	local load cpu0 cpu1
	read -r load _ </proc/loadavg
	cpu0="$(node_cpu)"
	t0="$(date +%s.%N)"
	timeout "$BL_RUN_TIMEOUT" "$BL_RT" exec "${eargs[@]}" -w /bench "${BL_EMU_PREFIX}1" \
		pipedpeer run --mem "${M3_JOBMEM:-1G}" "$@" train.py >"$log" 2>&1 || rc=$?
	t1="$(date +%s.%N)"
	cpu1="$(node_cpu)"
	flock -u 9
	((rc == 124)) && status=timeout
	((rc != 0 && rc != 124)) && status=fail
	# Every run record this run wrote: one per rank, all on the submitter.
	bl_exec 1 sh -c 'find / -xdev \( -path /nix -o -path /proc -o -path /bench \) -prune -o -name steps.json -newer /bench/.mark -print 2>/dev/null | tar -cf - -T - 2>/dev/null' \
		>"$log.steps.tar" || bl_log "warning: could not collect run records for $log"
	python3 - "$log" "$status" "$t0" "$t1" "$sweep" "$x" "$rep" "$BL_PROFILE" "$cenv $*" "$BL_REV" "$load" "$cpu0" "$cpu1" >>"$csv" <<'PY'
import csv, json, re, statistics, sys, tarfile, time
log, status, t0, t1, sweep, x, rep, prof, flags, rev, load, cpu0, cpu1 = sys.argv[1:]
cpu = sum(int(b) - int(a) for a, b in zip(cpu0.split(), cpu1.split())) / 1e6
text = open(log, errors="replace").read()
last = lambda p: (re.findall(p, text) or [""])[-1]
# One BENCH block per rank; ranks run in lock step after the start barrier,
# so the rank that started last (smallest elapsed) is the training time.
els = [float(v) for v in re.findall(r"BENCH_ELAPSED ([0-9.]+)", text)]
stp = [float(v) for v in re.findall(r"BENCH_STEP_S ([0-9.]+)", text)]
elapsed, nsteps = ("%.3f" % min(els) if els else ""), last(r"BENCH_STEPS_DONE (\d+)")
if status == "ok" and not elapsed:
    status = "noresult"
seen, per_rank = set(), []
try:
    with tarfile.open(log + ".steps.tar") as tf:
        for m in tf.getmembers():
            rec = json.load(tf.extractfile(m))
            mine = []
            for s in rec.get("steps") or rec.get("Steps") or []:
                key = (s.get("machine"), s.get("start_ns"), s.get("dur_ns"))
                if s.get("step") == "ddp_sync" and key not in seen:
                    seen.add(key)
                    mine.append(s)
            if mine:
                per_rank.append(mine)
except (tarfile.TarError, OSError, ValueError) as e:
    print("warning: run records of %s unreadable: %s" % (log, e), file=sys.stderr)
source = "steps.json"
if per_rank:
    allsteps = [s for r in per_rank for s in r]
    rounds = statistics.mean(len(r) for r in per_rank)
    sync_round = statistics.median(s["dur_ns"] for s in allsteps) / 1e9
    bytes_round = statistics.median(s.get("bytes", 0) for s in allsteps)
    sync_total = statistics.mean(sum(s["dur_ns"] for s in r) for r in per_rank) / 1e9
else:
    # No record (DDP ranks save none today): each rank's verbose summary line
    # (PIPEDPEER_VERBOSE=1), averaged over ranks; bytes include the opening
    # weight broadcast, which is not a sync round.
    source = "shim-line"
    m = [(int(a), float(b), float(c), float(d) + float(e)) for a, b, c, d, e in re.findall(
        r"ddp: (\d+) sync\(s\), ([0-9.]+)s total \(([0-9.]+) ms each\), ([0-9.]+) MiB sent, ([0-9.]+) MiB received", text)]
    if m:
        rounds = statistics.mean(r[0] for r in m)
        sync_round = statistics.mean(r[2] for r in m) / 1000
        # rounds x ms each: the line's own total is rounded to 0.1 s
        sync_total = statistics.mean(r[0] * r[2] for r in m) / 1000
        bytes_round = statistics.mean(r[3] * 1048576 / r[0] for r in m)
    else:
        rounds = sync_total = sync_round = bytes_round = 0
        if status == "ok":
            print("warning: %s has neither ddp_sync steps nor ddp summary lines" % log, file=sys.stderr)
comp = ""
if elapsed and nsteps and int(nsteps):
    comp = "%.5f" % max(0.0, (float(elapsed) - sync_total) / int(nsteps))
worlds = [int(w) for w in re.findall(r"BENCH_WORLD (\d+)", text)]
csv.writer(sys.stdout).writerow([
    sweep, x, prof, rep, status, "%.3f" % (float(t1) - float(t0)), elapsed, nsteps,
    last(r"BENCH_PARAMS (\d+)"), max(worlds) if worlds else "", "%.1f" % rounds, "%.5f" % sync_round,
    "%d" % bytes_round, "%.3f" % sync_total, comp, "%.5f" % min(stp) if stp else "", source,
    flags.strip(), rev, 1, time.strftime("%Y-%m-%dT%H:%M:%S"), log, load, "%.3f" % cpu])
PY
	[[ "$(tail -1 "$csv" | cut -d, -f5)" == ok ]] && return 0
	bl_log "run failed: $sweep x=$x - see $log"
	return 1
}

# cell <sweep> <x> <cli-env> [flags...]: BL_REPS reps; the first failure ends the cell.
cell() {
	local r
	m3_want "$1:$BL_PROFILE:$2" || return 0
	for r in $(seq 1 "$BL_REPS"); do
		m3_run "$1" "$2" "$r" "$3" "${@:4}" || return 0
	done
	# One CPU calibration run of a third of the steps: CPU per step is the
	# difference over the extra steps, so start-up (python, torch import,
	# sandbox, opening broadcast) cancels out.
	local -a short=("${@:4}")
	short=("${short[@]/#BENCH_STEPS=*/BENCH_STEPS=$((steps / 3))}")
	m3_run "$1" "$2" c "$3" "${short[@]}" || true
}

base=(-e BENCH_WIDTH=512 -e BENCH_DEPTH=2 -e BENCH_BATCH=1024 -e BENCH_STEPS="$steps" -e PIPEDPEER_VERBOSE=1)
with() { # with K=V... : the baseline job env with the given keys replaced
	local -a a=("${base[@]}")
	for kv in "$@"; do
		local i
		for ((i = 1; i < ${#a[@]}; i += 2)); do [[ "${a[i]%%=*}" == "${kv%%=*}" ]] && a[i]="$kv" && continue 2; done
		a+=(-e "$kv")
	done
	printf '%s\n' "${a[@]}"
}

up() { # up <nodes> <cpus> <mem> <profile>
	# bl_lab_up pins 2 CPUs per node; the ranks sweep needs 2 per rank, so
	# this is bl_lab_up with LAB_CPUS free (same refuse-and-retry loop).
	local log="$out/lab-up.log" waited=0
	# M3_REUSE=1 keeps a running lab of the right size: its environment is
	# already built (a cold torch build here is a ~4 GB download).
	if [[ "${M3_REUSE:-0}" == 1 && "$("$BL_RT" ps -q --filter "label=pipedpeer.lab=$BL_LAB_NAME" | wc -l)" == "$1" ]]; then
		bl_log "reusing the running $BL_LAB_NAME lab ($1 nodes)"
		BL_NODES_UP="$1" BL_PROFILE="" BL_LOSS=0
		bl_lab_shape "$4" 0
		bl_put 1 "$work"
		m3_warm "$1"
		return 0
	fi
	bl_lab_down
	bl_log "lab up: $1 x ($2 cpu, $3), profile $4, $(basename "$bin")"
	until LAB_MEM="$3" LAB_PROFILE="$4" LAB_LOSS=0 LAB_CPUS="$2" PIPEDPEER="$bin" \
		LAB_API_PORT="$BL_API_PORT" LAB_NAME="$BL_LAB_NAME" "$BL_EMU" up "$1" >"$log" 2>&1; do
		cat "$log" >&2
		grep -q "refusing to start" "$log" || bl_die "lab-emu.sh up failed"
		((waited < ${BL_LAB_WAIT:-3600})) || bl_die "memory cap still full after ${waited}s"
		bl_log "memory cap full (other labs running); retrying in 60 s (waited ${waited}s)"
		sleep 60
		waited=$((waited + 60))
	done
	BL_NODES_UP="$1" BL_PROFILE="$4" BL_LOSS=0
	bl_put 1 "$work"
	m3_warm "$1"
}

# m3_warm <ranks>: one untimed run so the environment is built and shipped
# before anything is timed. Its own long timeout: a cold torch build takes
# longer than BL_RUN_TIMEOUT, and a timed run must never pay for it.
m3_warm() {
	local l="$out/logs/warm_$(date +%s).log"
	bl_log "warming the environment (log $l)"
	timeout "${M3_WARM_TIMEOUT:-3600}" "$BL_RT" exec -w /bench "${BL_EMU_PREFIX}1" \
		pipedpeer run --mem "${M3_JOBMEM:-1G}" --ddp "$1" -e BENCH_STEPS=1 train.py >"$l" 2>&1 ||
		bl_die "warm-up failed; see $l"
}

# cpus <n>: give every running node n CPUs in place (docker/podman update),
# so the ranks sweep keeps the built environment instead of a cold re-up.
cpus() {
	local i
	for ((i = 1; i <= BL_NODES_UP; i++)); do
		"$BL_RT" update --cpus "$1" "${BL_EMU_PREFIX}$i" >/dev/null || bl_die "cannot set $1 CPUs on ${BL_EMU_PREFIX}$i"
	done
	bl_log "every node now has $1 CPUs"
}

sweep_size() {
	local p w
	for p in ${SIZE_PROFILES:-lan wifi 5g}; do
		bl_lab_shape "$p" 0
		for w in 128 256 512 1024 2048; do
			mapfile -t e < <(with BENCH_WIDTH=$w)
			cell size "$w" "" --ddp 2 "${e[@]}"
		done
	done
	bl_lab_shape "$profile" 0
}
sweep_batch() {
	local b
	for b in 256 1024 4096 8192; do
		mapfile -t e < <(with BENCH_BATCH=$b)
		cell batch "$b" "" --ddp 2 "${e[@]}"
	done
}
sweep_interval() {
	local h
	for h in 1 2 4 8 16; do
		mapfile -t e < <(with PIPEDPEER_DDP_SYNC_EVERY=$h)
		cell interval "$h" "" --ddp 2 "${e[@]}"
	done
}
sweep_int8() {
	local w q
	for w in 512 2048; do
		for q in 1 0; do
			mapfile -t e < <(with BENCH_WIDTH=$w PIPEDPEER_DDP_INT8=$q)
			cell "int8-$q" "$w" "" --ddp 2 "${e[@]}"
		done
	done
}

case "${1:-}" in
run)
	shift
	sweeps=("$@")
	((${#sweeps[@]})) || sweeps=(size batch interval int8 ranks)
	bl_init scaling-train
	[[ "${M3_KEEP:-0}" == 1 ]] && trap - EXIT
	mkdir -p "$out/logs"
	[[ -f "$csv" ]] || echo "sweep,x,profile,rep,status,wall_s,elapsed_s,steps,params,world,rounds,sync_round_s,bytes_round,sync_total_s,compute_step_s,step_s,source,flags,rev,emulated,when,log,load1,cpu_s" >"$csv"
	if [[ -n "${M3_BIN:-}" ]]; then
		bin="$M3_BIN" BL_REV="${M3_REV:-$(basename "$M3_BIN")}" BL_API_PORT=4242
	else
		bl_build after
		bin="$BL_BIN"
	fi
	work="$out/work"
	bl_workloads "$work"
	two=0
	for s in "${sweeps[@]}"; do
		case "$s" in
		size | batch | interval | int8)
			((two)) || { up 2 2 "$mem" "$profile"; two=1; }
			bl_lab_shape "$profile" 0
			"sweep_$s"
			;;
		ranks)
			# Each rank gets 2 CPUs of its own (CPUs raised in place, same
			# 2g nodes, 512M per rank so three ranks fit one node).
			((two)) || { up 2 2 "$mem" "$profile"; two=1; }
			bl_lab_shape "$profile" 0
			for k in 1 2 3; do
				cpus $((2 * k))
				mapfile -t e < <(with)
				M3_JOBMEM=512M cell ranks "$k" "PIPEDPEER_DDP_RANKS_PER_NODE=$k" --ddp $((2 * k)) "${e[@]}"
			done
			cpus 2
			;;
		*) bl_die "unknown sweep $s (size batch interval int8 ranks)" ;;
		esac
	done
	[[ "${M3_KEEP:-0}" == 1 ]] || bl_lab_down
	"$0" fit
	;;
raw)
	# The control: train.py with no pipedpeer at all, run by the job
	# environment's own wrapper (<store>/bin/run, no shim) inside node 1 (same 2 CPUs), at each rank's
	# share of the batch (half: 2 ranks). Ring compute/step minus this is
	# what pipedpeer adds to a step outside the sync round trip.
	bl_init scaling-train
	trap - EXIT # node 1 is only read here, never taken down
	mkdir -p "$out/logs"
	BL_NODES_UP=1 BL_REV="${M3_REV:-raw}"
	py="$(bl_exec 1 sh -c 'for p in /nix/store/*-run/bin/run; do "$p" -c "import torch" 2>/dev/null && echo "$p" && break; done; true')"
	[[ -n "$py" ]] || bl_die "no python with torch in ${BL_EMU_PREFIX}1 (run the sweep first: it builds the environment)"
	raw() { # raw <sweep> <x> <rep> K=V...
		local l="$out/logs/$1_$2_r$3.log" t0 t1 st=ok load cpu0 cpu1
		m3_want "$1:none:$2" || return 0
		flock 9
		read -r load _ </proc/loadavg
		cpu0="$(node_cpu)"
		t0="$(date +%s.%N)"
		bl_exec 1 env BENCH_WIDTH=512 BENCH_DEPTH=2 BENCH_BATCH=512 BENCH_STEPS="$steps" "${@:4}" "$py" /bench/train.py >"$l" 2>&1 || st=fail
		t1="$(date +%s.%N)"
		cpu1="$(node_cpu)"
		flock -u 9
		python3 - "$l" "$st" "$t0" "$t1" "$1" "$2" "$3" "$BL_REV" "${*:4}" "$load" "$cpu0" "$cpu1" >>"$csv" <<'PY2'
import csv, re, sys, time
l, st, t0, t1, sweep, x, rep, rev, flags, load, cpu0, cpu1 = sys.argv[1:]
t = open(l, errors="replace").read()
g = lambda p: (re.findall(p, t) or [""])[-1]
step = g(r"BENCH_STEP_S ([0-9.]+)")
csv.writer(sys.stdout).writerow([sweep, x, "none", rep, st if step else "noresult", "%.3f" % (float(t1) - float(t0)),
    g(r"BENCH_ELAPSED ([0-9.]+)"), g(r"BENCH_STEPS_DONE (\d+)"), g(r"BENCH_PARAMS (\d+)"), 1, 0, 0, 0, 0, step, step,
    "raw", flags, rev, 1, time.strftime("%Y-%m-%dT%H:%M:%S"), l, load, "%.3f" % ((int(cpu1) - int(cpu0)) / 1e6)])
PY2
	}
	for r in $(seq 1 "$BL_REPS") c; do
		s=""
		[[ "$r" == c ]] && s="BENCH_STEPS=$((steps / 3))" # CPU calibration, as in cell
		for w in 128 256 512 1024 2048; do raw raw-size "$w" "$r" BENCH_WIDTH=$w $s; done
		for b in 256 1024 4096 8192; do raw raw-batch "$b" "$r" BENCH_BATCH=$((b / 2)) $s; done
	done
	;;
gpu)
	mkdir -p "$out"
	work="$out/gpu"
	bl_workloads "$work"
	py="${BL_GPU_PYTHON:-python3}"
	"$py" -c 'import torch, sys; sys.exit(0 if torch.cuda.is_available() else 1)' ||
		bl_die "$py has no CUDA torch; set BL_GPU_PYTHON to one that does"
	echo "sweep,x,step_s,params,when" >"$out/gpu.csv"
	run_gpu() { # run_gpu <sweep> <x> K=V...
		local s="$1" x="$2" l="$work/$1_$2.log"
		shift 2
		env BENCH_WIDTH=512 BENCH_DEPTH=2 BENCH_BATCH=1024 BENCH_STEPS="$steps" BENCH_DEVICE=cuda "$@" "$py" "$work/train.py" >"$l" 2>&1 ||
			{ bl_log "gpu baseline $s/$x failed: $l"; return 0; }
		printf '%s,%s,%s,%s,%s\n' "$s" "$x" "$(grep -oE 'BENCH_STEP_S [0-9.]+' "$l" | cut -d' ' -f2)" \
			"$(grep -oE 'BENCH_PARAMS [0-9]+' "$l" | cut -d' ' -f2)" "$(date +%FT%T)" >>"$out/gpu.csv"
	}
	for w in 128 256 512 1024 2048; do run_gpu size $w BENCH_WIDTH=$w; done
	for b in 256 1024 4096 8192; do run_gpu batch $b BENCH_BATCH=$b; done
	cat "$out/gpu.csv"
	;;
fit)
	python3 - "$csv" "$out/gpu.csv" "${2:-}" "$here" <<'PY'
import csv, os, statistics, sys
from collections import defaultdict
runs, gpu, plots, here = sys.argv[1:]
sys.path.insert(0, here)
from fit import slope  # M2's least-squares log-log exponent (scripts/scaling/fit.py)
MARGIN = 0.15
# Expected exponent per (sweep, metric); x is params for size, the knob otherwise.
# Fits run on CPU time and bytes only: wall times move with other agents' load
# (they are in the tables with the host load, never fitted). x is the
# parameter count for size, the knob otherwise. Expected exponents:
#   cpu_step_s       CPU per rank per step: (median CPU of the full runs - the
#                    short calibration run) / extra steps / ranks, both nodes' cgroups
#   shim_cpu_step_s  cpu_step_s minus the no-pipedpeer control at the same x:
#                    what the shim + daemons cost per step, per rank
#   cpu_total_step_s CPU of the whole ring per step
EXPECTED = {
    "size": {"bytes_round": 1.0, "cpu_step_s": 1.0, "shim_cpu_step_s": 1.0},
    "batch": {"bytes_round": 0.0, "cpu_step_s": 1.0, "shim_cpu_step_s": 0.0},
    # bytes/round is left out here: the shim line folds the opening weight
    # broadcast into it, which weighs more the fewer rounds there are.
    "interval": {"cpu_step_s": 0.0},
    # Per-rank shim work is per parameter, so the ring's total can at most
    # grow with the rank count; compute alone would stay flat.
    "ranks": {"bytes_round": 0.0, "cpu_total_step_s": 1.0},
    "raw-size": {"cpu_step_s": 1.0},
    "raw-batch": {"cpu_step_s": 1.0},
}
cells, calib = defaultdict(list), defaultdict(list)
for r in csv.DictReader(open(runs)):
    if r["status"] == "ok" and r["rep"] == "c":
        calib[(r["sweep"], r["profile"], int(r["x"]))].append(r)
    elif r["status"] == "ok":
        if r["source"] == "shim-line" and r["steps"]:
            # Rebuilt from rounds x ms each (rows written before that fix used
            # the line's own total, rounded to 0.1 s).
            st = float(r["rounds"]) * float(r["sync_round_s"])
            r["sync_total_s"] = "%.4f" % st
            r["compute_step_s"] = "%.5f" % max(0.0, (float(r["elapsed_s"]) - st) / int(r["steps"]))
        cells[(r["sweep"], r["profile"], int(r["x"]))].append(r)
# CPU per step from the difference between the full runs and the short
# calibration run of the same cell (start-up cancels out).
for k, rs in cells.items():
    full = [r for r in rs if r.get("cpu_s")]
    if not full or not calib.get(k) or not calib[k][0].get("cpu_s"):
        continue
    c = calib[k][0]
    dn = int(full[0]["steps"]) - int(c["steps"])
    if dn <= 0:
        continue
    tot = (statistics.median(float(r["cpu_s"]) for r in full) - float(c["cpu_s"])) / dn
    w = int(full[0]["world"] or 1)
    for r in full:
        r["cpu_total_step_s"] = "%.5f" % max(1e-6, tot)
        r["cpu_step_s"] = "%.5f" % max(1e-6, tot / w)
med = lambda rs, k: statistics.median(float(r[k]) for r in rs if r.get(k) not in ("", None))
# What pipedpeer adds to a step outside the sync round trip: ring compute/step
# minus the no-pipedpeer control at the same x (scripts/scaling/train.sh raw).
for (s, p, x), rs in list(cells.items()):
    ctl = cells.get(("raw-" + s, "none", x))
    if ctl and not s.startswith("raw"):
        base = med(ctl, "compute_step_s")
        cbase = med(ctl, "cpu_step_s") if any(r.get("cpu_step_s") for r in ctl) else None
        for r in rs:
            r["shim_step_s"] = "%.5f" % max(1e-6, float(r["compute_step_s"]) - base)
            if cbase is not None and r.get("cpu_step_s"):
                r["shim_cpu_step_s"] = "%.5f" % max(1e-6, float(r["cpu_step_s"]) - cbase)
has = lambda rs, k: any(r.get(k) not in ("", None) for r in rs)
opt = lambda rs, k, f="%.4f": f % med(rs, k) if has(rs, k) else "-"
flags = []
def series(sweep, prof):
    pts = sorted((x, rs) for (s, p, x), rs in cells.items() if s == sweep and p == prof)
    return [(med(rs, "params") if sweep.endswith("size") else x, rs) for x, rs in pts]
def table(sweep, prof):
    pts = series(sweep, prof)
    if not pts:
        return
    xname = "params" if sweep.endswith("size") else {"interval": "sync every", "ranks": "ranks/node"}.get(sweep, "batch")
    print("\n#### %s, %s (emulated)\n" % (sweep, prof))
    print("| %s | runs | world | rounds | bytes/round | CPU/step/rank s | shim CPU/step/rank s | ring CPU/step s "
          "| wall: sync/round s | wall: compute/step s | wall: shim/step s | wall: step s | load |" % xname)
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for x, rs in pts:
        print("| %s | %d | %s | %.0f | %.0f | %s | %s | %s | %.3f | %.4f | %s | %.4f | %s |" % (
            "%.2fM" % (x / 1e6) if sweep.endswith("size") else x, len(rs), rs[0]["world"], med(rs, "rounds"),
            med(rs, "bytes_round"), opt(rs, "cpu_step_s"), opt(rs, "shim_cpu_step_s"), opt(rs, "cpu_total_step_s"),
            med(rs, "sync_round_s"), med(rs, "compute_step_s"), opt(rs, "shim_step_s"), med(rs, "step_s"),
            opt(rs, "load1", "%.0f")))
    print("\n| metric | exponent (all points) | exponent (top 3) | expected | flag |")
    print("|---|---|---|---|---|")
    for metric, exp in EXPECTED[sweep].items():
        xs = [x for x, _ in pts]
        if not all(has(rs, metric) for _, rs in pts):
            continue
        ys = [med(rs, metric) for _, rs in pts]
        whole = slope(xs, ys)
        tail = slope(xs[-3:], ys[-3:]) if len(xs) > 3 else None
        worst = max([k for k in (whole, tail) if k is not None] or [float("-inf")])
        bad = worst > exp + MARGIN
        f = lambda k: "-" if k is None else "%.2f" % k
        print("| %s | %s | %s | %.2f | %s |" % (metric, f(whole), f(tail), exp, "SUPERLINEAR" if bad else "ok"))
        if bad:
            flags.append("%s/%s: %s grows as x^%s (expected %.2f)" % (sweep, prof, metric, f(worst), exp))
    if plots:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(5, 3.5))
        for metric in ("cpu_step_s", "shim_cpu_step_s", "cpu_total_step_s"):
            if not all(has(rs, metric) for _, rs in pts):
                continue
            ax.loglog([x for x, _ in pts], [max(med(rs, metric), 1e-6) for _, rs in pts], "o-", label=metric)
        ax.set_xlabel(xname); ax.set_ylabel("CPU seconds per step"); ax.set_title("%s, %s (emulated)" % (sweep, prof))
        ax.legend(fontsize=7); fig.tight_layout()
        os.makedirs(plots, exist_ok=True)
        fig.savefig(os.path.join(plots, "%s-%s.png" % (sweep, prof)), dpi=90); plt.close(fig)
profiles = sorted({p for (_, p, _) in cells})
for sweep in ("size", "batch", "interval", "ranks", "raw-size", "raw-batch"):
    for prof in profiles:
        table(sweep, prof)
# int8 on/off: two points per width, so a ratio, not a fit.
pairs = sorted({(p, x) for (s, p, x) in cells if s.startswith("int8")})
if pairs:
    print("\n#### int8 on vs off (emulated)\n")
    print("| profile | width | bytes/round on | off | off/on | CPU/step/rank s on | off | wall: sync/round s on | off |")
    print("|---|---|---|---|---|---|---|---|---|")
    for p, w in pairs:
        on, off = cells.get(("int8-1", p, w)), cells.get(("int8-0", p, w))
        if on and off:
            b1, b0 = med(on, "bytes_round"), med(off, "bytes_round")
            print("| %s | %d | %.0f | %.0f | %.2f | %s | %s | %.3f | %.3f |" % (
                p, w, b1, b0, b0 / b1 if b1 else 0, opt(on, "cpu_step_s"), opt(off, "cpu_step_s"),
                med(on, "sync_round_s"), med(off, "sync_round_s")))
if os.path.exists(gpu):
    print("\n#### single-GPU baselines (real RTX 3060 Laptop GPU, no pipedpeer; not emulated)\n")
    print("| sweep | x | params | step s |")
    print("|---|---|---|---|")
    for r in csv.DictReader(open(gpu)):
        print("| %s | %s | %s | %s |" % (r["sweep"], r["x"], r["params"], r["step_s"]))
print("\n#### superlinear flags\n")
print("\n".join("- " + f for f in flags) or "none")
PY
	;;
*)
	sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
