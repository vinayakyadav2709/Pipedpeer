# Shared runner for the emulated-lab benchmarks (bench-ddp-switch.sh,
# bench-w10.sh, and the scaling sweeps that reuse it). Source it; do not run it.
#
# What it owns, so no benchmark re-invents it:
#   - which binary: BEFORE is built from a fixed commit (default c7e2c7f, the
#     pure-move split every W10 change starts from), AFTER from this checkout.
#     Both live under ~/.local/share/pipedpeer-bench/bin, never in a temp dir.
#   - the lab: scripts/lab-emu.sh brings up N equal containers (same --cpus,
#     same --memory) on a shaped bridge. Speedup is 1 container against N of
#     the same size, never against the 15-core host.
#   - the bench lock: each timed run holds ~/.cache/pipedpeer-bench.lock only
#     while it runs (not while a lab comes up or warms), so two recorded runs
#     never share the CPU.
#   - result capture: one CSV row per run, every row labelled emulated=1, with
#     the host's 1-minute load average when it ended (load1): rows taken while
#     other agents load the host are not paper numbers; rerun them quietly
#     with ONLY= in each driver.
#     Medians and markdown tables come from bl_table.
#
# Every number this produces is EMULATED: equal cgroup quotas plus tc netem on
# one host. It measures how overhead grows and whether work crossing nodes
# pays; it is not a real-hardware speedup.
#
# Knobs (environment):
#   BL_BEFORE_REV   commit for the BEFORE binary (c7e2c7f)
#   BL_EMU          path to lab-emu.sh (scripts/lab-emu.sh)
#   BL_LAB_NAME     LAB_NAME for lab-emu.sh (bench: containers ppbench-<i>, never
#                   shared with the functional tests' default "emu" lab)
#   BL_REPS         timed repetitions per cell (3)
#   BL_RUN_TIMEOUT  seconds before one run counts as failed (600)
#   BL_LAB_WAIT     seconds to wait for other labs to free the memory cap (3600)
#   BL_OUT          results directory (~/.local/state/pipedpeer/bench)

BL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BL_BEFORE_REV="${BL_BEFORE_REV:-c7e2c7f}"
BL_EMU="${BL_EMU:-$BL_ROOT/scripts/lab-emu.sh}"
BL_LAB_NAME="${BL_LAB_NAME:-bench}"
BL_EMU_PREFIX="pp${BL_LAB_NAME}-"
BL_REPS="${BL_REPS:-3}"
BL_RUN_TIMEOUT="${BL_RUN_TIMEOUT:-600}"
BL_OUT="${BL_OUT:-${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/bench}"
BL_BINDIR="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer-bench/bin"
BL_LOCK="${BL_LOCK:-$HOME/.cache/pipedpeer-bench.lock}"
BL_NODES_UP=0

bl_log() { printf '[bench %(%H:%M:%S)T] %s\n' -1 "$*" >&2; }
bl_die() { bl_log "FAIL: $*"; exit 1; }

# bl_init <suite> - picks the runtime, takes the bench lock, opens the CSV.
bl_init() {
	BL_SUITE="$1"
	source "$BL_ROOT/scripts/lib/runtime.sh"
	PP_NEED_COMPOSE=0 PP_RUNTIME_ORDER="docker podman" pp_pick_runtime || bl_die "no container runtime"
	BL_RT="$PP_RUNTIME"
	command -v python3 >/dev/null || bl_die "python3 required on the host (result parsing)"
	mkdir -p "$BL_OUT/$BL_SUITE/logs" "$(dirname "$BL_LOCK")"
	# The lock is taken per timed run (bl_run), never while a lab comes up or
	# warms, so other timed units are not queued behind a lab waiting for RAM.
	exec 9>"$BL_LOCK"
	BL_CSV="$BL_OUT/$BL_SUITE/results.csv"
	[[ -f "$BL_CSV" ]] || echo "suite,case,variant,binary,rev,profile,loss,nodes,rep,status,wall_s,script_s,step_s,sync_s,world,loss_val,steps,emulated,when,log,load1" > "$BL_CSV"
	trap 'bl_lab_down' EXIT
}

# bl_build <before|after> - sets BL_BIN, BL_REV, BL_API_PORT (call it directly,
# not in $(...): the globals must survive); builds only when missing
# (BEFORE) or always (AFTER: the checkout moves under it).
bl_build() {
	local which="$1" src rev out
	mkdir -p "$BL_BINDIR"
	if [[ "$which" == before ]]; then
		rev="$(git -C "$BL_ROOT" rev-parse --short "$BL_BEFORE_REV")"
		out="$BL_BINDIR/pipedpeer-$rev"
		if [[ ! -x "$out" ]]; then
			src="$BL_BINDIR/../src-$rev"
			rm -rf "$src" && mkdir -p "$src"
			# git archive reads the object store only: the index and the
			# working tree of whoever else is editing stay untouched.
			git -C "$BL_ROOT" archive "$rev" src | tar -x -C "$src"
			bl_log "building BEFORE binary from $rev"
			(cd "$src/src" && CGO_ENABLED=0 systemd-run --user --scope -q --slice=pipedpeer-agents.slice \
				go build -o "$out" .) >&2 || bl_die "BEFORE build failed"
		fi
	else
		rev="$(git -C "$BL_ROOT" rev-parse --short HEAD)"
		git -C "$BL_ROOT" diff --quiet HEAD -- src 2>/dev/null || rev="$rev-dirty"
		out="$BL_BINDIR/pipedpeer-after"
		bl_log "building AFTER binary from the checkout ($rev)"
		(cd "$BL_ROOT/src" && CGO_ENABLED=0 systemd-run --user --scope -q --slice=pipedpeer-agents.slice \
			go build -o "$out" .) >&2 || bl_die "AFTER build failed"
	fi
	BL_REV="$rev"
	# c7e2c7f predates the 4242 port block: its daemon API is on 38080.
	if [[ "$which" == before ]]; then BL_API_PORT=38080; else BL_API_PORT=4242; fi
	BL_BIN="$out"
}

# --- lab: the only place that knows lab-emu.sh's interface -------------------

# bl_lab_up <nodes> <memory> <profile> <loss%> <binary>
bl_lab_up() {
	local n="$1" mem="$2" profile="$3" loss="$4" bin="$5"
	[[ -x "$BL_EMU" ]] || bl_die "$BL_EMU missing (U23's emulation rig)"
	bl_lab_down
	bl_log "lab up: $n x (2 cpu, $mem), profile $profile, loss $loss%, $(basename "$bin")"
	# Other labs (functional tests) may hold the memory cap for a while:
	# lab-emu refuses, we wait and retry, up to BL_LAB_WAIT seconds.
	local out="$BL_OUT/$BL_SUITE/lab-up.log" waited=0
	until LAB_MEM="$mem" LAB_PROFILE="$profile" LAB_LOSS="$loss" LAB_CPUS=2 PIPEDPEER="$bin" \
		LAB_API_PORT="$BL_API_PORT" LAB_NAME="$BL_LAB_NAME" "$BL_EMU" up "$n" > "$out" 2>&1; do
		cat "$out" >&2
		grep -q "refusing to start" "$out" || bl_die "lab-emu.sh up failed"
		((waited < ${BL_LAB_WAIT:-3600})) || bl_die "memory cap still full after ${waited}s"
		bl_log "memory cap full (other labs running); retrying in 60 s (waited ${waited}s)"
		sleep 60
		waited=$((waited + 60))
	done
	cat "$out" >&2
	BL_NODES_UP="$n"
	BL_PROFILE="$profile"
	BL_LOSS="$loss"
}

# bl_lab_shape <profile> <loss%> - reshape the running lab's links.
bl_lab_shape() {
	[[ "$1" == "${BL_PROFILE:-}" && "$2" == "${BL_LOSS:-}" ]] && return 0
	LAB_NAME="$BL_LAB_NAME" "$BL_EMU" shape "$1" "$2" >&2 || bl_die "lab-emu.sh shape $1 $2 failed"
	BL_PROFILE="$1"
	BL_LOSS="$2"
}

bl_lab_down() {
	[[ "$BL_NODES_UP" -gt 0 ]] || return 0
	LAB_NAME="$BL_LAB_NAME" "$BL_EMU" down >&2 || bl_log "warning: lab-emu.sh down failed; containers ${BL_EMU_PREFIX}* may still run"
	BL_NODES_UP=0
}

# bl_exec <node> <cmd...> - run a command inside lab node <node> (1-based).
bl_exec() {
	local node="$1"
	shift
	"$BL_RT" exec "${BL_EMU_PREFIX}$node" "$@"
}

# --- workloads ----------------------------------------------------------------

# bl_workloads <dir> - writes every workload script into <dir>. Each prints
# BENCH_* lines that bl_run parses; none needs editing per case (all knobs
# arrive as -e BENCH_*=... on the run command line).
bl_workloads() {
	local d="$1"
	mkdir -p "$d"
	: > "$d/.pipedpeerignore"

	# CPU-training stand-in for GPU training: an MLP whose size and per-step
	# compute are knobs, fed by a DataLoader so the shim shards it the way it
	# shards a real script. Two threads: the container has two CPUs.
	cat > "$d/train.py" <<'PY'
import os, time
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

env = lambda k, v: int(os.environ.get(k, v))
WIDTH, DEPTH, BATCH = env("BENCH_WIDTH", "512"), env("BENCH_DEPTH", "2"), env("BENCH_BATCH", "2048")
STEPS, IN = env("BENCH_STEPS", "60"), env("BENCH_IN", "256")
torch.set_num_threads(env("BENCH_THREADS", "2"))
torch.manual_seed(7)
dev = os.environ.get("BENCH_DEVICE", "cpu")
per_epoch = 20
X = torch.randn(BATCH * per_epoch, IN)
y = (X[:, :4].sum(1, keepdim=True) > 0).float()
Xv = torch.randn(4096, IN, device=dev)
yv = (Xv[:, :4].sum(1, keepdim=True) > 0).float()
layers = [nn.Linear(IN, WIDTH), nn.ReLU()]
for _ in range(DEPTH):
    layers += [nn.Linear(WIDTH, WIDTH), nn.ReLU()]
model = nn.Sequential(*layers, nn.Linear(WIDTH, 1)).to(dev)
print("BENCH_PARAMS", sum(p.numel() for p in model.parameters()))
opt = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
loss_fn = nn.BCEWithLogitsLoss()
loader = DataLoader(TensorDataset(X, y), batch_size=BATCH, shuffle=True)
t0, steps = time.monotonic(), 0
for _ in range(max(1, STEPS // per_epoch)):
    for xb, yb in loader:
        xb, yb = xb.to(dev), yb.to(dev)
        opt.zero_grad()
        loss_fn(model(xb), yb).backward()
        opt.step()
        steps += 1
if dev != "cpu":
    torch.cuda.synchronize()
el = time.monotonic() - t0
with torch.no_grad():
    print("BENCH_LOSS %.6f" % loss_fn(model(Xv), yv).item())
print("BENCH_WORLD", os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
print("BENCH_STEPS_DONE", steps)
print("BENCH_STEP_S %.5f" % (el / max(steps, 1)))
print("BENCH_ELAPSED %.3f" % el)
PY

	# Pool workload: item count, cost per item, bytes in and out, one large
	# shared argument (sent once per peer when inputs go by fingerprint),
	# compressible or random payloads, and uneven item cost (tail, chunking).
	cat > "$d/pool.py" <<'PY'
import functools, multiprocessing, os, time

env = lambda k, v: int(os.environ.get(k, v))
ITEMS, SPIN = env("BENCH_ITEMS", "96"), env("BENCH_SPIN", "200000")
IN_KB, OUT_KB, SHARED_KB = env("BENCH_IN_KB", "0"), env("BENCH_OUT_KB", "0"), env("BENCH_SHARED_KB", "0")
ZEROS, UNEVEN = env("BENCH_COMPRESSIBLE", "0"), env("BENCH_UNEVEN", "0")

def blob(kb, seed):
    if kb <= 0:
        return b""
    return bytes(kb * 1024) if ZEROS else os.urandom(kb * 1024)

def work(shared, item):
    x, payload = item
    spin = SPIN * (1 + (x * 7919) % 10) if UNEVEN else SPIN
    total = len(shared) + len(payload)
    for i in range(spin):
        total += (i ^ x) % 7
    return total, blob(OUT_KB, x)

if __name__ == "__main__":
    shared = blob(SHARED_KB, -1)
    items = [(x, blob(IN_KB, x)) for x in range(ITEMS)]
    t0 = time.monotonic()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        out = pool.map(functools.partial(work, shared), items)
    print("BENCH_ELAPSED %.3f" % (time.monotonic() - t0))
    print("BENCH_CHECK", sum(t for t, _ in out))
PY

	# numpy payloads, for pickle-5 out-of-band buffers. Kept apart from
	# pool.py so the plain pool cases do not pay for a numpy environment.
	cat > "$d/poolnp.py" <<'PY'
import multiprocessing, os, time
import numpy as np

ITEMS, MB = int(os.environ.get("BENCH_ITEMS", "48")), int(os.environ.get("BENCH_IN_MB", "8"))

def work(a):
    return float((a * 1.0001).sum()), a[: a.size // 2]

if __name__ == "__main__":
    rng = np.random.default_rng(3)
    items = [rng.standard_normal(MB * 131072) for _ in range(ITEMS)]
    t0 = time.monotonic()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        out = pool.map(work, items)
    print("BENCH_ELAPSED %.3f" % (time.monotonic() - t0))
    print("BENCH_CHECK %.3f" % sum(s for s, _ in out))
PY

	# Nothing at all: the whole wall clock is pipedpeer (seed, env wrapper,
	# sandbox start).
	cat > "$d/noop.py" <<'PY'
print("BENCH_ELAPSED 0.000")
PY
}

# bl_put <node> <dir> - copy a workload dir into a node at /bench.
bl_put() {
	bl_exec "$1" sh -c 'rm -rf /bench && mkdir -p /bench'
	"$BL_RT" cp "$2/." "${BL_EMU_PREFIX}$1:/bench/" >/dev/null
}

# bl_run <case> <variant> <binary-label> <rep> <script> [run flags...]
# (flags go before the script on the command line, so they stay pipedpeer's)
# Runs `pipedpeer run` on node 1 (the submitter, same quota as every peer),
# times it from outside, parses the BENCH_* lines and the run record when the
# binary writes one (steps.json, C24), and appends one CSV row.
bl_run() {
	local case="$1" variant="$2" binlabel="$3" rep="$4" script="$5"
	shift 5
	local log="$BL_OUT/$BL_SUITE/logs/${case}_${variant}_${binlabel}_${BL_PROFILE}_n${BL_NODES_UP}_r${rep}.log"
	local status=ok t0 t1 rc=0
	bl_exec 1 touch /bench/.mark
	if ! flock -n 9; then
		bl_log "another timed run holds $BL_LOCK; waiting for it"
		flock 9
	fi
	t0="$(date +%s.%N)"
	timeout "$BL_RUN_TIMEOUT" "$BL_RT" exec -w /bench "${BL_EMU_PREFIX}1" \
		pipedpeer run "$@" "$script" > "$log" 2>&1 || rc=$?
	t1="$(date +%s.%N)"
	flock -u 9
	if ((rc == 124)); then status=timeout; elif ((rc != 0)); then status=fail; fi
	# The newest run record on the submitter, if this binary writes one.
	local steps="$log.steps.json"
	rm -f "$steps"
	bl_exec 1 sh -c 'f=$(find / -xdev \( -path /nix -o -path /proc \) -prune -o -name steps.json -newer /bench/.mark -print 2>/dev/null | head -1); [ -n "$f" ] && cat "$f"' \
		> "$steps" 2>/dev/null || rm -f "$steps"
	python3 - "$log" "$steps" "$t0" "$t1" "$status" >> "$BL_CSV" <<PY
import json, os, re, sys, time
log, steps, t0, t1, status = sys.argv[1:]
text = open(log, errors="replace").read()
def last(pat):
    m = re.findall(pat, text)
    return m[-1] if m else ""
script = last(r"BENCH_ELAPSED ([0-9.]+)")
if status == "ok" and not script:
    status = "noresult"
worlds = [int(w) for w in re.findall(r"BENCH_WORLD (\d+)", text)]
sync = last(r"ddp: \d+ sync\(s\), ([0-9.]+)s total")
if os.path.exists(steps) and os.path.getsize(steps):
    try:
        rec = json.load(open(steps))
        ns = sum(s.get("dur_ns", 0) for s in rec.get("Steps") or rec.get("steps") or [] if s.get("step") == "ddp_sync")
        if ns:
            sync = "%.3f" % (ns / 1e9)
    except Exception as e:
        print("warning: unreadable run record %s: %s" % (steps, e), file=sys.stderr)
row = ["$BL_SUITE", "$case", "$variant", "$binlabel", "$BL_REV", "$BL_PROFILE", "$BL_LOSS", "$BL_NODES_UP", "$rep",
       status, "%.3f" % (float(t1) - float(t0)), script, last(r"BENCH_STEP_S ([0-9.]+)"), sync,
       str(max(worlds)) if worlds else "", last(r"BENCH_LOSS ([0-9.]+)"), last(r"BENCH_STEPS_DONE (\d+)"),
       "1", time.strftime("%Y-%m-%dT%H:%M:%S"), log, open("/proc/loadavg").read().split()[0]]
print(",".join(row))
PY
	[[ "$(tail -1 "$BL_CSV" | cut -d, -f10)" == ok ]] && return 0
	bl_log "run failed: $case/$variant ($binlabel) - see $log"
	return 1
}

# bl_reps <case> <variant> <binary-label> <script> [run flags...] - BL_REPS
# timed runs. The first failure or timeout ends the cell: it is recorded once
# ("fails/times out on <rev>" in bl_table, with its log) and the remaining reps
# are skipped. A known-broken build failing is a valid BEFORE result; burning
# 3 x BL_RUN_TIMEOUT on it only holds the bench lock.
bl_reps() {
	local case="$1" variant="$2" binlabel="$3" script="$4" r
	shift 4
	for r in $(seq 1 "$BL_REPS"); do
		bl_run "$case" "$variant" "$binlabel" "$r" "$script" "$@" || {
			bl_log "$case/$variant fails or times out on $BL_REV; skipping its remaining reps"
			return 0
		}
	done
}

# bl_warm <script> [args...] - one untimed run so the environment is built and
# shipped before anything is timed (cold cost is its own case).
bl_warm() {
	local script="$1"
	shift
	bl_log "warming the environment for $script"
	timeout "$BL_RUN_TIMEOUT" "$BL_RT" exec -w /bench "${BL_EMU_PREFIX}1" pipedpeer run "$@" "$script" \
		> "$BL_OUT/$BL_SUITE/logs/warm_${script%.py}_$(date +%s).log" 2>&1 ||
		bl_log "warning: warm-up of $script failed; its timed runs will show it"
}

# bl_table [csv] - medians per (case, variant, binary, profile, nodes) as a
# markdown table. Every row says emulated.
bl_table() {
	python3 - "${1:-$BL_CSV}" <<'PY'
import csv, statistics, sys
from collections import defaultdict
cells = defaultdict(list)
for r in csv.DictReader(open(sys.argv[1])):
    cells[(r["case"], r["variant"], r["binary"], r["rev"], r["profile"], r["loss"], r["nodes"])].append(r)
med = lambda rs, k: (lambda v: "%.2f" % statistics.median(v) if v else "-")([float(r[k]) for r in rs if r["status"] == "ok" and r.get(k)])
print("| case | variant | binary | profile | loss | nodes | runs ok | wall s | script s | step s | sync s | world | loss | host load | label |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
bad = []
for (c, v, b, rev, p, l, n), rs in sorted(cells.items()):
    ok = sum(r["status"] == "ok" for r in rs)
    for r in rs:
        if r["status"] != "ok":
            bad.append("- %s / %s on %s, %s, %s nodes: %s on %s (%ss): `%s`" % (
                c, v, b, p, n, "times out" if r["status"] == "timeout" else "fails",
                rev, r["wall_s"].split(".")[0], r.get("log") or "?"))
    worlds = sorted({r["world"] for r in rs if r["world"]})
    print("| %s | %s | %s %s | %s | %s%% | %s | %d/%d | %s | %s | %s | %s | %s | %s | %s | emulated |" % (
        c, v, b, rev, p, l, n, ok, len(rs), med(rs, "wall_s"), med(rs, "script_s"), med(rs, "step_s"),
        med(rs, "sync_s"), "/".join(worlds) or "-", med(rs, "loss_val"), med(rs, "load1")))
if bad:
    print()
    print("Failed cells (remaining reps skipped):")
    print("\n".join(bad))
PY
}
