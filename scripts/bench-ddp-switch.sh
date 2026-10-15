#!/usr/bin/env bash
# Where splitting one training run across machines starts to pay (D2).
#
# A matrix of model size x per-step compute x link profile, each cell run
# three ways on equal emulated nodes (2 CPUs each, links shaped by netem):
#   solo   one node, no ring                     (the baseline)
#   split  two nodes, --ddp 2                    (always split)
#   auto   two nodes, no flag                    (what the auto decision chose;
#                                                 BENCH_WORLD in the log says)
# Sync cost comes from the run record (ddp_sync steps) when the binary writes
# one, else from the shim's "ddp: N sync(s), Xs total" line.
#
# The matrix is built so both outcomes exist: a small model with heavy
# per-step compute on lan should split; a big model with light compute on
# wifi/5g should not. If no cell has split faster than solo, or none has solo
# faster than split, the script says so and exits 1.
#
# Every number is EMULATED (equal cgroup quotas + netem on one host), except
# --gpu-baseline, which times the same model on this machine's real GPU with
# no pipedpeer at all: the single-GPU reference, never a scaling number.
#
#   scripts/bench-ddp-switch.sh before          # the c7e2c7f binary
#   scripts/bench-ddp-switch.sh after           # this checkout
#   scripts/bench-ddp-switch.sh table           # medians as markdown
#   scripts/bench-ddp-switch.sh --gpu-baseline  # needs a CUDA torch (BL_GPU_PYTHON)
#
# Knobs: MODELS ("small large"), COMPUTE ("light heavy"), PROFILES
# ("lan wifi 5g"), LOSS (0), TRAIN_MEM (2g), STEPS (60), BL_REPS (3),
# ONLY ("<model>-<compute>:<variant>[:<profile>]" tokens, e.g.
# "large-light:split:wifi small-heavy:solo"): rerun exactly those rows.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bench-lib.sh"

models=(${MODELS:-small large})
compute=(${COMPUTE:-light heavy})
profiles=(${PROFILES:-lan wifi 5g})
loss="${LOSS:-0}"
mem="${TRAIN_MEM:-2g}"
steps="${STEPS:-60}"

# small ~0.2M params (0.4 MB fp16 per sync), large ~4.5M (9 MB: the demo's size)
model_env() {
	case "$1" in
	small) echo "-e BENCH_WIDTH=256 -e BENCH_DEPTH=2" ;;
	large) echo "-e BENCH_WIDTH=1024 -e BENCH_DEPTH=4" ;;
	*) bl_die "unknown model $1" ;;
	esac
}
# per-step compute through the batch: light 256 rows, heavy 8192 rows
compute_env() {
	case "$1" in
	light) echo "-e BENCH_BATCH=256" ;;
	heavy) echo "-e BENCH_BATCH=8192" ;;
	*) bl_die "unknown compute $1" ;;
	esac
}

mode="${1:-}"
case "$mode" in
before | after) ;;
table)
	bl_table "$BL_OUT/ddp-switch/results.csv"
	exit 0
	;;
--gpu-baseline)
	work="$BL_OUT/ddp-switch/gpu"
	mkdir -p "$BL_OUT/ddp-switch"
	bl_workloads "$work"
	py="${BL_GPU_PYTHON:-python3}"
	"$py" -c 'import torch, sys; sys.exit(0 if torch.cuda.is_available() else 1)' ||
		bl_die "$py has no CUDA torch; set BL_GPU_PYTHON to one that does"
	for m in "${models[@]}"; do
		for c in "${compute[@]}"; do
			envs="$(model_env "$m") $(compute_env "$c") -e BENCH_STEPS=$steps -e BENCH_DEVICE=cuda"
			# shellcheck disable=SC2086
			env $(echo "$envs" | sed 's/-e //g') "$py" "$work/train.py" > "$work/${m}_${c}.log" 2>&1 ||
				bl_log "gpu baseline $m/$c failed: $work/${m}_${c}.log"
			printf '%s %s: step %ss (real GPU, single-GPU baseline, not emulated)\n' "$m" "$c" \
				"$(grep -oE 'BENCH_STEP_S [0-9.]+' "$work/${m}_${c}.log" | cut -d' ' -f2)"
		done
	done
	exit 0
	;;
*)
	sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac

bl_init ddp-switch
bl_build "$mode"
bin="$BL_BIN"
work="$BL_OUT/ddp-switch/work"
bl_workloads "$work"

# shellcheck disable=SC2086
wanted() { # wanted <cell> <variant> <profile>
	[[ -z "${ONLY:-}" || " $ONLY " == *" $1:$2 "* || " $ONLY " == *" $1:$2:$3 "* ]]
}
any_variant() { # any_variant <variant>: does ONLY select anything of it?
	[[ -z "${ONLY:-}" || " $ONLY " == *":$1 "* || " $ONLY " == *":$1:"* ]]
}

# --gpu off --mem 1G: the daemon estimates 2.1 GB for a torch job, more than a 2g node.
cell() { # cell <variant> <model> <compute> [flags...] - BL_REPS reps, first failure ends it
	local v="$1" m="$2" c="$3"
	shift 3
	wanted "$m-$c" "$v" "${BL_PROFILE:-lan}" || return 0
	bl_reps "$m-$c" "$v" "$mode" train.py --gpu off --mem 1G $(model_env "$m") $(compute_env "$c") -e BENCH_STEPS="$steps" "$@"
}

# solo: one node; the link profile cannot matter, so it runs once on lan.
if any_variant solo; then
bl_lab_up 1 "$mem" lan 0 "$bin"
bl_put 1 "$work"
bl_warm train.py --gpu off --mem 1G -e BENCH_STEPS=1
for m in "${models[@]}"; do
	for c in "${compute[@]}"; do
		cell solo "$m" "$c"
	done
done
fi

# split and auto: two nodes, links reshaped per profile without a restart.
if any_variant split || any_variant auto; then
bl_lab_up 2 "$mem" "${profiles[0]}" "$loss" "$bin"
bl_put 1 "$work"
bl_warm train.py --gpu off --mem 1G --ddp 2 -e BENCH_STEPS=1
for p in "${profiles[@]}"; do
	bl_lab_shape "$p" "$loss"
	for m in "${models[@]}"; do
		for c in "${compute[@]}"; do
			cell split "$m" "$c" --ddp 2
			cell auto "$m" "$c"
		done
	done
done
fi
bl_lab_down

[[ "${BL_DRY:-}" == 1 ]] && exit 0
bl_table
# Did the matrix contain both outcomes? Compare split vs solo medians per cell.
python3 - "$BL_CSV" "$mode" <<'PY'
import csv, statistics, sys
from collections import defaultdict
v = defaultdict(list)
for r in csv.DictReader(open(sys.argv[1])):
    if r["binary"] == sys.argv[2] and r["status"] == "ok" and r["script_s"]:
        v[(r["case"], r["variant"], r["profile"] if r["variant"] != "solo" else "")].append(float(r["script_s"]))
m = {k: statistics.median(x) for k, x in v.items()}
split_wins = solo_wins = 0
for (case, var, prof), t in m.items():
    if var != "split" or (case, "solo", "") not in m:
        continue
    solo = m[(case, "solo", "")]
    print("%-12s %-5s solo %7.2fs split %7.2fs -> %s" % (case, prof, solo, t, "split" if t < solo else "solo"))
    split_wins += t < solo
    solo_wins += t >= solo
if not (split_wins and solo_wins):
    print("FAIL: the matrix needs a cell where split wins (%d) and one where solo wins (%d)" % (split_wins, solo_wins))
    sys.exit(1)
print("both outcomes present: split wins %d cell(s), solo wins %d (emulated)" % (split_wins, solo_wins))
PY
