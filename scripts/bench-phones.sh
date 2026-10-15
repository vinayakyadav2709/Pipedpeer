#!/usr/bin/env bash
# Phone-fleet benchmark on the emulated lab (scripts/lab-emu.sh): N phone-sized
# nodes against one laptop-sized node, same workloads. EMULATED: cgroup CPU and
# memory quotas plus tc netem on one host; it says how a fleet of weak nodes
# scales, not what a real Snapdragon does.
#
# Calibration (the quota a phone container gets):
#   phone_cpus = laptop_cpus * PHONE_GB6 / LAPTOP_GB6
#   Geekbench 6 multi-core, public browser scores (approximate, from memory,
#   not re-fetched when this was written; override with the knobs below):
#     Samsung Galaxy S20 FE 5G, Snapdragon 865:  ~3000   (PHONE_GB6)
#     this laptop, AMD Ryzen 7 5800H:            ~7600   (LAPTOP_GB6)
#   ratio ~0.39, so a 3-cpu laptop node maps to 1.18-cpu phones (10 x 1.18 =
#   11.8 cpus fit the host's 15). Ceiling: a quota scales throughput, not per-core
#   speed (the SD865's cores are ~0.5x a Zen 3 core, and its 1+3+4 big.LITTLE
#   layout is not modelled), and proot/Android overhead is not included.
#
#   scripts/bench-phones.sh [--dry-run]      # prints the plan; a real run needs no flag
#
# Knobs: PHONES (10), LAPTOP_CPUS (3), PHONE_MEM (largest <= 768m that fits the
#   container cap with 1 GB headroom), LAPTOP_MEM (2g),
#   PHONE_GB6 (3000), LAPTOP_GB6 (7600), PHONE_PROFILE (wifi), PHONE_LOSS (0),
#   PHONE_BIN (binary; default: built from the checkout), plus bench-lib.sh's
#   BL_REPS, BL_OUT, BL_RUN_TIMEOUT, BL_LAB_WAIT.
#   PHONE_CASES ("fleet mixed"): fleet = N phones vs one laptop (above);
#   mixed = one ring of one laptop node + MIXED_PHONES (3) phone nodes, all
#   MIXED_MEM (1g), training train.py for MIXED_STEPS (400) steps under
#   --ddp-sync fast twice: "equal" (PIPEDPEER_DILOCO_ROUND_SLACK=0, every
#   local round waits for its slowest rank) and "timed" (MIXED_SLACK, default 2:
#   a round over 2x the ring's median step time skips to its sync); the laptop
#   node alone trains the same steps as the loss reference.
#   MIXED_ENV: extra -e flags for both variants (default: DILOCO-TUNE's fast
#   default, plain outer step lr 1 momentum 0, H 64).
# Memory: a real S20 FE has ~4-5 GB usable, so this emulation is memory-starved
#   (a few hundred MB per phone); torch workloads may be OOM-killed that real
#   phones would survive. The host is a Ryzen 7 5800H.
# Output: $BL_OUT/phones/results.csv (every row emulated=1). Memory: lab-emu.sh
# waits for the memory gate (cap = host RAM - 5 GB, 4 GB spare) before each lab.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bench-lib.sh"

dry=0
case "${1:-}" in
--dry-run) dry=1 ;;
"") ;;
*) sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 2 ;;
esac

n="${PHONES:-10}" lap_cpus="${LAPTOP_CPUS:-3}" lmem="${LAPTOP_MEM:-2g}"
# Container cap = host RAM - 5 GB (lab.sh). Default phone memory: the largest
# size (MB, max 768) that leaves 1 GB headroom under the cap with the laptop node.
to_mb() { case "$1" in *g) awk -v v="${1%g}" 'BEGIN { print int(v * 1024) }' ;; *m) echo "${1%m}" ;; *) echo "bad size '$1' (use 512m or 2g)" >&2; exit 2 ;; esac; }
cap_mb=$(awk '/^MemTotal:/ { print int($2 / 1024) - 5120 }' /proc/meminfo)
lmem_mb="$(to_mb "$lmem")"
pmem_mb="$(to_mb "${PHONE_MEM:-$(( (cap_mb - lmem_mb - 1024) / n ))m}")"
if [[ -z "${PHONE_MEM:-}" ]] && ((pmem_mb > 768)); then pmem_mb=768; fi
pmem="${pmem_mb}m"
if ((n * pmem_mb + lmem_mb > cap_mb)); then
	echo "bench-phones: refusing: $n x $pmem + $lmem = $((n * pmem_mb + lmem_mb)) MB is over the $cap_mb MB container cap (host RAM - 5 GB); use fewer PHONES or a smaller PHONE_MEM/LAPTOP_MEM" >&2
	exit 1
fi
profile="${PHONE_PROFILE:-wifi}" loss="${PHONE_LOSS:-0}"
phone_cpus="$(awk -v l="$lap_cpus" -v p="${PHONE_GB6:-3000}" -v t="${LAPTOP_GB6:-7600}" 'BEGIN { printf "%.2f", l * p / t }')"
cases=("pool pool.py" "train train.py")
pcases=" ${PHONE_CASES:-fleet mixed} "
mixn="${MIXED_PHONES:-3}" mixmem="${MIXED_MEM:-1g}" mixsteps="${MIXED_STEPS:-400}"
mixcpus="$lap_cpus$(for ((i = 0; i < mixn; i++)); do printf ' %s' "$phone_cpus"; done)"

if ((dry)); then
	cat <<PLAN
bench-phones plan (EMULATED, nothing is started):
  calibration: phone_cpus = $lap_cpus * ${PHONE_GB6:-3000} / ${LAPTOP_GB6:-7600} = $phone_cpus
  baseline lab: 1 x ($lap_cpus cpu, $lmem), profile lan
  phone lab:    $n x ($phone_cpus cpu, $pmem), profile $profile, loss $loss%
  total cpus:   baseline $lap_cpus, phones $(awk -v a="$n" -v b="$phone_cpus" 'BEGIN { print a * b }') (host has $(nproc))
  per lab, per workload: ${cases[*]##* } x $BL_REPS timed reps (BL_REPS)
    baseline: pipedpeer run <script>
    phones:   pool -> pipedpeer run pool.py ; train -> pipedpeer run --ddp $n train.py
  mixed ring:   cpus "$mixcpus", $mixmem each, profile $profile; train.py $mixsteps steps,
    --ddp $((mixn + 1)) --ddp-sync fast, variants equal (ROUND_SLACK=0) and timed (${MIXED_SLACK:-2})
  cases:        $pcases
  results: $BL_OUT/phones/results.csv
PLAN
	exit 0
fi

# phone_lab_up <nodes> <cpus> <mem> <profile> <loss>: bl_lab_up with its own
# cpu count (bl_lab_up fixes 2), same wait-for-memory retry.
phone_lab_up() {
	local nodes="$1" cpus="$2" mem="$3" prof="$4" ls="$5" out="$BL_OUT/$BL_SUITE/lab-up.log" waited=0
	bl_lab_down
	bl_log "lab up: $nodes x ($cpus cpu, $mem), profile $prof, loss $ls%"
	until LAB_MEM="$mem" LAB_PROFILE="$prof" LAB_LOSS="$ls" LAB_CPUS="$cpus" PIPEDPEER="$BL_BIN" \
		LAB_API_PORT="$BL_API_PORT" LAB_NAME="$BL_LAB_NAME" "$BL_EMU" up "$nodes" >"$out" 2>&1; do
		cat "$out" >&2
		grep -qE "refusing|gave up after" "$out" || bl_die "lab-emu.sh up failed"
		((waited < ${BL_LAB_WAIT:-3600})) || bl_die "memory gate still closed after ${waited}s"
		bl_log "memory gate closed; retrying in 60 s (waited ${waited}s)"
		sleep 60
		waited=$((waited + 60))
	done
	cat "$out" >&2
	BL_NODES_UP="$nodes" BL_PROFILE="$prof" BL_LOSS="$ls"
}

BL_LAB_NAME="${BL_LAB_NAME:-phones}"
BL_EMU_PREFIX="pp${BL_LAB_NAME}-"
bl_init phones
if [[ -n "${PHONE_BIN:-}" ]]; then
	BL_BIN="$PHONE_BIN" BL_REV="custom" BL_API_PORT=4242
else
	bl_build after
fi
work="$BL_OUT/phones/work"
bl_workloads "$work"

# Baseline first: one laptop-sized node, no peers, so the run stays local.
phone_lab_up 1 "$lap_cpus" "$lmem" lan 0
bl_put 1 "$work"
if [[ "$pcases" == *" fleet "* ]]; then
	for c in "${cases[@]}"; do
		read -r name script <<<"$c"
		bl_warm "$script"
		bl_reps "$name" laptop1 after "$script"
	done
fi
if [[ "$pcases" == *" mixed "* ]]; then
	bl_warm train.py
	bl_reps mixtrain laptop1 after train.py -e BENCH_STEPS="$mixsteps"
fi

# Then the phone fleet; node 1 submits and pools/trains across all n.
if [[ "$pcases" == *" fleet "* ]]; then
	phone_lab_up "$n" "$phone_cpus" "$pmem" "$profile" "$loss"
	bl_put 1 "$work"
	for c in "${cases[@]}"; do
		read -r name script <<<"$c"
		flags=()
		[[ "$name" == train ]] && flags=(--ddp "$n")
		bl_warm "$script" "${flags[@]}"
		bl_reps "$name" "phones$n" after "$script" "${flags[@]}"
	done
fi

# Unequal ranks in one ring (PHONE-DDP): node 1 is the laptop and submits.
if [[ "$pcases" == *" mixed "* ]]; then
	phone_lab_up "$((mixn + 1))" "$mixcpus" "$mixmem" "$profile" "$loss"
	bl_put 1 "$work"
	read -r -a menv <<<"${MIXED_ENV:--e PIPEDPEER_DILOCO_OUTER_LR=1 -e PIPEDPEER_DILOCO_MOMENTUM=0 -e PIPEDPEER_DILOCO_H=64}"
	mflags=(--ddp "$((mixn + 1))" --ddp-sync fast -v -e BENCH_STEPS="$mixsteps" "${menv[@]}")
	bl_warm train.py "${mflags[@]}"
	bl_reps mixtrain "equal$((mixn + 1))" after train.py "${mflags[@]}" -e PIPEDPEER_DILOCO_ROUND_SLACK=0
	bl_reps mixtrain "timed$((mixn + 1))" after train.py "${mflags[@]}" -e PIPEDPEER_DILOCO_ROUND_SLACK="${MIXED_SLACK:-2}"
fi
bl_lab_down
bl_table
