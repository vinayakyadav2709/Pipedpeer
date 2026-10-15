#!/usr/bin/env bash
# One before/after case per W10 speedup, on the emulated lab.
#
# BEFORE is the c7e2c7f binary (the pure-move split every W10 change starts
# from), AFTER is this checkout. Each case runs the same workload with the same
# flags on both, so a change in a default (int8 on, fp32 averaging) shows up
# exactly as a user would see it. A case whose feature has a new spelling
# gives BEFORE its own flags (diloco: SYNC_EVERY=auto before, --ddp-sync fast
# after). Data-path cases also run on one node of the same size: that is the
# speedup baseline (1 container vs N), never the 15-core host.
#
# Every number is EMULATED: equal cgroup quotas plus tc netem on one host.
#
#   scripts/bench-w10.sh before            # fill the BEFORE column
#   scripts/bench-w10.sh after             # fill the AFTER column
#   scripts/bench-w10.sh table             # medians as markdown
#   CASES="poolsend zstd" scripts/bench-w10.sh after
#
# Knobs: CASES (all), BL_REPS (3), POOL_NODES (3), POOL_MEM (1.5g),
# TRAIN_MEM (2g), HUB_MEM (1.5g), SPEEDUP (1: also run the one-node baseline),
# ONLY ("group:variant ...", e.g. "train:n2 pool:n1 cold:n3", to resume).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bench-lib.sh"

# id | group | script | profile | BEFORE flags | AFTER flags ("=" = same)
# Groups decide the lab: pool = POOL_NODES x POOL_MEM, train = 2 x TRAIN_MEM,
# hub = 3 x HUB_MEM, cold = a fresh lab per repetition (nothing warm).
# Training flags carry --gpu off (lab containers see the host NVIDIA driver and
# would fetch the 6.6 GB CUDA wheels) and --mem 1G (the daemon estimates 2.1 GB for a torch job,
# more than a 2g node has, and rejects it until the 600 s timeout).
# PERF-HUNT appends one line per finding here.
cases_table() {
	cat <<'EOF'
int8|train|train.py|wifi|--gpu off --mem 1G --ddp 2 -e BENCH_WIDTH=1024 -e BENCH_DEPTH=4 -e BENCH_BATCH=2048|=
fp32avg|train|train.py|wifi|--gpu off --mem 1G --ddp 2 -e PIPEDPEER_DDP_SYNC_EVERY=8 -e BENCH_WIDTH=512 -e BENCH_BATCH=2048 -e BENCH_STEPS=120|=
diloco|train|train.py|wifi|--gpu off --mem 1G --ddp 2 -e PIPEDPEER_DDP_SYNC_EVERY=auto -e BENCH_WIDTH=512 -e BENCH_BATCH=2048 -e BENCH_STEPS=120|--gpu off --mem 1G --ddp 2 --ddp-sync fast -e BENCH_WIDTH=512 -e BENCH_BATCH=2048 -e BENCH_STEPS=120
overlap|train|train.py|lan|--gpu off --mem 1G --ddp 2 -e BENCH_WIDTH=1024 -e BENCH_DEPTH=3 -e BENCH_BATCH=4096|=
nohub|hub|train.py|lan|--gpu off --mem 1G --ddp 3 -e BENCH_WIDTH=1024 -e BENCH_DEPTH=3 -e BENCH_BATCH=4096|=
chunksize|pool|pool.py|wifi|-e BENCH_ITEMS=2000 -e BENCH_SPIN=3000|=
prefetch|pool|pool.py|5g|-e BENCH_ITEMS=300 -e BENCH_SPIN=60000|=
tail|pool|pool.py|wifi|-e BENCH_ITEMS=96 -e BENCH_UNEVEN=1 -e BENCH_SPIN=60000|=
poolsend|pool|pool.py|lan|-e BENCH_ITEMS=48 -e BENCH_IN_KB=8192 -e BENCH_SPIN=100000|=
fingerprint|pool|pool.py|wifi|-e BENCH_ITEMS=96 -e BENCH_SHARED_KB=32768 -e BENCH_SPIN=100000|=
zstd|pool|pool.py|5g|-e BENCH_ITEMS=48 -e BENCH_IN_KB=2048 -e BENCH_COMPRESSIBLE=1 -e BENCH_SPIN=100000|=
pickle5|pool|poolnp.py|lan|-e BENCH_ITEMS=48 -e BENCH_IN_MB=8|=
linkcost|pool|pool.py|5g|-e BENCH_ITEMS=48 -e BENCH_IN_KB=4096 -e BENCH_SPIN=20000|=
stats|pool|pool.py|wifi|-e BENCH_ITEMS=192 -e BENCH_SPIN=80000|=
noop|pool|noop.py|lan||=
cold|cold|pool.py|lan|-e BENCH_ITEMS=48 -e BENCH_SPIN=100000|=
EOF
}
# Not in the lab: multi-GPU fast path (needs a host with two GPUs; recorded in
# w10.md as a real-hardware check), cache.nixos.org fetch (the cold case covers
# it when the containers have internet), env-wrapper skip and ms seed (both
# show in noop's wall clock).

mode="${1:-}"
case "$mode" in
before | after) ;;
table)
	bl_table "$BL_OUT/w10/results.csv"
	exit 0
	;;
*)
	sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac

want="${CASES:-}"
pool_nodes="${POOL_NODES:-3}"
speedup="${SPEEDUP:-1}"

bl_init w10
bl_build "$mode"
bin="$BL_BIN"
work="$BL_OUT/w10/work"
bl_workloads "$work"

selected() { # selected <group> -> "id|script|profile|flags" lines
	cases_table | while IFS='|' read -r id group script profile bflags aflags; do
		[[ "$group" == "$1" ]] || continue
		[[ -z "$want" || " $want " == *" $id "* ]] || continue
		local flags="$bflags"
		[[ "$mode" == after && "$aflags" != "=" ]] && flags="$aflags"
		echo "$id|$script|$profile|$flags"
	done
}

# shellcheck disable=SC2086
run_group() { # run_group <group> <nodes> <mem> <variant>
	local group="$1" nodes="$2" mem="$3" variant="$4" lines id script profile flags warmed=""
	# ONLY="pool:n1 train:n2 ..." runs just those group:variant sets (resume).
	[[ -z "${ONLY:-}" || " $ONLY " == *" $group:$variant "* ]] || return 0
	lines="$(selected "$group")"
	[[ -n "$lines" ]] || return 0
	bl_lab_up "$nodes" "$mem" lan 0 "$bin"
	bl_put 1 "$work"
	while IFS='|' read -r id script profile flags; do
		# One node: --ddp N cannot be met, so the baseline drops it.
		[[ "$nodes" == 1 ]] && flags="$(sed -E 's/--ddp [0-9]+//; s/--ddp-sync [a-z]+//' <<< "$flags")"
		bl_lab_shape "$profile" 0
		if [[ " $warmed " != *" $script "* ]]; then
			bl_warm "$script" $flags -e BENCH_STEPS=1 -e BENCH_ITEMS=4
			warmed="$warmed $script"
		fi
		bl_reps "$id" "$variant" "$mode" "$script" $flags
	done <<< "$lines"
}

run_group pool "$pool_nodes" "${POOL_MEM:-1.5g}" "n$pool_nodes"
run_group train 2 "${TRAIN_MEM:-2g}" n2
run_group hub 3 "${HUB_MEM:-1.5g}" n3
if [[ "$speedup" == 1 ]]; then
	run_group pool 1 "${POOL_MEM:-1.5g}" n1
	run_group train 1 "${TRAIN_MEM:-2g}" n1
fi

# cold: a fresh lab per repetition, nothing warmed; the wall clock is the
# environment build on node 1 plus shipping it (or peers fetching it).
# shellcheck disable=SC2086
while IFS='|' read -r id script profile flags; do
	[[ -n "$id" ]] || continue
	for r in $(seq 1 "$BL_REPS"); do
		bl_lab_up "$pool_nodes" "${POOL_MEM:-1.5g}" "$profile" 0 "$bin"
		bl_put 1 "$work"
		bl_run "$id" "n$pool_nodes" "$mode" "$r" "$script" $flags || break
	done
done <<< "$([[ -z "${ONLY:-}" || " $ONLY " == *" cold:"* ]] && selected cold)"
bl_lab_down

bl_table
