#!/usr/bin/env bash
# calibrate.sh (brief S9b): size the workloads so the plain N=1 run on pc00
# takes about --target seconds, and write $LAB/sizes.env.
#
#   calibrate.sh [--target 120] [--timeout 1800] [--force] [-h]
#
# On pc00 it runs the plain (venv python) W1 (BENCH_ITEMS=48, BENCH_PROCS=
# nproc), W2 (--items 128), W3 (BENCH_TREES=150) and T1 (BENCH_EPOCHS=30, one
# rank) through lab_detach, measures each from the run's own t0/t1, and sets
# size = round(default * target / measured). W1's weak size is the items one
# PC does in 60 s. A run that fails, times out or measures 0 s stops the
# script before any file is written: a half-calibrated sizes.env would
# change sizes mid-campaign. An existing sizes.env is never overwritten
# without --force.
#
# Sourced by calibrate_test.sh for calibrate_scale, which is pure.

set -euo pipefail

D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$D/lib.sh"

CAL_W1_ITEMS=48
CAL_W2_ITEMS=128
CAL_W3_TREES=150
CAL_T1_EPOCHS=30
CAL_WEAK_TARGET_S=60

calibrate_usage() {
	cat <<'USAGE'
usage: calibrate.sh [--target 120] [--timeout 1800] [--force] [-h]

Runs the plain W1, W2, W3 and T1 once each on pc00 and writes
$LAB/sizes.env so each takes about --target seconds (default 120).
Refuses to overwrite an existing sizes.env without --force.
--timeout is the per-run limit in seconds (default 1800).
USAGE
}

# calibrate_scale <default> <measured_s> <target_s>: round(default * target /
# measured), at least 1. Fails on a measured time that is not above 0: that is
# a failed measurement, not a very fast workload.
calibrate_scale() {
	local default="$1" measured="$2" target="$3"
	awk -v d="$default" -v m="$measured" -v t="$target" 'BEGIN {
		if (m !~ /^[0-9]+([.][0-9]+)?$/ || m + 0 <= 0) exit 1
		v = int(d * t / m + 0.5)
		if (v < 1) v = 1
		print v
	}'
}

# _cal_run <tag> <command>: run it on pc00 and print the measured seconds.
_cal_run() {
	local tag="$1" cmd="$2" run_id dir code rc=0 secs
	run_id="CAL-$tag-$(date +%s)"
	lab_detach pc00 "$run_id" "$cmd" >&2 || {
		lab_log "calibrate $tag: could not start the run"
		return 1
	}
	code="$(lab_wait pc00 "$run_id" "$TIMEOUT_S")" || rc=$?
	if ((rc != 0)); then
		lab_log "calibrate $tag: no exit code (lab_wait $rc); the unit may still be running"
		return 1
	fi
	dir="$LAB/hosts/pc00/runs/$run_id"
	lab_fetch pc00 "$run_id" "$dir" >&2 || {
		lab_log "calibrate $tag: fetch of $run_id failed"
		return 1
	}
	if [[ "$code" != 0 ]]; then
		lab_log "calibrate $tag: the plain run exited $code (see $dir/err)"
		return 1
	fi
	secs="$(awk -v a="$(cat "$dir/t0" 2>/dev/null || true)" -v b="$(cat "$dir/t1" 2>/dev/null || true)" \
		'BEGIN { if (a == "" || b == "" || b + 0 <= a + 0) exit 1; printf "%.3f", (b - a) / 1e9 }')" || {
		lab_log "calibrate $tag: no usable t0/t1 in $dir"
		return 1
	}
	printf '%s\n' "$secs"
}

calibrate_main() {
	local target=120 force=0
	TIMEOUT_S=1800
	while (($#)); do
		case "$1" in
			--target) target="${2:?--target needs seconds}"; shift 2 ;;
			--timeout) TIMEOUT_S="${2:?--timeout needs seconds}"; shift 2 ;;
			--force) force=1; shift ;;
			-h | --help) calibrate_usage; return 0 ;;
			*) calibrate_usage >&2; echo "calibrate.sh: unknown argument $1" >&2; return 2 ;;
		esac
	done
	if [[ -s "$LAB/sizes.env" && "$force" != 1 ]]; then
		lab_log "calibrate: $LAB/sizes.env exists; sizes never change mid-campaign. Use --force to redo it."
		echo "refused: $LAB/sizes.env exists (use --force)" >&2
		return 1
	fi

	local procs
	procs="$(awk -F'\t' '$1 == "pc00" { print $5; exit }' "$LAB/hosts.tsv" 2>/dev/null || true)"
	if [[ ! "$procs" =~ ^[0-9]+$ ]]; then
		lab_log "calibrate: no nproc for pc00 in $LAB/hosts.tsv; run scan.sh/inventory.sh first"
		return 1
	fi

	# shellcheck disable=SC2088  # the tilde is for the PC's shell
	local py='~/pipedpeer-lab/venv/bin/python'
	local s1 s2 s3 st
	s1="$(_cal_run W1 "export BENCH_ITEMS=$CAL_W1_ITEMS BENCH_PROCS=$procs; $py pool_map.py")" || return 1
	s2="$(_cal_run W2 "$py cpu.py --items $CAL_W2_ITEMS")" || return 1
	s3="$(_cal_run W3 "export BENCH_TREES=$CAL_W3_TREES; $py w3_rf.py")" || return 1
	st="$(_cal_run T1 "export BENCH_EPOCHS=$CAL_T1_EPOCHS; $py t1_mlp.py")" || return 1

	local w1 w1weak w2 w3 t1
	w1="$(calibrate_scale "$CAL_W1_ITEMS" "$s1" "$target")" || { lab_log "calibrate: W1 measured [$s1] s"; return 1; }
	w1weak="$(calibrate_scale "$CAL_W1_ITEMS" "$s1" "$CAL_WEAK_TARGET_S")" || return 1
	w2="$(calibrate_scale "$CAL_W2_ITEMS" "$s2" "$target")" || { lab_log "calibrate: W2 measured [$s2] s"; return 1; }
	w3="$(calibrate_scale "$CAL_W3_TREES" "$s3" "$target")" || { lab_log "calibrate: W3 measured [$s3] s"; return 1; }
	t1="$(calibrate_scale "$CAL_T1_EPOCHS" "$st" "$target")" || { lab_log "calibrate: T1 measured [$st] s"; return 1; }

	local day tmp
	day="$(date +%F)"
	tmp="$LAB/sizes.env.tmp"
	cat >"$tmp" <<ENV
BENCH_PROCS=$procs
W1_ITEMS=$w1
W1_WEAK_PER_PC=$w1weak
W2_ITEMS=$w2
W3_TREES=$w3
T1_EPOCHS=$t1
# calibrated $day on pc00 plain W1=${s1}s at $CAL_W1_ITEMS items, target ${target}s
# calibrated $day on pc00 plain W2=${s2}s at $CAL_W2_ITEMS items, target ${target}s
# calibrated $day on pc00 plain W3=${s3}s at $CAL_W3_TREES trees, target ${target}s
# calibrated $day on pc00 plain T1=${st}s at $CAL_T1_EPOCHS epochs, target ${target}s
# W1_WEAK_PER_PC: items one PC does in ${CAL_WEAK_TARGET_S}s
ENV
	mv "$tmp" "$LAB/sizes.env"
	lab_log "calibrate: wrote $LAB/sizes.env (W1=$w1 W1_WEAK_PER_PC=$w1weak W2=$w2 W3=$w3 T1=$t1)"
	cat "$LAB/sizes.env"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	calibrate_main "$@"
fi
