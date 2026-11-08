#!/usr/bin/env bash
# stage.sh: copy every file a run names onto each PC's ~/pipedpeer-lab/work/.
#
#   stage.sh [N] [-h]
#
# N is the number of PCs (default: every PC not in excluded.txt). Per PC it
# rsyncs, flat into work/: stamp.py, the workloads' *.py
# (docs/benchmarks/workloads/), pipedpeer-demo/cpu.py, and baselines/ into
# work/baselines/. Each PC is retried by lab_rsync on transport errors only;
# a PC that still fails is named in the output and in logs/stage/FAILED, the
# others stay staged, and the exit status is 1.

set -euo pipefail

D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$D/lib.sh"

stage_usage() {
	cat <<'USAGE'
usage: stage.sh [N] [-h]

Copies stamp.py, docs/benchmarks/workloads/*.py, baselines/ and
pipedpeer-demo/cpu.py to ~/pipedpeer-lab/work/ on the first N available PCs
(default: all). Exit 1 and a FAILED list when a PC cannot be staged.
USAGE
}

# _stage_one <pc>: make work/, copy the files; 0 or the failing rc.
_stage_one() {
	local pc="$1" wl="$REPO/docs/benchmarks/workloads"
	local -a files=("$REPO/scripts/campaign/stamp.py" "$REPO/pipedpeer-demo/cpu.py")
	local f
	shopt -s nullglob
	for f in "$wl"/*.py; do
		files+=("$f")
	done
	shopt -u nullglob
	lab_ssh "$pc" 'mkdir -p ~/pipedpeer-lab/work/baselines' || return $?
	lab_rsync "${files[@]}" "$pc:pipedpeer-lab/work/" || return $?
	lab_rsync --exclude '__pycache__' "$wl/baselines/" "$pc:pipedpeer-lab/work/baselines/" || return $?
}

stage_main() {
	local n=""
	while (($#)); do
		case "$1" in
			-h | --help) stage_usage; return 0 ;;
			[0-9]*) n="$1"; shift ;;
			*) stage_usage >&2; echo "stage.sh: unknown argument $1" >&2; return 2 ;;
		esac
	done
	if [[ -z "$n" ]]; then
		n="$(_lab_available_hosts | wc -l)"
	fi
	local hfile pc
	hfile="$(lab_hosts "$n")" || return 1

	local logdir="$LAB/logs/stage"
	mkdir -p "$logdir"
	rm -f "$logdir/FAILED" "$logdir"/rc-*
	local -a pids=()
	while IFS= read -r pc; do
		[[ -n "$pc" ]] || continue
		(
			rc=0
			_stage_one "$pc" >"$logdir/$pc.log" 2>&1 || rc=$?
			echo "$rc" >"$logdir/rc-$pc"
		) &
		pids+=("$!")
	done <"$hfile"
	wait "${pids[@]}" || true

	local ok=0 failed=0 rc
	while IFS= read -r pc; do
		[[ -n "$pc" ]] || continue
		rc="$(cat "$logdir/rc-$pc" 2>/dev/null || echo missing)"
		if [[ "$rc" == 0 ]]; then
			ok=$((ok + 1))
		else
			failed=$((failed + 1))
			echo "$pc" >>"$logdir/FAILED"
			lab_log "stage: $pc FAILED (rc $rc; see $logdir/$pc.log)"
			echo "stage FAILED $pc" >&2
		fi
	done <"$hfile"
	printf 'STAGE ok=%d failed=%d\n' "$ok" "$failed"
	lab_log "stage: ok=$ok failed=$failed"
	((failed == 0))
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	stage_main "$@"
fi
