#!/usr/bin/env bash
# Training: distributed against a single machine, with the numbers that decide
# whether distributing was worth it.
#
# Distributing is not automatically an improvement and the failure is silent -
# the loss comes out right either way, so a ring that costs more than it saves
# looks exactly like one that works. Measured on two machines over a home link:
#
#   one machine        55.0s   loss 0.0875
#   two ranks, fp16    71.1s   loss 0.0875   sync 37.0s
#   two ranks, int8    53.0s   loss 0.0922   sync 19.4s
#
# The pattern that matters is that the middle row is a loss dressed as a
# feature, and only the sync column says so.
#
#   scripts/bench-ddp.sh              # single vs 2 ranks, default encoding
#   RANKS=3 scripts/bench-ddp.sh      # wider ring
#   scripts/bench-ddp.sh --gpu        # let the closure use a GPU
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cli="${PIPEDPEER:-$repo_root/bin/pipedpeer}"
ranks="${RANKS:-2}"
gpu="off"
[[ "${1:-}" == "--gpu" ]] && gpu="force"

if [[ ! -x "$cli" ]]; then
	echo "no binary at $cli; build it first or set PIPEDPEER=" >&2
	exit 2
fi

# Not $TMPDIR: /tmp is a tmpfs of limited size on these machines, and the
# workspace travels with the job.
work="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/bench-ddp"
rm -rf "$work"
mkdir -p "$work"
: > "$work/.pipedpeerignore"
# Beside this script when copied to a test machine; under scripts/ in a
# checkout. Assuming the checkout is what stopped the other harnesses running
# where the cluster actually is.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
driver="$here/bench-train.py"
[[ -f "$driver" ]] || driver="$repo_root/scripts/bench-train.py"
[[ -f "$driver" ]] || { echo "cannot find bench-train.py beside $here" >&2; exit 2; }
cp "$driver" "$work/train.py"

# Returns "seconds loss" from a run, or "- -" when it did not finish.
run_one() {
	local label="$1"
	shift
	local log="$work/${label}.log"
	if ! (cd "$work" && "$cli" run train.py --gpu "$gpu" "$@") > "$log" 2>&1; then
		echo "- -"
		return
	fi
	local secs loss
	secs="$(grep -oE 'training done in [0-9.]+s' "$log" | head -1 | grep -oE '[0-9.]+' || true)"
	loss="$(grep -oE 'final loss: [0-9.e-]+' "$log" | head -1 | sed 's/final loss: //' || true)"
	echo "${secs:--} ${loss:--}"
}

# Sync seconds, from the receipt the shim prints at exit.
sync_secs() {
	grep -oE 'ddp: [0-9]+ sync\(s\), [0-9.]+s' "$work/$1.log" | head -1 |
		grep -oE '[0-9.]+s' | tail -1 | tr -d 's' || true
}

printf 'workload: %s\n' "$(grep -oE 'BATCH, EPOCHS = [0-9]+ // world, [0-9]+' "$work/train.py" || echo 'see bench-train.py')"
printf '\n%-22s %10s %12s %10s\n' "configuration" "time" "loss" "sync"

read -r t_single l_single <<< "$(run_one single)"
printf '%-22s %9ss %12s %10s\n' "one machine" "$t_single" "$l_single" "-"

read -r t_fp16 l_fp16 <<< "$(run_one "ranks${ranks}_fp16" --ddp "$ranks")"
printf '%-22s %9ss %12s %9ss\n' "$ranks ranks, fp16" "$t_fp16" "$l_fp16" "$(sync_secs "ranks${ranks}_fp16")"

read -r t_int8 l_int8 <<< "$(run_one "ranks${ranks}_int8" --ddp "$ranks" -e PIPEDPEER_DDP_INT8=1)"
printf '%-22s %9ss %12s %9ss\n' "$ranks ranks, int8" "$t_int8" "$l_int8" "$(sync_secs "ranks${ranks}_int8")"

echo
# The run says this itself now; repeat the best line here so the table is
# self-contained.
for label in "ranks${ranks}_fp16" "ranks${ranks}_int8"; do
	verdict="$(grep -oE 'the ring (paid for itself|did NOT pay for itself)[^\n]*' "$work/$label.log" | head -1 || true)"
	[[ -n "$verdict" ]] && echo "  $label: $verdict"
done

if [[ "$t_single" == "-" ]]; then
	echo "FAIL: the single-machine baseline did not finish; there is nothing to compare against" >&2
	exit 1
fi
echo
echo "logs in $work"
