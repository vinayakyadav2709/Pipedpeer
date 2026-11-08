#!/usr/bin/env bash
# Clock offsets between the driver and every PC: three samples per PC of the
# driver endpoint around one remote `date +%s%N` over the campaign ssh config,
# the sample with the lowest round trip kept, written to $LAB/clocks.csv
# (`id,offset_ms,rtt_ms,measured_at`). offset in ms = remote - (t_a+t_b)/2 with
# t_a/t_b taken on the driver; analyse.py corrects per-PC times with it.
set -euo pipefail

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
# shellcheck source=/dev/null
. "$REPO/scripts/campaign/lib.sh"

usage() {
	cat <<'EOF'
usage: clocks.sh [-h]

Measure the driver<->PC clock offsets and write $LAB/clocks.csv with columns
id,offset_ms,rtt_ms,measured_at (offset in ms = remote - midpoint of the two
driver timestamps; best round trip of three samples per PC).
EOF
}

# clock_sample <pc>: one sample; prints offset_ms then rtt_ms, one per line.
clock_sample() {
	local pc="${1:?usage: clock_sample <pc>}" ta tr tb
	ta=$(date +%s%N)
	tr=$(lab_ssh "$pc" 'date +%s%N') || return 1
	tb=$(date +%s%N)
	awk -v a="$ta" -v b="$tb" -v r="$tr" 'BEGIN {
		printf "%.3f\n%.3f\n", (r - (a + b) / 2) / 1e6, (b - a) / 1e6
	}'
}

main() {
	if [[ ! -f "$LAB/hosts.tsv" ]]; then
		echo "no $LAB/hosts.tsv: run scan.sh first" >&2
		return 1
	fi
	local pc off best_off="" best_rtt="" rc failed=0 ok=0
	{
		printf 'id,offset_ms,rtt_ms,measured_at\n'
		while IFS= read -r pc; do
			best_off=""
			best_rtt=""
			for _ in 1 2 3; do
				if off="$(clock_sample "$pc")"; then
					if [[ -z "$best_rtt" ]] || awk -v a="$(tail -n 1 <<<"$off")" -v b="$best_rtt" 'BEGIN { exit !(a + 0 < b + 0) }'; then
						best_off="$(head -n 1 <<<"$off")"
						best_rtt="$(tail -n 1 <<<"$off")"
					fi
				else
					rc=$?
					break
				fi
			done
			if [[ -z "$best_rtt" ]]; then
				lab_log "clocks: $pc sample failed (exit $rc); no clocks.csv row"
				failed=$((failed + 1))
				continue
			fi
			printf '%s,%s,%s,%s\n' "$pc" "$best_off" "$best_rtt" "$(date -Is)"
			ok=$((ok + 1))
		done < <(_lab_available_hosts)
	} >"$LAB/clocks.csv"
	lab_log "clocks: ok=$ok failed=$failed -> $LAB/clocks.csv"
	if ((ok == 0)); then
		return 1
	fi
	return 0
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	case "${1:-}" in
		-h | --help) usage; exit 0 ;;
		*) main "$@" ;;
	esac
fi
