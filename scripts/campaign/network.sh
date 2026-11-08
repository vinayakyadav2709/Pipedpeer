#!/usr/bin/env bash
# Network measurements, run after install.sh: per PC != pc00 an iperf3 TCP run
# from pc00 (Mb/s), ping rtt avg/mdev from pc00, reachability of the product
# port 4242 from pc00, and `pipedpeer doctor` on the PC -> $LAB/network.csv
# (`id,ip,iperf_mbps,rtt_avg_ms,rtt_mdev_ms,tcp4242,doctor_ok`).
# Text parsing lives in net_ping_parse / net_iperf_parse for the saved-text
# test; nothing here runs unless invoked.
set -euo pipefail

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
# shellcheck source=/dev/null
. "$REPO/scripts/campaign/lib.sh"

usage() {
	cat <<'EOF'
usage: network.sh [-h]

Measure the lab network and write $LAB/network.csv with columns
id,ip,iperf_mbps,rtt_avg_ms,rtt_mdev_ms,tcp4242,doctor_ok (one row per PC
except pc00; iperf3 and ping run from pc00). Run after install.sh.
EOF
}

# net_ping_parse <file>: "<avg> <mdev>" from `ping -c N -q` output; empty on
# 100% loss (no rtt line), with a warning logged.
net_ping_parse() {
	local file="${1:?usage: net_ping_parse <file>}" line
	if [[ ! -f "$file" ]]; then
		echo "net_ping_parse: no file $file" >&2
		return 1
	fi
	line=$(sed -n 's|^rtt min/avg/max/mdev = \([0-9.]*\)/\([0-9.]*\)/[0-9.]*/\([0-9.]*\) ms$|\2 \3|p' "$file" | tail -n 1)
	if [[ -z "$line" ]]; then
		lab_log "network: ping: no rtt line (100% loss?) in $file"
		return 0
	fi
	printf '%s\n' "$line"
}

# net_iperf_parse <file>: Mb/s from `iperf3 -J` output
# (end.sum_received.bits_per_second / 1e6); empty when iperf reported an error
# or the JSON is unusable, with a warning logged.
net_iperf_parse() {
	local file="${1:?usage: net_iperf_parse <file>}" out
	if [[ ! -f "$file" ]]; then
		echo "net_iperf_parse: no file $file" >&2
		return 1
	fi
	if ! out=$(python3 -c '
import json, sys
try:
    data = json.load(open(sys.argv[1]))
except Exception as exc:
    print("bad json: %s" % exc)
    sys.exit(3)
if "error" in data:
    print("error: %s" % data["error"])
    sys.exit(3)
bps = data.get("end", {}).get("sum_received", {}).get("bits_per_second")
print("" if bps is None else "%.1f" % (bps / 1e6))
' "$file"); then
		lab_log "network: iperf3: $out ($file)"
		return 0
	fi
	printf '%s\n' "$out"
}

main() {
	if [[ ! -f "$LAB/hosts.tsv" ]]; then
		echo "no $LAB/hosts.tsv: run scan.sh first" >&2
		return 1
	fi
	mkdir -p "$LAB/logs/network"
	local pc ip outdir="$LAB/logs/network" mbps pair avg mdev tcp dok ok=0
	{
		printf 'id,ip,iperf_mbps,rtt_avg_ms,rtt_mdev_ms,tcp4242,doctor_ok\n'
		while IFS= read -r pc; do
			[[ "$pc" == "pc00" ]] && continue
			if ! ip=$(lab_ip "$pc"); then
				continue
			fi
			# iperf3 server with -s -1 exits after one test; no cleanup needed.
			if ! lab_ssh "$pc" 'iperf3 -s -1 -D' >/dev/null 2>&1; then
				lab_log "network: iperf3 server failed on $pc; no row"
				continue
			fi
			mbps=""
			if out=$(lab_ssh pc00 "iperf3 -c $ip -t 5 -J"); then
				printf '%s\n' "$out" >"$outdir/iperf-$pc.json"
				mbps=$(net_iperf_parse "$outdir/iperf-$pc.json")
			else
				lab_log "network: iperf3 client failed pc00 -> $pc (exit $?); no row"
			fi
			pair=""
			avg="" # a failed ping must not keep the previous PC's numbers
			mdev=""
			if out=$(lab_ssh pc00 "ping -c 10 -q $ip"); then
				printf '%s\n' "$out" >"$outdir/ping-$pc.txt"
				pair=$(net_ping_parse "$outdir/ping-$pc.txt")
				if [[ -n "$pair" ]]; then
					avg=${pair% *}
					mdev=${pair#* }
				fi
			else
				lab_log "network: ping failed pc00 -> $pc (exit $?); no row"
			fi
			tcp=0
			if lab_ssh pc00 "timeout 3 bash -c 'echo > /dev/tcp/$ip/4242'" >/dev/null 2>&1; then
				tcp=1
			else
				lab_log "network: tcp 4242 from pc00 to $pc closed (tcp4242=0)"
			fi
			dok=0
			if lab_ssh "$pc" 'pipedpeer doctor' >/dev/null 2>&1; then
				dok=1
			else
				lab_log "network: pipedpeer doctor failed on $pc (doctor_ok=0)"
			fi
			if [[ -n "$mbps" && -n "$avg" ]]; then
				printf '%s,%s,%s,%s,%s,%s,%s\n' "$pc" "$ip" "$mbps" "$avg" "$mdev" "$tcp" "$dok"
			else
				lab_log "network: $pc: incomplete row (mbps='${mbps:-none}' ping='${avg:-none}')"
				printf '%s,%s,%s,%s,%s,%s,%s\n' "$pc" "$ip" "$mbps" "$avg" "$mdev" "$tcp" "$dok"
			fi
			ok=$((ok + 1))
		done < <(_lab_available_hosts)
	} >"$LAB/network.csv"
	lab_log "network: ok=$ok -> $LAB/network.csv"
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
