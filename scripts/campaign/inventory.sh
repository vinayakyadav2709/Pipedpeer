#!/usr/bin/env bash
# Machine inventory for the lab campaign: one key=value line per field from
# every PC, then $LAB/machines.csv (`id,hostname,ip,os_id,...` per Contracts).
# Run after scan.sh (hosts.tsv must exist). Pure parsing lives in inv_parse /
# inv_shared_home so the saved-text test can feed logs without ssh.
#
# The inventory fan-out runs the same command on every non-excluded PC
# (hosts-all.txt, the pssh host file of all ids). A home directory on shared
# storage stops the campaign: the per-PC paths under ~/pipedpeer-lab would
# collide across machines.
set -euo pipefail

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
# shellcheck source=/dev/null
. "$REPO/scripts/campaign/lib.sh"

usage() {
	cat <<'EOF'
usage: inventory.sh [-h]

Fan out one inventory command over every non-excluded PC and write
$LAB/machines.csv (columns id,hostname,ip,os_id,os_version,kernel,cpu_model,
sockets,cores_per_socket,threads_per_core,nproc,ram_gib,nic,nic_speed_mbps,
python3,home_fs). Needs $LAB/hosts.tsv (scan.sh).

Environment:
  LAB_DRIVER_IP  the driver's lab-side ip; default: the source ip of the
                 default route (per-PC NIC detection needs it).
EOF
}

# inv_parse <logfile>: key=value lines (the fan-out's per-PC log) -> one
# tab-separated row: os_id os_version kernel cpu_model sockets cores_per_socket
# threads_per_core nproc ram_gib nic nic_speed_mbps python3 home_fs — the
# machines.csv columns after the id,hostname,ip the caller prepends. A key
# missing from the log is an empty field; a WiFi speed (-1 or no file) is empty.
inv_parse() {
	local log="${1:?usage: inv_parse <logfile>}" line k v speed py
	local -A kv=()
	if [[ ! -f "$log" ]]; then
		echo "inv_parse: no log $log" >&2
		return 1
	fi
	while IFS= read -r line; do
		[[ "$line" == *=* ]] || continue
		k=${line%%=*}
		v=${line#*=}
		kv[$k]=${v//,/;} # machines.csv is comma separated
	done <"$log"
	speed=${kv[nic_speed_mbps]:-}
	if [[ "$speed" == "-1" ]]; then
		speed=""
	fi
	py=${kv[python3]:-}
	py=${py#Python }
	local -a f=(
		"${kv[os_id]:-}" "${kv[os_version]:-}" "${kv[kernel]:-}"
		"${kv[cpu_model]:-}" "${kv[sockets]:-}" "${kv[cores_per_socket]:-}"
		"${kv[threads_per_core]:-}" "${kv[nproc]:-}" "${kv[ram_gib]:-}"
		"${kv[nic]:-}" "$speed" "$py" "${kv[home_fs]:-}"
	)
	local IFS=$'\t'
	printf '%s\n' "${f[*]}"
}

# inv_shared_home <pc> <logfile>: 0 when the PC's home is on its own disk;
# a row on nfs/cifs prints `SHARED-HOME <pc>` and returns 1 (the campaigns's
# per-PC paths would collide), which the caller turns into a full stop.
inv_shared_home() {
	local pc="${1:?usage: inv_shared_home <pc> <logfile>}" log="${2:?}" fs
	fs=$(inv_parse "$log" | cut -f13) || return 1
	case "$fs" in
		nfs | nfs4 | cifs)
			printf 'SHARED-HOME %s\n' "$pc"
			lab_log "inventory: SHARED-HOME $pc (home_fs=$fs); per-PC ~/pipedpeer-lab paths would collide"
			return 1
			;;
	esac
	return 0
}

# inv_driver_ip: the ip PCs see as the driver (LAB_DRIVER_IP to override).
inv_driver_ip() {
	if [[ -n "${LAB_DRIVER_IP:-}" ]]; then
		printf '%s\n' "$LAB_DRIVER_IP"
		return 0
	fi
	local src
	src=$(ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]\+\).*/\1/p' | head -n 1)
	if [[ -z "$src" ]]; then
		echo "cannot determine the driver ip; set LAB_DRIVER_IP" >&2
		return 1
	fi
	printf '%s\n' "$src"
}

# hosts_all_file: hosts-all.txt = every non-excluded id, one per line, printed.
hosts_all_file() {
	local file="$LAB/hosts-all.txt"
	_lab_available_hosts >"${file}.tmp"
	mv "${file}.tmp" "$file"
	printf '%s\n' "$file"
}

# The one command the fan-out runs on every PC; @DRIVER_IP@ is replaced before
# the run (NIC detection routes to the driver).
IFS= read -r -d '' INV_REMOTE <<'EOF' || true
. /etc/os-release 2>/dev/null || true
echo "os_id=${ID:-}"
echo "os_version=${VERSION_ID:-}"
echo "kernel=$(uname -r)"
echo "cpu_model=$(lscpu | sed -n 's/^Model name:[[:space:]]*//p')"
echo "sockets=$(lscpu | sed -n 's/^Socket(s):[[:space:]]*//p')"
echo "cores_per_socket=$(lscpu | sed -n 's/^Core(s) per socket:[[:space:]]*//p')"
echo "threads_per_core=$(lscpu | sed -n 's/^Thread(s) per core:[[:space:]]*//p')"
echo "nproc=$(nproc)"
awk '/MemTotal/ { printf "ram_gib=%.2f\n", $2/1048576 }' /proc/meminfo
NIC=$(ip -4 route get @DRIVER_IP@ | sed -n 's/.* dev \([^ ]*\).*/\1/p' | head -n 1)
echo "nic=${NIC:-}"
SP=$(cat /sys/class/net/${NIC}/speed 2>/dev/null || true)
echo "nic_speed_mbps=${SP:-}"
echo "python3=$(python3 --version 2>&1)"
echo "home_fs=$(stat -f -c %T ~ 2>/dev/null || true)"
EOF

inv_parse_all() {
	local pc host ipa log row shared=0 msg
	{
		printf 'id,hostname,ip,os_id,os_version,kernel,cpu_model,sockets,cores_per_socket,threads_per_core,nproc,ram_gib,nic,nic_speed_mbps,python3,home_fs\n'
		while IFS=$'\t' read -r pc host ipa; do
			log="$LAB/logs/inventory/$pc"
			if [[ ! -f "$log" ]]; then
				lab_log "inventory: no log for $pc; no machines.csv row"
				continue
			fi
			if ! inv_shared_home "$pc" "$log"; then
				shared=1
				continue
			fi
			row=$(inv_parse "$log" | tr '\t' ',')
			printf '%s,%s,%s,%s\n' "$pc" "$host" "$ipa" "$row"
		done < <(awk -F'\t' '$1 != "" { printf "%s\t%s\t%s\n", $1, $4, $2 }' "$LAB/hosts.tsv")
	} >"$LAB/machines.csv"
	msg="inventory: wrote $LAB/machines.csv"
	if ((shared == 1)); then
		msg="$msg; PC(s) on shared home storage"
		lab_log "$msg"
		return 1
	fi
	lab_log "$msg"
	return 0
}

main() {
	if [[ ! -f "$LAB/hosts.tsv" ]]; then
		echo "no $LAB/hosts.tsv: run scan.sh first" >&2
		return 1
	fi
	local hf dip cmd n
	hf=$(hosts_all_file)
	n=$(grep -c . "$hf" || true)
	if ((n == 0)); then
		lab_log "inventory: no PCs in hosts.tsv"
		echo "no PCs in hosts.tsv" >&2
		return 1
	fi
	dip=$(inv_driver_ip) || return 1
	lab_log "inventory: $n PC(s); driver ip $dip"
	cmd="${INV_REMOTE//@DRIVER_IP@/$dip}"
	if ! lab_fanout inventory "$hf" "$cmd"; then
		lab_log "inventory: failed on every PC"
		return 1
	fi
	inv_parse_all
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	case "${1:-}" in
		-h | --help) usage; exit 0 ;;
		*) main "$@" ;;
	esac
fi
