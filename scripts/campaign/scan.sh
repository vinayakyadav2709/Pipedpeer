#!/usr/bin/env bash
# scan.sh <lab_cidr> [--user <u>] [--pc00 <ip>]: find the lab PCs and write
# $LAB/hosts.tsv (brief S4, docs/benchmarks/lab-campaign.md §1).
#
# A PC is found by its open 22/tcp; it is identified by the LAB-PC line that
# lab-pc.sh wrote to /var/lib/pipedpeer-lab/lab-pc.line, read as root over ssh
# because the lab username is not known before the line is read (lab-pc.sh
# also installs the driver key for root, and undo removes it again; root is
# used for nothing else). The scanned address is what goes in hosts.tsv, not
# the one the line reports: that is the address that answered.
#
# Ids are stable for the life of a lab. An address already in hosts.tsv keeps
# its row and its id, a new address becomes the next free pcNN in IP order,
# and --pc00 only reorders a first scan, because moving pc00 later would
# silently change what every earlier measurement meant.
#
# A PC that answers but whose line cannot be read is listed as unreachable
# and left out; a known PC that does not answer keeps its row (it has an id,
# and a switched-off PC is a normal thing between sessions).

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$here/lib.sh"

usage() {
	cat <<'EOF'
usage: scan.sh <lab_cidr> [--user <u>] [--pc00 <ip>]

Scans <lab_cidr> for hosts with 22/tcp open, reads each one's LAB-PC line as
root, and writes $LAB/hosts.tsv (keeping existing ids) plus $LAB/ssh_config.
Prints "SCAN found=<a> new=<b> unreachable=<c>" and lists the unreachable
addresses.

  --user <u>   use <u> for the ssh user instead of the one the PC reports
  --pc00 <ip>  first scan only: make <ip> pc00 (the driver-facing node)
EOF
}

lab_cidr=""
user_override=""
pc00_ip=""

while [[ $# -gt 0 ]]; do
	case "$1" in
		-h | --help)
			usage
			exit 0
			;;
		--user)
			[[ $# -ge 2 ]] || {
				echo "scan.sh: --user needs a value" >&2
				exit 2
			}
			user_override="$2"
			shift 2
			;;
		--user=*)
			user_override="${1#--user=}"
			shift
			;;
		--pc00)
			[[ $# -ge 2 ]] || {
				echo "scan.sh: --pc00 needs a value" >&2
				exit 2
			}
			pc00_ip="$2"
			shift 2
			;;
		--pc00=*)
			pc00_ip="${1#--pc00=}"
			shift
			;;
		-*)
			echo "scan.sh: unknown option $1" >&2
			usage >&2
			exit 2
			;;
		*)
			if [[ -n "$lab_cidr" ]]; then
				echo "scan.sh: one subnet at a time (got '$lab_cidr' and '$1')" >&2
				exit 2
			fi
			lab_cidr="$1"
			shift
			;;
	esac
done

if [[ -z "$lab_cidr" ]]; then
	usage >&2
	exit 2
fi

mkdir -p "$LAB"
tsv="$LAB/hosts.tsv"

# --pc00 is a first-scan decision: after that the ids are the campaign's
# history, and a row that changes meaning invalidates every runs.jsonl line.
if [[ -n "$pc00_ip" && -s "$tsv" ]] && awk -F'\t' '!/^#/ && NF >= 2 { found = 1 } END { exit !found }' "$tsv"; then
	echo "scan.sh: --pc00 refused: $tsv already has PCs, and ids are stable" >&2
	lab_log "scan: refused --pc00, $tsv already has PCs"
	exit 1
fi

tmpdir="$(mktemp -d "$LAB/scan.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

# The global half of lib.sh's ssh_config (no Host rows: the scan connects by
# address, since that is all nmap gives it). Kept here because before the
# first scan there is no hosts.tsv for lab_ssh_config_write to read.
ssh_config="$tmpdir/ssh_config"
cat >"$ssh_config" <<EOF
ServerAliveInterval 15
ServerAliveCountMax 4
ControlMaster auto
ControlPath ~/.ssh/cm-%r@%h:%p
ControlPersist 30m
BatchMode yes
StrictHostKeyChecking accept-new
UserKnownHostsFile $LAB/known_hosts
IdentityFile ~/.ssh/pipedpeer-lab
ConnectTimeout 10
EOF

# IPv4 in numeric order; a plain lexical sort puts 10.99.0.10 before 10.99.0.9.
ip_sort() { sort -t. -k1,1n -k2,2n -k3,3n -k4,4n; }

# scan_ips: the addresses nmap reports with 22/tcp open, sorted. nmap's own
# grepable output is parsed rather than its XML so nothing here needs a
# parser; the fake lab's nmap stub honours SCAN_NMAP_CMD for the tests.
scan_ips() {
	if command -v nmap >/dev/null 2>&1; then
		nmap -p22 --open -oG - "$lab_cidr"
	elif [[ -n "${SCAN_NMAP_CMD:-}" ]]; then
		bash -c "$SCAN_NMAP_CMD" scan
	else
		echo "MISSING: nmap" >&2
		return 1
	fi
}

# scan_local_ips: this machine's own addresses, which are never lab PCs.
scan_local_ips() {
	{
		ip -4 -o addr show scope global 2>/dev/null | awk '{ print $4 }' | cut -d/ -f1
		hostname -I 2>/dev/null | tr ' ' '\n' || true
	} | sed '/^$/d'
}

# scan_read_line <ip>: the LAB-PC line, as root. One attempt: a scan is
# discovery, and a PC that does not answer now is answered by rerunning.
scan_read_line() {
	local ip="$1"
	ssh -F "$ssh_config" -o User=root "$ip" -- \
		"cat /var/lib/pipedpeer-lab/lab-pc.line" 2>/dev/null
}

# scan_field <line> <key>: the value of <key>= in a LAB-PC line.
scan_field() {
	local line="$1" key="$2"
	printf '%s\n' "$line" | tr ' ' '\n' | sed -n "s/^$key=//p" | head -1
}

if ! scan_raw="$(scan_ips)"; then
	lab_log "scan: nmap failed on $lab_cidr"
	echo "scan.sh: nmap failed on $lab_cidr" >&2
	exit 1
fi

mapfile -t local_ips < <(scan_local_ips)
mapfile -t found < <(
	printf '%s\n' "$scan_raw" |
		awk '/^Host:/ { ip = $2; if ($0 ~ /(^|[^0-9])22\/open/) print ip }' |
		sort -u | ip_sort
)

# Drop our own addresses: the driver runs no lab-pc.sh and has no business
# being scanned into its own hosts.tsv.
if ((${#local_ips[@]} > 0)); then
	keep=()
	for ip in "${found[@]}"; do
		own=0
		for mine in "${local_ips[@]}"; do
			[[ "$ip" == "$mine" ]] && own=1
		done
		((own == 0)) && keep+=("$ip")
	done
	found=("${keep[@]}")
fi

if ((${#found[@]} == 0)); then
	printf 'SCAN found=0 new=0 unreachable=0\n'
	lab_log "scan: no host with 22/tcp open in $lab_cidr"
	echo "scan.sh: no host with 22/tcp open in $lab_cidr" >&2
	exit 1
fi

# --pc00 first, on a first scan only.
if [[ -n "$pc00_ip" ]]; then
	ordered=()
	seen_pc00=0
	for ip in "${found[@]}"; do
		if [[ "$ip" == "$pc00_ip" ]]; then
			ordered=("$ip" "${ordered[@]}")
			seen_pc00=1
		else
			ordered+=("$ip")
		fi
	done
	if ((seen_pc00 == 0)); then
		echo "scan.sh: WARNING: --pc00 $pc00_ip was not found by the scan" >&2
		lab_log "scan: --pc00 $pc00_ip not found"
	fi
	found=("${ordered[@]}")
fi

# Existing rows: id -> row, ip -> id. Read once; a row that is still in
# hosts.tsv keeps its id and position whatever the scan says today.
declare -A known_ip=()
declare -A row_of=()
declare -a old_order=()
next_id=0
if [[ -s "$tsv" ]]; then
	while IFS=$'\t' read -r id ip user hostname nproc ram_gib; do
		[[ "$id" == \#* || -z "${id:-}" || -z "${ip:-}" ]] && continue
		known_ip["$ip"]="$id"
		row_of["$id"]="$id"$'\t'"$ip"$'\t'"${user:-}"$'\t'"${hostname:-}"$'\t'"${nproc:-}"$'\t'"${ram_gib:-}"
		old_order+=("$id")
		if [[ "$id" =~ ^pc([0-9]+)$ ]]; then
			n=$((10#${BASH_REMATCH[1]}))
			((n >= next_id)) && next_id=$((n + 1))
		fi
	done <"$tsv"
fi

declare -a unreachable=()
declare -A seen=()
new_rows=0
for ip in "${found[@]}"; do
	if [[ -n "${known_ip[$ip]:-}" ]]; then
		seen["$ip"]=1
		# Known PC: keep the row. --user still wins, so a lab whose logins
		# changed can be corrected without losing the id.
		if [[ -n "$user_override" ]]; then
			id="${known_ip[$ip]}"
			row_of["$id"]="$(printf '%s\n' "${row_of[$id]}" |
				awk -F'\t' -v u="$user_override" 'BEGIN { OFS = "\t" } { $3 = u; print }')"
		fi
		continue
	fi
	if ! line="$(scan_read_line "$ip")" || [[ -z "${line// /}" ]]; then
		unreachable+=("$ip")
		lab_log "scan: $ip answered but its LAB-PC line is unreadable; left out"
		continue
	fi
	hostname="$(scan_field "$line" host)"
	reported="$(scan_field "$line" ip)"
	user="$(scan_field "$line" user)"
	nproc="$(scan_field "$line" cores)"
	ram_gib="$(scan_field "$line" ram_gib)"
	[[ -n "$reported" && "$reported" != "$ip" ]] &&
		lab_log "scan: $ip reports ip=$reported in its LAB-PC line; using $ip"
	[[ -n "$user_override" ]] && user="$user_override"
	id="$(printf 'pc%02d' "$next_id")"
	next_id=$((next_id + 1))
	known_ip["$ip"]="$id"
	seen["$ip"]=1
	row_of["$id"]="$id"$'\t'"$ip"$'\t'"$user"$'\t'"$hostname"$'\t'"$nproc"$'\t'"$ram_gib"
	old_order+=("$id")
	new_rows=$((new_rows + 1))
done

# Write through a temp file: a scan that dies half way must not leave a
# hosts.tsv that the campaign would then take for the lab.
{
	printf '#id\tip\tuser\thostname\tnproc\tram_gib\n'
	for id in "${old_order[@]}"; do
		printf '%s\n' "${row_of[$id]}"
	done
} >"$tmpdir/hosts.tsv"
mv "$tmpdir/hosts.tsv" "$tsv"

lab_ssh_config_write

# found = the PCs this campaign can use, i.e. the rows in hosts.tsv after the
# scan; a known PC that did not answer keeps its row and is listed as absent.
printf 'SCAN found=%d new=%d unreachable=%d\n' "${#old_order[@]}" "$new_rows" "${#unreachable[@]}"
for ip in "${unreachable[@]}"; do
	printf 'UNREACHABLE %s\n' "$ip"
	lab_log "scan: UNREACHABLE $ip"
done
for id in "${old_order[@]}"; do
	ip="$(printf '%s\n' "${row_of[$id]}" | cut -f2)"
	if [[ -z "${seen[$ip]:-}" ]]; then
		printf 'ABSENT %s %s\n' "$id" "$ip"
		lab_log "scan: ABSENT $id $ip (kept its row)"
	fi
done
lab_log "scan: found=${#old_order[@]} new=$new_rows unreachable=${#unreachable[@]} in $lab_cidr"

if ((${#old_order[@]} == 0)); then
	echo "scan.sh: no PC was added; nothing to ssh to" >&2
	exit 1
fi
