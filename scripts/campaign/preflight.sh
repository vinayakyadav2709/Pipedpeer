#!/usr/bin/env bash
# preflight.sh: what a real PC can get wrong, checked before the first run.
# Run after scan.sh (and after clocks.sh for the offsets); safe to rerun.
#
# One line per finding, "PREFLIGHT <pc> OK|WARN|FAIL <check>: <what to do>",
# all of them in $LAB/preflight.txt; the last line is
# "PREFLIGHT ok=<a> warn=<b> fail=<c>". Exit 1 when any PC has a FAIL: fix it
# as the line says, then run preflight.sh again. A WARN is worth fixing but
# does not block.
#
# Checks: RAM (pc00 carries the Ray head and Dask scheduler), CPUs the PC can
# really use (cgroup quota, cpuset), suspend targets masked, NTP sync and the
# clocks.sh offsets, free disk for the Nix store and the venv, Ubuntu version,
# firewall (ufw rules for 4242/tcp, 4242-4244/udp; another firewall with a drop
# policy). Not checked: whether the UDP ports are reachable between PCs (the
# daemons' health after install.sh shows 4242/tcp; network.sh shows ping).
#
# Environment: PF_MIN_RAM_GIB (worker, default 4), PF_MIN_RAM_GIB_PC00
# (default 8: a 2-node Ray cluster's head needed more than 3 GB on the
# emulated lab), PF_MIN_DISK_GIB (default 15: Nix closure plus venv took about
# 5.5 GB per PC there), PF_MAX_OFFSET_MS (default 100).
set -euo pipefail
_PF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$_PF_DIR/lib.sh"

PF_MIN_RAM_GIB="${PF_MIN_RAM_GIB:-4}"
PF_MIN_RAM_GIB_PC00="${PF_MIN_RAM_GIB_PC00:-8}"
PF_MIN_DISK_GIB="${PF_MIN_DISK_GIB:-15}"
PF_MAX_OFFSET_MS="${PF_MAX_OFFSET_MS:-100}"

# PF_REMOTE: run on a PC; key=value lines for pf_judge.
# shellcheck disable=SC2016  # expanded on the PC
PF_REMOTE='. /etc/os-release 2>/dev/null; echo "os_id=${ID:-}"; echo "os_version=${VERSION_ID:-}"
awk "/MemTotal/ {print \"mem_kib=\" \$2}" /proc/meminfo
lim=""; for d in $(awk -F: "\$1 == \"0\" || \$2 ~ /(^|,)memory(,|\$)/ {print \$3}" /proc/self/cgroup); do while :; do for f in /sys/fs/cgroup$d/memory.max /sys/fs/cgroup/memory$d/memory.limit_in_bytes; do if [ -r $f ]; then v=$(cat $f); case $v in max|"") ;; *) v=$((v / 1024)); if [ -z "$lim" ] || [ "$v" -lt "$lim" ]; then lim=$v; fi ;; esac; fi; done; if [ -z "$d" ] || [ "$d" = / ]; then break; fi; d=${d%/*}; done; done
echo "mem_limit_kib=$lim"
echo "nproc=$(nproc)"
echo "usable_cpus=$(sh -c '"'"'%PROBE%'"'"' 2>/dev/null)"
echo "disk_home_kib=$(df -Pk "$HOME" | awk "NR==2 {print \$4}")"
if [ -d /nix ]; then n=/nix; else n=/; fi
echo "disk_nix_kib=$(df -Pk $n | awk "NR==2 {print \$4}")"
for t in sleep.target suspend.target hibernate.target hybrid-sleep.target; do echo "$t=$(systemctl is-enabled $t 2>/dev/null || true)"; done
echo "ntp_sync=$(timedatectl show -p NTPSynchronized --value 2>/dev/null || echo unknown)"
if sudo -n ufw status 2>/dev/null | grep -q "Status: active"; then
  echo ufw=active
  echo "ufw_rules=$(sudo -n ufw status | awk "/ALLOW/ {print \$1}" | sort -u | tr "\n" " ")"
else echo ufw=inactive; fi
echo "nft_drop=$(sudo -n nft list ruleset 2>/dev/null | grep -c "hook input.*policy drop" || true)"'
PF_REMOTE="${PF_REMOTE//%PROBE%/${LAB_CPUS_PROBE//\'/\'\"\'\"\'}}"

# pf_judge <pc> <file>: the PREFLIGHT lines for one PC's PF_REMOTE output.
pf_judge() {
	local pc="$1" f="$2" k v n=0 gib min p
	local -A kv=()
	while IFS='=' read -r k v; do
		[[ -n "$k" ]] && kv[$k]="$v"
	done <"$f"
	if [[ -z "${kv[mem_kib]:-}" ]]; then
		echo "PREFLIGHT $pc FAIL probe: no answer; check the PC is on and reachable (scan.sh), then rerun preflight.sh"
		return 0
	fi
	out() { echo "PREFLIGHT $pc $*"; n=$((n + 1)); }

	if [[ "${kv[os_id]:-}" != ubuntu ]]; then
		out "FAIL os: ${kv[os_id]:-unknown} is not Ubuntu; the campaign supports Ubuntu 22.04 and 24.04 only: exclude this PC (results/lab/excluded.txt)"
	elif [[ " 22.04 24.04 " != *" ${kv[os_version]:-} "* ]]; then
		out "WARN os: Ubuntu ${kv[os_version]:-?} was never rehearsed (22.04 and 24.04 were); expect surprises, or exclude it"
	fi

	local mkib="${kv[mem_kib]}" via=""
	if [[ "${kv[mem_limit_kib]:-}" =~ ^[0-9]+$ ]] && ((kv[mem_limit_kib] < mkib)); then
		mkib="${kv[mem_limit_kib]}"
		via=" (a cgroup memory limit, under the machine's $((kv[mem_kib] / 1048576)) GiB)"
	fi
	gib=$((mkib / 1048576))
	min="$PF_MIN_RAM_GIB"
	[[ "$pc" == pc00 ]] && min="$PF_MIN_RAM_GIB_PC00"
	if ((gib < min)); then
		if [[ "$pc" == pc00 ]]; then
			out "FAIL ram: ${gib} GiB${via}, pc00 needs ${min} GiB (it runs the Ray head and the Dask scheduler next to its own work): rescan with --pc00 set to a PC with more memory"
		else
			out "FAIL ram: ${gib} GiB${via}, a worker needs ${min} GiB: exclude this PC (results/lab/excluded.txt) or close what uses the memory"
		fi
	fi

	if [[ "${kv[usable_cpus]:-}" =~ ^[0-9]+$ && "${kv[nproc]:-}" =~ ^[0-9]+$ ]] && ((kv[usable_cpus] < kv[nproc])); then
		out "WARN cpus: usable ${kv[usable_cpus]} of ${kv[nproc]} (a cgroup quota or cpuset limits this user); results are per usable CPU: write it down, or lift the limit"
	fi

	for p in sleep.target suspend.target hibernate.target hybrid-sleep.target; do
		if [[ "${kv[$p]:-}" != masked ]]; then
			out "FAIL sleep: $p is ${kv[$p]:-unknown}, not masked; the PC can suspend mid-run: sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target (or run the setup line again)"
			break
		fi
	done

	if [[ "${kv[ntp_sync]:-}" != yes ]]; then
		out "WARN clock: NTP synchronised is '${kv[ntp_sync]:-unknown}': sudo timedatectl set-ntp true, wait a minute, then clocks.sh again"
	fi

	for p in disk_home_kib disk_nix_kib; do
		[[ "${kv[$p]:-}" =~ ^[0-9]+$ ]] || continue
		gib=$((kv[$p] / 1048576))
		if ((gib < PF_MIN_DISK_GIB)); then
			local where='the home dir'
			[[ "$p" == disk_nix_kib ]] && where=/nix
			out "FAIL disk: ${gib} GiB free for $where; the Nix closure and the venv need ${PF_MIN_DISK_GIB} GiB: free space (or exclude this PC)"
		fi
	done

	if [[ "${kv[ufw]:-}" == active ]]; then
		local miss=""
		for p in 4242/tcp 4242/udp 4243/udp 4244/udp; do
			[[ " ${kv[ufw_rules]:-} " == *" $p "* ]] || miss+=" $p"
		done
		[[ -z "$miss" ]] || out "FAIL firewall: ufw is active without${miss}: from the driver run lab_fanout ports <hosts file> 'sudo bash /tmp/lab-pc.sh ports' (or the setup line again)"
	elif [[ "${kv[nft_drop]:-0}" =~ ^[0-9]+$ ]] && ((kv[nft_drop] > 0)); then
		out "WARN firewall: ufw is off but nftables drops input by default; allow 4242/tcp and 4242-4244/udp from the lab subnet, or the daemons cannot meet"
	fi

	((n > 0)) || echo "PREFLIGHT $pc OK"
	unset -f out
}

# pf_clock_offsets: WARN lines for PCs whose |offset| in $LAB/clocks.csv is over the limit.
pf_clock_offsets() {
	[[ -f "$LAB/clocks.csv" ]] || return 0
	awk -F, -v lim="$PF_MAX_OFFSET_MS" 'NR > 1 && $2 != "" {
		o = $2 + 0; if (o < 0) o = -o
		if (o > lim) printf "PREFLIGHT %s WARN clock-offset: %s ms from the driver (limit %s): sudo timedatectl set-ntp true on both, then clocks.sh again\n", $1, $2, lim
	}' "$LAB/clocks.csv"
}

pf_main() {
	case "${1:-}" in -h | --help) sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; return 0 ;; esac
	local hf pc res="$LAB/preflight.txt" ok=0 warn=0 fail=0
	mkdir -p "$LAB/logs"
	hf="$LAB/hosts-preflight.txt"
	_lab_available_hosts >"$hf" || { echo "no PCs: run scan.sh first" >&2; return 1; }
	lab_fanout preflight "$hf" "$PF_REMOTE" >/dev/null || lab_log "preflight: the probe failed on some PCs; they are reported as FAIL probe"
	{
		while IFS= read -r pc; do
			if [[ -s "$LAB/logs/preflight/$pc" ]]; then pf_judge "$pc" "$LAB/logs/preflight/$pc"; else pf_judge "$pc" /dev/null; fi
		done <"$hf"
		pf_clock_offsets
	} >"$res"
	ok="$(grep -c ' OK$' "$res" || true)"
	warn="$(grep -c ' WARN ' "$res" || true)"
	fail="$(grep -c ' FAIL ' "$res" || true)"
	cat "$res"
	echo "PREFLIGHT ok=$ok warn=$warn fail=$fail" | tee -a "$res"
	lab_log "preflight: ok=$ok warn=$warn fail=$fail ($res)"
	((fail == 0))
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	pf_main "$@"
fi
