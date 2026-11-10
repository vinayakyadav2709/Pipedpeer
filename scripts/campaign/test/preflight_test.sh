#!/usr/bin/env bash
# shellcheck disable=SC2317
# Case table for preflight.sh's pf_judge: saved key=value text from one PC in,
# "PREFLIGHT <pc> OK|WARN|FAIL <check>: <what to do>" lines out. No ssh.
#
#   case                                        want (line that must appear)
#   a good 24.04 PC, pc00, 32 GiB               no WARN/FAIL line; "OK"
#   pc00 with less RAM than a Ray head needs    FAIL ram ... pc00
#   a worker under the worker minimum           FAIL ram
#   a worker at the minimum (ordinary)          no ram FAIL
#   usable CPUs below nproc (quota/cpuset)      WARN cpus ... usable 3 of 16
#   suspend not masked                          FAIL sleep ... systemctl mask
#   NTP not synchronised                        WARN clock ... timedatectl set-ntp true
#   /nix filesystem short of space              FAIL disk
#   Ubuntu 20.04                                WARN os
#   not Ubuntu                                  FAIL os
#   ufw active without the 4243/udp rule        FAIL firewall ... lab-pc.sh ports
#   ufw active with every rule (ordinary)       no firewall line
#   ufw off, nftables drop policy               WARN firewall ... 4242/tcp
#   a cgroup memory limit under MemTotal        FAIL ram ... 3 GiB (the limit counts)
#   an empty answer                             FAIL probe ... no answer
#   clock offset over the limit (clocks.csv)    WARN clock-offset
set -euo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
T="$(mktemp -d "$STATE/preflight-test.XXXXXX")"
trap 'rm -rf "$T"' EXIT
export LAB_RESULTS="$T/lab"
mkdir -p "$LAB_RESULTS"
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/preflight.sh"
pass=0
failed=0

good() { # a healthy PC; later keys override
	cat <<'EOF'
os_id=ubuntu
os_version=24.04
mem_kib=33554432
nproc=16
usable_cpus=16
disk_home_kib=104857600
disk_nix_kib=104857600
sleep.target=masked
suspend.target=masked
hibernate.target=masked
hybrid-sleep.target=masked
ntp_sync=yes
ufw=inactive
nft_drop=0
EOF
}
# row <name> <pc> <want regex> <unwanted regex or -> [key=value overrides...]
row() {
	local name="$1" pc="$2" want="$3" unwanted="$4" f out
	shift 4
	f="$T/${name//[^a-z0-9]/_}.txt"
	{ good; printf '%s\n' "$@"; } >"$f"
	out="$(pf_judge "$pc" "$f" 2>&1)" || true
	if ! grep -qE -- "$want" <<<"$out"; then
		failed=$((failed + 1)); printf 'FAIL %s -- want /%s/ in: %s\n' "$name" "$want" "$(tr '\n' '|' <<<"$out")"
	elif [[ "$unwanted" != - ]] && grep -qE -- "$unwanted" <<<"$out"; then
		failed=$((failed + 1)); printf 'FAIL %s -- unwanted /%s/ in: %s\n' "$name" "$unwanted" "$(tr '\n' '|' <<<"$out")"
	else
		pass=$((pass + 1)); printf 'ok   %s\n' "$name"
	fi
}

row "good pc00" pc00 '^PREFLIGHT pc00 OK' ' (WARN|FAIL) '
row "pc00 below the Ray head minimum" pc00 'PREFLIGHT pc00 FAIL ram: .*Ray head' - mem_kib=4194304
row "worker below the worker minimum" pc01 'PREFLIGHT pc01 FAIL ram: ' - mem_kib=2097152
row "a cgroup memory limit under MemTotal" pc01 'PREFLIGHT pc01 FAIL ram: 3 GiB.*cgroup' - mem_limit_kib=3145728
row "worker at the minimum" pc01 '^PREFLIGHT pc01 OK' 'FAIL ram' "mem_kib=$((PF_MIN_RAM_GIB * 1048576))"
row "a nominal 8 GB pc00 reports 7.6 GiB" pc00 '^PREFLIGHT pc00 OK' 'FAIL ram' mem_kib=7969177
row "opposite: a real 6 GB pc00 is still refused" pc00 'PREFLIGHT pc00 FAIL ram: ' - mem_kib=6291456
row "usable CPUs below nproc" pc01 'PREFLIGHT pc01 WARN cpus: .*usable 3 of 16' - usable_cpus=3
row "suspend not masked" pc01 'PREFLIGHT pc01 FAIL sleep: .*systemctl mask' - suspend.target=enabled
row "NTP not synchronised" pc01 'PREFLIGHT pc01 WARN clock: .*timedatectl set-ntp true' - ntp_sync=no
row "/nix short of space" pc01 'PREFLIGHT pc01 FAIL disk: 1 GiB free for /nix' - disk_nix_kib=1048576
row "Ubuntu 20.04" pc01 'PREFLIGHT pc01 WARN os: ' - os_version=20.04
row "not Ubuntu" pc01 'PREFLIGHT pc01 FAIL os: ' - os_id=fedora
row "ufw active without 4243/udp" pc01 'PREFLIGHT pc01 FAIL firewall: .*source scripts/campaign/lib.sh.*lab-pc.sh ports' - ufw=active "ufw_rules=22/tcp 4242/tcp 4242/udp 4244/udp"
row "ufw active with every rule" pc01 '^PREFLIGHT pc01 OK' 'firewall' ufw=active "ufw_rules=22/tcp 4242/tcp 4242/udp 4243/udp 4244/udp"
row "ufw off, nftables drop policy" pc01 'PREFLIGHT pc01 WARN firewall: .*4242/tcp' - nft_drop=2

: >"$T/empty.txt"
out="$(pf_judge pc02 "$T/empty.txt" 2>&1)" || true
if grep -qE 'PREFLIGHT pc02 FAIL probe: .*no answer' <<<"$out"; then pass=$((pass + 1)); echo "ok   an empty answer"; else failed=$((failed + 1)); echo "FAIL an empty answer -- $out"; fi

# one status per PC: an OK line plus a clock-offset WARN is one warned PC, not
# one OK and one WARN
{
	printf 'PREFLIGHT pc00 OK\n'
	printf 'PREFLIGHT pc01 OK\n'
	printf 'PREFLIGHT pc01 WARN clock-offset: 180 ms\n'
} >"$T/count.txt"
counted="$(pf_count "$T/count.txt" 2>/dev/null || true)"
if [[ "$counted" == "1 1 0" ]]; then pass=$((pass + 1)); echo "ok   counting: a warned PC is not also counted OK"; else failed=$((failed + 1)); echo "FAIL counting: a warned PC is not also counted OK -- got [$counted] want [1 1 0]"; fi

# the opposite: plain OK PCs and a plain FAIL still count as they are
{
	printf 'PREFLIGHT pc00 OK\n'
	printf 'PREFLIGHT pc01 OK\n'
	printf 'PREFLIGHT pc02 FAIL ram: x\n'
} >"$T/count2.txt"
counted="$(pf_count "$T/count2.txt" 2>/dev/null || true)"
if [[ "$counted" == "2 0 1" ]]; then pass=$((pass + 1)); echo "ok   counting opposite: OK and FAIL unchanged"; else failed=$((failed + 1)); echo "FAIL counting opposite: OK and FAIL unchanged -- got [$counted] want [2 0 1]"; fi

# /nix on the home dir's filesystem: one disk line, not two
{ good; printf 'disk_home_kib=1048576\ndisk_nix_kib=1048576\ndisk_home_fs=/dev/sda1\ndisk_nix_fs=/dev/sda1\n'; } >"$T/same.txt"
same="$(pf_judge pc01 "$T/same.txt" 2>&1)" || true
n_disk="$(grep -c 'FAIL disk' <<<"$same" || true)"
if ((n_disk == 1)) && grep -q 'home dir and /nix' <<<"$same"; then pass=$((pass + 1)); echo "ok   same filesystem: one disk line naming both"; else failed=$((failed + 1)); echo "FAIL same filesystem: one disk line naming both -- $same"; fi

diff="$( { good; printf 'disk_home_kib=1048576\ndisk_nix_kib=1048576\ndisk_home_fs=/dev/sda1\ndisk_nix_fs=/dev/sdb1\n'; } >"$T/diff.txt" ; pf_judge pc01 "$T/diff.txt" 2>&1)" || true
n_disk="$(grep -c 'FAIL disk' <<<"$diff" || true)"
if ((n_disk == 2)); then pass=$((pass + 1)); echo "ok   opposite: separate filesystems still get two disk lines"; else failed=$((failed + 1)); echo "FAIL opposite: separate filesystems still get two disk lines -- $diff"; fi

printf 'id,offset_ms,rtt_ms,measured_at\npc00,3.1,0.4,x\npc01,-180.2,0.5,x\n' >"$LAB_RESULTS/clocks.csv"
out="$(pf_clock_offsets 2>&1)" || true
if grep -qE 'PREFLIGHT pc01 WARN clock-offset: ' <<<"$out" && ! grep -q 'pc00' <<<"$out"; then pass=$((pass + 1)); echo "ok   clock offset over the limit"; else failed=$((failed + 1)); echo "FAIL clock offset over the limit -- $out"; fi

printf 'PASS %s/%s\n' "$pass" "$((pass + failed))"
((failed == 0))
