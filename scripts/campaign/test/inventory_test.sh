#!/usr/bin/env bash
# shellcheck disable=SC2317  # cleanup runs through the trap
# The S5 case table (brief): one row per behaviour inventory.sh, clocks.sh and
# network.sh promise on saved text. These are pure parsing rows; the remote
# paths behind them run only as the scripts themselves (never in tests), so
# nothing here touches ssh or the real network.
#
# One ok/FAIL line per row; the run ends with "PASS <k>/<k>" and exits 0 only
# when every row passed.

set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
LIB="$REPO/scripts/campaign/lib.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"

pass=0
failed=0
problems=()
dirs=()

cleanup() {
	local d
	if [[ "${LAB_TEST_KEEP:-0}" == 1 ]]; then
		printf 'kept: %s\n' "${dirs[*]}" >&2
		return
	fi
	for d in "${dirs[@]}"; do
		rm -rf "$d"
	done
}
trap cleanup EXIT

ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }

verdict() {
	if ((${#problems[@]} == 0)); then
		ok "$1"
	else
		bad "$1" "${problems[*]}"
	fi
	problems=()
}

# row_setup: a scratch dir with its own $LAB (campaign.log included), lib.sh
# and the three S5 scripts sourced, -e off so a row can see exit codes.
row_setup() {
	local d
	d="$(mktemp -d "$STATE/campaign-inventory-test.XXXXXX")"
	dirs+=("$d")
	mkdir -p "$d/home" "$d/lab"
	printf 'HOME=%s\n' "$d/home" >"$d/env.log"
	(
		cd "$REPO"
		export LAB_RESULTS="$d/lab"
		# shellcheck source=/dev/null
		source "$LIB"
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/inventory.sh"
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/clocks.sh"
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/network.sh"
		export HOME="$d/home"
		# shellcheck source=/dev/null
		source "$1"
	) >"$d/log" 2>&1 || printf 'rc=%s\n' "$?" >>"$d/log"
	printf '%s\n' "$d"
}

# field <n> on stdin: field n of the saved row.
field() { cut -f"$1"; }

# --- the table ------------------------------------------------------------

# an Ubuntu 24.04 lscpu sample -> fields
t1() {
	local d f
	f="$(mktemp "$STATE/inv-row-t1.XXXXXX")"
	dirs+=("$(dirname "$f")")
	cat >"$f" <<'EOF'
cat >"$LAB/sample.txt" <<'SAMPLE'
os_id=ubuntu
os_version=24.04
kernel=6.8.0-48-generic
cpu_model=Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz
sockets=1
cores_per_socket=8
threads_per_core=2
nproc=8
ram_gib=15.75
nic=enp0s31f6
nic_speed_mbps=1000
python3=Python 3.12.3
home_fs=ext2/ext3
SAMPLE
inv_parse "$LAB/sample.txt"
printf 'rc=%s\n' "$?"
EOF
	d="$(row_setup "$f")"
	local out; out="$(grep -v '^rc=' "$d/log")"
	[[ "$(printf '%s\n' "$out" | wc -l)" == 1 ]] || pnote "inv_parse printed $(printf '%s\n' "$out" | wc -l) lines, want 1"
	[[ "$(printf '%s\n' "$out" | field 1)" == "ubuntu" ]] || pnote "os_id=$(printf '%s' "$out" | field 1)"
	[[ "$(printf '%s\n' "$out" | field 2)" == "24.04" ]] || pnote "os_version"
	[[ "$(printf '%s\n' "$out" | field 4)" == "Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz" ]] || pnote "cpu_model"
	[[ "$(printf '%s\n' "$out" | field 5)" == "1" ]] || pnote "sockets"
	[[ "$(printf '%s\n' "$out" | field 6)" == "8" ]] || pnote "cores_per_socket"
	[[ "$(printf '%s\n' "$out" | field 7)" == "2" ]] || pnote "threads_per_core"
	[[ "$(printf '%s\n' "$out" | field 8)" == "8" ]] || pnote "nproc"
	[[ "$(printf '%s\n' "$out" | field 9)" == "15.75" ]] || pnote "ram_gib"
	[[ "$(printf '%s\n' "$out" | field 10)" == "enp0s31f6" ]] || pnote "nic"
	[[ "$(printf '%s\n' "$out" | field 11)" == "1000" ]] || pnote "nic_speed_mbps"
	[[ "$(printf '%s\n' "$out" | field 12)" == "3.12.3" ]] || pnote "python3=$(printf '%s\n' "$out" | field 12)"
	[[ "$(printf '%s\n' "$out" | field 13)" == "ext2/ext3" ]] || pnote "home_fs"
	grep -q '^rc=0$' "$d/log" || pnote "inv_parse exited nonzero"
	verdict "Ubuntu 24.04 inventory sample -> 13 fields, python3=3.12.3"
}

# a WiFi NIC (no speed line) -> empty speed, no error
t2() {
	local d f
	f="$(mktemp "$STATE/inv-row.XXXXXX")"
	dirs+=("$(dirname "$f")")
	cat >"$f" <<'EOF'
cat >"$LAB/sample.txt" <<'SAMPLE'
os_id=ubuntu
os_version=24.04
kernel=6.8.0-48-generic
cpu_model=Intel(R) Core(TM) i7-1065G7 CPU @ 1.30GHz
sockets=1
cores_per_socket=8
threads_per_core=2
nproc=8
ram_gib=15.75
nic=wlp3s0
python3=Python 3.12.3
home_fs=ext2/ext3
SAMPLE
inv_parse "$LAB/sample.txt"
printf 'rc=%s\n' "$?"
EOF
	d="$(row_setup "$f")"
	local line; line="$(grep -v '^rc=' "$d/log" | head -1)"
	[[ "$(printf '%s\n' "$line" | cut -f11)" == "" ]] || pnote "speed=$(printf '%s' "$line" | cut -f11), want empty"
	grep -q '^rc=0$' "$d/log" || pnote "inv_parse errored on a WiFi speed file"
	verdict "WiFi sample (no nic_speed_mbps) -> empty speed, exit 0"
}

# net_ping_parse: the brief's ping line -> 0.305 0.081
t3() {
	local d f
	f="$(mktemp "$STATE/inv-row.XXXXXX")"
	dirs+=("$(dirname "$f")")
	cat >"$f" <<'EOF'
cat >"$LAB/ping.txt" <<'SAMPLE'
PING 192.168.1.51 (192.168.1.51) 56(84) bytes of data.

--- 192.168.1.51 ping statistics ---
10 packets transmitted, 10 received, 0% packet loss, time 9244ms
rtt min/avg/max/mdev = 0.211/0.305/0.502/0.081 ms
SAMPLE
net_ping_parse "$LAB/ping.txt"
printf 'rc=%s\n' "$?"
EOF
	d="$(row_setup "$f")"
	grep -q '^0.305 0.081$' "$d/log" || pnote "got '$(grep -v '^rc=' "$d/log")', want '0.305 0.081'"
	grep -q '^rc=0$' "$d/log" || pnote "net_ping_parse exited nonzero"
	verdict "ping rtt line -> avg=0.305 mdev=0.081"
}

# ping 100% loss (no rtt line) -> empty, warning
t4() {
	local d f
	f="$(mktemp "$STATE/inv-row.XXXXXX")"
	dirs+=("$(dirname "$f")")
	cat >"$f" <<'EOF'
cat >"$LAB/ping.txt" <<'SAMPLE'
PING 192.168.1.51 (192.168.1.51) 56(84) bytes of data.
--- 192.168.1.51 ping statistics ---
10 packets transmitted, 0 received, 100% packet loss, time 9244ms
SAMPLE
net_ping_parse "$LAB/ping.txt" >"$LAB/parsed.txt"
printf 'rc=%s\n' "$?"
EOF
	d="$(row_setup "$f")"
	[[ -s "$d/lab/parsed.txt" ]] && pnote "output was '$(cat "$d/lab/parsed.txt")', want empty"
	grep -q 'ping' "$d/lab/campaign.log" || pnote "no warning in campaign.log"
	verdict "ping 100% loss -> empty, warning logged"
}

# net_iperf_parse: an iperf3 JSON with an "error" key -> empty mbps, warning
t5() {
	local d f
	f="$(mktemp "$STATE/inv-row.XXXXXX")"
	dirs+=("$(dirname "$f")")
	cat >"$f" <<'EOF'
cat >"$LAB/iperf.json" <<'SAMPLE'
{"error": "unable to connect to server: Connection refused"}
SAMPLE
net_iperf_parse "$LAB/iperf.json" >"$LAB/parsed.txt"
printf 'rc=%s\n' "$?"
EOF
	d="$(row_setup "$f")"
	[[ -s "$d/lab/parsed.txt" ]] && pnote "output was '$(cat "$d/lab/parsed.txt")', want empty"
	grep -q 'iperf3' "$d/lab/campaign.log" || pnote "no warning in campaign.log"
	grep -q 'Connection refused' "$d/lab/campaign.log" || pnote "warning does not carry the error text"
	verdict "iperf3 JSON with 'error' key -> empty mbps, warning"
}

# net_iperf_parse: a good JSON -> sum_received/1e6 in Mb/s
t6() {
	local d f
	f="$(mktemp "$STATE/inv-row.XXXXXX")"
	dirs+=("$(dirname "$f")")
	cat >"$f" <<'EOF'
cat >"$LAB/iperf.json" <<'SAMPLE'
{"end": {"sum_received": {"bits_per_second": 941176470588.2354}}}
SAMPLE
net_iperf_parse "$LAB/iperf.json" >"$LAB/parsed.txt"
printf 'rc=%s\n' "$?"
EOF
	d="$(row_setup "$f")"
	[[ "$(cat "$d/lab/parsed.txt")" == "941176.5" ]] || pnote "mbps=$(cat "$d/lab/parsed.txt"), want 941176.5"
	verdict "iperf3 JSON -> Mb/s = bits_per_second/1e6"
}

# home_fs on shared storage (nfs) -> SHARED-HOME <pc>, exit 1
t7() {
	local d f
	f="$(mktemp "$STATE/inv-row.XXXXXX")"
	dirs+=("$(dirname "$f")")
	cat >"$f" <<'EOF'
cat >"$LAB/sample.txt" <<'SAMPLE'
home_fs=nfs4
SAMPLE
inv_shared_home pc01 "$LAB/sample.txt" >"$LAB/shared.txt"
printf 'rc=%s\n' "$?"
EOF
	d="$(row_setup "$f")"
	grep -q 'SHARED-HOME pc01' "$d/log" || pnote "no SHARED-HOME pc01 line on stdout of shared-home run"
	grep -q '^rc=1$' "$d/log" || pnote "inv_shared_home exited 0 on nfs4"
	grep -q 'SHARED-HOME pc01' "$d/lab/campaign.log" || pnote "no SHARED-HOME in campaign.log"
	verdict "home_fs=nfs4 -> SHARED-HOME pc01, exit 1"
}

printf 'scripts/campaign inventory/network/clocks case table\n\n'
t1
t2
t3
t4
t5
t6
t7
printf '\n'

total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1
