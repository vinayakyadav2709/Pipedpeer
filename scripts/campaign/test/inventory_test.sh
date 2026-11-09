#!/usr/bin/env bash
# shellcheck disable=SC2317,SC2016  # cleanup runs through the trap; row bodies are single-quoted on purpose
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
	dirs+=("$f")
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
	dirs+=("$f")
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
	dirs+=("$f")
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
	dirs+=("$f")
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
	dirs+=("$f")
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
	dirs+=("$f")
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
	dirs+=("$f")
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


# row_run <body>: write the row file from stdin-less text, run it, print its dir.
_row() {
	local f
	f="$(mktemp "$STATE/inv-row.XXXXXX")"
	dirs+=("$f")
	printf '%s\n' "$1" >"$f"
	row_setup "$f"
}

# network.sh main with ssh stubbed: pc01's ping works, pc02's fails. The failed
# ping must leave its columns empty, never pc01's numbers (D1).
NET_STUB='
printf "pc00\t10.0.0.1\n pc01\t10.0.0.2\npc02\t10.0.0.3\n" | sed "s/^ //" >"$LAB/hosts.tsv"
lab_ssh() {
	local pc=$1 cmd=$2
	case $cmd in
	"iperf3 -s"*) return 0 ;;
	"iperf3 -c"*) echo "{\"end\": {\"sum_received\": {\"bits_per_second\": 9.4e8}}}" ;;
	"ping"*)
		if [[ ${NET_PING_FAIL:-} == *"$cmd"* ]]; then return 1; fi
		case $cmd in
		*10.0.0.2*) echo "rtt min/avg/max/mdev = 0.100/0.200/0.300/0.050 ms" ;;
		*10.0.0.3*) echo "rtt min/avg/max/mdev = 0.400/0.500/0.600/0.070 ms" ;;
		esac ;;
	*4242*) return 0 ;;
	"pipedpeer doctor") return 0 ;;
	esac
}
'
t8() {
	local d
	d="$(_row "$NET_STUB
NET_PING_FAIL='ping -c 10 -q 10.0.0.3' main >/dev/null 2>&1
cp \"\$LAB/network.csv\" \"\$LAB/net.out\"")"
	grep -q '^pc01,10.0.0.2,940.0,0.200,0.050,1,1$' "$d/lab/net.out" || pnote "pc01 row: $(cat "$d/lab/net.out")"
	grep -q '^pc02,10.0.0.3,940.0,,,1,1$' "$d/lab/net.out" || pnote "pc02 (ping failed) kept pc01's numbers: $(grep '^pc02' "$d/lab/net.out")"
	verdict "network.sh: a failed ping leaves rtt empty, not the previous PC's"
}
t9() {
	local d
	d="$(_row "$NET_STUB
main >/dev/null 2>&1
cp \"\$LAB/network.csv\" \"\$LAB/net.out\"")"
	grep -q '^pc01,10.0.0.2,940.0,0.200,0.050,1,1$' "$d/lab/net.out" || pnote "pc01 row: $(cat "$d/lab/net.out")"
	grep -q '^pc02,10.0.0.3,940.0,0.500,0.070,1,1$' "$d/lab/net.out" || pnote "pc02 row: $(grep '^pc02' "$d/lab/net.out")"
	verdict "network.sh: both pings fine -> each PC has its own rtt"
}

# port-lines.sh: the glue count is non-blank, non-comment lines (its header says so)
t10() {
	local d want got
	d="$(_row 'bash "$REPO/scripts/campaign/port-lines.sh" >/dev/null 2>&1; cp "$LAB/port-lines.csv" "$LAB/pl.out"')"
	want=$(grep -Evc '^[[:space:]]*(#|$)' "$REPO/docs/benchmarks/workloads/baselines/trivial_ssh.sh")
	got=$(awk -F, '$1 == "baselines/trivial_ssh.sh" { print $3 }' "$d/lab/pl.out")
	[[ "$got" == "$want" ]] || pnote "trivial_ssh.sh counted $got, non-blank non-comment lines are $want"
	verdict "port-lines: ssh glue counts non-blank, non-comment lines"
}
t11() {
	local d want got
	d="$(_row 'bash "$REPO/scripts/campaign/port-lines.sh" >/dev/null 2>&1; cp "$LAB/port-lines.csv" "$LAB/pl.out"')"
	want=$(diff -u "$REPO/docs/benchmarks/workloads/pool_map.py" "$REPO/docs/benchmarks/workloads/baselines/w1_ray.py" | grep -c '^[+-][^+-]' || true)
	got=$(awk -F, '$1 == "baselines/w1_ray.py" { print $3 }' "$d/lab/pl.out")
	[[ "$got" == "$want" ]] || pnote "w1_ray.py counted $got, diff says $want"
	verdict "port-lines: a .py port counts its diff lines"
}

# inventory.sh: a comma in a value (cpu model) must not add a CSV column
t12() {
	local d n
	d="$(_row 'printf "pc00\t10.0.0.1\tx\thost0\n" >"$LAB/hosts.tsv"
mkdir -p "$LAB/logs/inventory"
printf "os_id=ubuntu\nos_version=24.04\nkernel=6.8\ncpu_model=Fancy CPU, 8-Core\nsockets=1\ncores_per_socket=4\nthreads_per_core=2\nnproc=8\nram_gib=15.50\nnic=eth0\nnic_speed_mbps=1000\npython3=Python 3.12.3\nhome_fs=ext2/ext3\n" >"$LAB/logs/inventory/pc00"
inv_parse_all >/dev/null 2>&1
awk -F, "NR == 2 { print NF }" "$LAB/machines.csv" >"$LAB/nf.out"')"
	n=$(cat "$d/lab/nf.out")
	[[ "$n" == 16 ]] || pnote "machines.csv row has $n fields, want 16"
	verdict "inventory: a comma in cpu_model keeps 16 CSV fields"
}
t13() {
	local d n
	d="$(_row 'printf "pc00\t10.0.0.1\tx\thost0\n" >"$LAB/hosts.tsv"
mkdir -p "$LAB/logs/inventory"
printf "os_id=ubuntu\ncpu_model=Intel(R) Core(TM) i5-8500 CPU @ 3.00GHz\nhome_fs=ext2/ext3\n" >"$LAB/logs/inventory/pc00"
inv_parse_all >/dev/null 2>&1
awk -F, "NR == 2 { print NF \" \" \$7 }" "$LAB/machines.csv" >"$LAB/nf.out"')"
	n=$(cat "$d/lab/nf.out")
	[[ "$n" == "16 Intel(R) Core(TM) i5-8500 CPU @ 3.00GHz" ]] || pnote "got [$n]"
	verdict "inventory: a plain cpu_model is written unchanged"
}

# pins.sh: the PyTorch CPU index lists "2.5.1+cpu"; the exact pin 2.5.1 exists
# there and must not fall back to a lower version.
PINS_FAKE='
source "$REPO/scripts/campaign/pins.sh"
mkdir -p "$LAB/fakebin"
cat >"$LAB/fakebin/python3" <<FAKE
#!/usr/bin/env bash
if [ "\$1 \$2 \$3" = "-m pip index" ]; then echo "torch (2.5.1+cpu)"; echo "Available versions: $AVAIL"; exit 0; fi
exec /usr/bin/python3 "\$@"
FAKE
chmod +x "$LAB/fakebin/python3"
PATH="$LAB/fakebin:$PATH"
'
t14() {
	local d
	d="$(_row "AVAIL='2.5.1+cpu, 2.5.0+cpu, 2.4.1+cpu'
$PINS_FAKE
_pin_nearest torch 2.5.1 >\"\$LAB/pin.out\" 2>/dev/null")"
	[[ "$(cat "$d/lab/pin.out")" == "2.5.1" ]] || pnote "torch 2.5.1 (index has 2.5.1+cpu) -> [$(cat "$d/lab/pin.out")]"
	verdict "pins: torch 2.5.1 found as 2.5.1+cpu keeps the exact pin"
}
t15() {
	local d
	d="$(_row "AVAIL='2.5.0+cpu, 2.4.1+cpu'
$PINS_FAKE
_pin_nearest torch 2.5.1 >\"\$LAB/pin.out\" 2>/dev/null")"
	[[ "$(cat "$d/lab/pin.out")" == "2.5.0" ]] || pnote "torch 2.5.1 absent -> [$(cat "$d/lab/pin.out")], want 2.5.0 (no +cpu)"
	verdict "pins: torch 2.5.1 absent -> nearest lower 2.5.0, local tag dropped"
}

# w2_dask.py imports dask.dataframe, which needs pyarrow; the lab venv and the
# dev venv both have to list it (D1). Static rows: pins.sh needs a full closure.
t16() {
	local here_dir
	here_dir="$(dirname "$0")/.."
	grep -q "printf 'pyarrow" "$here_dir/pins.sh" || pnote "pins.sh does not write pyarrow"
	grep -qx pyarrow "$here_dir/requirements-dev.txt" || pnote "requirements-dev.txt lacks pyarrow"
	verdict "requirements: pyarrow listed for dask.dataframe (lab and dev)"
}
t17() {
	local here_dir
	here_dir="$(dirname "$0")/.."
	grep -q "printf 'dask\\[distributed\\]" "$here_dir/pins.sh" || pnote "pins.sh lost dask[distributed]"
	grep -q "printf 'ray\\[default\\]" "$here_dir/pins.sh" || pnote "pins.sh lost ray[default]"
	verdict "requirements: dask[distributed] and ray[default] still listed"
}

# port-lines.sh: the torchrun port (t1) is counted: diff of the .py plus the .sh glue
t18() {
	local d want got
	d="$(_row 'bash "$REPO/scripts/campaign/port-lines.sh" >/dev/null 2>&1; cp "$LAB/port-lines.csv" "$LAB/pl.out"')"
	local b="$REPO/docs/benchmarks/workloads/baselines"
	want=$(( $(diff -u "$REPO/docs/benchmarks/workloads/t1_mlp.py" "$b/t1_torchrun.py" | grep -c '^[+-][^+-]' || true) + $(grep -Evc '^[[:space:]]*(#|$)' "$b/t1_torchrun.sh") ))
	got=$(awk -F, '$1 == "baselines/t1_torchrun.py" { print $3 }' "$d/lab/pl.out")
	[[ "$got" == "$want" && "$want" -gt 0 ]] || pnote "t1_torchrun counted '$got', want $want"
	[[ "$(grep -c 't1_torchrun' "$d/lab/pl.out")" == 1 ]] || pnote "t1_torchrun must be one row: $(grep t1_torchrun "$d/lab/pl.out" | tr '\n' '|')"
	verdict "port-lines: the torchrun port is one row (diff + glue); other ports unchanged"
}

# port-lines.sh: the sweep, no-op and latency ports are measured against their originals
t19() {
	local d w port orig
	d="$(_row 'bash "$REPO/scripts/campaign/port-lines.sh" >/dev/null 2>&1; cp "$LAB/port-lines.csv" "$LAB/pl.out"')"
	for w in sweep_ray:sweep_map noop_ray:noop_map noop_dask:noop_map lat_ray:lat_map lat_dask:lat_map sweep_dask:sweep_map; do
		port="${w%%:*}"
		orig="${w##*:}"
		grep -qE "^baselines/$port\.py,docs/benchmarks/workloads/$orig\.py,[0-9]+$" "$d/lab/pl.out" ||
			pnote "$port has no row against $orig.py"
	done
	verdict "port-lines: sweep, noop and lat ports each have a row against their originals"
}

# port-lines.sh: the labelled Pool arm for Ray is its own row; the idiomatic w1_ray.py keeps its own
t20() {
	local d want got
	d="$(_row 'bash "$REPO/scripts/campaign/port-lines.sh" >/dev/null 2>&1; cp "$LAB/port-lines.csv" "$LAB/pl.out"')"
	local b="$REPO/docs/benchmarks/workloads/baselines"
	want=$(diff -u "$REPO/docs/benchmarks/workloads/pool_map.py" "$b/w1_ray_pool.py" | grep -c '^[+-][^+-]' || true)
	got=$(awk -F, '$1 == "baselines/w1_ray_pool.py" { print $3 }' "$d/lab/pl.out")
	[[ "$got" == "$want" ]] || pnote "w1_ray_pool counted '$got', want $want"
	[[ "$(grep -c '^baselines/w1_ray\.py,' "$d/lab/pl.out")" == 1 ]] || pnote "w1_ray.py must keep its own row"
	verdict "port-lines: w1_ray_pool.py is its own row and w1_ray.py keeps its row"
}

printf 'scripts/campaign inventory/network/clocks/port-lines/pins case table\n\n'
t1
t2
t3
t4
t5
t6
t7
t8
t9
t10
t11
t12
t13
t14
t15
t16
t17
t18
t19
t20
printf '\n'

total=$((pass + failed))
if ((failed == 0)); then
	printf 'PASS %d/%d\n' "$pass" "$total"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$failed" "$total"
exit 1
