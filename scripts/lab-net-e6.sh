#!/usr/bin/env bash
# e6: the demo, end to end, between two home networks whose private subnets
# overlap. `scripts/lab-net.sh e6` (and so `make test-net`) runs this.
#
#   site 1: 192.168.0.0/24 behind router r1 (NAT)   laptop A = ppnode-a 192.168.0.2
#   site 2: 192.168.0.0/24 behind router r2 (NAT)   laptop B = ppnode-b 192.168.0.3
#   "internet": 203.0.113.0/24 (TEST-NET-3), introducer .10, routers .11/.12
#
# A has the secret and the introducer in cluster.json and prints the invite;
# B runs setup, then `connect <code>`, as the install line does. Each sees the
# other at 127.0.0.1:<forwarder>. A's own 192.168.0.2 means nothing on B's
# site (ARP for it fails there), the trap behind FIX-DDPINET. The routers also
# give the sites a way out to cache.nixos.org (an uplink bridge with the host
# on it) so cold environments build; peers only know the 203.0.113.x side.
#
# Every command of the testing log's demo block and laptop items runs on A,
# each asserting its exit code and, where work should cross, that both
# machines took a share. One PASS/FAIL row per check; exit 1 on any FAIL.
# Nodes are the lab's nix image with crun; the warm set (LAB_E6_WARM, default
# v3cpu: torch-cpu) is loaded first, so cpu.py's pandas/joblib build cold.
#
# Knobs: PIPEDPEER (binary; default built from src/), LAB_E6_MEM (per node,
# default 1.5g), LAB_E6_CPUS (2), LAB_E6_WARM, LAB_E6_TIMEOUT (per command,
# 1800 s), LAB_E6_KEEP=1 keeps the rig up. Logs: one file per command in
# ~/.local/state/pipedpeer/lab-net-e6/<time>/.
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
source "$repo_root/scripts/lib/lab.sh"
source "$repo_root/scripts/lib/cli.sh"

mem="${LAB_E6_MEM:-1.5g}"
cpus="${LAB_E6_CPUS:-2}"
cmd_timeout="${LAB_E6_TIMEOUT:-1800}"
build_dir="${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer/lab-net-e6"
logdir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/lab-net-e6/$(date +%Y%m%d-%H%M%S)"
nix_image=pp-net-nix
results=()
fails=0
n=0

pp_lab_runtime || exit 1

# --- rig -------------------------------------------------------------------

cleanup() {
	local ids net
	ids="$($rt ps -aq --filter label=pipedpeer.lab=net-e6) $($rt ps -aq --filter label=pipedpeer.lab=net-e6r)"
	# shellcheck disable=SC2086
	[[ -z "${ids// /}" ]] || $rt rm -f $ids >/dev/null 2>&1
	for net in $($rt network ls --format '{{.Name}}' | grep '^ppn6-'); do
		$rt network rm "$net" >/dev/null 2>&1 || echo "lab-net-e6: could not remove network $net" >&2
	done
}

x() {
	local c="$1"
	shift
	$rt exec "ppn6-$c" sh -c "$*"
}

# router <name> <site net> <uplink ip> <public ip> <site-side docker ip>
router() {
	$rt run -d --name "ppn6-$1" --hostname "$1" --label pipedpeer.lab=net-e6r \
		--privileged --memory 128m --memory-swap 128m \
		--network ppn6-up --ip "$3" pp-net >/dev/null &&
		$rt network connect --ip "$4" ppn6-pub "ppn6-$1" &&
		$rt network connect --ip "$5" "$2" "ppn6-$1" &&
		x "$1" "sif=\$(ip -o -4 addr show | awk '\$4 ~ /^${5%.*}\\./ { print \$2 }') &&
			ip addr flush dev \$sif && ip addr add 192.168.0.1/24 dev \$sif &&
			ip route replace default via 10.74.0.1 && router.sh nat 192.168.0.0/24"
}

# node <name> <site net> <docker ip> <lan ip>
node() {
	$rt run -d --name "ppn6-$1" --hostname "ppnode-$1" --label pipedpeer.lab=net-e6 \
		--privileged --cpus "$cpus" --memory "$mem" --memory-swap "$mem" \
		--network "$2" --ip "$3" \
		-v "$build_dir/pipedpeer:/usr/local/bin/pipedpeer:ro" \
		-e XDG_DATA_HOME=/var/lib/pipedpeer \
		"$nix_image" sleep infinity >/dev/null &&
		x "$1" "ip addr flush dev eth0 && ip addr add $4/24 dev eth0 && ip route replace default via 192.168.0.1 &&
			echo 'nameserver 1.1.1.1' >/etc/resolv.conf &&
			printf %s ppnode-$1 | md5sum | cut -c1-32 >/etc/machine-id &&
			mkdir -p /var/lib/pipedpeer/pipedpeer /var/log /tmp /root/demo"
}

rig() {
	local tok
	pp_lab_net ppn6-up 10.74.0.0/24 nat
	pp_lab_net ppn6-pub 203.0.113.0/24
	pp_lab_net ppn6-s1 10.75.1.0/24
	pp_lab_net ppn6-s2 10.75.2.0/24
	router r1 ppn6-s1 10.74.0.11 203.0.113.11 10.75.1.254 || return 1
	router r2 ppn6-s2 10.74.0.12 203.0.113.12 10.75.2.254 || return 1
	tok="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
	$rt run -d --name ppn6-intro --hostname introducer --label pipedpeer.lab=net-e6r \
		--privileged --memory 256m --memory-swap 256m --network ppn6-pub --ip 203.0.113.10 \
		-v "$build_dir/pipedpeer:/usr/local/bin/pipedpeer:ro" -e XDG_DATA_HOME=/var/lib/pipedpeer \
		pp-net >/dev/null || return 1
	x intro "mkdir -p /var/lib/pipedpeer/pipedpeer && umask 077 &&
		printf '{\"token\":\"%s\",\"introducers\":[\"127.0.0.1:4244\"]}\n' '$tok' >/var/lib/pipedpeer/pipedpeer/cluster.json &&
		pipedpeer start --introducer-only >/var/log/pp-start.log 2>&1" || { x intro 'cat /var/log/pp-start.log' >&2; return 1; }
	node a ppn6-s1 10.75.1.10 192.168.0.2 || return 1
	node b ppn6-s2 10.75.2.10 192.168.0.3 || return 1
	x a "umask 077 && printf '{\"token\":\"%s\",\"introducers\":[\"203.0.113.10:4244\"]}\n' '$tok' \
		>/var/lib/pipedpeer/pipedpeer/cluster.json"
	echo "loading warm set ${LAB_E6_WARM:-v3cpu}..."
	LAB_NAME=net-e6 LAB_WARM_SET="${LAB_E6_WARM:-v3cpu}" "$repo_root/scripts/lab-emu-warm.sh" load ||
		echo "lab-net-e6: warm set not loaded; torch builds cold" >&2
	$rt cp "$repo_root/pipedpeer-demo/cpu.py" ppn6-a:/root/demo/ &&
		$rt cp "$repo_root/pipedpeer-demo/gpu.py" ppn6-a:/root/demo/ &&
		$rt cp "$repo_root/pipedpeer-demo/both.py" ppn6-a:/root/demo/ || return 1
	for c in a b; do
		x "$c" 'pipedpeer setup -y >/var/log/pp-setup.log 2>&1' || {
			echo "lab-net-e6: setup failed on $c:" >&2
			x "$c" 'tail -20 /var/log/pp-setup.log' >&2
			return 1
		}
	done
}

# --- checks ----------------------------------------------------------------

now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
lt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a < b) }'; }

# on <node> <tag> <dir> <cmd...>: run one command; sets log, rc, took.
on() {
	local c="$1" tag="$2" dir="$3" t0
	shift 3
	n=$((n + 1))
	log="$logdir/$(printf %02d "$n")-$tag.log"
	echo "  \$ $*  (on $c)" >"$log"
	t0="$(now)"
	$rt exec -w "$dir" "ppn6-$c" timeout "$cmd_timeout" "$@" >>"$log" 2>&1
	rc=$?
	took="$(since "$t0")"
}

# row <check> <why>: PASS when why is empty. Prints the log's tail on FAIL.
row() {
	local res=PASS
	if [[ -n "$2" ]]; then
		res=FAIL
		fails=$((fails + 1))
		echo "  --- $(basename "$log"):" >&2
		tail -15 "$log" | sed 's/^/    /' >&2
	fi
	results+=("$(printf '%-4s %-44s %8ss  %s' "$res" "$1" "$took" "$2")")
	echo "$res  $1 (${took}s) $2"
}

# share <log> <machine>: that machine's % in the last "ran on" line, else 0.
share() {
	local s
	s="$(grep -o ' — ran on .*' "$1" | tail -1 | grep -oE "$2 [0-9]+%" | grep -oE '[0-9]+')"
	echo "${s:-0}"
}
crossed() { (($(share "$log" ppnode-a) > 0 && $(share "$log" ppnode-b) > 0)); }
only_b() { (($(share "$log" ppnode-a) == 0 && $(share "$log" ppnode-b) > 0)); }
has() { grep -qF -- "$1" "$log"; }

# Both machines healthy in A's status (one line each: name state ...).
both_healthy() {
	local st
	st="$($rt exec ppn6-a pipedpeer status 2>/dev/null)" || return 1
	awk '$1 == "ppnode-a" && $2 == "healthy" { a = 1 } $1 == "ppnode-b" && $2 == "healthy" { b = 1 } END { exit !(a && b) }' <<<"$st"
}

# b_path [want]: B's path in A's status (lan, internet, ...); with want, test it.
b_path() {
	local p
	p="$($rt exec ppn6-a pipedpeer status 2>/dev/null | awk '$1 == "ppnode-b" { print $3; exit }')"
	if (($#)); then [[ "$p" == "$1" ]]; else echo "${p:-none}"; fi
}

wait_for() { # wait_for <s> <cmd...>
	local limit="$1" t0
	shift
	t0="$(now)"
	until "$@"; do
		lt "$(since "$t0")" "$limit" || return 1
		sleep 1
	done
}

# ok: "exit N; " unless the last command exited 0.
ok() { ((rc == 0)) && echo "" || echo "exit $rc; "; }

checks() {
	local why code cold t0 url

	# connect
	on a invite / pipedpeer invite
	code="$(awk '$1 == "pipedpeer" && $2 == "connect" { print $3; exit }' "$log")"
	row "invite prints a connect line" "$(ok)$([[ -n "$code" ]] || echo "no connect line")"
	on b connect / pipedpeer connect "$code"
	why="$(ok)"
	has "connected" || why+="no 'connected'; "
	row "connect from site 2" "$why"
	took=-
	wait_for 90 both_healthy || { row "both healthy within 90 s" "not healthy"; return; }
	row "both healthy within 90 s" ""

	on a status / pipedpeer status
	why="$(ok)"
	awk '$1 == "ppnode-b" && $2 == "healthy" { f = 1 } END { exit !f }' "$log" || why+="ppnode-b not healthy; "
	b_path internet || why+="A reaches B by $(b_path) path; "
	row "status lists B healthy" "$why"

	# cpu.py, cold then warm environment
	on a cpu-cold /root/demo pipedpeer run cpu.py
	cold="$took"
	why="$(ok)"
	has "Pool: 128 items, checksum 3839999232" || why+="Pool checksum; "
	# Not asserted: B may still be receiving the new environment.
	row "run cpu.py (cold env)" "$why"
	results[-1]+="(ran on a=$(share "$log" ppnode-a)% b=$(share "$log" ppnode-b)%)"
	why=""
	has "joblib: 64 Mandelbrot tiles, 217451260 points inside" || why+="joblib result; "
	has "pandas: 3,000,000 rows -> 50 cities" || why+="pandas result; "
	row "run cpu.py: joblib + pandas results" "$why"
	on a cpu-warm /root/demo pipedpeer run cpu.py
	why="$(ok)"
	has "Pool: 128 items, checksum 3839999232" || why+="Pool checksum; "
	crossed || why+="ran on a=$(share "$log" ppnode-a)% b=$(share "$log" ppnode-b)%; "
	lt "$took" "$cold" || why+="warm ${took}s not under cold ${cold}s; "
	row "run cpu.py (warm env)" "$why"
	on a steps / pipedpeer jobs last --steps
	why="$(ok)"
	has ppnode-b || why+="no step on ppnode-b; "
	row "jobs last --steps" "$why"

	# gpu.py (CPU torch on both)
	on a gpu-auto /root/demo pipedpeer run gpu.py
	why="$(ok)"
	has "validation loss" || why+="no validation loss; "
	row "run gpu.py" "$why"
	on a ddp1 /root/demo pipedpeer run --ddp 1 gpu.py
	why="$(ok)"
	has "validation loss" || why+="no validation loss; "
	row "run --ddp 1 gpu.py" "$why"
	local sync
	for sync in auto exact fast; do
		if [[ "$sync" == auto ]]; then
			on a "ddp2-$sync" /root/demo pipedpeer run --ddp 2 gpu.py
		else
			on a "ddp2-$sync" /root/demo pipedpeer run --ddp 2 --ddp-sync "$sync" gpu.py
		fi
		# Both ranks print (rank 1's output is relayed from B), so two
		# validation losses = both trained to the end. A DDP run prints no
		# "ran on" line: the lead's record holds no rank-1 steps.
		why="$(ok)"
		[[ "$(grep -c '^validation loss' "$log")" == 2 ]] ||
			why+="$(grep -c '^validation loss' "$log")/2 ranks finished; "
		has "No route to host" && why+="No route to host; "
		row "run --ddp 2 --ddp-sync $sync gpu.py" "$why"
	done
	on a ddp-wrong /root/demo pipedpeer run --ddp-sync wrong gpu.py
	why=""
	((rc != 0)) || why+="exit 0; "
	has '--ddp-sync must be exact or fast, got "wrong"' || why+="no usage message; "
	row "run --ddp-sync wrong: refused" "$why"

	on a both /root/demo pipedpeer run both.py
	why="$(ok)"
	has "GPU phase: validation loss" || why+="no GPU phase result; "
	crossed || why+="ran on a=$(share "$log" ppnode-a)% b=$(share "$log" ppnode-b)%; "
	row "run both.py" "$why"

	# --remote: flags reach the script, it runs on B only
	on a remote /root/demo pipedpeer run --remote gpu.py --epochs 2
	why="$(ok)"
	has "epoch 1:" || why+="no epoch 1; "
	has "epoch 2:" && why+="--epochs 2 not passed; "
	only_b || why+="ran on a=$(share "$log" ppnode-a)% b=$(share "$log" ppnode-b)%; "
	row "run --remote gpu.py --epochs 2" "$why"
	on a remote-missing /root/demo pipedpeer run --remote --gpu force gpu.y
	why=""
	((rc != 0)) || why+="exit 0; "
	lt "$took" 30 || why+="not immediate; "
	row "run --remote missing script: refused" "$why"

	# workspace edit + rerun, on B
	x a 'mkdir -p /root/ws && printf "print(open(\"data.txt\").read().strip())\n" >/root/ws/ws.py && echo v1 >/root/ws/data.txt'
	on a ws1 /root/ws pipedpeer run --remote ws.py
	why="$(ok)"
	grep -qx v1 "$log" || why+="no v1; "
	only_b || why+="ran on a=$(share "$log" ppnode-a)% b=$(share "$log" ppnode-b)%; "
	row "workspace: run --remote ws.py" "$why"
	x a 'echo v2 >/root/ws/data.txt'
	on a ws2 /root/ws pipedpeer run --remote ws.py
	why="$(ok)"
	grep -qx v2 "$log" || why+="edit not shipped (no v2); "
	row "workspace: edit, rerun sees it" "$why"

	# failover: B's router stops forwarding mid-Pool (Wi-Fi off), then back
	n=$((n + 1))
	log="$logdir/$(printf %02d "$n")-failover.log"
	x a 'rm -f /root/fo.log /root/fo.rc'
	$rt exec -d -w /root/demo ppn6-a sh -c "timeout $cmd_timeout pipedpeer run cpu.py --items 240 >/root/fo.log 2>&1; echo \$? >/root/fo.rc"
	t0="$(now)"
	wait_for 600 x a 'grep -q "cpu.py on" /root/fo.log 2>/dev/null'
	sleep 10
	x r2 'iptables -I FORWARD 1 -j DROP'
	wait_for "$cmd_timeout" x a 'test -s /root/fo.rc'
	x r2 'iptables -D FORWARD 1'
	x a 'cat /root/fo.log' >"$log"
	rc="$(x a 'cat /root/fo.rc')"
	took="$(since "$t0")"
	why="$(ok)"
	has "Pool: 240 items, checksum 7199999008" || why+="Pool checksum; "
	row "failover: B cut mid-run, run completes" "$why"
	t0="$(now)"
	wait_for 60 both_healthy
	took="$(since "$t0")"
	both_healthy && why="" || why="B not back in A's status; "
	b_path internet || why+="A reaches B by $(b_path) path; "
	row "failover: B back within 60 s, no command" "$why"

	# ui, doctor, export
	on a ui-print / pipedpeer ui --print
	url="$(grep -o 'http://[^ ]*' "$log" | head -1)"
	code="$($rt exec ppn6-a curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url")"
	why="$(ok)"
	[[ "$code" == 200 ]] || why+="GET $url -> $code; "
	$rt exec ppn6-a sh -c 'tok="$(grep -o "\"token\": *\"[^\"]*\"" "$XDG_DATA_HOME/pipedpeer/cluster.json" | cut -d"\"" -f4)"
		curl -sf --max-time 5 -H "X-Pipedpeer-Token: $tok" http://127.0.0.1:4242/v1/nodes' >>"$log" 2>&1
	has ppnode-b || why+="/v1/nodes lacks ppnode-b; "
	[[ "$(grep -o '"state":"healthy"' "$log" | wc -l)" -ge 2 ]] || why+="fewer than 2 healthy nodes; "
	row "ui: page 200, both nodes listed" "$why"
	# The containers see the host's NVIDIA module without nvidia-smi: that
	# one ✗ is the lab's; any other fails.
	for c in a b; do
		on "$c" "doctor-$c" / pipedpeer doctor
		why="$(grep '^✗' "$log" | grep -v 'nvidia-smi is missing' | cut -c1-90 | tr '\n' ';')"
		row "doctor on ${c^^} (no ✗ but the lab's GPU one)" "$why"
	done
	on a export / pipedpeer jobs --export-all /root/pp-runs
	why="$(ok)"
	local csv json
	csv="$(x a 'find /root/pp-runs -name "*.csv" | wc -l')"
	json="$(x a 'find /root/pp-runs -name "*.json" | wc -l')"
	((csv >= 5 && json >= 5)) || why+="$csv csv, $json json; "
	row "jobs --export-all" "$why"
}

# --- main ------------------------------------------------------------------

pp_resolve_cli "$repo_root" || exit 2
mkdir -p "$build_dir" "$logdir"
install -m 0755 "$PP_CLI" "$build_dir/pipedpeer"
cp "$repo_root/lab/net/Dockerfile" "$repo_root/lab/net/ppnodes" "$repo_root/lab/net/router.sh" "$build_dir/"
echo "building images..."
pp_lab_image pp-net "$build_dir" || { echo "lab-net-e6: image build failed" >&2; exit 1; }
$rt build -q -t "$nix_image" -f "$repo_root/lab/nat/Dockerfile.node" "$repo_root/lab/nat" >/dev/null ||
	{ echo "lab-net-e6: nix image build failed" >&2; exit 1; }
pp_lab_mem_check "$((2 * $(pp_bytes "$mem") + 2 * $(pp_bytes 128m) + $(pp_bytes 256m)))" lab-net-e6 || exit 1

cleanup
[[ "${LAB_E6_KEEP:-0}" == 1 ]] || trap cleanup EXIT
echo "logs: $logdir"
echo "rig: two NATed 192.168.0.0/24 sites, introducer on 203.0.113.10"
rig || { echo "lab-net-e6: rig failed" >&2; exit 1; }
checks

echo
echo "res  check                                         took  note"
printf '%s\n' "${results[@]}"
echo "$((${#results[@]} - fails)) passed, $fails failed  (logs: $logdir)"
((fails == 0))
