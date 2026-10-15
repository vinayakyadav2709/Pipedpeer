#!/usr/bin/env bash
# Network scenarios against the built binary: the 2026-10-15 docker
# experiments (E1-E5), made permanent and asserting.
#
#   scripts/lab-net.sh [scenario...]    default: all; `make test-net` runs this
#
#   e1   secret-only LAN: three machines with the same token, all healthy < 15 s
#   e2   connect ends < 3 s after the second machine's daemon is up
#   e3a  a connected peer changes IP: back < 30 s, still one row
#   e3b  the introducer changes IP: everyone back < 60 s, no commands
#   e4   a discovery-only peer changes IP: re-found at the new address < 30 s
#   e5a  no route to the introducer: connect exits 1 and says it did not answer
#   e5d  router passes UDP only: doctor names TCP as blocked
#   e5e  LAN discovery never overwrites a working internet path
#
# Rig (experiments file): debian containers with the binary bind-mounted, on
# docker bridges the host has no address on (inhibit_ipv4), so this machine's
# own daemon can neither see nor be seen. A router container joins subnets
# where a scenario needs one. Every scenario builds its rig fresh and tears it
# down, which also restarts the introducer between cases (it remembers peers
# for 2 minutes). Each container is capped at 256 MB, swap included.
#
# Knobs: PIPEDPEER (binary; default built from src/), LAB_NET_KEEP=1 keeps
# the last rig up for a look.
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
source "$repo_root/scripts/lib/lab.sh"
source "$repo_root/scripts/lib/cli.sh"

image=pp-net
mem=256m
build_dir="${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer/lab-net"
all=(e1 e2 e3a e3b e4 e5a e5d e5e)
results=()

pp_lab_runtime || exit 1

# --- helpers ---------------------------------------------------------------

cleanup() {
	local ids
	ids="$($rt ps -aq --filter label=pipedpeer.lab=net)"
	# shellcheck disable=SC2086
	[[ -z "$ids" ]] || $rt rm -f $ids >/dev/null 2>&1
	for n in $($rt network ls --format '{{.Name}}' | grep '^ppn-'); do
		$rt network rm "$n" >/dev/null 2>&1 || echo "lab-net: could not remove network $n" >&2
	done
}

# node <name> <network> <ip> [extra run args...]
node() {
	local name="$1" net="$2" ip="$3"
	shift 3
	$rt run -d --name "ppn-$name" --hostname "$name" --label pipedpeer.lab=net \
		--privileged --memory "$mem" --memory-swap "$mem" \
		--network "$net" --ip "$ip" \
		-v "$build_dir/pipedpeer:/usr/local/bin/pipedpeer:ro" \
		-e XDG_DATA_HOME=/var/lib/pipedpeer \
		"$@" "$image" >/dev/null
}

x() {
	local n="$1"
	shift
	$rt exec "ppn-$n" sh -c "$*"
}

token_on() {
	x "$1" "mkdir -p /var/lib/pipedpeer/pipedpeer && umask 077 &&
		printf '{\"token\":\"%s\"}\n' '$2' >/var/lib/pipedpeer/pipedpeer/cluster.json"
}

start() {
	x "$1" 'pipedpeer start >/var/log/pp-start.log 2>&1' && return 0
	echo "  start failed on $1:" >&2
	x "$1" 'cat /var/log/pp-start.log' >&2
	return 1
}

# /health answers 401 without the token once a node has one (authtoken).
nid() {
	x "$1" 'tok="$(jq -r ".token // empty" "$XDG_DATA_HOME/pipedpeer/cluster.json" 2>/dev/null)"
		curl -sf --max-time 3 -H "X-Pipedpeer-Token: $tok" http://127.0.0.1:4242/health | jq -r .node_id' 2>/dev/null
}

# rows <node>: "node_id state path host" per /v1/nodes row.
rows() { x "$1" ppnodes 2>/dev/null; }

healthy() { rows "$1" | awk -v id="$2" '$1 == id && $2 == "healthy" { f = 1 } END { exit !f }'; }
row_count() { rows "$1" | awk -v id="$2" '$1 == id' | wc -l; }
host_of() { rows "$1" | awk -v id="$2" '$1 == id { print $4; exit }'; }

# mesh <node...>: every node lists every other one healthy. Uses ids[].
declare -A ids
mesh() {
	local a b
	for a in "$@"; do
		for b in "$@"; do
			[[ "$a" == "$b" ]] || healthy "$a" "${ids[$b]}" || return 1
		done
	done
}

invite_code() { x "$1" 'pipedpeer invite' 2>/dev/null | awk '$1 == "pipedpeer" && $2 == "connect" { print $3; exit }'; }

now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
lt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a < b) }'; }

# wait_for <timeout s> <cmd...>: poll every 0.5 s.
wait_for() {
	local limit="$1" t0
	shift
	t0="$(now)"
	until "$@"; do
		lt "$(since "$t0")" "$limit" || return 1
		sleep 0.5
	done
}

# recover <limit s> <cmd...>: after a move, wait for cmd to hold again. A
# check that holds before the old path was even noticed dead proves nothing,
# so success counts only after the node was seen failing, or after 12 s (one
# 10 s poll since the move) when the path survived the move outright.
# Prints the recovery time.
recover() {
	local limit="$1" t0 down=0 t
	shift
	t0="$(now)"
	while :; do
		t="$(since "$t0")"
		if "$@"; then
			if ((down)) || ! lt "$t" 12; then
				echo "$t"
				return 0
			fi
		else
			down=1
		fi
		lt "$t" "$limit" || { echo "$t"; return 1; }
		sleep 0.5
	done
}

move() { # move <node> <network> <new ip>
	$rt network disconnect "$2" "ppn-$1" && $rt network connect --ip "$3" "$2" "ppn-$1"
}

dump() {
	local n
	for n in "$@"; do
		echo "  --- $n /v1/nodes:" >&2
		rows "$n" | sed 's/^/    /' >&2
		echo "  --- $n daemon.log (tail):" >&2
		x "$n" 'tail -15 /root/.local/state/pipedpeer/daemon.log 2>/dev/null' | sed 's/^/    /' >&2
	done
}

result() { # result <scenario> <PASS|FAIL> <measured> <limit> [note]
	results+=("$(printf '%-4s %-4s %8s %6s  %s' "$1" "$2" "$3" "$4" "${5:-}")")
	echo "$1: $2 ($3 / limit $4) ${5:-}"
}

new_token() { od -An -N24 -tx1 /dev/urandom | tr -d ' \n'; }

# Same token on every node, LAN discovery on, nobody connects.
lan_trio() { # lan_trio <net> <subnet prefix, e.g. 10.70.1>
	local tok n i=2
	tok="$(new_token)"
	pp_lab_net "$1" "$2.0/24"
	for n in a b c; do
		node "$n" "$1" "$2.$i" || return 1
		token_on "$n" "$tok"
		i=$((i + 1))
	done
}

# a holds the token and introduces; b, c join it with connect. Discovery off,
# so only the introducer path is under test.
connected_trio() { # connected_trio <net> <subnet prefix>
	local tok code n i=2
	tok="$(new_token)"
	pp_lab_net "$1" "$2.0/24"
	for n in a b c; do
		node "$n" "$1" "$2.$i" -e PIPEDPEER_NO_LAN_DISCOVERY=1 || return 1
		i=$((i + 1))
	done
	token_on a "$tok"
	start a || return 1
	code="$(invite_code a)"
	[[ -n "$code" ]] || { echo "  invite printed no connect line" >&2; return 1; }
	for n in b c; do
		x "$n" "timeout 90 pipedpeer connect '$code'" >/dev/null 2>&1 ||
			{ echo "  connect on $n failed" >&2; return 1; }
	done
	for n in a b c; do ids[$n]="$(nid "$n")"; done
	wait_for 30 mesh a b c || { echo "  no full mesh after connect" >&2; dump a b c; return 1; }
}

# --- scenarios -------------------------------------------------------------

e1() {
	lan_trio ppn-e1 10.70.1 || { result e1 FAIL - 15s "rig"; return; }
	local t0 n
	t0="$(now)"
	for n in a b c; do start "$n" & done
	wait
	for n in a b c; do ids[$n]="$(nid "$n")"; done
	if wait_for 15 mesh a b c; then
		result e1 PASS "$(since "$t0")s" 15s
	else
		result e1 FAIL "$(since "$t0")s" 15s "not every node healthy"
		dump a b c
	fi
}

e2() {
	local tok code t0 t_up="" rc
	tok="$(new_token)"
	pp_lab_net ppn-e2 10.70.2.0/24
	node a ppn-e2 10.70.2.2 -e PIPEDPEER_NO_LAN_DISCOVERY=1
	node b ppn-e2 10.70.2.3 -e PIPEDPEER_NO_LAN_DISCOVERY=1
	token_on a "$tok"
	start a || { result e2 FAIL - 3s "a did not start"; return; }
	code="$(invite_code a)"
	[[ -n "$code" ]] || { result e2 FAIL - 3s "invite printed no connect line"; return; }
	$rt exec -d ppn-b sh -c "pipedpeer connect '$code' >/var/log/connect.log 2>&1; echo \$? >/var/log/connect.rc"
	t0="$(now)"
	while lt "$(since "$t0")" 90; do
		if [[ -z "$t_up" ]] && x b 'curl -s -o /dev/null --max-time 1 http://127.0.0.1:4242/health' >/dev/null 2>&1; then # any answer, 401 included, means up
			t_up="$(now)"
		fi
		x b 'test -f /var/log/connect.rc' && break
		sleep 0.2
	done
	rc="$(x b 'cat /var/log/connect.rc 2>/dev/null')"
	ids[a]="$(nid a)"
	ids[b]="$(nid b)"
	if [[ "$rc" != 0 || -z "$t_up" ]]; then
		result e2 FAIL "rc=${rc:-none}" 3s "connect did not succeed"
		x b 'cat /var/log/connect.log' >&2
		return
	fi
	local took
	took="$(since "$t_up")"
	if lt "$took" 3 && mesh a b; then
		result e2 PASS "${took}s" 3s "after b's daemon was up"
	else
		result e2 FAIL "${took}s" 3s "connect slow, or the pair is not healthy"
		x b 'cat /var/log/connect.log' >&2
		dump a b
	fi
}

e3a() {
	connected_trio ppn-e3 10.70.3 || { result e3a FAIL - 30s "rig"; return; }
	b_back() { healthy a "${ids[b]}" && healthy c "${ids[b]}" &&
		[[ "$(row_count a "${ids[b]}")" == 1 && "$(row_count c "${ids[b]}")" == 1 ]]; }
	move b ppn-e3 10.70.3.23 || { result e3a FAIL - 30s "move failed"; return; }
	local t
	if t="$(recover 30 b_back)"; then
		result e3a PASS "${t}s" 30s "one row"
	else
		result e3a FAIL "${t}s" 30s "b not back, or more than one row"
		dump a c
	fi
}

e3b() {
	connected_trio ppn-e3b 10.70.5 || { result e3b FAIL - 60s "rig"; return; }
	all_back() { mesh a b c; }
	move a ppn-e3b 10.70.5.22 || { result e3b FAIL - 60s "move failed"; return; }
	local t
	if t="$(recover 60 all_back)"; then
		result e3b PASS "${t}s" 60s "no commands run"
	else
		result e3b FAIL "${t}s" 60s "cluster did not recover by itself"
		dump a b c
	fi
}

e4() {
	lan_trio ppn-e4 10.70.4 || { result e4 FAIL - 30s "rig"; return; }
	local n t
	for n in a b c; do start "$n" & done
	wait
	for n in a b c; do ids[$n]="$(nid "$n")"; done
	wait_for 20 mesh a b c || { result e4 FAIL - 30s "no mesh before the move"; dump a b c; return; }
	b_refound() {
		local o
		for o in a c; do
			healthy "$o" "${ids[b]}" || return 1
			[[ "$(row_count "$o" "${ids[b]}")" == 1 ]] || return 1
			[[ "$(host_of "$o" "${ids[b]}")" == 10.70.4.23 ]] || return 1
		done
	}
	move b ppn-e4 10.70.4.23 || { result e4 FAIL - 30s "move failed"; return; }
	if t="$(recover 30 b_refound)"; then
		result e4 PASS "${t}s" 30s "one row, at the new address"
	else
		result e4 FAIL "${t}s" 30s "b not re-found at 10.70.4.23 with one row"
		dump a c
	fi
}

# Two subnets joined by a router container at .254 on each.
routed_pair() { # routed_pair <router mode>
	pp_lab_net ppn-n1 10.71.1.0/24
	pp_lab_net ppn-n2 10.71.2.0/24
	node r ppn-n1 10.71.1.254 || return 1
	$rt network connect --ip 10.71.2.254 ppn-n2 ppn-r || return 1
	x r "router.sh $1" || return 1
	node a ppn-n1 10.71.1.2 || return 1
	node b ppn-n2 10.71.2.2 || return 1
	x a 'ip route add 10.71.2.0/24 via 10.71.1.254'
	x b 'ip route add 10.71.1.0/24 via 10.71.2.254'
}

e5a() {
	routed_pair drop || { result e5a FAIL - 90s "rig"; return; }
	local code out rc t0
	token_on a "$(new_token)"
	start a || { result e5a FAIL - 90s "a did not start"; return; }
	code="$(invite_code a)"
	[[ -n "$code" ]] || { result e5a FAIL - 90s "invite printed no connect line"; return; }
	t0="$(now)"
	out="$(x b "timeout 120 pipedpeer connect '$code'" 2>&1)"
	rc=$?
	if [[ "$rc" == 1 ]] && grep -qiE "introducer.*(not|n't|never) answer|no answer.*introducer|nobody answered" <<<"$out"; then
		result e5a PASS "$(since "$t0")s" 90s "exit 1, names the introducer"
	else
		result e5a FAIL "rc=$rc" 90s "want exit 1 and 'introducer did not answer'"
		printf '%s\n' "$out" | tail -15 | sed 's/^/    /' >&2
	fi
}

# The venue: one L2 segment, two /24s, a router-on-a-stick between them.
venue() { # venue <router mode>
	pp_lab_net ppn-l2 10.72.0.0/16
	node g ppn-l2 10.72.70.1 || return 1
	node a ppn-l2 10.72.70.2 || return 1
	node b ppn-l2 10.72.220.165 || return 1
	# The router must not tell a and b to talk directly (ICMP redirect).
	x g "ip addr flush dev eth0 && ip addr add 10.72.70.1/24 dev eth0 && ip addr add 10.72.220.1/24 dev eth0 &&
		sysctl -qw net.ipv4.conf.all.send_redirects=0 net.ipv4.conf.eth0.send_redirects=0 && router.sh $1" || return 1
	x a "ip addr flush dev eth0 && ip addr add 10.72.70.2/24 dev eth0 && ip route add default via 10.72.70.1 &&
		sysctl -qw net.ipv4.conf.all.accept_redirects=0" || return 1
	x b "ip addr flush dev eth0 && ip addr add 10.72.220.165/24 dev eth0 && ip route add default via 10.72.220.1 &&
		sysctl -qw net.ipv4.conf.all.accept_redirects=0" || return 1
}

e5d() {
	venue udp-only || { result e5d FAIL - 60s "rig"; return; }
	local tok out rc t0
	tok="$(new_token)"
	token_on a "$tok"
	token_on b "$tok"
	start a || { result e5d FAIL - 60s "a did not start"; return; }
	start b || { result e5d FAIL - 60s "b did not start"; return; }
	sleep 20 # discovery finds the peer over UDP; its TCP checks then time out
	t0="$(now)"
	out="$(x a 'timeout 90 pipedpeer doctor' 2>&1)"
	rc=$?
	if [[ "$rc" != 0 ]] && grep -qiE "tcp.*(block|closed|filtered|not reach|unreachable|time)|(block|filtered).*tcp" <<<"$out"; then
		result e5d PASS "$(since "$t0")s" 60s "doctor exit $rc, names TCP"
	else
		result e5d FAIL "rc=$rc" 60s "want doctor to fail naming TCP as blocked"
		printf '%s\n' "$out" | tail -20 | sed 's/^/    /' >&2
	fi
}

e5e() {
	venue udp-only || { result e5e FAIL - 30s "rig"; return; }
	local code rc bad=0 i
	token_on a "$(new_token)"
	start a || { result e5e FAIL - 30s "a did not start"; return; }
	code="$(invite_code a)"
	[[ -n "$code" ]] || { result e5e FAIL - 30s "invite printed no connect line"; return; }
	x b "timeout 90 pipedpeer connect '$code'" >/dev/null 2>&1
	rc=$?
	ids[a]="$(nid a)"
	ids[b]="$(nid b)"
	wait_for 30 mesh a b || { result e5e FAIL "rc=$rc" 30s "internet link never came up"; dump a b; return; }
	# 30 s of samples: discovery keeps seeing the LAN address it cannot use
	# (TCP blocked); the working internet path must stay the one in use.
	for i in $(seq 1 30); do
		mesh a b || bad=$((bad + 1))
		rows a | awk -v id="${ids[b]}" '$1 == id && $3 == "lan" { f = 1 } END { exit !f }' && bad=$((bad + 1))
		sleep 1
	done
	if ((bad == 0)); then
		result e5e PASS "0/30" "0" "connect exit $rc; stayed on the internet path"
	else
		result e5e FAIL "$bad/30" "0" "samples unhealthy or switched to the lan path"
		dump a b
	fi
}

# --- main ------------------------------------------------------------------

scenarios=("$@")
((${#scenarios[@]})) || scenarios=("${all[@]}")
for s in "${scenarios[@]}"; do
	[[ " ${all[*]} " == *" $s "* ]] || { echo "unknown scenario $s (${all[*]})" >&2; exit 2; }
done

pp_resolve_cli "$repo_root" || exit 2
mkdir -p "$build_dir"
install -m 0755 "$PP_CLI" "$build_dir/pipedpeer"
cp "$repo_root/lab/net/Dockerfile" "$repo_root/lab/net/ppnodes" "$repo_root/lab/net/router.sh" "$build_dir/"
echo "building image..."
pp_lab_image "$image" "$build_dir" || { echo "lab-net: image build failed" >&2; exit 1; }

# Largest rig: three nodes and a router.
pp_lab_mem_check "$((4 * $(pp_bytes "$mem")))" lab-net || exit 1

cleanup
[[ "${LAB_NET_KEEP:-0}" == 1 ]] || trap cleanup EXIT
last="${scenarios[${#scenarios[@]} - 1]}"
for s in "${scenarios[@]}"; do
	echo "== $s"
	ids=()
	"$s"
	[[ "$s" == "$last" && "${LAB_NET_KEEP:-0}" == 1 ]] || cleanup
done

echo
echo "scen res  measured  limit  note"
printf '%s\n' "${results[@]}"
fails="$(printf '%s\n' "${results[@]}" | awk '$2 == "FAIL"' | wc -l)"
echo "$((${#results[@]} - fails)) passed, $fails failed"
((fails == 0))
