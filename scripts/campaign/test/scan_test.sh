#!/usr/bin/env bash
# scan_test.sh: the S4 case table for serve.sh and scan.sh, against the fake
# lab (brief S4). Nothing here touches the network, ~/.ssh or results/lab.
#
# Two things about the fake lab shape this test. Its PCs are directories, and
# its ssh stub takes the host from the argument, so a PC is reached by the
# address scan.sh scanned only if that address resolves to a directory: each
# scanned IP is a symlink to a pcNN home. And the LAB-PC line lives at an
# absolute path on a real PC, which a redirected HOME cannot fake, so a small
# ssh wrapper first in PATH rewrites that one path to the host's own copy and
# logs the arguments (which is also how the root login is checked).

set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$here/../../.." && pwd)"
export REPO
# shellcheck source=/dev/null
source "$here/fakelab.sh"

scan="$here/../scan.sh"
serve="$here/../serve.sh"

state="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/scan-test.$$"
serve_pid=""
cleanup() {
	if [[ -n "$serve_pid" ]]; then
		kill "$serve_pid" 2>/dev/null || true
		wait "$serve_pid" 2>/dev/null || true
	fi
	rm -rf "$state"
}
trap cleanup EXIT
mkdir -p "$state"

pass=0
fail=0
ok() {
	pass=$((pass + 1))
	printf 'ok   %s\n' "$*"
}
bad() {
	fail=$((fail + 1))
	printf 'FAIL %s\n' "$*"
}
check_eq() { # <what> <want> <got>
	if [[ "$2" == "$3" ]]; then
		ok "$1"
	else
		bad "$1"
		printf '       want: %s\n       got:  %s\n' "$2" "$3"
	fi
}
check_has() { # <what> <text> <substring>
	if [[ "$2" == *"$3"* ]]; then
		ok "$1"
	else
		bad "$1"
		printf '       no [%s] in: %s\n' "$3" "$2"
	fi
}

# --- the fake lab ---------------------------------------------------------

# new_lab <tag> <n>: a fake lab with n PCs, its own results dir, its own HOME,
# and the ssh wrapper first on PATH. Sets LAB and FAKELAB_DIR for the caller.
new_lab() {
	local tag="$1" n="$2"
	local d="$state/$tag"
	mkdir -p "$d/fake" "$d/lab" "$d/home" "$d/wrap"
	export HOME="$d/home"
	export LAB_RESULTS="$d/lab"
	# shellcheck disable=SC2086
	fakelab_init "$d/fake" "$n" || return 1
	export PATH="$d/wrap:$PATH"
	cat >"$d/wrap/ssh" <<'SCAN-TEST-SSH'
#!/usr/bin/env bash
# Test-only ssh: rewrite the absolute LAB-PC line path to the fake host's own
# copy, log the arguments, then hand over to the fake-lab stub.
set -uo pipefail
printf '%s\n' "$*" >>"${FAKELAB_DIR:?}/scan-args.log"
host=""
skip=0
for a in "$@"; do
	if ((skip)); then
		skip=0
		continue
	fi
	case "$a" in
		-F | -o | -p | -l | -i | -e | -c | -b | -D | -J | -L | -R | -W) skip=1 ;;
		--) break ;;
		-*) ;;
		*)
			host="$a"
			break
			;;
	esac
done
if [[ -n "$host" ]]; then
	args=()
	line="/var/lib/pipedpeer-lab/lab-pc.line"
	for a in "$@"; do
		args+=("${a//$line/$FAKELAB_DIR/$host/lab-pc.line}")
	done
	exec "$FAKELAB_DIR/bin/ssh" "${args[@]}"
fi
exec "$FAKELAB_DIR/bin/ssh" "$@"
SCAN-TEST-SSH
	chmod +x "$d/wrap/ssh"

	cat >"$d/nmap.sh" <<'SCAN-TEST-NMAP'
#!/usr/bin/env bash
# Fake nmap -oG -: one grepable line per address in $NMAP_IPS.
while read -r ip; do
	[[ -z "$ip" ]] && continue
	printf 'Host: %s ()  Ports: 22/open/tcp//ssh///\n' "$ip"
done <"${NMAP_IPS:?}"
SCAN-TEST-NMAP
	: >"$d/ips"
	export NMAP_IPS="$d/ips"
	export SCAN_NMAP_CMD="bash $d/nmap.sh"
	export LAB="$d/lab"
}

# pc_line <ip> <pc> <user>: this address is that PC, and it has the LAB-PC
# line lab-pc.sh would have written.
pc_line() {
	ln -sfn "$FAKELAB_DIR/$2" "$FAKELAB_DIR/$1"
	printf 'LAB-PC host=%s.lab ip=%s user=%s cores=8 ram_gib=31\n' \
		"$2" "$1" "$3" >"$FAKELAB_DIR/$2/lab-pc.line"
}

# scan_add <ip>: the next scan finds this address.
scan_add() { printf '%s\n' "$1" >>"$NMAP_IPS"; }

# rows <tsv>: id, ip, user, hostname per row.
rows() { awk -F'\t' '!/^#/ && NF { printf "%s %s %s %s\n", $1, $2, $3, $4 }' "$1"; }

# cfg_user <ssh_config> <host>: the User line under "Host <host>".
cfg_user() {
	awk -v h="Host $2" '
		$0 == h { f = 1; next }
		/^Host / { f = 0 }
		f && $1 == "User" { print $2; exit }
	' "$1"
}

# --- row 1: two PCs, two lab users ---------------------------------------

row_users() {
	local out rc=0
	new_lab users 2 || return
	pc_line 10.99.0.21 pc00 alice
	pc_line 10.99.0.22 pc01 lab
	scan_add 10.99.0.21
	scan_add 10.99.0.22

	out="$(bash "$scan" 10.99.0.0/24 2>"$state/users.err")" || rc=$?
	check_eq "two users: scan exit" 0 "$rc"
	check_eq "two users: summary" "SCAN found=2 new=2 unreachable=0" "$out"
	check_eq "two users: hosts.tsv rows" \
		$'pc00 10.99.0.21 alice pc00.lab\npc01 10.99.0.22 lab pc01.lab' \
		"$(rows "$LAB/hosts.tsv")"
	check_eq "two users: ssh_config User for pc00" alice "$(cfg_user "$LAB/ssh_config" pc00)"
	check_eq "two users: ssh_config User for pc01" lab "$(cfg_user "$LAB/ssh_config" pc01)"
	check_eq "two users: ssh_config HostName for pc01" 10.99.0.22 \
		"$(awk '/^Host pc01$/ { f = 1; next } /^Host / { f = 0 } f && $1 == "HostName" { print $2 }' "$LAB/ssh_config")"
	check_eq "two users: later ssh reaches pc01" hi \
		"$(ssh -F "$LAB/ssh_config" pc01 -- echo hi 2>/dev/null)"
	check_has "two users: the line is read as root" \
		"$(cat "$FAKELAB_DIR/scan-args.log" 2>/dev/null)" "-o User=root"
}

# --- row 2: fresh scan of three, ids by IP order -------------------------

row_fresh() {
	local out rc=0
	new_lab fresh 3 || return
	# Deliberately not pc00 first, and .9 last lexically: ids follow the IPs.
	pc_line 10.99.0.11 pc01 lab
	pc_line 10.99.0.9 pc02 lab
	pc_line 10.99.0.10 pc00 lab
	scan_add 10.99.0.11
	scan_add 10.99.0.9
	scan_add 10.99.0.10

	out="$(bash "$scan" 10.99.0.0/24 2>"$state/fresh.err")" || rc=$?
	check_eq "fresh: scan exit" 0 "$rc"
	check_eq "fresh: summary" "SCAN found=3 new=3 unreachable=0" "$out"
	check_eq "fresh: ids follow the addresses numerically" \
		$'pc00 10.99.0.9 lab pc02.lab\npc01 10.99.0.10 lab pc00.lab\npc02 10.99.0.11 lab pc01.lab' \
		"$(rows "$LAB/hosts.tsv")"
}

# --- row 3: rescan with one new address ----------------------------------

row_rescan() {
	local out rc=0 before
	new_lab rescan 4 || return
	pc_line 10.99.0.11 pc01 lab
	pc_line 10.99.0.9 pc02 lab
	pc_line 10.99.0.10 pc00 lab
	scan_add 10.99.0.11
	scan_add 10.99.0.9
	scan_add 10.99.0.10
	bash "$scan" 10.99.0.0/24 >/dev/null 2>&1 || true
	before="$(rows "$LAB/hosts.tsv")"

	pc_line 10.99.0.12 pc03 lab
	scan_add 10.99.0.12
	out="$(bash "$scan" 10.99.0.0/24 2>"$state/rescan.err")" || rc=$?
	check_eq "rescan: scan exit" 0 "$rc"
	check_eq "rescan: summary" "SCAN found=4 new=1 unreachable=0" "$out"
	check_eq "rescan: old rows unchanged" "$before" \
		"$(rows "$LAB/hosts.tsv" | head -3)"
	check_eq "rescan: new address is the next id" "pc03 10.99.0.12 lab pc03.lab" \
		"$(rows "$LAB/hosts.tsv" | tail -1)"
}

# --- row 4: a PC whose key login fails -----------------------------------

row_unreachable() {
	local out rc=0
	new_lab unreach 3 || return
	pc_line 10.99.0.31 pc00 lab
	pc_line 10.99.0.32 pc01 lab
	pc_line 10.99.0.33 pc02 lab
	scan_add 10.99.0.31
	scan_add 10.99.0.32
	scan_add 10.99.0.33
	export FAKELAB_FAIL=10.99.0.33

	out="$(bash "$scan" 10.99.0.0/24 2>"$state/unreach.err")" || rc=$?
	unset FAKELAB_FAIL
	check_eq "unreachable: scan exit" 0 "$rc"
	check_has "unreachable: summary counts it" "$out" "SCAN found=2 new=2 unreachable=1"
	check_has "unreachable: listed" "$out" "UNREACHABLE 10.99.0.33"
	check_eq "unreachable: left out of hosts.tsv" \
		$'pc00 10.99.0.31 lab pc00.lab\npc01 10.99.0.32 lab pc01.lab' \
		"$(rows "$LAB/hosts.tsv")"
}

# --- row 5: --pc00 on a fresh scan ---------------------------------------

row_pc00_fresh() {
	local out
	new_lab pc00fresh 2 || return
	pc_line 10.99.0.41 pc00 lab
	pc_line 10.99.0.42 pc01 lab
	scan_add 10.99.0.41
	scan_add 10.99.0.42

	out="$(bash "$scan" 10.99.0.0/24 --pc00 10.99.0.42 2>"$state/pc00.err")" || true
	check_eq "pc00 fresh: summary" "SCAN found=2 new=2 unreachable=0" "$out"
	check_eq "pc00 fresh: the named address is pc00" "pc00 10.99.0.42 lab pc01.lab" \
		"$(rows "$LAB/hosts.tsv" | head -1)"
	check_eq "pc00 fresh: the other one follows" "pc01 10.99.0.41 lab pc00.lab" \
		"$(rows "$LAB/hosts.tsv" | tail -1)"
}

# --- row 6: --pc00 on an existing hosts.tsv is refused --------------------

row_pc00_refused() {
	local rc=0 err before after
	new_lab pc00refused 2 || return
	pc_line 10.99.0.51 pc00 lab
	pc_line 10.99.0.52 pc01 lab
	scan_add 10.99.0.51
	scan_add 10.99.0.52
	bash "$scan" 10.99.0.0/24 >/dev/null 2>&1 || true
	before="$(sha256sum "$LAB/hosts.tsv" | cut -d' ' -f1)"

	err="$(bash "$scan" 10.99.0.0/24 --pc00 10.99.0.52 2>&1)" || rc=$?
	after="$(sha256sum "$LAB/hosts.tsv" | cut -d' ' -f1)"
	check_eq "pc00 refused: exit" 1 "$rc"
	check_has "pc00 refused: says refused" "$err" "refused"
	check_eq "pc00 refused: hosts.tsv untouched" "$before" "$after"
}

# --- row 7: serve.sh stages and serves ----------------------------------

free_port() {
	python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()'
}

row_serve() {
	local d="$state/serve" port key sha started=0 fetched=""
	mkdir -p "$d/lab" "$d/home"
	export HOME="$d/home"
	export LAB_RESULTS="$d/lab"
	export LAB="$d/lab"
	port="$(free_port)"

	SERVE_PORT="$port" bash "$serve" 127.0.0.1 10.99.0.0/24 \
		>"$d/out" 2>"$d/err" &
	serve_pid=$!

	local i
	for ((i = 0; i < 100; i++)); do
		if [[ -s "$LAB/serve/SETUP-LINE.txt" ]]; then
			fetched="$(python3 - "$port" <<'PY' 2>/dev/null || true
import sys, urllib.request
print(urllib.request.urlopen("http://127.0.0.1:%s/lab-pc.sh" % sys.argv[1], timeout=2).read().decode())
PY
)"
			[[ -n "$fetched" ]] && break
		fi
		sleep 0.1
	done
	[[ -n "$fetched" ]] && started=1
	check_eq "serve: the file is served" 1 "$started"

	key="$HOME/.ssh/pipedpeer-lab"
	if [[ -f "$LAB/serve/lab-pc.sh" ]]; then
		ok "serve: staged lab-pc.sh"
	else
		bad "serve: staged lab-pc.sh"
	fi
	check_eq "serve: no placeholders left" "" \
		"$(grep -o '__DRIVER_IP__\|__LAB_CIDR__' "$LAB/serve/lab-pc.sh" 2>/dev/null | sort -u | tr '\n' ' ')"
	check_has "serve: driver ip filled in" "$(cat "$LAB/serve/lab-pc.sh" 2>/dev/null)" 'DRIVER="127.0.0.1"'
	check_has "serve: lab cidr filled in" "$(cat "$LAB/serve/lab-pc.sh" 2>/dev/null)" 'LAB_CIDR="10.99.0.0/24"'
	check_eq "serve: driver.pub is the driver key" "$(cat "$key.pub" 2>/dev/null)" \
		"$(cat "$LAB/serve/driver.pub" 2>/dev/null)"
	check_eq "serve: the key was created" 1 "$([[ -f "$key" && -f "$key.pub" ]] && echo 1 || echo 0)"
	check_eq "serve: campaign_id" "lab-$(date +%F)" "$(cat "$LAB/campaign_id" 2>/dev/null)"

	sha="$(sha256sum "$LAB/serve/lab-pc.sh" | cut -d' ' -f1)"
	check_has "serve: setup line has the sha" "$(cat "$LAB/serve/SETUP-LINE.txt" 2>/dev/null)" "$sha  /tmp/lab-pc.sh"
	check_has "serve: setup line has the url" "$(cat "$LAB/serve/SETUP-LINE.txt" 2>/dev/null)" \
		"wget -qO /tmp/lab-pc.sh http://127.0.0.1:$port/lab-pc.sh && echo \"$sha  /tmp/lab-pc.sh\" | sha256sum -c && sudo bash /tmp/lab-pc.sh setup"
	check_eq "serve: the served file is the staged one" "$sha" "$(printf '%s\n' "$fetched" | sha256sum | cut -d' ' -f1)"
	check_has "serve: it printed the setup line" "$(cat "$d/out" 2>/dev/null)" "$(cat "$LAB/serve/SETUP-LINE.txt" 2>/dev/null)"

	kill "$serve_pid" 2>/dev/null || true
	wait "$serve_pid" 2>/dev/null || true
	serve_pid=""
}

row_users
row_fresh
row_rescan
row_unreachable
row_pc00_fresh
row_pc00_refused
row_serve

total=$((pass + fail))
printf 'PASS %d/%d\n' "$pass" "$total"
((fail == 0))
