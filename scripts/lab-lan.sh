#!/usr/bin/env bash
# The demo venue, in containers: one LAN (docker bridge 10.88.0.0/24), this
# laptop on it as LAPTOP-A at 10.88.0.1, and four college PCs (lab/lan/compose.yml).
#
#   scripts/lab-lan.sh up                 fresh pcs, setup -y on each, not joined
#   scripts/lab-lan.sh join <pc...|all>   auth set <this laptop's token> + join 10.88.0.1
#   scripts/lab-lan.sh x <pc> <cmd...>    run a command on a pc, in /work (a copy
#                                         of pipedpeer-demo/); pc4 runs it as `pp`
#   scripts/lab-lan.sh logs               daemon logs -> ~/.local/state/pipedpeer/lab-lan/<time>/
#   scripts/lab-lan.sh down               remove the pcs and the network
#
# This laptop plays LAPTOP-A with its own daemon and rendezvous, as at the
# venue. Before `join`, on this laptop:
#   pipedpeer auth set                    (once; the pcs get the same token)
#   pipedpeer rendezvous                  (in its own terminal)
#   pipedpeer join 10.88.0.1
# PIPEDPEER picks this laptop's binary (default bin/pipedpeer, else PATH).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cmd="${1:-}"
[[ $# -gt 0 ]] && shift

usage() {
	sed -n '5,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
	exit 2
}

source "$repo_root/scripts/lib/runtime.sh"
# docker, not rootless podman: this laptop must sit on the bridge as 10.88.0.1.
PP_RUNTIME_ORDER=docker pp_pick_runtime || exit 1
compose() { (cd "$repo_root/lab/lan" && pp_compose "$@"); }

if [[ -n "${PIPEDPEER:-}" ]]; then
	cli="$PIPEDPEER"
elif [[ -x "$repo_root/bin/pipedpeer" ]]; then
	cli="$repo_root/bin/pipedpeer"
else
	cli="$(command -v pipedpeer || true)"
fi

all_pcs=(pc1 pc2 pc3 pc4)
host_ip=10.88.0.1
logroot="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/lab-lan"

ctr() { echo "pp-lan-$1"; }
ip_of() { echo "10.88.0.$((10 + ${1#pc}))"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
# wait_for <seconds> <command...>: poll every 3 s until it succeeds.
wait_for() {
	local secs="$1"; shift
	local end=$((SECONDS + secs))
	until "$@" >/dev/null 2>&1; do
		((SECONDS < end)) || return 1
		sleep 3
	done
}

# x <pc> <cmd...>: pc4 is the multi-user machine, so its commands run as `pp`
# in pp's user session (the daemon is pp's user service there).
x() {
	local pc="$1"; shift
	local flags=(-i -w /work)
	[[ -t 0 && -t 1 ]] && flags+=(-t)
	if [[ "$pc" == pc4 ]]; then
		flags+=(-u pp -e HOME=/home/pp -e XDG_RUNTIME_DIR=/run/user/1000
			-e DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus
			-e PATH=/nix/var/nix/profiles/default/bin:/usr/local/bin:/usr/bin:/bin)
	fi
	docker exec "${flags[@]}" "$(ctr "$pc")" "$@"
}
root() { local pc="$1"; shift; docker exec "$(ctr "$pc")" "$@"; }
healthy() { root "$1" curl -sf --max-time 5 "http://127.0.0.1:38080/health"; }

check_pc() {
	case "$1" in pc[1-4]) ;; *) fail "no such pc: $1 (pc1-pc4)" ;; esac
}

# The demo scripts, fresh, without local leftovers (bytecode, earlier results).
copy_work() {
	local pc="$1" c
	c="$(ctr "$pc")"
	root "$pc" mkdir -p /work
	docker cp -q "$repo_root/pipedpeer-demo/." "$c:/work"
	root "$pc" rm -rf /work/__pycache__ /work/results /work/demo/finale-logs
	[[ "$pc" != pc4 ]] || root pc4 chown -R pp:pp /work
}

# pc4 as the Ubuntu stand-in, the way scripts/lab-nat.sh trust mode does it:
# Determinate multi-user nix, then `pp` runs setup -y with sudo once.
setup_pc4() {
	local log="$1/pc4-setup.log"
	wait_for 120 root pc4 systemctl is-active dbus || fail "systemd never came up in pc4"
	echo "pc4: installing multi-user nix (Determinate installer; a few minutes)..."
	root pc4 sh -c "curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix |
		sh -s -- install linux --no-confirm" >"$log" 2>&1 ||
		{ tail -20 "$log"; fail "nix install in pc4 (log: $log)"; }
	root pc4 loginctl enable-linger pp
	wait_for 30 root pc4 test -S /run/user/1000/bus || fail "no user session for pp in pc4"
	wait_for 60 x pc4 nix store info || fail "pp cannot reach the nix-daemon in pc4"
	# The one sudo. A temporary blanket rule stands in for the user typing the
	# password; it is removed straight after.
	root pc4 sh -c "echo 'pp ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/zz-lab-setup && chmod 0440 /etc/sudoers.d/zz-lab-setup"
	echo "pc4: pipedpeer setup -y as pp..."
	if ! x pc4 pipedpeer setup -y </dev/null >>"$log" 2>&1; then
		root pc4 rm -f /etc/sudoers.d/zz-lab-setup
		tail -20 "$log"
		fail "setup as pp in pc4 (log: $log)"
	fi
	root pc4 rm -f /etc/sudoers.d/zz-lab-setup
}

cmd_up() {
	local logdir pc
	logdir="$logroot/$(date +%Y%m%d-%H%M%S)-up"
	mkdir -p "$logdir"
	echo "logs: $logdir"
	# Static, so the same binary runs on the nix image and on debian.
	if [[ -d "$repo_root/src" ]]; then
		(cd "$repo_root/src" && CGO_ENABLED=0 go build -o "$repo_root/lab/pipedpeer" .)
	elif [[ -x "$cli" ]]; then
		cp "$cli" "$repo_root/lab/pipedpeer"
	else
		fail "no source to build from; set PIPEDPEER to a static binary"
	fi

	compose down --remove-orphans >/dev/null 2>&1 || echo "note: nothing to tear down first"
	echo "bringing up: ${all_pcs[*]}"
	compose up -d --build >"$logdir/compose-up.log" 2>&1 ||
		{ tail -30 "$logdir/compose-up.log"; fail "compose up (log: $logdir/compose-up.log)"; }

	# New containers carry new certificates; this laptop's pins for their
	# addresses are stale by design. Only the lab's addresses are forgotten.
	if [[ -n "$cli" ]]; then
		for pc in "${all_pcs[@]}"; do
			"$cli" auth forget "$(ip_of "$pc"):38080" >/dev/null 2>&1 ||
				echo "warn: could not forget the pin for $(ip_of "$pc"):38080" >&2
		done
	else
		echo "warn: no pipedpeer on this laptop (set PIPEDPEER); skipped forgetting old pins" >&2
	fi

	for pc in "${all_pcs[@]}"; do copy_work "$pc"; done
	setup_pc4 "$logdir"

	for pc in "${all_pcs[@]}"; do
		wait_for 240 healthy "$pc" || {
			docker logs "$(ctr "$pc")" 2>&1 | tail -20
			fail "$pc's daemon never answered on /health"
		}
		echo "  $pc up at $(ip_of "$pc") (not joined)"
	done
	echo "next: scripts/lab-lan.sh join all   (or leave one out for the live join)"
}

cmd_join() {
	(($#)) || usage
	local pcs=("$@") token pc
	[[ "$1" == all ]] && pcs=("${all_pcs[@]}")
	for pc in "${pcs[@]}"; do check_pc "$pc"; done
	[[ -n "$cli" ]] || fail "no pipedpeer on this laptop (set PIPEDPEER)"
	token="$("$cli" auth show)"
	[[ -n "$token" && "$token" != "no token set"* ]] ||
		fail "this laptop has no cluster token; run: $cli auth set"
	if ! ss -Hlun "sport = :38445" 2>/dev/null | grep -q .; then
		echo "warn: nothing listens on udp 38445 here; start the rendezvous: $cli rendezvous" >&2
	fi
	for pc in "${pcs[@]}"; do
		echo "== $pc"
		x "$pc" pipedpeer auth set "$token" </dev/null >/dev/null
		x "$pc" pipedpeer join "$host_ip" </dev/null || echo "warn: join on $pc exited non-zero" >&2
	done
	echo "== this laptop sees:"
	"$cli" nodes
}

cmd_x() {
	(($# >= 2)) || usage
	check_pc "$1"
	x "$@"
}

cmd_logs() {
	local logdir pc f host_log
	logdir="$logroot/$(date +%Y%m%d-%H%M%S)-logs"
	mkdir -p "$logdir"
	for pc in "${all_pcs[@]}"; do
		root "$pc" sh -c 'cat $(find /var/lib/pipedpeer-state /root/.local/state /home/pp/.local/state \
			-name daemon.log 2>/dev/null)' >"$logdir/$pc.daemon.log" 2>&1 ||
			echo "warn: no daemon.log from $pc (see $logdir/$pc.daemon.log)" >&2
		docker logs "$(ctr "$pc")" >"$logdir/$pc.out" 2>&1 ||
			echo "warn: no container output from $pc" >&2
	done
	host_log="${PIPEDPEER_DAEMON_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer}/daemon.log"
	if [[ -f "$host_log" ]]; then
		cp "$host_log" "$logdir/laptop-a.daemon.log"
	else
		echo "warn: this laptop's daemon log is not at $host_log" >&2
	fi
	echo "logs: $logdir"
	for f in "$logdir"/*; do printf '  %-24s %s lines\n' "$(basename "$f")" "$(wc -l <"$f")"; done
}

cmd_down() {
	compose down --remove-orphans
	echo "this laptop is still joined to $host_ip; at the venue: pipedpeer join <real rendezvous IP>"
}

case "$cmd" in
up) cmd_up ;;
join) cmd_join "$@" ;;
x) cmd_x "$@" ;;
logs) cmd_logs ;;
down) cmd_down ;;
*) usage ;;
esac
