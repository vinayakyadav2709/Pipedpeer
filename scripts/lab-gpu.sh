#!/usr/bin/env bash
# The 2-GPU DDP rehearsal: gpu1 and gpu2 share this laptop's one GPU (docker +
# CDI), sub1 has no GPU and submits (lab/gpu/compose.yml, 10.89.0.0/24).
# gpu1 starts the cluster and invites the others. This laptop's own daemon is not involved.
#
#   scripts/lab-gpu.sh up                 fresh nodes, setup -y on each, not connected
#   scripts/lab-gpu.sh connect <node...|all> gpu1 starts the cluster, the others connect
#   scripts/lab-gpu.sh x <node> <cmd...>  run a command on a node, in /work
#   scripts/lab-gpu.sh logs               daemon logs -> ~/Downloads/pipedpeer-work/evidence/gpulab/<time>/
#   scripts/lab-gpu.sh down               remove the nodes and the network
#
# Memory: 3 nodes x 1500m. Never run it with lab-lan or lab-nat up.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cmd="${1:-}"
[[ $# -gt 0 ]] && shift

usage() {
	sed -n '5,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
	exit 2
}

source "$repo_root/scripts/lib/runtime.sh"
# docker: it has CDI (nvidia.com/gpu=all) and the nvidia runtime here; checked
# with `docker run --device nvidia.com/gpu=all ... nvidia-smi`.
PP_RUNTIME_ORDER=docker pp_pick_runtime || exit 1
compose() { (cd "$repo_root/lab/gpu" && pp_compose "$@"); }

all_nodes=(gpu1 gpu2 sub1)
rdv_ip=10.89.0.11
logroot="$HOME/Downloads/pipedpeer-work/evidence/gpulab"
tokenfile="$logroot/.token"

ctr() { echo "pp-gpu-$1"; }
ip_of() { case "$1" in gpu1) echo 10.89.0.11 ;; gpu2) echo 10.89.0.12 ;; sub1) echo 10.89.0.13 ;; esac; }
fail() { echo "FAIL: $*" >&2; exit 1; }
wait_for() {
	local secs="$1"; shift
	local end=$((SECONDS + secs))
	until "$@" >/dev/null 2>&1; do
		((SECONDS < end)) || return 1
		sleep 3
	done
}

x() {
	local n="$1"; shift
	local flags=(-i -w /work)
	[[ -t 0 && -t 1 ]] && flags+=(-t)
	docker exec "${flags[@]}" "$(ctr "$n")" "$@"
}
root() { local n="$1"; shift; docker exec "$(ctr "$n")" "$@"; }
healthy() { root "$1" curl -sf --max-time 5 "http://127.0.0.1:4242/health"; }
check_node() {
	case "$1" in gpu1 | gpu2 | sub1) ;; *) fail "no such node: $1 (gpu1 gpu2 sub1)" ;; esac
}

cmd_up() {
	local logdir n
	logdir="$logroot/$(date +%Y%m%d-%H%M%S)-up"
	mkdir -p "$logdir"
	echo "logs: $logdir"
	free -g | sed -n 2p
	# Static, so the same binary runs on the nix image.
	(cd "$repo_root/src" && CGO_ENABLED=0 go build -o "$repo_root/lab/pipedpeer" .)

	compose down --remove-orphans >/dev/null 2>&1 || echo "note: nothing to tear down first"
	compose up -d --build >"$logdir/compose-up.log" 2>&1 ||
		{ tail -30 "$logdir/compose-up.log"; fail "compose up (log: $logdir/compose-up.log)"; }

	for n in "${all_nodes[@]}"; do
		root "$n" mkdir -p /work
		docker cp -q "$repo_root/pipedpeer-demo/." "$(ctr "$n"):/work"
		root "$n" rm -rf /work/__pycache__ /work/results /work/demo/finale-logs
	done
	for n in "${all_nodes[@]}"; do
		wait_for 240 healthy "$n" || {
			docker logs "$(ctr "$n")" 2>&1 | tail -20
			fail "$n's daemon never answered on /health"
		}
		echo "  $n up at $(ip_of "$n") (not connected)"
	done
	echo "GPU as gpu1 sees it:"
	x gpu1 nvidia-smi -L || echo "warn: gpu1 cannot run nvidia-smi" >&2
	echo "next: scripts/lab-gpu.sh connect all"
}

cmd_connect() {
	(($#)) || usage
	local nodes=("$@") token n
	[[ "$1" == all ]] && nodes=("${all_nodes[@]}")
	for n in "${nodes[@]}"; do check_node "$n"; done
	mkdir -p "$logroot"
	if [[ ! -s "$tokenfile" ]]; then
		od -An -tx1 -N16 /dev/urandom | tr -d ' \n' >"$tokenfile"
	fi
	token="$(<"$tokenfile")"
	# gpu1 starts the cluster (token in cluster.json) and hands out the invite;
	# every daemon introduces, so no separate introducer process.
	if ! root gpu1 curl -sf --max-time 3 http://127.0.0.1:4242/health >/dev/null 2>&1; then
		root gpu1 sh -c "mkdir -p \"\${XDG_DATA_HOME:-\$HOME/.local/share}/pipedpeer\" && umask 077 &&
			printf '{\"token\":\"%s\"}\n' '$token' >\"\${XDG_DATA_HOME:-\$HOME/.local/share}/pipedpeer/cluster.json\""
		x gpu1 pipedpeer start </dev/null >/dev/null
	fi
	local code
	code="$(x gpu1 pipedpeer invite </dev/null | awk '$2 == "connect" { print $3; exit }')"
	[[ -n "$code" ]] || { echo "gpu1 printed no invite" >&2; exit 1; }
	for n in "${nodes[@]}"; do
		[[ "$n" == gpu1 ]] && continue
		echo "== $n"
		x "$n" pipedpeer connect "$code" </dev/null || echo "warn: connect on $n exited $?" >&2
	done
	echo "== sub1 sees:"
	x sub1 pipedpeer status </dev/null
}

cmd_x() {
	(($# >= 2)) || usage
	check_node "$1"
	x "$@"
}

cmd_logs() {
	local logdir n f
	logdir="$logroot/$(date +%Y%m%d-%H%M%S)-logs"
	mkdir -p "$logdir"
	for n in "${all_nodes[@]}"; do
		root "$n" sh -c 'cat $(find /var/lib/pipedpeer-state /root/.local/state -name daemon.log 2>/dev/null)' \
			>"$logdir/$n.daemon.log" 2>&1 ||
			echo "warn: no daemon.log from $n (see $logdir/$n.daemon.log)" >&2
		docker logs "$(ctr "$n")" >"$logdir/$n.out" 2>&1 ||
			echo "warn: no container output from $n" >&2
	done
	echo "logs: $logdir"
	for f in "$logdir"/*; do printf '  %-24s %s lines\n' "$(basename "$f")" "$(wc -l <"$f")"; done
}

cmd_down() { compose down --remove-orphans; }

case "$cmd" in
up) cmd_up ;;
connect) cmd_connect "$@" ;;
x) cmd_x "$@" ;;
logs) cmd_logs ;;
down) cmd_down ;;
*) usage ;;
esac
