#!/usr/bin/env bash
# The emulated lab: N equal containers with fixed CPU and memory quotas and a
# shaped link each (tc netem), the benchmark method for the W10 numbers.
#
# Speedup is measured as 1 container vs N containers of the same size. Every
# container carries PIPEDPEER_EMULATED=1, so run records made here are
# labelled emulated. The plain lab (lab-up.sh) proves work crosses nodes and
# nothing more; this one is the emulation method, and its numbers are always
# labelled "emulated".
#
#   scripts/lab-emu.sh up [N]              N containers (default LAB_N or 3), shaped, meshed
#   scripts/lab-emu.sh shape <profile> [loss%]   re-shape every running container
#   scripts/lab-emu.sh exec <i> <cmd...>   run a command in container i (1-based)
#   scripts/lab-emu.sh status              containers, quotas, shaping, healthy peers
#   scripts/lab-emu.sh down
#
# Knobs (environment):
#   PIPEDPEER     binary to run (default: built from src/ into bin/)
#   LAB_CPUS      CPUs per container: one number for equal containers (default 2),
#                 or a list ("8 2 1") for a deliberately uneven set
#   LAB_MEM       memory per container (default 1.5g; use 4g for training)
#   LAB_PROFILE   lan | wifi | 5g | none (default lan)
#   LAB_LOSS      packet loss in percent on every link (default 0)
#   LAB_WORK      host dir mounted read-write at /work in every container
#   LAB_NAME      lab name (default emu): containers pp<name>-<i>, network
#                 pp<name>, its own token file; parallel labs need distinct names
#   LAB_SUBNET    /24 for this lab (default derived from LAB_NAME)
#   LAB_TOKEN     cluster token (default: random, saved for exec/status); passed
#                 as cluster.json {"token"} and PIPEDPEER_TOKEN (older builds)
#   LAB_API_PORT  daemon API port the health checks use (default 4242; 38080
#                 for builds before the port move)
#
# Profiles are one-way per container egress, so a round trip between two
# containers is twice the delay:
#   lan   0.25 ms +-0.05, 1000 mbit   (RTT ~0.5 ms)
#   wifi  4 ms +-2,        80 mbit    (RTT ~8 ms, a busy 5 GHz Wi-Fi)
#   5g    15 ms +-5,      100 mbit    (RTT ~30 ms)
# Shaping covers the container's whole eth0, internet included: a cold
# environment build under "5g" is slower too, as it would be.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/lib/runtime.sh"
source "$repo_root/scripts/lib/lab.sh"
source "$repo_root/scripts/lib/cli.sh"

# Several labs can run side by side (benchmarks, sweeps, functional tests):
# everything a lab owns derives from LAB_NAME, and `down` touches only its own.
lab_name="${LAB_NAME:-emu}"
[[ "$lab_name" =~ ^[a-z0-9-]+$ ]] || { echo "lab-emu: LAB_NAME must be [a-z0-9-]" >&2; exit 2; }
net="pp$lab_name"
prefix="pp$lab_name"
label="pipedpeer.lab=$lab_name"
image=pp-emu
# "emu" keeps 10.86.0.0/24; other names hash into 10.128-191.x.0/24.
if [[ "$lab_name" == emu && -z "${LAB_SUBNET:-}" ]]; then
	subnet=10.86.0.0/24
else
	subnet="$(pp_lab_subnet "$lab_name" 128)"
fi
base="${subnet%.*}"
state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
token_file="$state_dir/lab-$lab_name.token"
build_dir="${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer/lab-$lab_name"

die() { echo "lab-emu: $*" >&2; exit 1; }

netem_args() {
	local loss="${2:-0}"
	case "$1" in
	lan) echo "delay 0.25ms 0.05ms rate 1000mbit loss ${loss}%" ;;
	wifi) echo "delay 4ms 2ms rate 80mbit loss ${loss}%" ;;
	5g) echo "delay 15ms 5ms rate 100mbit loss ${loss}%" ;;
	none) echo "" ;;
	*) die "unknown profile '$1' (lan, wifi, 5g, none)" ;;
	esac
}

running() { $rt ps --filter "label=$label" --format '{{.Names}}' | sort -V; }

shape_one() {
	local name="$1" args
	args="$(netem_args "$2" "${3:-0}")"
	if [[ -z "$args" ]]; then
		$rt exec "$name" tc qdisc del dev eth0 root >/dev/null 2>&1 || true
		return 0
	fi
	# shellcheck disable=SC2086 # args is a word list on purpose
	$rt exec "$name" tc qdisc replace dev eth0 root netem $args ||
		die "tc netem failed in $name (is the sch_netem module available on the host? sudo modprobe sch_netem)"
}

token() {
	if [[ -n "${LAB_TOKEN:-}" ]]; then
		echo "$LAB_TOKEN"
	elif [[ -s "$token_file" ]]; then
		cat "$token_file"
	else
		od -An -N24 -tx1 /dev/urandom | tr -d ' \n'
	fi
}

healthy_peers() {
	$rt exec "$1" sh -c 'curl -sf --max-time 3 -H "X-Pipedpeer-Token: $LAB_TOKEN" "http://127.0.0.1:${LAB_API_PORT:-4242}/v1/nodes"' 2>/dev/null |
		grep -o '"state":"healthy"' | wc -l
}

cmd_up() {
	local n="${1:-${LAB_N:-3}}" mem cpus_list=() i total_cpus=0 host_cpus tok
	mem="${LAB_MEM:-1.5g}"
	read -r -a cpus_list <<<"${LAB_CPUS:-2}"
	if ((${#cpus_list[@]} == 1)); then
		for ((i = 1; i < n; i++)); do cpus_list+=("${cpus_list[0]}"); done
	fi
	((${#cpus_list[@]} == n)) || die "LAB_CPUS lists ${#cpus_list[@]} values for $n containers"
	for c in "${cpus_list[@]}"; do total_cpus=$(awk -v a="$total_cpus" -v b="$c" 'BEGIN { print a + b }'); done
	host_cpus="$(nproc)"
	awk -v a="$total_cpus" -v b="$host_cpus" 'BEGIN { exit !(a <= b) }' ||
		die "refusing: $total_cpus CPUs wanted, the host has $host_cpus (use fewer or smaller containers)"

	pp_lab_runtime || exit 1
	cmd_down >/dev/null
	pp_lab_mem_check "$(($(pp_bytes "$mem") * n))" lab-emu || exit 1

	pp_resolve_cli "$repo_root" || exit 2
	mkdir -p "$build_dir" "$state_dir"
	install -m 0755 "$PP_CLI" "$build_dir/pipedpeer"
	cat >"$build_dir/Dockerfile" <<'DOCKER'
FROM docker.io/nixos/nix:2.31.3
RUN nix-env -iA nixpkgs.crun nixpkgs.iproute2 nixpkgs.curl
CMD ["sh", "-c", "sleep infinity"]
DOCKER
	echo "building image..."
	pp_lab_image "$image" "$build_dir"
	pp_lab_net "$net" "$subnet" nat

	tok="$(token)"
	(umask 077 && printf '%s\n' "$tok" >"$token_file")
	local work=()
	[[ -n "${LAB_WORK:-}" ]] && work=(-v "$(cd "$LAB_WORK" && pwd):/work")

	for ((i = 1; i <= n; i++)); do
		local name="$prefix-$i" c="${cpus_list[$((i - 1))]}"
		echo "starting $name: $c cpu(s), $mem"
		$rt run -d --name "$name" --hostname "$name" \
			--label "$label" \
			--privileged \
			--network "$net" --ip "$base.$((10 + i))" \
			--cpus="$c" --memory="$mem" --memory-swap="$mem" \
			-v "$build_dir/pipedpeer:/usr/local/bin/pipedpeer:ro" \
			"${work[@]}" \
			-e PATH=/usr/local/bin:/root/.nix-profile/bin:/nix/var/nix/profiles/default/bin:/nix/var/nix/profiles/default/sbin:/usr/bin:/bin \
			-e XDG_DATA_HOME=/var/lib/pipedpeer \
			-e PIPEDPEER_EMULATED=1 \
			-e LAB_TOKEN="$tok" \
			-e LAB_NODE="$lab_name-$i" \
			-e PIPEDPEER_TOKEN="$tok" \
			-e LAB_API_PORT="${LAB_API_PORT:-4242}" \
			"$image" sh -c '
				mkdir -p /var/lib/pipedpeer/pipedpeer /var/log /tmp
				# Own machine id per container: the machine key hashes it.
				printf %s "$LAB_NODE" | md5sum | cut -c1-32 >/etc/machine-id
				umask 077
				printf "{\"token\":\"%s\"}\n" "$LAB_TOKEN" >/var/lib/pipedpeer/pipedpeer/cluster.json
				pipedpeer setup -y >/var/log/pipedpeer-setup.log 2>&1 || cat /var/log/pipedpeer-setup.log >&2
				exec sleep infinity' >/dev/null
		shape_one "$name" "${LAB_PROFILE:-lan}" "${LAB_LOSS:-0}"
	done

	echo "waiting for a full mesh ($n nodes)..."
	local deadline=$((SECONDS + 120)) ok
	while :; do
		ok=1
		for ((i = 1; i <= n; i++)); do
			# Each node lists itself and n-1 peers.
			[[ "$(healthy_peers "$prefix-$i")" -ge "$n" ]] || { ok=0; break; }
		done
		((ok)) && break
		if ((SECONDS > deadline)); then
			echo "lab-emu: no full mesh after 120 s; node $i sees $(healthy_peers "$prefix-$i") healthy" >&2
			$rt logs --tail 20 "$prefix-$i" >&2 || true
			exit 1
		fi
		sleep 1
	done
	echo "lab-emu up ($lab_name, $subnet): $n x (${cpus_list[*]} cpu, $mem), profile ${LAB_PROFILE:-lan}, loss ${LAB_LOSS:-0}% (emulated)"
}

cmd_shape() {
	[[ $# -ge 1 ]] || die "usage: shape <lan|wifi|5g|none> [loss%]"
	pp_lab_runtime || exit 1
	local names
	names="$(running)"
	[[ -n "$names" ]] || die "no lab-emu containers running"
	for name in $names; do shape_one "$name" "$1" "${2:-0}"; done
	echo "shaped: $1, loss ${2:-0}%"
}

cmd_exec() {
	[[ $# -ge 2 ]] || die "usage: exec <i> <cmd...>"
	pp_lab_runtime || exit 1
	local i="$1"
	shift
	$rt exec -i -w "${LAB_EXEC_DIR:-/}" "$prefix-$i" "$@"
}

cmd_status() {
	pp_lab_runtime || exit 1
	for name in $(running); do
		printf '%s  cpus=%s mem=%s  healthy=%s  qdisc: %s\n' "$name" \
			"$($rt inspect -f '{{.HostConfig.NanoCpus}}' "$name" | awk '{ print $1 / 1e9 }')" \
			"$(bc_g "$($rt inspect -f '{{.HostConfig.Memory}}' "$name")")G" \
			"$(healthy_peers "$name")" \
			"$($rt exec "$name" tc qdisc show dev eth0 | head -1)"
	done
}

cmd_down() {
	pp_lab_runtime || exit 1
	local names
	names="$($rt ps -aq --filter "label=$label")"
	# shellcheck disable=SC2086
	[[ -z "$names" ]] || $rt rm -f $names >/dev/null
	$rt network rm "$net" >/dev/null 2>&1 || true
	echo "lab-emu down ($lab_name)"
}

verb="${1:-}"
shift || true
case "$verb" in
up) cmd_up "$@" ;;
shape) cmd_shape "$@" ;;
exec) cmd_exec "$@" ;;
status) cmd_status ;;
down) cmd_down ;;
*)
	sed -n '2,/^set -euo/p' "$0" | sed '$d'
	exit 2
	;;
esac
