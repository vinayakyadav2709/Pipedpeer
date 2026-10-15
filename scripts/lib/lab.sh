# Shared helpers for the container labs (lab-emu.sh, lab-net.sh).
# Source it after lib/runtime.sh; do not execute it.
#
# Memory rules (brief Decisions, 2026-10-15): every container gets a hard
# --memory with --memory-swap equal to it, so the container is OOM-killed and
# never the laptop; the sum of all running containers stays within host RAM
# minus 5 GB; and a set starts only when it fits in MemAvailable minus 4 GB at
# that moment, otherwise we wait and say so.

# pp_lab_runtime: docker first. The isolated bridges below use docker's
# inhibit_ipv4 option, and every rig so far was proven on docker.
pp_lab_runtime() {
	PP_NEED_COMPOSE=0 PP_RUNTIME_ORDER="${PP_RUNTIME_ORDER:-docker podman}" pp_pick_runtime || return 1
	rt="$PP_RUNTIME"
}

# pp_bytes 1.5g -> bytes. Accepts k/m/g suffixes and decimals.
pp_bytes() {
	awk -v s="$1" 'BEGIN {
		n = s + 0; u = tolower(substr(s, length(s)))
		if (u == "k") n *= 1024; else if (u == "m") n *= 1048576; else if (u == "g") n *= 1073741824
		printf "%d\n", n }'
}

pp_meminfo() { awk -v k="$1:" '$1 == k { printf "%d\n", $2 * 1024 }' /proc/meminfo; }

# Sum of --memory limits of every running container (any lab, any owner).
# A running container with no limit cannot be counted: it is named, because
# it can take the headroom this check believes is free.
pp_running_mem() {
	local ids total=0 lim name
	ids="$($rt ps -q 2>/dev/null || true)"
	for id in $ids; do
		read -r lim name < <($rt inspect -f '{{.HostConfig.Memory}} {{.Name}}' "$id" 2>/dev/null || echo "0 ?")
		if [[ "${lim:-0}" -eq 0 ]]; then
			echo "lab: warning: running container ${name#/} has no memory limit; not counted" >&2
		fi
		total=$((total + ${lim:-0}))
	done
	echo "$total"
}

# pp_lab_mem_check <bytes wanted> [label]: refuse a set that breaks the cap,
# wait (with a log line) while MemAvailable is short.
pp_lab_mem_check() {
	local want="$1" label="${2:-lab}" gib=1073741824
	local total running cap avail waited=0 limit="${LAB_MEM_WAIT:-600}"
	total="$(pp_meminfo MemTotal)"
	running="$(pp_running_mem)"
	cap=$((total - 5 * gib))
	if ((running + want > cap)); then
		printf '%s: refusing to start: %.1f GB wanted + %.1f GB already running > %.1f GB cap (host RAM - 5 GB).\n' \
			"$label" "$(bc_g "$want")" "$(bc_g "$running")" "$(bc_g "$cap")" >&2
		echo "  use fewer nodes or a smaller LAB_MEM" >&2
		return 1
	fi
	while :; do
		avail="$(pp_meminfo MemAvailable)"
		((avail - 4 * gib >= want)) && return 0
		if ((waited >= limit)); then
			printf '%s: gave up after %ds: %.1f GB wanted, MemAvailable %.1f GB (needs 4 GB spare)\n' \
				"$label" "$waited" "$(bc_g "$want")" "$(bc_g "$avail")" >&2
			return 1
		fi
		printf '%s: waiting for memory: %.1f GB wanted, MemAvailable %.1f GB (needs 4 GB spare); retry in 15 s\n' \
			"$label" "$(bc_g "$want")" "$(bc_g "$avail")" >&2
		sleep 15
		waited=$((waited + 15))
	done
}

bc_g() { awk -v b="$1" 'BEGIN { printf "%.1f", b / 1073741824 }'; }

# pp_lab_net <name> <subnet> [nat]: a bridge the host has no address on, so
# the host's own daemon can neither see nor be seen (experiments rig). With
# "nat" it is a plain bridge instead: the containers reach the internet (Nix
# substitutes from cache.nixos.org) and the host is on it.
pp_lab_net() {
	$rt network rm "$1" >/dev/null 2>&1 || true
	if [[ "${3:-}" == nat ]]; then
		$rt network create --subnet "$2" "$1" >/dev/null
	elif [[ "$rt" == docker ]]; then
		$rt network create --subnet "$2" -o com.docker.network.bridge.inhibit_ipv4=true "$1" >/dev/null
	else
		echo "lab: warning: $rt has no inhibit_ipv4; $1 is --internal but the host is on it" >&2
		$rt network create --internal --subnet "$2" "$1" >/dev/null
	fi
}

# pp_lab_image <tag> <dir with Dockerfile>
pp_lab_image() {
	$rt build -q -t "$1" "$2" >/dev/null
}
