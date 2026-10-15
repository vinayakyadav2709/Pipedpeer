# Shared helpers for the container labs (lab-emu.sh, lab-net.sh).
# Source it after lib/runtime.sh; do not execute it.
#
# Memory rules (brief Decisions, 2026-10-15): every container gets a hard
# --memory with --memory-swap equal to it, so the container is OOM-killed and
# never the laptop; the sum of all running containers stays within host RAM
# minus 5 GB; and a set starts only when it fits in MemAvailable minus 4 GB at
# that moment. A set that does not fit either way waits (another lab may be
# about to finish), logging each retry, until LAB_MEM_WAIT seconds (600).

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

# pp_lab_mem_check <bytes wanted> [label]: wait (with a log line per retry)
# while the set breaks the cap or MemAvailable is short.
pp_lab_mem_check() {
	local want="$1" label="${2:-lab}" gib=1073741824
	local total running cap avail why waited=0 limit="${LAB_MEM_WAIT:-600}"
	# Every lab container has a memory cap, so a short spare only risks the
	# lab OOMing, never the host; 4 GB is the default for a desktop in use.
	local spare="${LAB_MEM_SPARE:-4}"
	total="$(pp_meminfo MemTotal)"
	cap=$((total - 5 * gib))
	if ((want > cap)); then
		printf '%s: refusing: %.1f GB wanted is over the %.1f GB cap (host RAM - 5 GB) on its own; use fewer nodes or a smaller LAB_MEM\n' \
			"$label" "$(bc_g "$want")" "$(bc_g "$cap")" >&2
		return 1
	fi
	while :; do
		running="$(pp_running_mem)"
		avail="$(pp_meminfo MemAvailable)"
		why=""
		if ((running + want > cap)); then
			why="$(printf '%.1f GB wanted + %.1f GB in running containers > %.1f GB cap (host RAM - 5 GB)' \
				"$(bc_g "$want")" "$(bc_g "$running")" "$(bc_g "$cap")")"
		elif ((avail - spare * gib < want)); then
			why="$(printf '%.1f GB wanted, MemAvailable %.1f GB (needs %d GB spare; LAB_MEM_SPARE)' "$(bc_g "$want")" "$(bc_g "$avail")" "$spare")"
		fi
		[[ -z "$why" ]] && return 0
		if ((waited >= limit)); then
			echo "$label: gave up after ${waited}s: $why; use fewer nodes or a smaller LAB_MEM" >&2
			return 1
		fi
		echo "$label: waiting for memory: $why; retry in 15 s" >&2
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

# pp_lab_subnet <name> <second-octet base>: a /24 from a hash of the name,
# 10.<base..base+63>.<0-255>.0/24, so labs with different names never share
# a bridge. LAB_SUBNET overrides.
pp_lab_subnet() {
	if [[ -n "${LAB_SUBNET:-}" ]]; then
		echo "$LAB_SUBNET"
		return
	fi
	local h
	h="$(printf '%s' "$1" | cksum | cut -d' ' -f1)"
	echo "10.$(($2 + h % 64)).$(((h / 64) % 256)).0/24"
}

# pp_plain_lab_vars: names for the plain lab (lab-up/lab-fail/lab-down) from
# LAB_NAME. The default name "lab" keeps the compose file's own defaults
# (containers pipedpeer-lab-<i>, 10.87.0.0/24), which the integration tests use.
pp_plain_lab_vars() {
	lab_name="${LAB_NAME:-lab}"
	[[ "$lab_name" =~ ^[a-z0-9-]+$ ]] || { echo "LAB_NAME must be [a-z0-9-]" >&2; return 2; }
	if [[ "$lab_name" == lab ]]; then
		LAB_CTR=pipedpeer-lab
		LAB_SUBNET="${LAB_SUBNET:-10.87.0.0/24}"
	else
		LAB_CTR="pipedpeer-lab-$lab_name"
		LAB_SUBNET="$(pp_lab_subnet "$lab_name" 192)"
	fi
	LAB_BASE="${LAB_SUBNET%.*}"
	export LAB_CTR LAB_SUBNET LAB_BASE
	lab_project="pplab-$lab_name"
	[[ "$lab_name" == lab ]] && lab_project=lab
	lab_token_file="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/lab-$lab_name.token"
}
