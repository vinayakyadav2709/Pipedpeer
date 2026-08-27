# Pick a container runtime that can actually bring a compose stack up.
#
# Being on PATH is not the same as being able to run one. On one of the test
# machines podman is installed and `podman info` answers happily, but there is
# no podman-compose and no user socket, so `podman compose` hands off to
# docker-compose, which dials that missing socket and dies - while docker
# itself was up the whole time. Every lab script preferred podman on presence
# alone, so the whole stack refused to start on a machine that could have run
# it.
#
# Hence two questions per runtime, not one: does the CLI answer, and is there
# a compose implementation behind it.
#
# Sets PP_RUNTIME and defines pp_compose. Source it; do not execute it.

pp_podman_compose_works() {
	# podman-compose drives the podman CLI directly and needs no socket.
	command -v podman-compose >/dev/null 2>&1 && return 0
	# `podman compose` delegates to docker-compose over the user socket.
	local sock="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"
	[[ -S "$sock" ]] || [[ -S /run/podman/podman.sock ]]
}

# Set PP_NEED_COMPOSE=0 before calling pp_pick_runtime if you only ever run
# plain containers - the compose gap does not disqualify a runtime then.
pp_runtime_works() {
	timeout 20 "$1" info >/dev/null 2>&1 || return 1
	[[ "${PP_NEED_COMPOSE:-1}" == 1 ]] || return 0
	[[ "$1" != podman ]] || pp_podman_compose_works
}

pp_pick_runtime() {
	local candidate
	for candidate in podman docker; do
		command -v "$candidate" >/dev/null 2>&1 || continue
		if pp_runtime_works "$candidate"; then
			PP_RUNTIME="$candidate"
			return 0
		fi
	done
	# Nothing usable. Name what was tried: "no container runtime found" on a
	# machine with both installed sends you looking in the wrong place.
	local installed=()
	for candidate in podman docker; do
		command -v "$candidate" >/dev/null 2>&1 && installed+=("$candidate")
	done
	if ((${#installed[@]})); then
		echo "no usable container runtime: ${installed[*]} installed, none able to run compose" >&2
		echo "  podman needs podman-compose, or its socket: systemctl --user start podman.socket" >&2
		echo "  docker needs its daemon:                     systemctl start docker" >&2
	else
		echo "no container runtime found (podman/docker)" >&2
	fi
	return 1
}

pp_compose() {
	case "$PP_RUNTIME" in
	podman)
		if command -v podman-compose >/dev/null 2>&1; then
			podman-compose "$@"
		else
			podman compose "$@"
		fi
		;;
	*)
		docker compose "$@"
		;;
	esac
}
