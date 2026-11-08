#!/usr/bin/env bash
# shellcheck disable=SC2088  # tildes in quoted remote commands expand on the PC
# cluster.sh: bring the campaign's shared systems up together on the first n
# PCs, or take them down again (brief S7).
#
# At each N every system's cluster is up at once on pc00..pc(N-1); systems the
# cluster does not use are stopped on the remaining PCs, so a run measures the
# machine it is on, not a busy neighbour.
#
#   cluster.sh up <n> [--systems pipedpeer,ray,dask] [--cap <pc>=<cpus>...]
#   cluster.sh down
#
# Readiness is waited for (CLUSTER_READY_S seconds, default 120 per system);
# a system that does not fill in time makes the script exit 1 with
# NOT-READY <system> want=<n> have=<k>, and the EXIT trap undoes the --cap
# limits of a failed attempt so a crash never leaves a PC capped silently.
# On a clean up the caps stay until `cluster.sh down` (e4 measures under
# them); the next `up` clears whatever a previous hard-killed run left.
set -euo pipefail

_CLUSTER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$_CLUSTER_DIR/lib.sh"

CLUSTER_READY_S="${CLUSTER_READY_S:-120}"

CLUSTER_SYS=()
CLUSTER_IDS=()
CLUSTER_N=0
CLUSTER_CAP_PC=()
CLUSTER_CAP_CPU=()
CLUSTER_RESET_ON_EXIT=0

# --- pipedpeer status parsing (unit-tested on saved text) ------------------

# status_healthy_count <file>: rows with STATE "healthy" in pc00's
# `pipedpeer status` table. The table starts at the NAME/STATE header and
# ends at the first blank line; STATE is the second column, so a NOTE with
# spaces does not hide it. The self row counts like any other row. There is
# no count printed when the table is missing: that is an error, not a zero.
status_healthy_count() {
	local file="${1:?usage: status_healthy_count <file>}" line
	local in_table=0 count=0
	local _name state _rest
	if [[ ! -f "$file" ]]; then
		lab_log "status_healthy_count: no $file"
		echo "status table not found" >&2
		return 1
	fi
	while IFS= read -r line; do
		if ((in_table)); then
			if [[ -z "$line" ]]; then
				break
			fi
			IFS=' ' read -r _name state _rest <<<"$line"
			if [[ "$state" == "healthy" ]]; then
				count=$((count + 1))
			fi
		elif [[ "$line" =~ ^NAME[[:space:]]+STATE ]]; then
			in_table=1
		fi
	done <"$file"
	if ((in_table == 0)); then
		lab_log "status_healthy_count: status table not found in $file"
		echo "status table not found" >&2
		return 1
	fi
	printf '%s\n' "$count"
}

# status_cpu_sum <file>: the cluster's total pipedpeer cores, the <k>c of
# the CPU column summed over the healthy rows. Used for logs/cluster-<n>/cores.txt.
status_cpu_sum() {
	local file="${1:?usage: status_cpu_sum <file>}" line
	local in_table=0 total=0
	local _name state _path cpu _rest
	if [[ ! -f "$file" ]]; then
		lab_log "status_cpu_sum: no $file"
		echo "status table not found" >&2
		return 1
	fi
	while IFS= read -r line; do
		if ((in_table)); then
			if [[ -z "$line" ]]; then
				break
			fi
			IFS=' ' read -r _name state _path cpu _rest <<<"$line"
			if [[ "$state" == "healthy" && "$cpu" =~ ^([0-9]+)c ]]; then
				total=$((total + BASH_REMATCH[1]))
			fi
		elif [[ "$line" =~ ^NAME[[:space:]]+STATE ]]; then
			in_table=1
		fi
	done <"$file"
	if ((in_table == 0)); then
		lab_log "status_cpu_sum: status table not found in $file"
		echo "status table not found" >&2
		return 1
	fi
	printf '%s\n' "$total"
}

# --- small helpers ---------------------------------------------------------

_cluster_tmp() {
	local state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
	mkdir -p "$state_dir/cluster"
	mktemp "$state_dir/cluster/status.XXXXXXX"
}

# _caps_file: the caps the current/last `up` set, "<pc> <cpus>" per line.
# It is what `down` and the EXIT trap read (and then delete), and what the
# next `up` clears before applying its own.
_caps_file() { printf '%s\n' "$LAB/caps.txt"; }

# _cores_file <n>: the detected per-system cores of that cluster size.
_cores_file() { printf '%s\n' "$LAB/logs/cluster-$1/cores.txt"; }

_cluster_usage() {
	cat <<'EOF'
usage: cluster.sh up <n> [--systems pipedpeer[,ray[,...]]] [--cap <pc>=<cpus>...]
       cluster.sh down

Bring the lab's first n PCs (lab_hosts) up as one cluster of pipedpeer
daemons, Ray and a Dask scheduler+workers; or stop the service units again.
The --cap <pc>=<cpus> limits a PC to cpus CPUs (AllowedCPUs) before anything
starts; `down` and a failed run reset them. Readiness is waited for
CLUSTER_READY_S seconds per system (default 120); the script exits 1 with
NOT-READY <system> want=<n> have=<k> when one does not fill.

Environment:
  LAB_RESULTS       results dir (default <repo>/results/lab)
  CLUSTER_READY_S   readiness budget per system, seconds (default 120)
EOF
}

_cluster_usage_die() {
	echo "cluster.sh: $*" >&2
	_cluster_usage >&2
	exit 2
}

_cluster_has() {
	local s
	for s in "${CLUSTER_SYS[@]}"; do
		[[ "$s" == "$1" ]] && return 0
	done
	return 1
}

_cluster_fail() {
	lab_log "$*"
	CLUSTER_RESET_ON_EXIT=1
	return 1
}

_cluster_not_ready() {
	local sys="$1" want="$2" have="$3"
	printf 'NOT-READY %s want=%s have=%s\n' "$sys" "$want" "$have"
	lab_log "NOT-READY $sys want=$want have=$have"
	CLUSTER_RESET_ON_EXIT=1
}

_cluster_parse_args() {
	if [[ ! "${1:-}" =~ ^[0-9]+$ ]] || (($1 < 1)); then
		_cluster_usage_die "up needs how many PCs (a number), got '${1:-}'"
	fi
	CLUSTER_N="$1"
	shift
	CLUSTER_SYS=()
	CLUSTER_CAP_PC=()
	CLUSTER_CAP_CPU=()
	local have_systems=0 sys systems_spec spec_pc spec_cpus
	while (($#)); do
		case "$1" in
			--systems)
				(($# >= 2)) || _cluster_usage_die "--systems needs a value"
				systems_spec="$2"
				have_systems=1
				shift 2
				;;
			--cap)
				(($# >= 2)) || _cluster_usage_die "--cap needs <pc>=<cpus>"
				spec="$2"
				shift 2
				if [[ "$spec" =~ ^(pc[0-9]+)=([0-9]+)$ && "${BASH_REMATCH[2]}" -ge 1 ]]; then
					spec_pc="${BASH_REMATCH[1]}"
					spec_cpus="${BASH_REMATCH[2]}"
					CLUSTER_CAP_PC+=("$spec_pc")
					CLUSTER_CAP_CPU+=("$spec_cpus")
				else
					_cluster_usage_die "--cap wants <pc>=<cpus>, got '$spec'"
				fi
				;;
			-h | --help)
				_cluster_usage
				exit 0
				;;
			*)
				_cluster_usage_die "unknown argument: $1"
				;;
		esac
	done
	if ((have_systems == 1)); then
		IFS=',' read -r -a CLUSTER_SYS <<<"$systems_spec"
	else
		CLUSTER_SYS=(pipedpeer ray dask)
	fi
	for sys in "${CLUSTER_SYS[@]}"; do
		case "$sys" in
			pipedpeer | ray | dask) : ;;
			*) _cluster_usage_die "unknown system '$sys' (pipedpeer, ray, dask)" ;;
		esac
	done
}

# --- caps ------------------------------------------------------------------

# _cluster_restart_daemon <pc>: the pipedpeer daemon reads the CPU count once
# at start, so a cap (or its reset) only counts after a restart.
_cluster_restart_daemon() {
	local pc="$1"
	if ! lab_ssh "$pc" "pipedpeer stop; pipedpeer start"; then
		lab_log "cap $pc: daemon restart FAILED; the daemon may still advertise the old core count"
		return 1
	fi
	lab_log "cap $pc: daemon restarted to read the new core count"
}

# cluster_cap_apply <pc> <cpus> (logged with its undo)
cluster_cap_apply() {
	local pc="$1" cpus="$2"
	if ! lab_ssh "$pc" "sudo systemctl set-property --runtime user-\$(id -u).slice AllowedCPUs=0-$((cpus - 1))"; then
		lab_log "cap $pc: set-property FAILED; AllowedCPUs=0-$((cpus - 1)) is not in place"
		return 1
	fi
	lab_log "cap $pc: allowed 0-$((cpus - 1)) (undo: sudo systemctl set-property --runtime user-\$(id -u).slice AllowedCPUs=)"
	printf '%s %s\n' "$pc" "$cpus" >>"$(_caps_file)"
	if _cluster_has pipedpeer; then
		_cluster_restart_daemon "$pc" || return 1
	fi
}

cluster_reset_caps() {
	local cf pc cpus
	cf="$(_caps_file)"
	if [[ ! -f "$cf" ]]; then
		return 0
	fi
	while IFS=' ' read -r pc cpus; do
		[[ -n "$pc" ]] || continue
		lab_log "cap ${pc}: resetting AllowedCPUs="
		if ! lab_ssh "$pc" "sudo systemctl set-property --runtime user-\$(id -u).slice AllowedCPUs=''"; then
			lab_log "cap ${pc}: resetting AllowedCPUs= FAILED; check sudo on $pc"
		fi
		_cluster_restart_daemon "$pc" || true
	done <"$cf"
	rm -f "$cf"
	return 0
}

cluster_cleanup() {
	if ((CLUSTER_RESET_ON_EXIT == 1)); then
		cluster_reset_caps || true
	fi
}

# --- readiness -------------------------------------------------------------

# _cluster_wait_count <system> <want> <cmd>: poll the command on pc00; its
# single stdout line is the current count. ssh trouble is "not yet".
_cluster_wait_count() {
	local sys="$1" want="$2" count_cmd="$3"
	local f out have deadline
	f="$(_cluster_tmp)"
	deadline=$(( $(date +%s) + CLUSTER_READY_S ))
	while :; do
		have=""
		if lab_ssh "${CLUSTER_IDS[0]}" "$count_cmd" >"$f" 2>/dev/null; then
			out="$(tr -d '[:space:]' <"$f" || true)"
			if [[ "$out" =~ ^[0-9]+$ ]]; then
				have="$out"
			fi
		fi
		if [[ "$have" == "$want" ]]; then
			rm -f "$f"
			return 0
		fi
		if (($(date +%s) >= deadline)); then
			rm -f "$f"
			_cluster_not_ready "$sys" "$want" "${have:-unknown}"
			return 1
		fi
		sleep "$(lab_poll_sleep)"
	done
}

# cluster_wait_pipedpeer <n>: rows with STATE healthy in pc00's
# `pipedpeer status` table == n. A table not (yet) there is "not yet",
# not a count of 0, and only the final NOT-READY carries the failure.
cluster_wait_pipedpeer() {
	local n="$1"
	local f have deadline
	f="$(_cluster_tmp)"
	deadline=$(( $(date +%s) + CLUSTER_READY_S ))
	while :; do
		have=""
		if lab_ssh "${CLUSTER_IDS[0]}" 'pipedpeer status' >"$f" 2>/dev/null; then
			if have="$(status_healthy_count "$f" 2>/dev/null)"; then
				: # have is the count
			else
				have=""
			fi
		fi
		if [[ "$have" == "$n" ]]; then
			rm -f "$f"
			return 0
		fi
		if (($(date +%s) >= deadline)); then
			rm -f "$f"
			_cluster_not_ready pipedpeer "$n" "${have:-unknown}"
			return 1
		fi
		sleep "$(lab_poll_sleep)"
	done
}

# --- cores ------------------------------------------------------------------

_cluster_write_cores() {
	local f v
	f="$(_cluster_tmp)"
	if lab_ssh "${CLUSTER_IDS[0]}" 'pipedpeer status' >"$f" 2>/dev/null \
		&& v="$(status_cpu_sum "$f" 2>/dev/null)"; then
		printf 'pipedpeer=%s\n' "$v" >>"$(_cores_file "$CLUSTER_N")"
	else
		lab_log "cores: could not read pc00's pipedpeer status; no pipedpeer line in cores.txt"
	fi
	rm -f "$f"
	local ray_cmd dask_cmd ip
	ip="$(lab_ip "${CLUSTER_IDS[0]}")"
	if _cluster_has ray; then
		ray_cmd="~/pipedpeer-lab/venv/bin/python -c \"import ray; ray.init(address='auto'); print(int(ray.cluster_resources().get('CPU', 0)))\""
		_cluster_cores_line ray "$ray_cmd"
	fi
	if _cluster_has dask; then
		dask_cmd="~/pipedpeer-lab/venv/bin/python -c \"from dask.distributed import Client; c=Client('tcp://$ip:8786'); print(sum(w.get('nthreads', 0) for w in c.scheduler_info()['workers'].values()))\""
		_cluster_cores_line dask "$dask_cmd"
	fi
}

_cluster_cores_line() {
	# _cluster_cores_line <system> <cmd>: one "<system>=<cores>" line; a
	# query that fails is logged, never written as a fake zero.
	local sys="$1" cmd="$2" f v
	f="$(_cluster_tmp)"
	if lab_ssh "${CLUSTER_IDS[0]}" "$cmd" >"$f" 2>/dev/null
		then v="$(tr -d '[:space:]' <"$f" || true)"
	else
		v=""
	fi
	rm -f "$f"
	if [[ "$v" =~ ^[0-9]+$ ]]; then
		printf '%s=%s\n' "$sys" "$v" >>"$(_cores_file "$CLUSTER_N")"
	else
		lab_log "cores: $sys query gave no count; no $sys line in cores.txt"
	fi
}

# --- ray caps ---------------------------------------------------------------

_cluster_ray_cap_fix() {
	# Ray invents its worker CPU count from the scheduler affinity mask that
	# AllowedCPUs does not touch on every build; where it disagrees with the
	# cap on a worker PC, restart that worker with --num-cpus instead. The
	# head is not restarted (that would tear the cluster down to rebuild it);
	# a head mismatch is logged for the experiment to block on.
	local q f pc ip node_ip node_cpus found pc00_ip i k
	pc00_ip="$(lab_ip "${CLUSTER_IDS[0]}")"
	q="import ray
ray.init(address='auto')
for n in ray.nodes():
    if n['Alive']:
        print(n['NodeManagerAddress'].split(':')[0], int(n['Resources'].get('CPU', 0)))"
	q="~/pipedpeer-lab/venv/bin/python -c \"$q\""
	f="$(_cluster_tmp)"
	if ! lab_ssh "${CLUSTER_IDS[0]}" "$q" >"$f" 2>/dev/null; then
		lab_log "cap: could not list Ray's nodes; the --num-cpus check is skipped this round"
		rm -f "$f"
		return 0
	fi
	for ((i = 1; i < CLUSTER_N; i++)); do
		pc="${CLUSTER_IDS[$i]}"
		for ((k = 0; k < ${#CLUSTER_CAP_PC[@]}; k++)); do
			[[ "${CLUSTER_CAP_PC[$k]}" == "$pc" ]] || continue
			ip="$(lab_ip "$pc")"
			found=""
			while IFS=' ' read -r node_ip node_cpus; do
				[[ -n "${node_ip:-}" ]] || continue
				if [[ "$node_ip" == "$ip" ]]; then
					found="$node_cpus"
				fi
			done <"$f"
			if [[ ! "$found" =~ ^[0-9]+$ ]]; then
				lab_log "cap $pc: not (yet) among Ray's nodes; the --num-cpus check is skipped for it"
			elif ((found != CLUSTER_CAP_CPU[k])); then
				lab_log "ray ignores AllowedCPUs on $pc; passed --num-cpus=${CLUSTER_CAP_CPU[$k]}"
				if ! lab_ssh "$pc" "systemctl --user stop lab-ray; systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --address=$pc00_ip:6379 --num-cpus=${CLUSTER_CAP_CPU[$k]} --block"; then
					lab_log "cap $pc: restarting the Ray worker with --num-cpus FAILED"
				fi
			else
				lab_log "cap $pc: Ray reports $found CPUs, as capped"
			fi
		done
	done
	rm -f "$f"
}

# --- up / down --------------------------------------------------------------

cluster_up() {
	_cluster_parse_args "$@"
	# a previous run that died hard may have left caps behind
	cluster_reset_caps || true
	trap cluster_cleanup EXIT

	local hostfile pc00_id pc00_ip i k pc
	local -a others=()
	hostfile="$(lab_hosts "$CLUSTER_N")" || return 1
	mapfile -t CLUSTER_IDS <"$hostfile"
	pc00_id="${CLUSTER_IDS[0]}"
	pc00_ip="$(lab_ip "$pc00_id")"
	mapfile -t others < <(_lab_available_hosts)

	# systems this cluster does not use are stopped on the remaining PCs
	for ((i = CLUSTER_N; i < ${#others[@]}; i++)); do
		pc="${others[$i]}"
		if ! lab_ssh "$pc" "pipedpeer stop; systemctl --user stop lab-ray lab-dask-worker lab-dask-scheduler"; then
			lab_log "up: could not stop the other systems on $pc; they may idle next to the cluster"
		fi
	done

	# caps go on before anything starts
	for ((k = 0; k < ${#CLUSTER_CAP_PC[@]}; k++)); do
		cluster_cap_apply "${CLUSTER_CAP_PC[$k]}" "${CLUSTER_CAP_CPU[$k]}" || CLUSTER_RESET_ON_EXIT=1
	done

	if _cluster_has pipedpeer; then
		if ! lab_fanout cluster-start "$hostfile" 'pipedpeer start' >/dev/null; then
			lab_log "up: pipedpeer start failed on some PCs (they may already be running)"
		fi
	fi
	if _cluster_has ray; then
		if ! lab_fanout cluster-ray-stop "$hostfile" '~/pipedpeer-lab/venv/bin/ray stop --force' >/dev/null; then
			lab_log "up: ray stop failed somewhere (maybe nothing was running); see logs/cluster-ray-stop"
		fi
		if ! lab_ssh "$pc00_id" "systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --head --port=6379 --block"; then
			_cluster_fail "up: could not start the Ray head on $pc00_id" || return 1
		fi
		for ((i = 1; i < CLUSTER_N; i++)); do
			if ! lab_ssh "${CLUSTER_IDS[$i]}" "systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --address=$pc00_ip:6379 --block"; then
				_cluster_fail "up: could not start the Ray worker on ${CLUSTER_IDS[$i]}" || return 1
			fi
		done
	fi
	if _cluster_has dask; then
		if ! lab_ssh "$pc00_id" "systemd-run --user --unit=lab-dask-scheduler --collect ~/pipedpeer-lab/venv/bin/dask scheduler --port 8786"; then
			_cluster_fail "up: could not start the Dask scheduler on $pc00_id" || return 1
		fi
		for ((i = 0; i < CLUSTER_N; i++)); do
			if ! lab_ssh "${CLUSTER_IDS[$i]}" "systemd-run --user --unit=lab-dask-worker --collect ~/pipedpeer-lab/venv/bin/dask worker tcp://$pc00_ip:8786"; then
				_cluster_fail "up: could not start the Dask worker on ${CLUSTER_IDS[$i]}" || return 1
			fi
		done
	fi

	# the PCs' ssh/torchrun settings need ips, not ids
	local ipsfile="$LAB/logs/cluster-$CLUSTER_N/hosts-$CLUSTER_N.ips"
	mkdir -p "$LAB/logs/cluster-$CLUSTER_N"
	: >"$ipsfile"
	for ((i = 0; i < CLUSTER_N; i++)); do
		lab_ip "${CLUSTER_IDS[$i]}" >>"$ipsfile"
	done
	for ((i = 0; i < CLUSTER_N; i++)); do
		if ! lab_rsync "$ipsfile" "${CLUSTER_IDS[$i]}:pipedpeer-lab/hosts-$CLUSTER_N.txt"; then
			lab_log "up: could not push hosts-$CLUSTER_N.txt to ${CLUSTER_IDS[$i]}"
		fi
	done

	# readiness, then the cores the systems actually see
	for pc in pipedpeer ray dask; do
		_cluster_has "$pc" || continue
		case "$pc" in
			pipedpeer) cluster_wait_pipedpeer "$CLUSTER_N" || return 1 ;;
			ray) _cluster_wait_count ray "$CLUSTER_N" "$_RAY_COUNT_CMD" || return 1 ;;
			dask) _cluster_wait_count dask "$CLUSTER_N" "$(_dask_count_cmd "$pc00_ip")" || return 1 ;;
		esac
	done
	if _cluster_has ray && ((${#CLUSTER_CAP_PC[@]} > 0)); then
		_cluster_ray_cap_fix
	fi
	mkdir -p "$LAB/logs/cluster-$CLUSTER_N"
	: >"$(_cores_file "$CLUSTER_N")"
	_cluster_write_cores
	printf 'CLUSTER-UP n=%s systems=%s\n' "$CLUSTER_N" "${CLUSTER_SYS[*]}"
	lab_log "up: n=$CLUSTER_N systems=${CLUSTER_SYS[*]}"
}

cluster_down() {
	# stop the service units on every PC; the pipedpeer daemons stay up
	local file all id rc=0
	file="$LAB/hosts-all.txt"
	mkdir -p "$LAB"
	mapfile -t all < <(_lab_available_hosts)
	: >"$file"
	for id in "${all[@]}"; do
		printf '%s\n' "$id" >>"$file"
	done
	if ! lab_fanout cluster-down "$file" 'systemctl --user stop lab-ray lab-dask-worker lab-dask-scheduler' >/dev/null; then
		lab_log "down: systemctl stop failed on every PC"
		rc=1
	fi
	cluster_reset_caps
	printf 'CLUSTER-DOWN\n'
	lab_log "down: service units stopped, caps reset"
	if ((rc == 1)); then
		return 1
	fi
}

# --- the systems' readiness and cores commands ------------------------------

_RAY_COUNT_CMD="~/pipedpeer-lab/venv/bin/python -c \"import ray; ray.init(address='auto'); print(len([x for x in ray.nodes() if x['Alive']]))\""

# _dask_count_cmd <scheduler ip>: the Dask worker count on that scheduler.
_dask_count_cmd() {
	local ip="$1"
	printf '%s\n' "~/pipedpeer-lab/venv/bin/python -c \"from dask.distributed import Client; print(len(Client('tcp://$ip:8786').scheduler_info()['workers']))\""
}

# --- main -------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	case "${1:-}" in
		up)
			shift || true
			trap cluster_cleanup EXIT
			cluster_up "$@"
			;;
		down)
			trap cluster_cleanup EXIT
			cluster_down
			;;
		-h | --help)
			_cluster_usage
			exit 0
			;;
		*)
			_cluster_usage >&2
			exit 2
			;;
	esac
fi
