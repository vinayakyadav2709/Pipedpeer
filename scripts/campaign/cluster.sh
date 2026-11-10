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
_CLUSTER_WORKER_FAILED=0
# CLUSTER_DASK_ALLOWED_FAILURES: unset keeps Dask's default; set it to change
# how often a task may lose its worker before the scheduler gives up on it.

CLUSTER_SYS=()
CLUSTER_IDS=()
CLUSTER_N=0
CLUSTER_CAP_PC=()
# CLUSTER_CAP_CPU is also allowed as an environment-wide scalar cap (CLUSTER_CAP_CPU=4)
# for the Dask worker core count; keep it before the array is reset below.
_CLUSTER_CAP_CPU_ENV="${CLUSTER_CAP_CPU:-}"
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

# _cluster_worker_fail <sys> <pc> <why>: a worker that would not start. The PC
# is listed NOT-READY, the other PCs are still started, and up fails at the end:
# a smaller cluster is never run and reported as N.
_cluster_worker_fail() {
	local sys="$1" pc="$2" why="$3"
	printf 'NOT-READY %s pc=%s\n' "$sys" "$pc"
	lab_log "NOT-READY $sys pc=$pc ($why)"
	CLUSTER_RESET_ON_EXIT=1
	_CLUSTER_WORKER_FAILED=1
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
	local f e out have deadline
	f="$(_cluster_tmp)"
	e="$(_cluster_tmp)"
	deadline=$(( $(date +%s) + CLUSTER_READY_S ))
	while :; do
		have=""
		# a probe that hangs (Ray with no head waits for one) must not outlast the deadline
		if lab_ssh "${CLUSTER_IDS[0]}" "timeout 30 $count_cmd" >"$f" 2>"$e"; then
			out="$(tr -d '[:space:]' <"$f" || true)"
			if [[ "$out" =~ ^[0-9]+$ ]]; then
				have="$out"
			fi
		fi
		if [[ "$have" == "$want" ]]; then
			rm -f "$f" "$e"
			return 0
		fi
		if (($(date +%s) >= deadline)); then
			if [[ -s "$e" ]]; then
				lab_log "NOT-READY $sys: last probe error: $(tail -n 8 "$e" | tr '\n' ' ' | cut -c1-800)"
			fi
			rm -f "$f" "$e"
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
		# Dask's per-worker memory limit is total divided by workers; summing
		# nthreads gives the cores the cluster actually uses (one thread per worker).
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

# _cluster_ray_cpus_flag <pc>: " --num-cpus=N" for every Ray start. A --cap
# wins; otherwise N is lab_usable_cpus (lib.sh) on that PC (Ray's own
# count ignores AllowedCPUs on some builds and a cgroup quota always). A probe
# that fails is logged and no flag is passed, so Ray falls back to its own count.
_cluster_ray_cpus_flag() {
	local pc="$1" k n
	for ((k = 0; k < ${#CLUSTER_CAP_PC[@]}; k++)); do
		if [[ "${CLUSTER_CAP_PC[$k]}" == "$pc" ]]; then
			printf ' --num-cpus=%s' "${CLUSTER_CAP_CPU[$k]}"
			return 0
		fi
	done
	if n="$(lab_usable_cpus "$pc")"; then
		lab_log "ray on $pc: --num-cpus=$n (affinity and cgroup quota as the PC reports them)"
		printf ' --num-cpus=%s' "$n"
	else
		lab_log "ray on $pc: no usable CPU count; Ray picks its own, which may count CPUs a quota denies"
	fi
}

# _cluster_ray_mem_check <pc> head|worker: refuse (return 1) a PC with less
# memory than Ray needs there, warn below a comfortable amount; both logged as
# "RAY-MEMORY <pc> have=<n> MiB ...". On the emulated lab (one VM, cgroup v1)
# pc00's container peaked at 1.24 GB RSS and 3.0-3.3 GB with page cache while
# Ray, the Dask scheduler and workers and the daemon ran there; Ray itself took
# the host's memory for its own (10 GiB in a 4 GiB container), so it does not
# guard this. A probe that fails is logged and not refused.
_cluster_ray_mem_check() {
	local pc="$1" role="$2" have min warn
	if [[ "$role" == head ]]; then
		min="${CLUSTER_RAY_HEAD_MIN_MIB:-2048}"
		warn="${CLUSTER_RAY_HEAD_WARN_MIB:-4096}"
	else
		min="${CLUSTER_RAY_WORKER_MIN_MIB:-1024}"
		warn="${CLUSTER_RAY_WORKER_WARN_MIB:-2048}"
	fi
	have="$(lab_mem_mib "$pc")" || {
		lab_log "ray on $pc: memory unknown; not checked against the $min MiB a Ray $role needs"
		return 0
	}
	# The minimum is a nominal amount (a 2 GB PC reports about 1.9 GiB after
	# the kernel's own reservation): compare with 10% slack, so a nominal PC
	# passes and a truly smaller one does not.
	if ((have < min * 9 / 10)); then
		lab_log "RAY-MEMORY $pc have=$have MiB need=$min MiB for a Ray $role: refused (give it more memory, or exclude it / pick another pc00)"
		return 1
	fi
	if ((have < warn * 9 / 10)); then
		lab_log "RAY-MEMORY $pc have=$have MiB: warning, a Ray $role is comfortable from $warn MiB; a stall at the limit is possible"
	fi
	return 0
}

# _cluster_dask_cores <pc>: the core count for a Dask worker on <pc>, from
# hosts.tsv column 5, capped by a per-PC --cap and by the CLUSTER_CAP_CPU env
# cap. Empty when the PC has no usable core count (so the worker is skipped).
_cluster_dask_cores() {
	local pc="$1" base="" cap="" eff="" k
	base="$(awk -F'\t' -v pc="$pc" '$1 == pc { print $5; exit }' "$LAB/hosts.tsv" 2>/dev/null || true)"
	if [[ ! "$base" =~ ^[0-9]+$ ]] || ((base < 1)); then
		printf ''
		return 0
	fi
	eff="$base"
	# per-PC cap from --cap
	for ((k = 0; k < ${#CLUSTER_CAP_PC[@]}; k++)); do
		if [[ "${CLUSTER_CAP_PC[$k]}" == "$pc" ]]; then
			cap="${CLUSTER_CAP_CPU[$k]}"
			if [[ "$cap" =~ ^[0-9]+$ ]] && ((cap >= 1 && cap < eff)); then
				eff="$cap"
			fi
		fi
	done
	# env-wide cap
	if [[ "$_CLUSTER_CAP_CPU_ENV" =~ ^[0-9]+$ ]] && ((_CLUSTER_CAP_CPU_ENV >= 1 && _CLUSTER_CAP_CPU_ENV < eff)); then
		eff="$_CLUSTER_CAP_CPU_ENV"
	fi
	printf '%s\n' "$eff"
}

# _cluster_wait_ray_cpus <pc> <ip> <want>: poll Ray's node list until <ip>
# reports <want> CPUs (a restarted worker reconnects late), up to
# CLUSTER_READY_S. Returns 1, with a log line, when it never does.
_cluster_wait_ray_cpus() {
	local pc="$1" ip="$2" want="$3" q f deadline got=""
	q="import ray
ray.init(address='auto')
for n in ray.nodes():
    if n['Alive']:
        print(n['NodeManagerAddress'].split(':')[0], int(n['Resources'].get('CPU', 0)))"
	q="timeout 30 ~/pipedpeer-lab/venv/bin/python -c \"$q\""
	f="$(_cluster_tmp)"
	deadline=$(( $(date +%s) + CLUSTER_READY_S ))
	while :; do
		if lab_ssh "${CLUSTER_IDS[0]}" "$q" >"$f" 2>/dev/null; then
			got="$(awk -v ip="$ip" '$1 == ip {c = $2} END {print c}' "$f")"
			if [[ "$got" == "$want" ]]; then
				rm -f "$f"
				return 0
			fi
		fi
		if (($(date +%s) >= deadline)); then
			rm -f "$f"
			lab_log "cap $pc: Ray did not report $want CPUs for $ip within ${CLUSTER_READY_S}s (last: ${got:-none})"
			return 1
		fi
		sleep "$(lab_poll_sleep)"
	done
}

_cluster_ray_cap_fix() {
	# Ray invents its worker CPU count from the scheduler affinity mask that
	# AllowedCPUs does not touch on every build; where it disagrees with the
	# cap on a worker PC, restart that worker with --num-cpus instead. The
	# head is started with --num-cpus when capped (see cluster_up) and is not
	# restarted here (that would tear the cluster down to rebuild it); a head
	# mismatch is logged for the experiment to block on.
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
	for ((i = 0; i < CLUSTER_N; i++)); do
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
			elif ((found != CLUSTER_CAP_CPU[k] && i == 0)); then
				# the head is not restarted (that rebuilds the cluster): logged for the experiment to block on
				lab_log "cap $pc: Ray HEAD reports $found CPUs, capped to ${CLUSTER_CAP_CPU[$k]}; not restarted"
			elif ((found != CLUSTER_CAP_CPU[k])); then
				lab_log "ray ignores AllowedCPUs on $pc; passed --num-cpus=${CLUSTER_CAP_CPU[$k]}"
				if ! lab_ssh "$pc" "systemctl --user stop lab-ray; systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --address=$pc00_ip:6379 --num-cpus=${CLUSTER_CAP_CPU[$k]} --block"; then
					lab_log "cap $pc: restarting the Ray worker with --num-cpus FAILED"
				else
					# the old node entry lingers until it times out: wait for the new one
					_cluster_wait_ray_cpus "$pc" "$ip" "${CLUSTER_CAP_CPU[$k]}" || true
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
	# a failed worker in an earlier call must not fail this one
	_CLUSTER_WORKER_FAILED=0
	# a previous run that died hard may have left caps behind
	cluster_reset_caps || true
	trap cluster_cleanup EXIT

	local hostfile pc00_id pc00_ip i k pc cores dask_total=0
	local -a others=()
	hostfile="$(lab_hosts "$CLUSTER_N")" || return 1
	mapfile -t CLUSTER_IDS <"$hostfile"
	pc00_id="${CLUSTER_IDS[0]}"
	pc00_ip="$(lab_ip "$pc00_id")"
	mapfile -t others < <(_lab_available_hosts)

	# systems this cluster does not use are stopped on the remaining PCs
	for ((i = CLUSTER_N; i < ${#others[@]}; i++)); do
		pc="${others[$i]}"
		if ! lab_ssh "$pc" "pipedpeer stop; $_CLUSTER_STOP_UNITS"; then
			lab_log "up: could not stop the other systems on $pc; they may idle next to the cluster"
		fi
	done

	# units left by an earlier run (or a crashed one) would make the starts below fail
	if ! lab_fanout cluster-reset "$hostfile" "$_CLUSTER_STOP_UNITS" >/dev/null; then
		_cluster_fail "up: could not clear earlier Ray/Dask units; see logs/cluster-reset" || return 1
	fi

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
		if ! _cluster_ray_mem_check "$pc00_id" head; then
			_cluster_worker_fail ray "$pc00_id" "too little memory for the Ray head"
			_cluster_fail "up: the Ray head on $pc00_id was not started: too little memory (RAY-MEMORY in the log)" || return 1
		fi
		for ((i = 1; i < CLUSTER_N; i++)); do
			_cluster_ray_mem_check "${CLUSTER_IDS[$i]}" worker ||
				_cluster_worker_fail ray "${CLUSTER_IDS[$i]}" "too little memory for a Ray worker"
		done
		if ((_CLUSTER_WORKER_FAILED)); then
			_cluster_fail "up: a PC has too little memory for Ray; nothing of Ray was started" || return 1
		fi
		if ! lab_ssh "$pc00_id" "systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --head --port=6379$(_cluster_ray_cpus_flag "$pc00_id") --block"; then
			_cluster_fail "up: could not start the Ray head on $pc00_id" || return 1
		fi
		for ((i = 1; i < CLUSTER_N; i++)); do
			if ! lab_ssh "${CLUSTER_IDS[$i]}" "systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --address=$pc00_ip:6379$(_cluster_ray_cpus_flag "${CLUSTER_IDS[$i]}") --block"; then
				_cluster_worker_fail ray "${CLUSTER_IDS[$i]}" "could not start the Ray worker"
			fi
		done
	fi
	if _cluster_has dask; then
		if ! lab_ssh "$pc00_id" "systemd-run --user --unit=lab-dask-scheduler --collect ${CLUSTER_DASK_ALLOWED_FAILURES:+env DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES=$CLUSTER_DASK_ALLOWED_FAILURES }~/pipedpeer-lab/venv/bin/dask scheduler --port 8786"; then
			_cluster_fail "up: could not start the Dask scheduler on $pc00_id" || return 1
		fi
		for ((i = 0; i < CLUSTER_N; i++)); do
			pc="${CLUSTER_IDS[$i]}"
			cores="$(_cluster_dask_cores "$pc")"
			if [[ -z "$cores" ]]; then
				lab_log "up: dask: no core count for $pc in hosts.tsv; not starting its worker (listed as not ready)"
				continue
			fi
			dask_total=$((dask_total + cores))
			if ! lab_ssh "$pc" "systemd-run --user --unit=lab-dask-worker --collect ~/pipedpeer-lab/venv/bin/dask worker tcp://$pc00_ip:8786 --nworkers $cores --nthreads 1"; then
				_cluster_worker_fail dask "$pc" "could not start the Dask worker"
			fi
		done
	fi

	if ((_CLUSTER_WORKER_FAILED)); then
		lab_log "up: $CLUSTER_N PCs wanted, a worker did not start; this N is unavailable"
		return 1
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
			dask) _cluster_wait_count dask "$CLUSTER_N" "$(_dask_count_cmd "$pc00_ip" "$dask_total")" || return 1 ;;
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
	if ! lab_fanout cluster-down "$file" "$_CLUSTER_STOP_UNITS" >/dev/null; then
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

# stops the Ray/Dask units that exist; a unit that is not loaded is not an error
# shellcheck disable=SC2016  # expanded on the PC, not here
_CLUSTER_STOP_UNITS='systemctl --user stop lab-ray lab-dask-worker lab-dask-scheduler 2>/dev/null; for u in lab-ray lab-dask-worker lab-dask-scheduler; do if [ "$(systemctl --user show -p ActiveState --value $u 2>/dev/null)" = active ]; then echo "$u is still active" >&2; exit 1; fi; systemctl --user reset-failed $u 2>/dev/null; done; exit 0'
_RAY_COUNT_CMD="~/pipedpeer-lab/venv/bin/python -c \"import ray; ray.init(address='auto'); print(len([x for x in ray.nodes() if x['Alive']]))\""

# _dask_count_cmd <scheduler ip> <total workers>: the number of distinct hosts
# reporting workers, or 0 while fewer than <total> workers have started (a
# PC's first worker registers before the rest of its --nworkers).
_dask_count_cmd() {
	local ip="$1" total="$2"
	printf '%s\n' "~/pipedpeer-lab/venv/bin/python -c \"from dask.distributed import Client; w=list(Client('tcp://$ip:8786').scheduler_info()['workers'].values()); print(len({x['host'] for x in w}) if len(w) >= $total else 0)\""
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
