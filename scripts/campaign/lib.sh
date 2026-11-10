#!/usr/bin/env bash
# Campaign driver helpers: the files under $LAB, ssh/rsync/pssh fan-out, and
# detached runs on a PC (brief S1, docs/benchmarks/lab-campaign.md §1).
#
# Sourced by every script in scripts/campaign/, never executed; running it
# directly prints usage. Every function logs through lab_log, which writes
# "<ISO time> <script>: <message>" to stderr and $LAB/campaign.log, so a run
# that ends badly still says why in one place.
#
# Retries are narrow on purpose. ssh is retried only on exit 255, which is the
# connection rather than the remote command; rsync only on its transport
# errors (255, 12, 30, 35); a fan-out only on the hosts pssh reported as
# failed. A remote command's own exit code comes back at once: a workload
# that fails is a result, not a transport problem, and retrying it would turn
# one measurement into three.
#
# LAB_RESULTS moves the results dir (tests and the Docker lab use it, so the
# real results/lab is never written). LAB_RETRY_SLEEP and LAB_POLL_S override
# the 5 s waits for the tests, which would otherwise sleep for a minute; the
# campaign leaves both alone.

set -euo pipefail

REPO="${REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"
LAB="${LAB_RESULTS:-$REPO/results/lab}"
export REPO LAB

# pssh -x and rsync -e take the ssh_config path inside one option string, so
# a space in $LAB would split it. Refuse it up front with a clear error.
if [[ "$LAB" == *[[:space:]]* ]]; then
	echo "lib.sh: the results dir [$LAB] contains a space; the pssh/rsync options cannot carry it. Set LAB_RESULTS to a path without spaces." >&2
	# shellcheck disable=SC2317  # return when sourced, exit when run
	return 1 2>/dev/null || exit 1
fi

# lab_log <msg...>: one line to stderr and to $LAB/campaign.log.
lab_log() {
	local ts msg
	ts="$(date -Is)"
	msg="$*"
	printf '%s %s: %s\n' "$ts" "${0##*/}" "$msg" >&2
	mkdir -p "$LAB" 2>/dev/null || true
	printf '%s %s: %s\n' "$ts" "${0##*/}" "$msg" >>"$LAB/campaign.log" || true
}

lab_retry_sleep() { printf '%s\n' "${LAB_RETRY_SLEEP:-5}"; }
lab_poll_sleep() { printf '%s\n' "${LAB_POLL_S:-5}"; }

# --- $LAB files -----------------------------------------------------------

# lab_ssh_config_write: $LAB/hosts.tsv -> $LAB/ssh_config. The global block
# is the transport policy (keepalives, one shared master, no host prompts);
# each PC gets a Host block so the scripts can say "pc07" and mean the row.
lab_ssh_config_write() {
	local tsv="$LAB/hosts.tsv"
	if [[ ! -f "$tsv" ]]; then
		echo "no $tsv: run scan.sh first" >&2
		lab_log "ssh_config: no hosts.tsv"
		return 1
	fi
	mkdir -p "$LAB"
	{
		cat <<EOF
ServerAliveInterval 15
ServerAliveCountMax 4
ControlMaster auto
ControlPath ~/.ssh/cm-%r@%h:%p
ControlPersist 30m
BatchMode yes
StrictHostKeyChecking accept-new
UserKnownHostsFile $LAB/known_hosts
IdentityFile ~/.ssh/pipedpeer-lab
ConnectTimeout 10
EOF
		awk -F'\t' '!/^#/ && NF >= 3 { printf "\nHost %s\n  HostName %s\n  User %s\n", $1, $2, $3 }' "$tsv"
	} >"$LAB/ssh_config"
	lab_log "wrote $LAB/ssh_config ($(grep -c '^Host ' "$LAB/ssh_config" || true) PCs)"
}

# lab_hosts <n>: the first n non-excluded ids in $LAB/hosts-<n>.txt, printed.
lab_hosts() {
	local n="${1:?usage: lab_hosts <n>}"
	local file="$LAB/hosts-$n.txt"
	local -a avail=()
	local id msg i
	if [[ ! -f "$LAB/hosts.tsv" ]]; then
		lab_log "hosts: no $LAB/hosts.tsv"
		echo "no $LAB/hosts.tsv: run scan.sh first" >&2
		return 1
	fi
	while IFS= read -r id; do
		avail+=("$id")
	done < <(_lab_available_hosts)
	if ((${#avail[@]} < n)); then
		msg="only ${#avail[@]} PCs available, need $n"
		echo "$msg" >&2
		lab_log "hosts: $msg"
		return 1
	fi
	: >"$file"
	for ((i = 0; i < n; i++)); do
		printf '%s\n' "${avail[$i]}" >>"$file"
	done
	printf '%s\n' "$file"
}

# _lab_available_hosts: ids in hosts.tsv that are not in excluded.txt.
_lab_available_hosts() {
	local ex=""
	if [[ -f "$LAB/excluded.txt" ]]; then
		ex="$(sed -e 's/#.*//' -e 's/[[:space:]]//g' "$LAB/excluded.txt" | tr '\n' ' ')"
	fi
	awk -F'\t' -v ex=" $ex " '
		/^#/ || NF < 2 { next }
		index(ex, " " $1 " ") > 0 { next }
		{ print $1 }
	' "$LAB/hosts.tsv"
}

# lab_available_count: how many PCs lab_hosts can hand out (hosts.tsv rows
# minus excluded.txt).
lab_available_count() {
	_lab_available_hosts | awk 'END { print NR + 0 }'
}

# lab_ip <pc>: the ip column for that id.
lab_ip() {
	local pc="${1:?usage: lab_ip <pc>}" ip
	ip="$(awk -F'\t' -v pc="$pc" '$1 == pc { print $2; exit }' "$LAB/hosts.tsv" 2>/dev/null || true)"
	if [[ -z "$ip" ]]; then
		echo "no ip for $pc in $LAB/hosts.tsv" >&2
		lab_log "ip: no row for $pc"
		return 1
	fi
	printf '%s\n' "$ip"
}

# LAB_CPUS_PROBE: a sh script run on a PC; prints how many CPUs its
# processes can really use. nproc is the affinity mask (cpuset, AllowedCPUs);
# it is lowered to the CPU quota of the PC's own cgroup or any ancestor (v2
# cpu.max, v1 cpu.cfs_quota_us/cpu.cfs_period_us), rounded down, never below
# 1. Ray counts the host's CPUs instead: in the laptop's container lab it saw
# 16 under a 3-CPU docker cap. PP_CG_ROOT, PP_PROC_CGROUP and PP_NPROC let
# test/cluster_cpus_test.sh feed it fixture trees.
# shellcheck disable=SC2016  # expanded on the PC
LAB_CPUS_PROBE='r=${PP_CG_ROOT:-/sys/fs/cgroup}; pcg=${PP_PROC_CGROUP:-/proc/self/cgroup}; n=${PP_NPROC:-$(nproc)}
lim() { [ "$1" -gt 0 ] 2>/dev/null && [ "$2" -gt 0 ] 2>/dev/null || return 0; c=$(($1 / $2)); [ "$c" -lt 1 ] && c=1; [ "$c" -lt "$n" ] && n=$c; return 0; }
walk() { p=$2; while :; do
  if [ "$3" = 2 ] && [ -r "$1$p/cpu.max" ]; then read -r q per <"$1$p/cpu.max"; [ "$q" = max ] || lim "$q" "$per"; fi
  if [ "$3" = 1 ] && [ -r "$1$p/cpu.cfs_quota_us" ] && [ -r "$1$p/cpu.cfs_period_us" ]; then lim "$(cat "$1$p/cpu.cfs_quota_us")" "$(cat "$1$p/cpu.cfs_period_us")"; fi
  if [ -z "$p" ] || [ "$p" = / ]; then break; fi; p=${p%/*}; done; }
while IFS=: read -r _ ctl path; do case ",$ctl," in
  ,,) walk "$r" "$path" 2 ;;
  *,cpu,*) for d in "$r/cpu,cpuacct" "$r/cpuacct,cpu" "$r/cpu"; do if [ -d "$d" ]; then walk "$d" "$path" 1; break; fi; done ;;
esac; done <"$pcg"
echo "$n"'

# lab_usable_cpus <pc>: LAB_CPUS_PROBE's count on that PC, printed. Returns 1
# (and logs the PC and what came back) when the probe fails or prints no number.
lab_usable_cpus() {
	local pc="${1:?usage: lab_usable_cpus <pc>}" n raw err rc=0
	mkdir -p "$LAB"
	err="$(mktemp "$LAB/cpus_err.XXXXXX")"
	trap 'rm -f "$err"' RETURN INT TERM
	# stdout is the count; stderr (ssh's own message) is kept for the log only
	n="$(lab_ssh "$pc" "$LAB_CPUS_PROBE" 2>"$err")" || rc=$?
	raw="$(tr '\n' ' ' <"$err")"
	rm -f "$err"
	trap - RETURN INT TERM
	n="$(printf '%s' "$n" | tr -d '[:space:]')"
	if ((rc == 0)) && [[ "$n" =~ ^[0-9]+$ ]] && ((n >= 1)); then
		printf '%s\n' "$n"
		return 0
	fi
	lab_log "$pc: could not read its usable CPU count (ssh exit $rc, got [$n], stderr [$raw])"
	return 1
}

# LAB_MEM_PROBE: a sh script run on a PC; prints "<MemTotal kB> <limit kB>",
# the limit being the smallest cgroup memory limit on the PC's own cgroup or
# an ancestor (v2 memory.max, v1 memory.limit_in_bytes), empty when none.
# MemTotal alone misses a limit (inside the container lab it is the host's).
# PP_MEMTOTAL_KIB, PP_CG_ROOT and PP_PROC_CGROUP are for the tests.
# shellcheck disable=SC2016  # expanded on the PC
LAB_MEM_PROBE='r=${PP_CG_ROOT:-/sys/fs/cgroup}; pcg=${PP_PROC_CGROUP:-/proc/self/cgroup}
t=${PP_MEMTOTAL_KIB:-$(awk "/MemTotal/ {print \$2}" /proc/meminfo)}; lim=""
for d in $(awk -F: "\$1 == \"0\" || \$2 ~ /(^|,)memory(,|\$)/ {print \$3}" "$pcg"); do while :; do
  for f in "$r$d/memory.max" "$r/memory$d/memory.limit_in_bytes"; do
    if [ -r "$f" ]; then v=$(cat "$f"); case $v in max|"") ;; *) v=$((v / 1024)); if [ -z "$lim" ] || [ "$v" -lt "$lim" ]; then lim=$v; fi ;; esac; fi
  done
  if [ -z "$d" ] || [ "$d" = / ]; then break; fi; d=${d%/*}; done; done
echo "$t $lim"'

# lab_mem_mib <pc>: the memory that PC's processes can use, in MiB (MemTotal or
# a smaller cgroup limit). Returns 1, logged with the PC, when the probe fails.
lab_mem_mib() {
	local pc="${1:?usage: lab_mem_mib <pc>}" out t lim err raw rc=0
	mkdir -p "$LAB"
	err="$(mktemp "$LAB/mem_err.XXXXXX")"
	trap 'rm -f "$err"' RETURN INT TERM
	out="$(lab_ssh "$pc" "$LAB_MEM_PROBE" 2>"$err")" || rc=$?
	raw="$(tr '\n' ' ' <"$err")"
	rm -f "$err"
	trap - RETURN INT TERM
	read -r t lim <<<"$out"
	if ((rc != 0)) || [[ ! "${t:-}" =~ ^[0-9]+$ ]]; then
		lab_log "$pc: memory unknown (ssh exit $rc, probe gave [$out], stderr [$raw])"
		return 1
	fi
	if [[ "${lim:-}" =~ ^[0-9]+$ ]] && ((lim < t)); then t="$lim"; fi
	printf '%s\n' "$((t / 1024))"
}

# _cluster_ray_mem_flag <pc>: " --memory=B --object-store-memory=O" for every
# Ray start, B being the PC's usable memory in bytes (lab_mem_mib: MemTotal or a
# smaller cgroup limit; Ray's docs advise passing it in containers, where it
# otherwise takes the host's) and O 30% of B (Ray's own default fraction). An
# explicit CLUSTER_RAY_MEMORY_BYTES / CLUSTER_RAY_OBJECT_STORE_BYTES wins over
# the probe. A probe that fails is logged and no flag is passed.
_cluster_ray_mem_flag() {
	local pc="$1" mib b o
	b="${CLUSTER_RAY_MEMORY_BYTES:-}"
	if [[ -z "$b" ]]; then
		if ! mib="$(lab_mem_mib "$pc")" || ! [[ "$mib" =~ ^[0-9]+$ ]] || ((mib == 0)); then
			lab_log "ray on $pc: no usable memory figure (got '${mib:-}'); Ray picks its own, which may count memory a cgroup limit denies"
			return 0
		fi
		# Ray's own split of the total: 30% object store, the rest heap.
		b=$((mib * 1024 * 1024 * 70 / 100))
		o="${CLUSTER_RAY_OBJECT_STORE_BYTES:-$((mib * 1024 * 1024 * 30 / 100))}"
	fi
	o="${o:-${CLUSTER_RAY_OBJECT_STORE_BYTES:-$((b * 30 / 70))}}"
	lab_log "ray on $pc: --memory=$b --object-store-memory=$o"
	printf ' --memory=%s --object-store-memory=%s' "$b" "$o"
}

# --- transport ------------------------------------------------------------

# lab_ssh <pc> <cmd...>: ssh -F $LAB/ssh_config <pc> -- <cmd>, retried up to
# three attempts 5 s apart on exit 255 only.
lab_ssh() {
	local pc="${1:?usage: lab_ssh <pc> <cmd...>}"
	shift
	local attempt rc=0
	for attempt in 1 2 3; do
		rc=0
		ssh -F "$LAB/ssh_config" "$pc" -- "$@" || rc=$?
		if ((rc == 0)); then
			return 0
		fi
		if ((rc != 255)); then
			return "$rc"
		fi
		if ((attempt < 3)); then
			lab_log "ssh $pc: exit 255 (attempt $attempt/3); retrying in $(lab_retry_sleep)s"
			sleep "$(lab_retry_sleep)"
		else
			lab_log "ssh $pc: exit 255 (attempt 3/3); giving up"
		fi
	done
	return 255
}

# lab_rsync <args...>: rsync -a over the campaign's ssh config, retried on
# the transport errors only.
lab_rsync() {
	local attempt rc=0
	for attempt in 1 2 3; do
		rc=0
		rsync -a -e "ssh -F $LAB/ssh_config" "$@" || rc=$?
		if ((rc == 0)); then
			return 0
		fi
		case "$rc" in
			255 | 12 | 30 | 35)
				if ((attempt < 3)); then
					lab_log "rsync: exit $rc (attempt $attempt/3); retrying in $(lab_retry_sleep)s"
					sleep "$(lab_retry_sleep)"
				else
					lab_log "rsync: exit $rc (attempt 3/3); giving up"
				fi
				;;
			*) return "$rc" ;;
		esac
	done
	return "$rc"
}

# lab_pssh_bin: pssh, else the Ubuntu build's name parallel-ssh.
lab_pssh_bin() {
	local bin
	bin="$(command -v pssh 2>/dev/null || true)"
	if [[ -z "$bin" ]]; then
		bin="$(command -v parallel-ssh 2>/dev/null || true)"
	fi
	if [[ -z "$bin" ]]; then
		echo "MISSING: pssh" >&2
		lab_log "MISSING: pssh"
		return 1
	fi
	printf '%s\n' "$bin"
}

# lab_fanout <step> <hostsfile> <cmd>: pssh over the file, output in
# logs/<step>/<pc> and logs/<step>/err/<pc>. Hosts pssh reported as failed
# are retried up to three attempts; whatever still fails is listed in
# logs/<step>/FAILED. Returns 1 only when every host failed.
lab_fanout() {
	local step="${1:?usage: lab_fanout <step> <hostsfile> <cmd>}"
	local hostsfile="${2:?usage: lab_fanout <step> <hostsfile> <cmd>}"
	shift 2
	local cmd="$*"
	local outdir="$LAB/logs/$step" errdir="$LAB/logs/$step/err"
	local failed_file="$LAB/logs/$step/FAILED" retry_file="$LAB/logs/$step/retry-hosts.txt"
	local pssh attempt=1 total ok failed=0 rc=0 answer=""
	local -a failed_ids=()
	local host

	mkdir -p "$outdir" "$errdir"
	rm -f "$failed_file" "$retry_file"
	pssh="$(lab_pssh_bin)" || return 1
	total="$(awk 'NF && $1 !~ /^#/' "$hostsfile" | wc -l)"

	while :; do
		rc=0
		answer="$("$pssh" -h "$hostsfile" -x "-F $LAB/ssh_config" -t 0 -p 64 \
			-o "$outdir" -e "$errdir" "$cmd" 2>&1)" || rc=$?
		if [[ -n "$answer" ]]; then
			printf '%s\n' "$answer"
		fi
		failed_ids=()
		while IFS= read -r host; do
			if [[ -n "$host" ]]; then
				failed_ids+=("$host")
			fi
		done < <(printf '%s\n' "$answer" | sed -n 's/^\[[0-9]*\][[:space:]]*[0-9:]*[[:space:]]*\[FAILURE\][[:space:]]*\([^[:space:]]*\).*/\1/p')
		if ((${#failed_ids[@]} == 0 && rc != 0)); then
			# pssh failed but printed no report line we know (usage error,
			# changed format): never read that as success.
			lab_log "fanout $step: pssh exit $rc with no [FAILURE] line; treating every host as failed"
			while IFS= read -r host; do
				[[ -n "$host" && "$host" != \#* ]] && failed_ids+=("$host")
			done <"$hostsfile"
		fi
		if ((${#failed_ids[@]} == 0 || attempt >= 3)); then
			break
		fi
		printf '%s\n' "${failed_ids[@]}" >"$retry_file"
		lab_log "fanout $step: ${#failed_ids[@]} host(s) failed on attempt $attempt/3; retrying in $(lab_retry_sleep)s"
		hostsfile="$retry_file"
		attempt=$((attempt + 1))
		sleep "$(lab_retry_sleep)"
	done

	failed=${#failed_ids[@]}
	ok=$((total - failed))
	if ((failed > 0)); then
		printf '%s\n' "${failed_ids[@]}" >"$failed_file"
		lab_log "fanout $step: FAILED ${failed_ids[*]}"
	fi
	printf 'FANOUT step=%s ok=%d failed=%d\n' "$step" "$ok" "$failed"
	lab_log "fanout $step: ok=$ok failed=$failed"
	if ((total > 0 && failed >= total)); then
		return 1
	fi
	return 0
}

# --- detached runs --------------------------------------------------------

# _lab_detach_script <run_id> <cmd>: the remote shell that reserves the run
# dir and starts cmd.sh. The mkdir is the guard against a retry starting the
# same run twice: it fails when the dir exists, and that answer is
# ALREADY-STARTED, not an error.
_lab_detach_script() {
	local run_id="$1" cmd="$2"
	# The tilde is meant for the remote shell: quoting it keeps it out of the
	# driver's expansion.
	# shellcheck disable=SC2088
	local r="~/pipedpeer-lab/runs/$run_id"
	cat <<EOF
set -u
R=$r
mkdir -p ~/pipedpeer-lab/runs || exit 1
if ! mkdir "\$R" 2>/dev/null; then
	echo "ALREADY-STARTED $run_id"
	exit 0
fi
cat >"\$R/cmd.sh" <<'__PIPEDPEER_CMD_EOF__'
cd ~/pipedpeer-lab/work || exit 1
R=$r
date +%s%N >"\$R/t0"
# A pipe, not a process substitution: the shell waits for stamp.py, so the unit
# cannot end (and kill it) before it has written the last line.
( $cmd ) 2>"\$R/err" | python3 ~/pipedpeer-lab/work/stamp.py "\$R/first_out_ns" >"\$R/out"
rc=\${PIPESTATUS[0]}
echo "\$rc" >"\$R/exit.tmp"
date +%s%N >"\$R/t1"
mv "\$R/exit.tmp" "\$R/exit"
exit "\$rc"
__PIPEDPEER_CMD_EOF__
chmod +x "\$R/cmd.sh"
if ! systemd-run --user --unit=lab-$run_id --collect bash "\$R/cmd.sh"; then
	echo "WARNING systemd-run --user failed on $run_id; using nohup" >&2
	nohup bash "\$R/cmd.sh" >/dev/null 2>&1 &
	echo \$! >"\$R/nohup.pid"
fi
EOF
}

# lab_detach <pc> <run_id> <cmd...>: start the command on the PC in its own
# transient user unit and return at once. Prints ALREADY-STARTED <run_id>
# when the run dir is already there, and returns 0 then too.
lab_detach() {
	local pc="${1:?usage: lab_detach <pc> <run_id> <cmd...>}"
	local run_id="${2:?usage: lab_detach <pc> <run_id> <cmd...>}"
	shift 2
	local script answer rc=0
	script="$(_lab_detach_script "$run_id" "$*")"
	answer="$(lab_ssh "$pc" "$script")" || rc=$?
	if ((rc != 0)); then
		lab_log "detach $run_id on $pc: failed (exit $rc)"
		return "$rc"
	fi
	if [[ -n "$answer" ]]; then
		printf '%s\n' "$answer"
	fi
	if [[ "$answer" == *ALREADY-STARTED* ]]; then
		lab_log "detach $run_id on $pc: already started; not starting it again"
	elif [[ "$answer" == *WARNING* ]]; then
		lab_log "detach $run_id on $pc: systemd-run --user failed; the run went to nohup"
	fi
	return 0
}

# lab_wait <pc> <run_id> <timeout_s>: poll for the run's exit file. A failed
# ssh is "not yet" (logged), never a reason to touch the run. On timeout the
# unit is left running, the timeout is logged, and the answer is 124. A unit
# that is no longer running (and no nohup process) while there is still no
# exit file is dead (OOM, killed): after LAB_DEAD_GRACE_S (default 10) of
# consecutive DEAD answers it prints DEAD <run_id> and returns 125.
lab_wait() {
	local pc="${1:?usage: lab_wait <pc> <run_id> <timeout_s>}"
	local run_id="${2:?usage: lab_wait <pc> <run_id> <timeout_s>}"
	local timeout_s="${3:-0}" poll elapsed=0 wait_for=0 left=0 code=""
	local grace="${LAB_DEAD_GRACE_S:-10}" dead_s=0 was_dead=0 probe
	# shellcheck disable=SC2088
	local rdir="~/pipedpeer-lab/runs/$run_id"
	probe="cat $rdir/exit 2>/dev/null || { if [ -f $rdir/cmd.sh ] && ! systemctl --user is-active --quiet lab-$run_id 2>/dev/null && ! { [ -f $rdir/nohup.pid ] && kill -0 \$(cat $rdir/nohup.pid) 2>/dev/null; }; then echo DEAD; else echo WAITING; fi; }"
	poll="$(lab_poll_sleep)"

	while :; do
		code="" was_dead=0
		if code="$(lab_ssh "$pc" "$probe")"; then
			code="${code//[[:space:]]/}"
			case "$code" in
				DEAD)
					if ((dead_s >= grace)); then
						printf 'DEAD %s\n' "$run_id"
						lab_log "wait $run_id on $pc: the unit is gone and there is no exit file; recording a failure"
						return 125
					fi
					was_dead=1
					lab_log "wait $run_id on $pc: unit gone, no exit file yet (${dead_s}s of ${grace}s grace)"
					;;
				'' | WAITING) lab_log "wait $run_id on $pc: not finished yet" ;;
				*[!0-9]*) lab_log "wait $run_id on $pc: unexpected exit file content [$code]" ;;
				*)
					printf '%s\n' "$code"
					return 0
					;;
			esac
		else
			lab_log "wait $run_id on $pc: ssh failed; still waiting"
		fi
		((was_dead)) || dead_s=0

		if ((timeout_s > 0 && elapsed >= timeout_s)); then
			printf 'TIMEOUT %s\n' "$run_id"
			lab_log "wait $run_id on $pc: TIMEOUT after ${elapsed}s; the unit is left running"
			return 124
		fi

		wait_for=$poll
		if ((timeout_s > 0)); then
			left=$((timeout_s - elapsed))
			((wait_for > left)) && wait_for=$left
		fi
		if ((wait_for > 0)); then
			sleep "$wait_for"
			elapsed=$((elapsed + wait_for))
			((was_dead)) && dead_s=$((dead_s + wait_for))
		else
			elapsed=$((elapsed + 1))
			((was_dead)) && dead_s=$((dead_s + 1))
		fi
	done
}

# lab_fetch <pc> <run_id> <dir>: copy the run dir's contents into <dir>.
lab_fetch() {
	local pc="${1:?usage: lab_fetch <pc> <run_id> <dir>}"
	local run_id="${2:?usage: lab_fetch <pc> <run_id> <dir>}"
	local dir="${3:?usage: lab_fetch <pc> <run_id> <dir>}"
	mkdir -p "$dir"
	lab_rsync "$pc:pipedpeer-lab/runs/$run_id/" "$dir/"
}

lab_usage() {
	cat <<'EOF'
usage: lib.sh [-h]

Helpers for the pipedpeer lab campaign (scripts/campaign/): the files under
$LAB, ssh/rsync/pssh fan-out with narrow retries, and detached runs on a PC
under systemd-run --user. It is sourced, not run; there is nothing to do here.

Environment:
  LAB_RESULTS      results dir (default: <repo>/results/lab)
  LAB_RETRY_SLEEP  seconds between ssh/rsync/fan-out retries (default 5)
  LAB_POLL_S       seconds between lab_wait polls (default 5)
EOF
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	case "${1:-}" in
		-h | --help)
			lab_usage
			exit 0
			;;
		"")
			lab_usage >&2
			exit 2
			;;
		*)
			lab_usage >&2
			exit 2
			;;
	esac
fi
