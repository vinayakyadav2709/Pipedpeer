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
		done < <(printf '%s\n' "$answer" | sed -n 's/^\[FAILURE\][[:space:]]*\([^[:space:]]*\).*/\1/p')
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
( $cmd ) > >(python3 ~/pipedpeer-lab/work/stamp.py "\$R/first_out_ns" >"\$R/out") 2>"\$R/err"
rc=\$?
echo "\$rc" >"\$R/exit.tmp"
date +%s%N >"\$R/t1"
wait
mv "\$R/exit.tmp" "\$R/exit"
exit "\$rc"
__PIPEDPEER_CMD_EOF__
chmod +x "\$R/cmd.sh"
if ! systemd-run --user --unit=lab-$run_id --collect bash "\$R/cmd.sh"; then
	echo "WARNING systemd-run --user failed on $run_id; using nohup" >&2
	nohup bash "\$R/cmd.sh" >/dev/null 2>&1 &
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
# unit is left running, the timeout is logged, and the answer is 124.
lab_wait() {
	local pc="${1:?usage: lab_wait <pc> <run_id> <timeout_s>}"
	local run_id="${2:?usage: lab_wait <pc> <run_id> <timeout_s>}"
	local timeout_s="${3:-0}" poll elapsed=0 wait_for=0 left=0 code=""
	# shellcheck disable=SC2088
	local path="~/pipedpeer-lab/runs/$run_id/exit"
	poll="$(lab_poll_sleep)"

	while :; do
		code=""
		if code="$(lab_ssh "$pc" "cat $path 2>/dev/null || echo WAITING")"; then
			code="${code//[[:space:]]/}"
			case "$code" in
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
		else
			elapsed=$((elapsed + 1))
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
