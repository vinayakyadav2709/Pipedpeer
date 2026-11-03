#!/usr/bin/env bash
# node.sh start <pc_id> <driver[:port]> [--pcap] [--session S] | stop | status | pcap-stop
#
# The node collector on a lab PC, plus pp-poll.py (daemon API, sockets,
# processes, clock; README "Sources") and, with --pcap, a tcpdump of the
# pipedpeer ports. Collector and poller run without root and detached
# (setsid), so stop is kill + rm and leaves nothing behind. Everything is
# under ~/.local/share/pipedpeer-lab: otel/ (config, pids, logs, read
# offsets), logs/ (the runner's ray/, dask/, pipedpeer-run/ and the poller's
# JSON lines), pcap/. Run install-otelcol.sh first, as the user the daemon
# runs as.
#
# --session S, else $PIPEDPEER_LAB_SESSION, else the file $LAB_HOME/session,
# else "none": the lab.session attribute on every record.
# --pcap: needs root (sudo -n); tcpdump from the system, else built from the
# campaign's nixpkgs pin. Files rotate every 10 min:
# pcap/<pc_id>-<UTC time>.pcap, 96-byte snaplen, ports 4242-4244 only.
#
# Daemon log: journald when pipedpeer runs as the systemd user unit
# (`pipedpeer setup --boot`), else the file the daemon appends to,
# $XDG_STATE_HOME/pipedpeer/daemon.log. PIPEDPEER_LAB_DAEMON_LOG=journald|<file>
# overrides the choice. PIPEDPEER_LAB_RAY_TMP: Ray's temp dir if not /tmp/ray.
# PIPEDPEER_LAB_RAY_METRICS_PORT: default 8090.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$here/lib.sh"

usage() {
	echo "usage: $0 start <pc_id> <driver[:port]> [--pcap] [--session S] | stop | status | pcap-stop" >&2
	exit 2
}
pidfile="$LAB_OTEL/node.pid"
logfile="$LAB_OTEL/node.log"
config="$LAB_OTEL/node.yaml"
poll_pid="$LAB_OTEL/poll.pid"
poll_log="$LAB_OTEL/poll.log"
pcap_pid="$LAB_OTEL/pcap.pid"
pcap_dir="$LAB_HOME/pcap"
bin="$LAB_BIN/otelcol-contrib"
SELF_METRICS_PORT=18888

daemon_log_source() {
	local unit="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/pipedpeer.service"
	if [[ -n "${PIPEDPEER_LAB_DAEMON_LOG:-}" ]]; then
		echo "$PIPEDPEER_LAB_DAEMON_LOG"
	elif [[ -f "$unit" ]] && command -v journalctl >/dev/null; then
		echo journald
	else
		echo "${PIPEDPEER_DAEMON_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer}/daemon.log"
	fi
}

# The python that runs the poller: the campaign env's, else the system's.
lab_python() {
	if [[ -x "$LAB_HOME/env/bin/python3" ]]; then
		echo "$LAB_HOME/env/bin/python3"
	elif command -v python3 >/dev/null; then
		command -v python3
	else
		echo "no python3 for pp-poll.py (neither $LAB_HOME/env nor PATH)" >&2
		return 1
	fi
}

# detach <cmd...>: call only as `detach ... &`. exec replaces the background
# subshell, so no bash is left holding the caller's stdout (an ssh or
# Ansible session would otherwise never end). setsid detaches from the
# session; minimal images lack it, and nohup alone is enough there.
detach() {
	if command -v setsid >/dev/null; then exec setsid nohup "$@"; else exec nohup "$@"; fi
}

as_root() {
	if ((EUID == 0)); then "$@"; else sudo -n "$@"; fi
}

tcpdump_bin() {
	local rev out
	if command -v tcpdump >/dev/null; then command -v tcpdump; return; fi
	if [[ -x "$LAB_HOME/tcpdump/bin/tcpdump" ]]; then echo "$LAB_HOME/tcpdump/bin/tcpdump"; return; fi
	rev="$(grep -o '"rev": *"[0-9a-f]*"' "$here/../env/flake.lock" | head -1 | grep -o '[0-9a-f]\{40\}')" || true
	[[ -n "$rev" ]] || { echo "no tcpdump and no nixpkgs pin in env/flake.lock" >&2; return 1; }
	echo "building tcpdump from nixpkgs $rev" >&2
	nix --extra-experimental-features 'nix-command flakes' build --out-link "$LAB_HOME/tcpdump" \
		"github:NixOS/nixpkgs/$rev#tcpdump" >&2 || return 1
	echo "$LAB_HOME/tcpdump/bin/tcpdump"
}

start_pcap() {
	local pc_id="$1" td user
	td="$(tcpdump_bin)" || { echo "pcap: no tcpdump; capture off" >&2; return 1; }
	as_root true 2>/dev/null || { echo "pcap: needs root (sudo -n failed); capture off" >&2; return 1; }
	mkdir -p "$pcap_dir"
	user="$(id -un)"
	# The root shell writes tcpdump's own pid (exec keeps it), so stop can
	# signal tcpdump itself. -Z drops to the user: the files are theirs.
	# -U writes each packet out at once, so a killed capture keeps its tail.
	local root=()
	((EUID == 0)) || root=(sudo -n)
	detach "${root[@]}" sh -c 'echo $$ >"$1"; exec "$2" -i any -n -U -s 96 -Z "$3" -G 600 -w "$4/$5-%Y%m%dT%H%M%SZ.pcap" "portrange 4242-4244"' \
		sh "$pcap_pid" "$td" "$user" "$pcap_dir" "$pc_id" >>"$LAB_OTEL/pcap.log" 2>&1 </dev/null &
	sleep 1
	if [[ -f "$pcap_pid" ]] && as_root kill -0 "$(cat "$pcap_pid")" 2>/dev/null; then
		echo "pcap: tcpdump pid $(cat "$pcap_pid") -> $pcap_dir"
	else
		echo "pcap: tcpdump did not start; tail of $LAB_OTEL/pcap.log:" >&2
		tail -5 "$LAB_OTEL/pcap.log" >&2 || true
		return 1
	fi
}

stop_pcap() {
	[[ -f "$pcap_pid" ]] || return 0
	local pid i
	pid="$(cat "$pcap_pid")"
	if as_root kill -0 "$pid" 2>/dev/null; then
		as_root kill -INT "$pid" 2>/dev/null || true
		for i in $(seq 1 50); do as_root kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
	fi
	as_root rm -f "$pcap_pid"
	echo "pcap stopped; files in $pcap_dir"
}

render() {
	local pc_id="$1" driver="$2" session="$3" src receiver file t
	src="$(daemon_log_source)"
	if [[ "$src" == journald ]]; then
		receiver=journald file=/nonexistent
	else
		receiver=file_log file="$src"
	fi
	t="$(<"$here/node.yaml.tmpl")"
	t="${t//@PC_ID@/$pc_id}" t="${t//@DRIVER@/$driver}" t="${t//@STORAGE@/$LAB_OTEL/storage}"
	t="${t//@HOST_NAME@/$(uname -n)}" t="${t//@SESSION@/$session}" t="${t//@LOGS@/$LAB_LOGS}"
	t="${t//@RAY_TMP@/${PIPEDPEER_LAB_RAY_TMP:-/tmp/ray}}"
	t="${t//@SELF_METRICS_PORT@/$SELF_METRICS_PORT}"
	t="${t//@DAEMON_LOG_RECEIVER@/$receiver}" t="${t//@DAEMON_LOG_FILE@/$file}"
	printf '%s\n' "$t" >"$config"
	echo "$src"
}

start() {
	local pc_id="$1" driver="$2" pcap="$3" session="$4" src py
	[[ "$pc_id" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "bad pc_id: $pc_id" >&2; exit 2; }
	[[ "$driver" =~ ^[A-Za-z0-9.:-]+$ ]] || { echo "bad driver address: $driver" >&2; exit 2; }
	[[ "$driver" == *:* ]] || driver="$driver:4317"
	[[ "$session" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "bad session: $session" >&2; exit 2; }
	[[ -x "$bin" ]] || { echo "no $bin; run install-otelcol.sh first" >&2; exit 1; }
	if lab_running "$pidfile"; then
		echo "node collector already running (pid $(cat "$pidfile")); stop it first" >&2
		exit 1
	fi
	py="$(lab_python)"
	mkdir -p "$LAB_OTEL/storage" "$LAB_LOGS"/{ray,dask,pipedpeer-run,sd}
	src="$(render "$pc_id" "$driver" "$session")"
	"$bin" validate --config "$config"

	# Facts first, so the record is there before the collector reads the file.
	"$py" "$here/pp-poll.py" facts --logs "$LAB_LOGS" --lab-home "$LAB_HOME" ||
		echo "warning: facts record failed" >&2
	lab_stop_pid "$poll_pid"
	detach "$py" "$here/pp-poll.py" run --logs "$LAB_LOGS" --lab-home "$LAB_HOME" >>"$poll_log" 2>&1 </dev/null &
	echo $! >"$poll_pid"

	detach "$bin" --config "$config" >>"$logfile" 2>&1 </dev/null &
	echo $! >"$pidfile"
	sleep 2
	if ! lab_running "$pidfile"; then
		echo "node collector exited at once; tail of $logfile:" >&2
		tail -20 "$logfile" >&2
		rm -f "$pidfile"
		lab_stop_pid "$poll_pid"
		exit 1
	fi
	lab_running "$poll_pid" || { echo "pp-poll.py exited; tail of $poll_log:" >&2; tail -20 "$poll_log" >&2; }
	echo "node collector up: pid $(cat "$pidfile"), lab.pc_id=$pc_id lab.session=$session -> $driver, daemon log from $src"
	echo "pp-poll.py up: pid $(cat "$poll_pid") ($py), JSON lines in $LAB_LOGS"
	if ((pcap)); then start_pcap "$pc_id" || true; fi
}

cmd="${1:-}"
case "$cmd" in
start)
	shift
	[[ $# -ge 2 ]] || usage
	pc_id="$1" driver="$2" pcap=0
	session="${PIPEDPEER_LAB_SESSION:-}"
	[[ -n "$session" || ! -f "$LAB_HOME/session" ]] || session="$(tr -d '[:space:]' <"$LAB_HOME/session")"
	shift 2
	while (($#)); do
		case "$1" in
		--pcap) pcap=1; shift ;;
		--session) session="${2:-}"; shift 2 ;;
		*) usage ;;
		esac
	done
	start "$pc_id" "$driver" "$pcap" "${session:-none}"
	;;
stop)
	lab_stop_pid "$pidfile"
	lab_stop_pid "$poll_pid"
	stop_pcap
	echo "node collector and poller stopped"
	;;
pcap-stop) stop_pcap ;;
status)
	rc=0
	if lab_running "$pidfile"; then echo "collector running: pid $(cat "$pidfile"), config $config, log $logfile"; else echo "collector not running"; rc=1; fi
	if lab_running "$poll_pid"; then echo "pp-poll.py running: pid $(cat "$poll_pid"), log $poll_log"; else echo "pp-poll.py not running"; rc=1; fi
	if [[ -f "$pcap_pid" ]]; then echo "pcap: pid $(cat "$pcap_pid"), $(find "$pcap_dir" -name '*.pcap' 2>/dev/null | wc -l) file(s) in $pcap_dir"; fi
	exit "$rc"
	;;
*) usage ;;
esac
