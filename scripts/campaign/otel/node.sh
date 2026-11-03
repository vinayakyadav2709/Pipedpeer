#!/usr/bin/env bash
# node.sh start|stop|status <pc_id> <driver_ip>: the node collector on a lab
# PC, without root. It runs detached (setsid), so stop is kill + rm and leaves
# nothing behind; everything is under ~/.local/share/pipedpeer-lab/otel
# (config, pid, log, read offsets). Run install-otelcol.sh first.
#
# Daemon log: journald when pipedpeer runs as the systemd user unit
# (`pipedpeer setup --boot`), else the file the daemon appends to,
# $XDG_STATE_HOME/pipedpeer/daemon.log. Run it as the user the daemon runs as.
# PIPEDPEER_LAB_DAEMON_LOG=journald|<file> overrides the choice.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$here/lib.sh"

usage() {
	echo "usage: $0 start <pc_id> <driver_ip> | stop | status" >&2
	exit 2
}
pidfile="$LAB_OTEL/node.pid"
logfile="$LAB_OTEL/node.log"
config="$LAB_OTEL/node.yaml"
bin="$LAB_BIN/otelcol-contrib"

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

start() {
	local pc_id="$1" driver="$2" src receiver file
	[[ "$pc_id" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "bad pc_id: $pc_id" >&2; exit 2; }
	[[ "$driver" =~ ^[A-Za-z0-9.:-]+$ ]] || { echo "bad driver address: $driver" >&2; exit 2; }
	[[ -x "$bin" ]] || { echo "no $bin; run install-otelcol.sh first" >&2; exit 1; }
	if lab_running "$pidfile"; then
		echo "node collector already running (pid $(cat "$pidfile")); stop it first" >&2
		exit 1
	fi
	src="$(daemon_log_source)"
	if [[ "$src" == journald ]]; then
		receiver=journald file=/nonexistent
	else
		receiver=file_log file="$src"
	fi
	mkdir -p "$LAB_OTEL/storage"
	local t
	t="$(<"$here/node.yaml.tmpl")"
	t="${t//@PC_ID@/$pc_id}" t="${t//@DRIVER@/$driver}" t="${t//@STORAGE@/$LAB_OTEL/storage}"
	t="${t//@DAEMON_LOG_RECEIVER@/$receiver}" t="${t//@DAEMON_LOG_FILE@/$file}"
	printf '%s\n' "$t" >"$config"
	"$bin" validate --config "$config"
	# setsid detaches it from the ssh session; minimal images lack it, and
	# nohup alone is enough there.
	local detach=(nohup)
	command -v setsid >/dev/null && detach=(setsid nohup)
	"${detach[@]}" "$bin" --config "$config" >>"$logfile" 2>&1 </dev/null &
	echo $! >"$pidfile"
	sleep 2
	if ! lab_running "$pidfile"; then
		echo "node collector exited at once; tail of $logfile:" >&2
		tail -20 "$logfile" >&2
		rm -f "$pidfile"
		exit 1
	fi
	echo "node collector up: pid $(cat "$pidfile"), lab.pc_id=$pc_id -> $driver:4317, daemon log from $src"
}

case "${1:-}" in
start) [[ $# -eq 3 ]] || usage; start "$2" "$3" ;;
stop) lab_stop_pid "$pidfile"; echo "node collector stopped" ;;
status)
	if lab_running "$pidfile"; then
		echo "running: pid $(cat "$pidfile"), config $config, log $logfile"
	else
		echo "not running"
		exit 1
	fi
	;;
*) usage ;;
esac
