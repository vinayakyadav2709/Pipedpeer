#!/bin/sh
# energy.sh start | stop | status: package energy (RAPL) sampled every
# second, as JSON lines in $LAB_HOME/logs/energy.jsonl (E16, FIGURES.md).
#
# node.sh starts it beside the collector, which ships the file
# (file_log/energy, lab.source energy); the runner's E16 starts it itself on
# a PC where it is not running. Plain sh with no awk, sed or python: the
# lab image has none of them.
#
# Counters: /sys/class/powercap/intel-rapl:<n>/energy_uj, the top-level
# zones only (package-<n>, and psys where the CPU has it; their subzones
# core, uncore and dram are inside the package). AMD Zen reports through
# the same powercap driver on Linux 5.8+; older kernels' hwmon amd_energy
# (Esocket<n>) is the fallback. energy_uj is root's (0400) since 2020, so
# the loop runs as root through sudo -n (lab.sh grants it); the file is
# still the user's, since this script opens it before sudo.
#
# Where the counters cannot be read, the file gets one line with
# "available": false and the reason, and nothing else runs. In a container
# the counters are the host's package, shared with every other container,
# so they are refused there unless PL_ENERGY_IN_CONTAINER=1.
#
# Lines (ts_ns on this PC's clock):
#   {"ts_ns":..,"kind":"energy-start","available":true,"source":"powercap",
#    "zones":{"intel-rapl:0":{"name":"package-0","max_uj":262143328850}}}
#   {"ts_ns":..,"kind":"energy","uj":{"intel-rapl:0":123456789}}
#   {"ts_ns":..,"kind":"energy-start","available":false,"reason":"..."}
set -u
self="$0"
case "$self" in /*) ;; *) self="$(pwd)/$self" ;; esac
home="${PIPEDPEER_LAB_HOME:-$HOME/.local/share/pipedpeer-lab}"
out="${PL_ENERGY_FILE:-$home/logs/energy.jsonl}"
pidfile="$home/otel/energy.pid"
period="${PL_ENERGY_PERIOD_S:-1}"
# Tests only: a fake /sys/class tree (powercap/, hwmon/).
sys="${PL_ENERGY_SYSFS:-/sys/class}"

# zones: "<id> <counter file> <max_uj> <name>" lines, top-level zones only.
zones() {
	for z in "$sys"/powercap/intel-rapl:*; do
		[ -r "$z/name" ] || continue
		case "${z##*/intel-rapl:}" in *:*) continue ;; esac
		read -r name <"$z/name" || continue
		max=0
		[ -r "$z/max_energy_range_uj" ] && read -r max <"$z/max_energy_range_uj"
		echo "${z##*/} $z/energy_uj $max $name"
	done
	for h in "$sys"/hwmon/hwmon*; do
		[ -r "$h/name" ] || continue
		read -r hn <"$h/name"
		[ "$hn" = amd_energy ] || continue
		for l in "$h"/energy*_label; do
			[ -r "$l" ] || continue
			read -r name <"$l"
			case "$name" in Esocket*) ;; *) continue ;; esac
			f="${l%_label}_input"
			echo "${h##*/}/${f##*/} $f 0 $name"
		done
	done
}

as_root() {
	if [ "$(id -u)" = 0 ]; then "$@"; else sudo -n "$@"; fi
}

json_unavailable() {
	printf '{"ts_ns":%s,"kind":"energy-start","available":false,"reason":"%s"}\n' "$(date +%s%N)" "$1"
}

# probe: "available <source> user|root" or "unavailable <reason>".
probe() {
	if [ -z "${PL_ENERGY_IN_CONTAINER:-}${PL_ENERGY_SYSFS:-}" ] && { [ -f /run/.containerenv ] || [ -f /.dockerenv ]; }; then
		echo "unavailable container: the counters are the host package's, shared with every container"
		return
	fi
	first="$(zones | { read -r id file max name && echo "$file"; })"
	if [ -z "$first" ]; then
		echo "unavailable no RAPL zone (/sys/class/powercap/intel-rapl:*, hwmon amd_energy)"
		return
	fi
	how=user
	if ! cat "$first" >/dev/null 2>&1; then
		how=root
		if ! as_root cat "$first" >/dev/null 2>&1; then
			echo "unavailable $first is not readable (it needs root, and sudo -n failed)"
			return
		fi
	fi
	case "$first" in "$sys"/powercap/*) echo "available powercap $how" ;; *) echo "available hwmon-amd_energy $how" ;; esac
}

# loop (as root): one line per period.
loop() {
	src="$1"
	hdr=""
	ids=""
	files=""
	while read -r id file max name; do
		[ -n "$id" ] || continue
		hdr="$hdr${hdr:+,}\"$id\":{\"name\":\"$name\",\"max_uj\":$max}"
		ids="$ids $id"
		files="$files $file"
	done <<ZONES
$(zones)
ZONES
	printf '{"ts_ns":%s,"kind":"energy-start","available":true,"source":"%s","period_s":%s,"zones":{%s}}\n' \
		"$(date +%s%N)" "$src" "$period" "$hdr"
	trap 'exit 0' TERM INT HUP
	while :; do
		t="$(date +%s%N)"
		body=""
		set -- $ids
		for f in $files; do
			v=""
			read -r v <"$f" 2>/dev/null
			body="$body${body:+,}\"$1\":${v:-null}"
			shift
		done
		printf '{"ts_ns":%s,"kind":"energy","uj":{%s}}\n' "$t" "$body"
		sleep "$period"
	done
}

running() {
	[ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

start() {
	mkdir -p "${out%/*}" "${pidfile%/*}"
	if running; then
		echo "PLENERGY running $(cat "$pidfile")"
		return 0
	fi
	set -- $(probe)
	state="$1"
	shift
	if [ "$state" != available ]; then
		json_unavailable "$*" >>"$out"
		echo "PLENERGY unavailable $*"
		return 0
	fi
	# `run` writes its own pid, then execs sudo (same pid): the user can
	# signal sudo, which passes the signal on to the root loop.
	if command -v setsid >/dev/null 2>&1; then
		setsid sh "$self" run "$1" "$2" >>"$out" 2>>"$home/otel/energy.log" </dev/null &
	else
		nohup sh "$self" run "$1" "$2" >>"$out" 2>>"$home/otel/energy.log" </dev/null &
	fi
	i=0
	while [ $i -lt 20 ] && ! running; do sleep 0.1; i=$((i + 1)); done
	if running; then
		echo "PLENERGY started $(cat "$pidfile") $1"
	else
		echo "PLENERGY unavailable the sampler exited at once (see $home/otel/energy.log)"
	fi
}

stop() {
	if running; then
		kill "$(cat "$pidfile")" 2>/dev/null
	fi
	rm -f "$pidfile"
	echo "PLENERGY stopped"
}

case "${1:-}" in
start) start ;;
stop) stop ;;
status) if running; then echo "PLENERGY running $(cat "$pidfile")"; else echo "PLENERGY stopped"; fi ;;
run)
	echo $$ >"$pidfile"
	if [ "$3" = user ] || [ "$(id -u)" = 0 ]; then exec sh "$self" loop "$2"; fi
	exec sudo -n sh "$self" loop "$2"
	;;
loop) loop "$2" ;;
probe) probe ;;
*) echo "usage: energy.sh start | stop | status" >&2; exit 2 ;;
esac
