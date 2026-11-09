#!/usr/bin/env bash
# Host metrics for one experiment tag (brief S10): start vmstat + sar on every
# lab PC under systemd-run --user, and on stop convert sar to CSV, pull the
# daemon log, then rsync metrics/ and runs/ back into $LAB/hosts/<pc>/.
# All changes land on the PCs and in $LAB only; run.sh calls start/stop
# around each experiment (tag = <experiment>-<workload>-n<n>, e.g. E1-W1-n8).
#
# Why a tag and not a per-run unit: the metrics cover a whole experiment at
# one N, so host metrics line up with every run in that tag.
set -euo pipefail

_D="$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=/dev/null
. "$_D/lib.sh"

metrics_usage() {
	cat <<'EOF'
usage: metrics.sh start <tag> <hostsfile>
       metrics.sh stop <tag> <hostsfile>

start: on every PC in <hostsfile> (ids, one per line), start
  lab-vmstat-<tag>  (vmstat -t 1) and lab-sar-<tag> (sar -n DEV 1) under
  systemd-run --user, writing ~/pipedpeer-lab/metrics/<tag>.{vmstat,sa}
  and the start time to <tag>.start. Starting a tag that is already
  running is a warning, not an error: the running units are left alone.
stop: on every PC, stop both units, convert sar's binary file to
  <tag>.net.csv with sadf -d, copy the pipedpeer daemon log since the tag
  started to <tag>.daemon.log, then rsync ~/pipedpeer-lab/metrics/ and
  runs/ into $LAB/hosts/<pc>/. A PC with no sar data (it never started)
  is a warning; every other PC is still pulled.

Environment:
  LAB_RESULTS   results dir (default: <repo>/results/lab)
EOF
}

# The tag inside a systemd unit name: what run.sh builds, rejected here so a
# stray space does not become a second unit silently.
metrics_check_tag() {
	local tag="$1"
	if [[ -z "$tag" || "$tag" =~ [^A-Za-z0-9_.@-] ]]; then
		echo "metrics: bad tag '$tag' (unit name chars only)" >&2
		lab_log "metrics: bad tag '$tag'"
		return 1
	fi
}

# shellcheck disable=SC2016  # template: expanded per tag below, not here
_metrics_start_tpl='set -u
M="$HOME/pipedpeer-lab/metrics"
mkdir -p "$M" || { echo "NO-HOME"; exit 1; }
if [ -s "$M/@@TAG@@.start" ]; then
	echo "ALREADY-METRICS @@TAG@@"
else
	date +%s >"$M/@@TAG@@.start"
	echo '"'"'exec vmstat -t 1 >>"$HOME/pipedpeer-lab/metrics/@@TAG@@.vmstat"'"'"' >"$M/vmstat-@@TAG@@.cmd"
	echo '"'"'exec sar -n DEV -o "$HOME/pipedpeer-lab/metrics/@@TAG@@.sa" 1'"'"' >"$M/sar-@@TAG@@.cmd"
	systemd-run --user --unit=lab-vmstat-@@TAG@@ --collect bash "$M/vmstat-@@TAG@@.cmd"
	systemd-run --user --unit=lab-sar-@@TAG@@ --collect bash "$M/sar-@@TAG@@.cmd"
	echo "METRICS-START @@TAG@@"
fi'

# shellcheck disable=SC2016  # template: expanded per tag below, not here
_metrics_stop_tpl='set -u
M="$HOME/pipedpeer-lab/metrics"
cd "$M" 2>/dev/null || { echo "WARN no metrics dir for @@TAG@@"; exit 0; }
systemctl --user stop lab-vmstat-@@TAG@@ lab-sar-@@TAG@@ || echo "WARN systemctl stop failed for @@TAG@@"
if [ -s "@@TAG@@.sa" ]; then
	sadf -d "@@TAG@@.sa" -- -n DEV >"@@TAG@@.net.csv" || echo "WARN sadf failed for @@TAG@@"
else
	echo "WARN no sar data for @@TAG@@"
fi
rm -f "vmstat-@@TAG@@.cmd" "sar-@@TAG@@.cmd"
start="$(cat "@@TAG@@.start" 2>/dev/null || echo 0)"
if ! journalctl --user -u pipedpeer.service --since "@$start" -o short-iso-precise >"@@TAG@@.daemon.log" 2>/dev/null; then
	journalctl _SYSTEMD_USER_UNIT=pipedpeer.service >"@@TAG@@.daemon.log" 2>/dev/null || echo "WARN no daemon log for @@TAG@@"
fi
echo "METRICS-STOP @@TAG@@"'

metrics_start() {
	local tag="$1" hostsfile="$2"
	metrics_check_tag "$tag"
	lab_log "metrics start $tag"
	lab_fanout "metrics-start-$tag" "$hostsfile" \
		"${_metrics_start_tpl//'@@TAG@@'/$tag}"
	if grep -rq 'ALREADY-METRICS' "$LAB/logs/metrics-start-$tag/" 2>/dev/null; then
		lab_log "metrics $tag: already running on some PCs; those were skipped"
	fi
	if grep -rq 'NO-HOME' "$LAB/logs/metrics-start-$tag/" 2>/dev/null; then
		lab_log "metrics $tag: some PCs have no pipedpeer-lab work dir (not set up?)"
	fi
}

metrics_stop() {
	local tag="$1" hostsfile="$2" pc
	metrics_check_tag "$tag"
	lab_log "metrics stop $tag"
	lab_fanout "metrics-stop-$tag" "$hostsfile" \
		"${_metrics_stop_tpl//'@@TAG@@'/$tag}" || true
	while IFS= read -r pc; do
		[[ -z "$pc" || "$pc" == \#* ]] && continue
		mkdir -p "$LAB/hosts/$pc"
		# runs/ exists only on pc00; --ignore-missing-args keeps a worker's pull
		# (metrics/ alone) from ending in exit 23
		if ! lab_rsync --ignore-missing-args "$pc:pipedpeer-lab/metrics" "$pc:pipedpeer-lab/runs" \
			"$LAB/hosts/$pc/"; then
			lab_log "metrics stop: pull $pc failed; the other PCs continue"
		fi
	done < <(awk 'NF && $1 !~ /^#/ { print $1 }' "$hostsfile")
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	case "${1:-}" in
		-h | --help)
			metrics_usage
			exit 0
			;;
		start)
			if (($# != 3)); then
				metrics_usage >&2
				exit 2
			fi
			metrics_start "$2" "$3"
			;;
		stop)
			if (($# != 3)); then
				metrics_usage >&2
				exit 2
			fi
			metrics_stop "$2" "$3"
			;;
		*)
			metrics_usage >&2
			exit 2
			;;
	esac
fi
