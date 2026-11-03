#!/usr/bin/env bash
# telemetry.sh up|down|status|check [seconds]: the driver's OpenTelemetry
# collector (otel/driver.yaml.tmpl). It receives OTLP/gRPC on :4317 from the
# node collectors (otel/node.sh) and tails results/lab/runs.jsonl; it writes
# rotating OTLP JSON into results/lab/otel/ and, when
# ~/.config/pipedpeer-lab/r2.env exists, also uploads to R2
# (pipedpeer-lab/otel/). Without r2.env it runs file-only and says so.
#
# check: metric datapoints and log records per lab.pc_id in the last minute
# (or [seconds]), read from the file output; with R2, the objects under otel/.
#
# Binary, rendered config, pid and log: ~/.local/share/pipedpeer-lab/otel.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
source "$here/otel/lib.sh"

results="$repo/results/lab"
pidfile="$LAB_OTEL/driver.pid"
logfile="$LAB_OTEL/driver.log"
config="$LAB_OTEL/driver.yaml"
bin="$LAB_BIN/otelcol-contrib"

up() {
	local r2=0 args
	if lab_running "$pidfile"; then
		echo "driver collector already running (pid $(cat "$pidfile"))"
		return 0
	fi
	lab_install_otelcol
	lab_r2_load && r2=1 || echo "WARNING: telemetry goes to $results/otel only, not to R2" >&2
	mkdir -p "$results/otel" "$LAB_OTEL/storage-driver"
	sed -e "s|@RESULTS@|$results|g" -e "s|@STORAGE@|$LAB_OTEL/storage-driver|g" \
		-e "s|@R2_ACCOUNT_ID@|${R2_ACCOUNT_ID:-not-configured}|g" \
		"$here/otel/driver.yaml.tmpl" >"$config"
	if [[ -n "${PIPEDPEER_LAB_S3_ENDPOINT:-}" ]]; then
		# Tests only: an S3 stand-in (rclone serve s3) instead of R2.
		sed -i "s|endpoint: https://.*r2.cloudflarestorage.com|endpoint: $PIPEDPEER_LAB_S3_ENDPOINT|" "$config"
	fi
	args=(--config "$config")
	((r2)) && args+=(--config "$here/otel/driver-r2.yaml")
	"$bin" validate "${args[@]}"
	# The keys exist only in this process's environment.
	(
		if ((r2)); then
			export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
		fi
		cd "$repo"
		setsid nohup "$bin" "${args[@]}" >>"$logfile" 2>&1 </dev/null &
		echo $! >"$pidfile"
	)
	sleep 2
	if ! lab_running "$pidfile"; then
		echo "driver collector exited at once; tail of $logfile:" >&2
		tail -20 "$logfile" >&2
		rm -f "$pidfile"
		return 1
	fi
	echo "driver collector up: pid $(cat "$pidfile"), OTLP/gRPC :4317, files in $results/otel, R2 $( ((r2)) && echo on || echo OFF)"
}

status() {
	if ! lab_running "$pidfile"; then
		echo "driver collector not running"
		return 1
	fi
	echo "driver collector running: pid $(cat "$pidfile"), config $config, log $logfile"
	if tr '\0' ' ' <"/proc/$(cat "$pidfile")/cmdline" | grep -q driver-r2.yaml; then echo "R2: on"; else echo "R2: OFF (file only)"; fi
	ls -la "$results/otel" 2>/dev/null || true
}

check() {
	local window="${1:-60}"
	python3 -I - "$results/otel" "$window" <<'PY'
import collections, json, os, sys, time

d, window = sys.argv[1], int(sys.argv[2])
since = time.time_ns() - window * 10**9
TAIL = 128 << 20  # the last minute is near the end; do not read gigabytes


def lines(path):
    if not os.path.exists(path):
        return
    with open(path, "rb") as f:
        size = f.seek(0, 2)
        f.seek(max(0, size - TAIL))
        if size > TAIL:
            f.readline()  # partial line
        for line in f:
            if line.strip():
                try:
                    yield json.loads(line)
                except ValueError:
                    pass  # a line still being written


def pc_id(resource):
    attrs = {a["key"]: a["value"] for a in resource.get("attributes", [])}
    for k in ("lab.pc_id", "service.name"):
        if k in attrs:
            v = attrs[k].get("stringValue", "?")
            return v if k == "lab.pc_id" else f"({v})"
    return "(none)"


points = collections.Counter()
for doc in lines(os.path.join(d, "metrics.jsonl")):
    for rm in doc.get("resourceMetrics", []):
        who = pc_id(rm.get("resource", {}))
        for sm in rm.get("scopeMetrics", []):
            for m in sm.get("metrics", []):
                for kind in ("gauge", "sum", "histogram", "summary", "exponentialHistogram"):
                    for dp in m.get(kind, {}).get("dataPoints", []):
                        if int(dp.get("timeUnixNano", 0)) >= since:
                            points[who] += 1

records = collections.Counter()
for doc in lines(os.path.join(d, "logs.jsonl")):
    for rl in doc.get("resourceLogs", []):
        who = pc_id(rl.get("resource", {}))
        for sl in rl.get("scopeLogs", []):
            for r in sl.get("logRecords", []):
                if int(r.get("observedTimeUnixNano") or r.get("timeUnixNano") or 0) >= since:
                    records[who] += 1

print(f"last {window} s, from {d}")
print(f"{'lab.pc_id':<20} {'datapoints':>10} {'log records':>12}")
for who in sorted(set(points) | set(records)):
    print(f"{who:<20} {points[who]:>10} {records[who]:>12}")
if not points and not records:
    print("nothing arrived")
    sys.exit(1)
PY
	if lab_r2_load 2>/dev/null; then
		lab_install_rclone
		lab_rclone_env
		echo "R2 objects under $R2_BUCKET/otel/ (newest 10 of $("$LAB_BIN/rclone" lsf -R --files-only "r2:$R2_BUCKET/otel" | wc -l)):"
		"$LAB_BIN/rclone" lsl "r2:$R2_BUCKET/otel" | sort -k2,3 | tail -10
	else
		echo "R2 not configured ($R2_ENV missing); skipped the bucket listing"
	fi
}

case "${1:-}" in
up) up ;;
down) lab_stop_pid "$pidfile"; echo "driver collector stopped (data kept in $results/otel)" ;;
status) status ;;
check) check "${2:-60}" ;;
*)
	echo "usage: $0 up|down|status|check [seconds]" >&2
	exit 2
	;;
esac
