#!/usr/bin/env bash
# Time every demo script on the docker rig, three ways, for 1..3 workers.
#
#   cd docker && COMPOSE_FILE=docker-compose.yml:docker-compose.bench.yml docker compose up -d
#   scripts/bench-matrix.sh                                   # everything, ~40 min
#   SCRIPTS="00_pool.py 07_capstone.py" WORKERS="3" RUNS=1 scripts/bench-matrix.sh
#
# Modes, all submitted from pp-orch (the "weak laptop", 3 cores):
#   local  the same closure with interception off (-e PIPEDPEER_SHIM=0): the
#          baseline, same Python, same packages, same 3 cores, nothing shipped
#   cold   every running daemon restarted just before the run: no warm worker
#          pools, no cached chunks. The closure is already in the shared store,
#          so this is "first run after the machines came up", not "first run
#          ever" - that one adds the Nix build, which the timeline shows.
#   warm   RUNS more runs straight after, the way a rehearsed demo runs
#
# Workers are switched by stopping and starting their daemons; the run only
# starts once pp-orch sees exactly that many healthy peers. Any other node
# (your host's own daemon, say, over the docker bridge) would add cores the
# table does not account for, so the bench refuses to start while one is
# healthy - stop it, or set BENCH_ALLOW_FOREIGN=1 to measure with it.
#
# Output: docker/bench-results/matrix-<stamp>/ with matrix.csv (one row per
# run, stage timings from the job record), summary.txt (medians), logs/.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEMO="$ROOT/pipedpeer-demo"
ORCH=pp-orch
ALL_WORKERS=(pp-worker1 pp-worker2 pp-worker3)
SCRIPTS="${SCRIPTS:-00_pool.py 06_pool_lambda.py 01_sklearn_rf.py 02_numpy_heavy.py 03_pandas_ooc.py 07_capstone.py}"
WORKERS="${WORKERS:-1 2 3}"
RUNS="${RUNS:-2}"
MODES="${MODES:-local cold warm}"

# A laptop that suspends mid-run turns a 20 s run into a 90 minute one in the
# record. Hold a sleep inhibitor for the whole bench where systemd offers one.
if [[ -z "${BENCH_INHIBITED:-}" ]] && command -v systemd-inhibit >/dev/null 2>&1; then
	BENCH_INHIBITED=1 exec systemd-inhibit --what=sleep:idle --who=pipedpeer-bench \
		--why="benchmark in progress" "$0" "$@"
fi

stamp="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT/docker/bench-results/matrix-$stamp"
mkdir -p "$OUT/logs"
CSV="$OUT/matrix.csv"
echo "workers,mode,script,run,exit,wall_s,script_s,nix_build_ms,upload_ms,seed_ms,run_ms,sync_ms,items,items_elsewhere" >"$CSV"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# Where each script runs from and what it needs. 03 and 07 have their own
# workspace (their data.csv ships only with them) and force out-of-core reads,
# which a box with more free RAM than the file would otherwise skip.
script_dir() { case "$1" in 03_pandas_ooc.py) echo 03_pandas_ooc ;; 07_capstone.py) echo 07_capstone ;; *) echo . ;; esac; }
script_env() { case "$1" in 03_pandas_ooc.py) echo "-e PIPEDPEER_OOC_MIN=1e8" ;; 07_capstone.py) echo "-e PIPEDPEER_OOC_MIN=5e7" ;; esac; }

for c in "$ORCH" "${ALL_WORKERS[@]}"; do
	docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -q true ||
		{ echo "$c is not running - bring the rig up first (see the top of this file)" >&2; exit 1; }
done

# Data. Generated on the host: the demo dir is bind-mounted into pp-orch.
[[ -f "$DEMO/07_capstone/data.csv" ]] || (cd "$DEMO/07_capstone" && python3 make_data.py) || exit 1
[[ -f "$DEMO/03_pandas_ooc/data.csv" ]] || { echo "03_pandas_ooc/data.csv missing - run pipedpeer-demo/setup.sh once" >&2; exit 1; }

daemon_up() { docker exec "$1" pgrep -f 'pipedpeer __daemon__' >/dev/null 2>&1; }

healthy_peers() { # healthy nodes pp-orch sees, other than itself, one host per line
	docker exec "$ORCH" pipedpeer nodes 2>/dev/null | awk 'NR>1 && $3=="healthy" && $NF!="self" {print $2}'
}

set_workers() { # set_workers <n>: daemons up on worker1..n, down on the rest
	local n=$1 i
	for i in "${!ALL_WORKERS[@]}"; do
		if ((i < n)); then
			daemon_up "${ALL_WORKERS[$i]}" || docker exec "${ALL_WORKERS[$i]}" pipedpeer start >/dev/null 2>&1
		else
			docker exec "${ALL_WORKERS[$i]}" pipedpeer stop >/dev/null 2>&1
		fi
	done
	wait_for "$n"
}

wait_for() { # wait_for <n>: until pp-orch sees exactly n healthy rig workers
	local n=$1 t peers foreign
	for t in $(seq 1 90); do
		peers="$(healthy_peers)"
		foreign="$(grep -vE '^worker[0-9]+$' <<<"$peers" | grep -v '^$')"
		if [[ -n "$foreign" && -z "${BENCH_ALLOW_FOREIGN:-}" ]]; then
			echo "a node outside the rig is healthy: $foreign" >&2
			echo "stop it (e.g. 'pipedpeer stop' on the host) or set BENCH_ALLOW_FOREIGN=1" >&2
			exit 1
		fi
		[[ "$(grep -cE '^worker[0-9]+$' <<<"$peers")" -eq "$n" ]] && return 0
		sleep 2
	done
	echo "pp-orch never saw $n healthy workers; it sees: $(echo $peers)" >&2
	exit 1
}

restart_running() { # cold start: restart every daemon that is up
	local c
	for c in "$ORCH" "${ALL_WORKERS[@]}"; do
		if daemon_up "$c"; then
			docker exec "$c" sh -c 'pipedpeer stop >/dev/null 2>&1; pipedpeer start >/dev/null 2>&1'
		fi
	done
}

last_job_id() { docker exec "$ORCH" sh -c 'ls -t /root/.local/share/pipedpeer/jobs 2>/dev/null | grep -E "^[0-9]+$" | head -1'; }

run_one() { # run_one <workers> <mode> <script> <run#>
	local w=$1 mode=$2 s=$3 n=$4 dir env log before t0 t1 rc wall total stats
	dir="$(script_dir "$s")"
	env="$(script_env "$s")"
	[[ $mode == local ]] && env="$env -e PIPEDPEER_SHIM=0"
	log="$OUT/logs/w${w}_${mode}_${s%.py}_$n.log"
	before="$(last_job_id)"
	t0=$(date +%s.%N)
	# shellcheck disable=SC2086 # env is a list of flags
	docker exec -w "/workspace/$dir" "$ORCH" pipedpeer run "$s" $env >"$log" 2>&1
	rc=$?
	t1=$(date +%s.%N)
	wall=$(python3 -c "print(f'{$t1 - $t0:.2f}')")
	total=$(grep -Eo '^TOTAL [0-9.]+s' "$log" | tail -1 | awk '{print $2}' | tr -d s)
	# The job record holds the stage timings and the shim's receipt. Only a
	# record newer than the one before the run is this run's own.
	stats=",,,,,,"
	if [[ "$(last_job_id)" != "$before" ]]; then
		stats="$(docker exec "$ORCH" pipedpeer stats --json --jobs 1 2>/dev/null | python3 -c '
import json, sys
try:
    j = json.load(sys.stdin)["jobs"][0]
except Exception:
    print(",,,,,,"); sys.exit()
ms = {s["name"]: s["ms"] for s in j.get("stages") or []}
r = j.get("receipt") or {}
pick = lambda k: str(ms[k]) if k in ms else ""
print(",".join([pick("nix build"), pick("upload"), pick("seed peers"),
                pick("script") or pick("execute"), pick("sync results"),
                str(r.get("mapped_items", "")), str(r.get("remote_used", ""))]))
')"
	fi
	echo "$w,$mode,$s,$n,$rc,$wall,${total:-},$stats" >>"$CSV"
	printf '  %-6s %-20s run %s  exit %-3s wall %7ss  script %6ss\n' "$mode" "$s" "$n" "$rc" "$wall" "${total:-?}"
}

say "pipedpeer-bench $stamp  scripts: $SCRIPTS  workers: $WORKERS  warm runs: $RUNS"
echo "binary: $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet -- src || echo '+dirty'), sha256 $(docker exec "$ORCH" sha256sum /usr/local/bin/pipedpeer | cut -c1-12)" | tee "$OUT/binary.txt"

if [[ " $MODES " == *" local "* ]]; then
	say "local baseline on pp-orch (interception off)"
	set_workers 0
	for s in $SCRIPTS; do run_one 0 local "$s" 1; done
fi

for w in $WORKERS; do
	say "$w worker(s)"
	set_workers "$w"
	for s in $SCRIPTS; do
		if [[ " $MODES " == *" cold "* ]]; then
			restart_running
			wait_for "$w"
			run_one "$w" cold "$s" 1
		fi
		if [[ " $MODES " == *" warm "* ]]; then
			for n in $(seq 1 "$RUNS"); do run_one "$w" warm "$s" "$n"; done
		fi
	done
done

set_workers 3 >/dev/null

say "summary (median script seconds; wall includes the build check, upload and sync)"
python3 - "$CSV" <<'PY' | tee "$OUT/summary.txt"
import csv, statistics, sys
from collections import defaultdict
rows = list(csv.DictReader(open(sys.argv[1])))
cells = defaultdict(list)
for r in rows:
    if r["exit"] != "0" or not r["script_s"]:
        continue
    key = "local" if r["mode"] == "local" else "%s %sw" % (r["mode"], r["workers"])
    cells[(r["script"], key)].append((float(r["script_s"]), float(r["wall_s"])))
cols = ["local"] + sorted({k for _, k in cells if k != "local"}, key=lambda k: (k.split()[1], k))
scripts = list(dict.fromkeys(r["script"] for r in rows))
print("%-20s" % "script" + "".join("%15s" % c for c in cols))
for s in scripts:
    line = "%-20s" % s
    for c in cols:
        v = cells.get((s, c))
        if not v:
            failed = any(r["script"] == s and r["exit"] != "0" and
                         ("local" if r["mode"] == "local" else "%s %sw" % (r["mode"], r["workers"])) == c
                         for r in rows)
            line += "%15s" % ("failed" if failed else "-")
        else:
            line += "%15s" % ("%.1f / %.0f" % (statistics.median(x[0] for x in v),
                                               statistics.median(x[1] for x in v)))
    print(line)
print("\ncells are  script TOTAL / wall clock, in seconds (median of runs that exited 0)")
PY
echo
echo "rows: $CSV"
