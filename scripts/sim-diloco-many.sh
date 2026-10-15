#!/bin/bash
# MANY-SIM: run sim-diloco-ranks.py over a job list in parallel inside the
# agents slice, one output file per run, then print the markdown table
# (docs/benchmarks/diloco.md "Many ranks").
# usage: sim-diloco-many.sh JOBS OUTDIR [SHIMDIR] [PARALLEL]
#   JOBS: one "MODEL SEED ARM SCEN" per line; finished runs are skipped
set -euo pipefail
jobs=$1 out=$2
here=$(cd "$(dirname "$0")" && pwd)
shim=${3:-$here/../src/internal/nixgen/shim}
par=${4:-6}
mkdir -p "$out"
export here shim out
one() {
    f="$out/$(echo "$*" | tr ' +' '_p').txt"
    [ -s "$f" ] && grep -q LOSS "$f" && return 0
    r=$(timeout 5400 python3 "$here/sim-diloco-ranks.py" "$@" "$shim" 2>&1 | tail -1)
    echo "$* | $r" > "$f"
    case $r in LOSS*) ;; *) echo "failed: $* | $r" >&2 ;; esac
}
export -f one
systemd-run --user --scope -q --slice=pipedpeer-agents.slice \
    xargs -P "$par" -L 1 bash -c 'one "$@"' _ < "$jobs" || echo "some runs failed (see above)" >&2
cat "$out"/*.txt | python3 "$here/sim-diloco-many-table.py"
