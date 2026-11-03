#!/usr/bin/env bash
# Count the lines each baseline port changes in its original, the campaign's
# measure of porting effort (docs/benchmarks/lab-campaign.md 1.3), and write
# results/lab/port-lines.csv. Pairs come from scripts/campaign/workloads.json.
#
#   scripts/campaign/port-lines.sh          # rewrite the CSV, print it
#   scripts/campaign/port-lines.sh --check  # fail if the CSV is stale
#
# A changed line is one `diff -u orig port | grep -c '^[+-][^+-]'` counts: an
# added or removed line that is not a header and not blank. plain and
# pipedpeer run the original file, so their rows are 0 by construction.
# Also fails when t1_train.py has drifted from the diloco.py heredoc in
# scripts/bench-diloco.sh, the copy it was taken from.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
out=results/lab/port-lines.csv

heredoc=$(awk '/cat > "\$1\/diloco.py" <<.PY./{f=1;next} /^PY$/{if(f){exit}} f' scripts/bench-diloco.sh)
[[ "$heredoc" == "$(cat docs/benchmarks/workloads/baselines/t1_train.py)" ]] ||
	{ echo "t1_train.py differs from the diloco.py heredoc in scripts/bench-diloco.sh; re-extract it" >&2; exit 1; }

count() { diff -u "$1" "$2" | grep -c '^[+-][^+-]' || true; }

csv=$(
	echo "workload,system,changed_lines"
	python3 -c '
import json
for w in json.load(open("scripts/campaign/workloads.json"))["workloads"]:
    for s in ("plain", "pipedpeer"):
        print(w["id"], s, w["script"], w["script"])
    for s, port in w["ports"].items():
        print(w["id"], s, w["script"], port)
' | while read -r id system orig port; do
		echo "$id,$system,$(count "$orig" "$port")"
	done
)

if [[ ${1:-} == --check ]]; then
	[[ "$csv" == "$(cat "$out" 2>/dev/null)" ]] || { echo "$out is stale; run $0" >&2; exit 1; }
	echo "$out up to date"
else
	mkdir -p "$(dirname "$out")"
	echo "$csv" >"$out"
	cat "$out"
fi
