#!/usr/bin/env bash
# Same script, twice: this machine alone, then the cluster.
#
#   ./compare.sh 00_pool.py
#   ./compare.sh 01_sklearn_rf.py
#
# The local run is plain `python3`, with pipedpeer nowhere in it - that is the
# number to compare against, and it is the number the audience already has.
# The cluster run is `pipedpeer run`, the same file, unedited.
#
# Both numbers come from the script's own final TOTAL line, so neither is
# measured by the thing being sold.
set -uo pipefail

script="${1:-}"
if [[ -z "$script" || ! -f "$script" ]]; then
	echo "usage: ./compare.sh <script.py>" >&2
	echo >&2
	echo "available:" >&2
	ls -1 ./*.py 2>/dev/null | sed 's|^|  |' >&2
	exit 2
fi

# The installed binary by default; PIPEDPEER_BIN to point at a build from a
# checkout, which is what setup.sh honours too.
cli="${PIPEDPEER_BIN:-}"
if [[ -z "$cli" ]]; then
	if command -v pipedpeer >/dev/null 2>&1; then
		cli="$(command -v pipedpeer)"
	elif [[ -x ../bin/pipedpeer ]]; then
		cli="$(cd .. && pwd)/bin/pipedpeer"
	fi
fi
if [[ -z "$cli" || ! -x "$cli" ]]; then
	echo "pipedpeer not found. Install it first - see DEMO.md." >&2
	exit 2
fi

took() { grep -Eo '^TOTAL [0-9.]+s' | tail -1 | awk '{print $2}'; }

echo "============================================================"
echo " $script - this machine only (plain python3)"
echo "============================================================"
local_out="$(mktemp)"
python3 "$script" 2>&1 | tee "$local_out"
local_t="$(took < "$local_out")"

# Said before the run, not after, so a disappointing number is explained
# rather than explained away. One machine plus a sandbox is legitimately a
# little slower than one machine; the cluster is the whole point.
peers="$("$cli" nodes 2>/dev/null | awk 'NR>1 && $NF != "self" && $3 == "healthy"' | wc -l)"
echo
echo "============================================================"
echo " $script - across the cluster (pipedpeer run)"
echo "============================================================"
if [[ "${peers:-0}" -eq 0 ]]; then
	echo "NOTE: no peers are connected, so this is the same one machine with a"
	echo "      sandbox around it and is expected to be slightly SLOWER."
	echo "      Join the others first:  pipedpeer join <introducer-ip>"
	echo
else
	echo "($peers peer(s) connected)"
	echo
fi
cluster_out="$(mktemp)"
"$cli" run "$script" 2>&1 | tee "$cluster_out"
cluster_rc="${PIPESTATUS[0]}"
cluster_t="$(took < "$cluster_out")"

if [[ "$cluster_rc" -ne 0 ]]; then
	echo
	echo "The cluster run failed. The usual causes, in order:" >&2
	echo "  * the daemon is not running here     -> pipedpeer start" >&2
	echo "  * this machine has not joined        -> pipedpeer join <introducer-ip>" >&2
	echo "  * a stale binary is first on PATH    -> PIPEDPEER_BIN=/path/to/pipedpeer $0 $script" >&2
	echo "                                          (using: $cli)" >&2
fi

echo
echo "============================================================"
printf ' %-28s %s\n' "this machine only:" "${local_t:-did not finish}"
printf ' %-28s %s\n' "across the cluster:" "${cluster_t:-did not finish}"
if [[ -n "${local_t:-}" && -n "${cluster_t:-}" ]]; then
	python3 - "$local_t" "$cluster_t" <<'PY'
import sys
loc = float(sys.argv[1].rstrip("s"))
clu = float(sys.argv[2].rstrip("s"))
if clu > 0:
    print(" %-28s %.2fx" % ("speedup:", loc / clu))
PY
fi
echo "============================================================"
echo
echo "Where the work actually ran (not what it says above):"
echo "  pipedpeer tasks       # while it runs"
echo "  pipedpeer traffic     # after: the pool ledger, per peer"
rm -f "$local_out" "$cluster_out"
