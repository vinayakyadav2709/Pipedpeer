#!/usr/bin/env bash
# Step 6 - pandas on a file treated as bigger than one machine's memory.
source "$(dirname "$0")/_lib.sh"

banner "Step 6 - pandas read_csv + groupby, out-of-core"
cd 03_pandas_ooc
show ls -lh data.csv
show grep -n "pd.read_csv\|\.groupby(" 03_pandas_ooc.py
note "Plain pandas: read the whole CSV, then groupby().mean()."
note "PIPEDPEER_OOC_MIN=1e8 says 'treat anything over 100 MB as too big for one machine'"
note "- on a small laptop it would trigger on its own."
pause
log="$(mktemp)"
typed pipedpeer run 03_pandas_ooc.py -e PIPEDPEER_OOC_MIN=1e8
"$PIPE" run 03_pandas_ooc.py -e PIPEDPEER_OOC_MIN=1e8 2>&1 | tee "$log" | grep -E 'out-of-core|chunk partials|fallback|^read|^groupby|^(cat|alpha|beta|gamma|delta)|^TOTAL'
echo
if grep -q 'ooc csv fallback' "$log"; then
	echo "${R}  It fell back to reading on one machine. Answer is still correct; say so honestly.${N}"
else
	note "'combining N chunk partials': each machine parsed and reduced its own chunks;"
	note "only the tiny per-group partial sums came back. No machine held the whole file."
fi
rm -f "$log"
