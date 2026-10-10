#!/usr/bin/env bash
# Step 2 - the headline: the same file, one machine vs the cluster.
source "$(dirname "$0")/_lib.sh"

banner "Step 2 - same script, this machine vs the cluster"
note "Show the file first: plain multiprocessing.Pool, no pipedpeer anywhere in it."
show grep -n "Pool\|import" 00_pool.py
pause
note "While the cluster half runs: point at the dashboard / 'pipedpeer tasks' on another screen."
./compare.sh 00_pool.py
echo
note "The file was not edited between the two runs. Both times come from the script's own TOTAL line."
