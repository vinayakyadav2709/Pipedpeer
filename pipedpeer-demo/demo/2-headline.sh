#!/usr/bin/env bash
# The headline: the same file, one machine vs the cluster (LAPTOP-A).
source "$(dirname "$0")/_lib.sh"

banner "Same script, this machine vs the cluster"
note "Show the file first: plain multiprocessing.Pool, no pipedpeer anywhere in it."
show grep -n "Pool\|import" 00_pool.py
pause
note "While the cluster half runs: point at the dashboard - every row's CPU column rises."
./compare.sh 00_pool.py
echo
note "The file was not edited between the two runs. Both times come from the script's own TOTAL line."
note "Summary 'spread': how many items each machine took."
