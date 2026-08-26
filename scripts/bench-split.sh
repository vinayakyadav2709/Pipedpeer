#!/usr/bin/env bash
# Measures the split policy itself, which the general pool benchmark cannot.
#
# Run against an uneven cluster - scripts/lab-hetero.sh brings one up with 8,
# 2 and 1 CPU workers. On a uniform cluster this measures nothing, because
# splitting by headcount and splitting by throughput give the same answer.
#
# Measured on a 16-core machine with an 8:2:1 cluster, five runs each:
#   even split (headcount)   3.39 3.49 3.61 3.81 3.90   median 3.61s
#   scheduled (throughput)   2.29 2.81 2.87 2.97 3.75   median 2.87s
# The scheduler's slowest run is its first after a restart, before any rate
# has been measured and while the core-count prior is all it has to go on.
#
# In that benchmark the submitting machine's own pool does most of the work, so
# how the remainder is divided between peers barely moves the total. Here the
# origin is held to a single worker, so the cluster's share is the critical
# path and the policy that divides it is what decides the wall time.
set -euo pipefail
cli="$HOME/bin/pipedpeer"
items="${ITEMS:-48}"
spin="${SPIN:-1500000}"
W="$HOME/.local/state/pipedpeer/splitbench"
rm -rf "$W"; mkdir -p "$W"; : > "$W/.pipedpeerignore"

cat > "$W/task.py" <<PY
import time
from multiprocessing import Pool

def work(i):
    t = 0
    for _ in range($spin):
        t += 1
    return t

if __name__ == "__main__":
    with Pool(1) as p:
        out = p.map(work, range($items))
    print("SPLIT-OK", len(out))
PY

cd "$W"
start=$(date +%s.%N)
PIPEDPEER_RESPECT_POOL_SIZE=1 "$cli" run task.py --distribute force > "$W/run.log" 2>&1 || {
	echo "RUN FAILED"; tail -20 "$W/run.log"; exit 1
}
end=$(date +%s.%N)
grep -q "SPLIT-OK" "$W/run.log" || { echo "no result"; tail -10 "$W/run.log"; exit 1; }
printf "elapsed %.2fs\n" "$(echo "$end - $start" | bc)"
