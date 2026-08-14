#!/usr/bin/env bash
# Measures the shim's overhead on a small workload with no daemon reachable.
# The shim must fall back to a purely-local Pool (structural D2 guarantee), so
# shim-on must not be grossly slower than plain Python — anything beyond ~3x
# signals an accidental remote round-trip / spawn and fails.
#
# This is a report job, not a wall-clock gate: run-to-run noise on shared CI
# makes a tight timing threshold flaky, and the never-slower invariant itself
# is enforced by the Go tests (TestShimEnabledStillLocal, dead-peer fallback,
# warm-worker reuse). This only trips on a gross regression.
set -euo pipefail

cd "$(dirname "$0")/.."

which python3 >/dev/null || { echo "python3 required"; exit 1; }

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

# Extract the embedded sitecustomize.py so we can put it on PYTHONPATH.
python3 - "$workdir" <<'PY'
import re, sys
src = open("src/internal/nixgen/shim.go").read()
m = re.search(r"ShimSitecustomize = `(.*)`\n\n// WriteShim", src, re.S)
assert m, "could not extract ShimSitecustomize"
open(sys.argv[1] + "/sitecustomize.py", "w").write(m.group(1))
PY

cat > "$workdir/workmod.py" <<'PY'
def double(x):
    return x * 2
PY

cat > "$workdir/work.py" <<PY
import sys
sys.path.insert(0, "$workdir")
import multiprocessing
from workmod import double
if __name__ == "__main__":
    p = multiprocessing.Pool(2)
    r = p.map(double, range(200))
    p.close()
    assert r == [x * 2 for x in range(200)], r
PY

measure() {
    local env=$1
    local best=999999
    for _ in $(seq 1 5); do
        # Time from within a wrapper Python so only the child is measured.
        local t
        t=$( env $env python3 -c 'import os, subprocess, sys, time
t0 = time.monotonic()
subprocess.run([sys.executable, sys.argv[1]], check=True)
print("%.4f" % (time.monotonic() - t0))' "$workdir/work.py" )
        best=$(awk -v a="$best" -v b="$t" 'BEGIN{print (b<a)?b:a}')
    done
    echo "$best"
}

plain=$(measure "")
# Enable the shim but point it at nothing: _remote stays false (no URL/shard
# count), so the shim must run the exact same local Pool.
shim=$(measure "PYTHONPATH=$workdir PIPEDPEER_SHIM=1 PIPEDPEER_DAEMON_URL= PIPEDPEER_NUM_SHARDS=0")
echo "local-only best: ${plain}s"
echo "shim-on   best: ${shim}s"

ratio=$(awk -v p="$plain" -v s="$shim" 'BEGIN{print s/p}')
echo "ratio: ${ratio}x"

ok=$(awk -v r="$ratio" 'BEGIN{print (r < 3.0) ? 1 : 0}')
if [ "$ok" != "1" ]; then
    echo "FAIL: shim overhead exceeds 3x — D2 (never slower) regression."
    exit 1
fi
echo "PASS: shim overhead within budget."
