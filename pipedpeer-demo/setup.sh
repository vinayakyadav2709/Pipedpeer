#!/usr/bin/env bash
# Pipedpeer demo setup: generate data, verify the cluster, and rehearse every
# script once so the live run is fast (closures pre-built, store already
# materialized on every worker, data uploaded).
set -euo pipefail
cd "$(dirname "$0")"

# --- locate the pipedpeer binary -------------------------------------------
PIPE="${PIPEDPEER_BIN:-}"
if [ -z "$PIPE" ]; then
  if [ -x "../bin/pipedpeer" ]; then PIPE="$(cd .. && pwd)/bin/pipedpeer"
  elif command -v pipedpeer >/dev/null 2>&1; then PIPE="$(command -v pipedpeer)"
  else echo "pipedpeer binary not found — run 'make build' in the repo root first"; exit 1
  fi
fi
echo "using pipedpeer: $PIPE"
export PIPEDPEER_BIN="$PIPE"   # demo/prep-3-gpu.sh uses the same binary

# --- data for script 03 -----------------------------------------------------
# 03 has its own workspace dir (with its own .pipedpeerignore): the CSV must
# ship with 03, but nothing else should drag ~1 GB into its upload.
cd 03_pandas_ooc
# DEMO_CSV_ROWS sizes it: 10M rows is ~1.1 GB, 3M is ~330 MB. Over home
# internet links every chunk crosses the network, so rehearse with the size
# you will actually present.
ROWS="${DEMO_CSV_ROWS:-10000000}"
if [ ! -f data.csv ]; then
  echo "generating data.csv ($ROWS rows, ~$((ROWS * 110 / 1000000)) MB) with plain python3 + pandas ..."
  # Written under a temporary name: a half-written data.csv left by Ctrl+C
  # would otherwise pass the -f test above on the next run.
  DEMO_CSV_ROWS="$ROWS" python3 - <<'PYEOF'
import os
import numpy as np
import pandas as pd

rng = np.random.RandomState(42)
n = int(os.environ["DEMO_CSV_ROWS"])
cats = rng.choice(["alpha", "beta", "gamma", "delta"], size=n)
df = pd.DataFrame({
    "id": np.arange(n, dtype=np.int64),
    "cat": cats,
    "g1": rng.rand(n),
    "g2": rng.rand(n),
    "g3": rng.rand(n),
    "g4": rng.rand(n),
    "out": rng.rand(n),
})
df.to_csv("data.csv.tmp", index=False)
print(f"wrote data.csv ({n:,} rows)")
PYEOF
  mv data.csv.tmp data.csv
else
  echo "data.csv already present: $(du -h data.csv | cut -f1)"
fi
cd ..

# --- data for script 07 -----------------------------------------------------
# Its own workspace too, for the same reason. ~200 MB; DEMO_CAPSTONE_ROWS
# sizes it (2M rows by default).
if [ ! -f 07_capstone/data.csv ]; then
  echo "generating 07_capstone/data.csv with plain python3 + pandas ..."
  (cd 07_capstone && python3 make_data.py)
else
  echo "07_capstone/data.csv already present: $(du -h 07_capstone/data.csv | cut -f1)"
fi

# --- cluster sanity ---------------------------------------------------------
echo
echo "=== cluster nodes (expect every joined machine healthy, GPU shown on the two laptops) ==="
"$PIPE" nodes

# --- rehearsal pass ---------------------------------------------------------
# First run of each script builds the Nix closure (minutes) and materializes
# the store on whichever node it lands on. Running them once beforehand means
# the live demo starts instantly AND pool fan-out can reach every worker.
echo
echo "=== rehearsal pass (builds closures + warms stores on all workers) ==="
# Exactly the command the demo uses, so the rehearsal warms the same path.
run() { echo; echo "--- rehearsal: $1"; "$PIPE" run "$@" 2>&1 | tail -8; }

run 00_pool.py
run 06_pool_lambda.py
run 01_sklearn_rf.py
run 02_numpy_heavy.py
# From inside its own directory: the script opens data.csv relative to where
# it runs. PIPEDPEER_OOC_MIN forces the out-of-core path, which a machine
# with more than ~2x the file in free RAM would otherwise skip - and then the
# rehearsal would warm a path the demo never takes.
(cd 03_pandas_ooc && run 03_pandas_ooc.py -e PIPEDPEER_OOC_MIN=1e8)
run 05_file_sync.py
(cd 07_capstone && run 07_capstone.py -e PIPEDPEER_OOC_MIN=5e7)
# Common packages a teacher's script likely imports, so it starts fast.
echo
echo "=== common packages (demo/prep-4-common.sh) ==="
if ! ./demo/prep-4-common.sh; then
	echo "WARNING: common packages not built - a new script will build its own (minutes); see above"
fi

# GPU environment: CUDA torch is several GB, so it is built and
# shipped once here, by the same script each sender PC runs at the setup
# visit. A CPU-only rehearsal is still a valid rehearsal for everything else,
# so a GPU failure is reported, not fatal.
echo
echo "=== GPU environment (demo/prep-3-gpu.sh) ==="
if ! ./demo/prep-3-gpu.sh; then
	echo "WARNING: GPU prep did not finish - the GPU and everything steps will be slow or fail; see above"
fi

echo
echo "=== DEMO READY ==="
echo "Every script has been built and shipped once, so the live run is fast."
echo "Open 'pipedpeer dashboard' and follow DEMO-SCRIPT.md."