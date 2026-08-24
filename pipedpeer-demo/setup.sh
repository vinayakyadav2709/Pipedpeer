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

# --- data for script 03 -----------------------------------------------------
# 03 has its own workspace dir (with its own .pipedpeerignore): the CSV must
# ship with 03, but nothing else should drag ~1 GB into its upload.
cd 03_pandas_ooc
if [ ! -f data.csv ]; then
  echo "generating data.csv (~1.7 GB) with plain python3 + pandas ..."
  python3 - <<'PYEOF'
import numpy as np
import pandas as pd

rng = np.random.RandomState(42)
n = 10_000_000
cats = np.random.choice(["alpha", "beta", "gamma", "delta"], size=n)
df = pd.DataFrame({
    "id": np.arange(n, dtype=np.int64),
    "cat": cats,
    "g1": rng.rand(n),
    "g2": rng.rand(n),
    "g3": rng.rand(n),
    "g4": rng.rand(n),
    "out": rng.rand(n),
})
df.to_csv("data.csv", index=False)
print(f"wrote data.csv ({n:,} rows)")
PYEOF
else
  echo "data.csv already present: $(du -h data.csv | cut -f1)"
fi
cd ..

# --- cluster sanity ---------------------------------------------------------
echo
echo "=== cluster nodes (expect yourself + the 2 dGPU workers, all healthy) ==="
"$PIPE" nodes

# --- rehearsal pass ---------------------------------------------------------
# First run of each script builds the Nix closure (minutes) and materializes
# the store on whichever node it lands on. Running them once beforehand means
# the live demo starts instantly AND pool fan-out can reach every worker.
echo
echo "=== rehearsal pass (builds closures + warms stores on all workers) ==="
run() { echo; echo "--- rehearsal: $1"; "$PIPE" run "$1" \
      --remote --isolate=false "${@:2}" 2>&1 | tail -8; }

run 01_sklearn_rf.py
run 02_numpy_heavy.py
run 03_pandas_ooc/03_pandas_ooc.py
run 04_torch_ddp.py --ddp 2 --gpu force

echo
echo "=== DEMO READY ==="
echo "Workers warmed. Open pipedpeer dashboard and run the 4 scripts (see DEMO.md)."