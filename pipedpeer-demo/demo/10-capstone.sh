#!/usr/bin/env bash
# Step 10 - everything at once: one plain script that reads a CSV with pandas,
# builds features with numpy, trains a forest with scikit-learn, bootstraps
# with multiprocessing.Pool and writes result files.
source "$(dirname "$0")/_lib.sh"

banner "Step 10 - one ordinary script, five kinds of work"
cd 07_capstone
[[ -f data.csv ]] || { note "no data.csv yet - generating it (plain python3 + pandas)"; python3 make_data.py || exit 1; }
show ls -lh data.csv
show grep -nE "read_csv|groupby|np\.matmul|RandomForest|Pool\(|to_csv|json.dump" 07_capstone.py
note "No pipedpeer anywhere in the file. Each stage is a library call the shim intercepts on its own."
pause
rm -rf results
log="$(mktemp)"
typed pipedpeer run 07_capstone.py -e PIPEDPEER_OOC_MIN=5e7
"$PIPE" run 07_capstone.py -e PIPEDPEER_OOC_MIN=5e7 2>&1 | tee "$log" |
	grep -E '^\[[1-5]/5\]|^  [a-z]+: [0-9.]+s|out-of-core|chunk partials|joblib: [0-9]+ tasks|accuracy|resamples between|^TOTAL|^Shim:|Synced'
echo
show ls results/
note "Each stage printed its own time. The 'Shim:' line counts every item it intercepted -"
note "trees, resamples, CSV chunks - and how many ran on the other machines."
note "results/ was written on the cluster and synced back here."
rm -f "$log"
