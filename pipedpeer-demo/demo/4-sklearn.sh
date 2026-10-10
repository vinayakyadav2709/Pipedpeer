#!/usr/bin/env bash
# Step 4 - scikit-learn, untouched. n_jobs=-1 becomes "every core in the cluster".
source "$(dirname "$0")/_lib.sh"

banner "Step 4 - scikit-learn RandomForest, n_jobs=-1"
show grep -n "n_jobs" 01_sklearn_rf.py
note "Only present this if rehearsal-results.txt showed a speedup on these networks."
note "The longest step (~3-4 min). Talk over it: each tree is a task the cluster schedules."
pause
./compare.sh 01_sklearn_rf.py
