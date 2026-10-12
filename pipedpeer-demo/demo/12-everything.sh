#!/usr/bin/env bash
# Every kind of work in one file (run on PC-2: LAPTOP-C in layout 1).
#
#   ./demo/12-everything.sh
#
# 07_everything.py imports torch: its environment must be built here first
# (demo/prep-3-gpu.sh at the setup visit), or this step waits on a GB download.
source "$(dirname "$0")/_lib.sh"

if [[ ! -f 07_everything.py ]]; then
	echo "${R}07_everything.py not found in $DEMO_DIR${N}" >&2
	exit 2
fi

banner "Everything in one script"
show grep -nE "^def |Pool|n_jobs|linalg|groupby|torch|tqdm" 07_everything.py
note "Pool, a lambda Pool, sklearn, numpy, pandas, torch - one plain file."
pause
# Straight to the terminal: the tqdm bar redraws in place.
show "$PIPE" run 07_everything.py
rc=$?
echo
if [[ $rc -ne 0 ]]; then
	echo "${R}  the run failed (exit $rc): read the Error line above${N}" >&2
	exit "$rc"
fi
note "'section <name> <s>' lines: the script's own timing per part."
note "Summary 'spread': which machines took the Pool items."
