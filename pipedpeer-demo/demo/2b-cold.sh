#!/usr/bin/env bash
# First run vs every run after it (run on PC-2: LAPTOP-C in layout 1).
#
#   ./demo/2b-cold.sh
#
# --cold sends the whole environment as if the other machine had none. It does
# not redo this machine's build, so it times the transfer, not a first-ever
# install. Run straight to the terminal (no pipe) so the progress bar draws.
source "$(dirname "$0")/_lib.sh"

banner "First run vs every run after it"
note "Cold: the environment (Python, its packages) travels to the other machine."
note "Watch the progress bar, then the summary's 'closure' line."
pause
t0=$SECONDS
show "$PIPE" run 06_pool_lambda.py --remote --cold
rc=$?
cold=$((SECONDS - t0))
if [[ $rc -ne 0 ]]; then
	echo "${R}  the cold run failed (exit $rc): read the Error line above${N}" >&2
	exit "$rc"
fi
echo
pause
note "Same command without --cold: only the script travels."
t0=$SECONDS
show "$PIPE" run 06_pool_lambda.py --remote
rc=$?
warm=$((SECONDS - t0))
if [[ $rc -ne 0 ]]; then
	echo "${R}  the second run failed (exit $rc): read the Error line above${N}" >&2
	exit "$rc"
fi
echo
echo "${B}  first run (cold): ${cold} s     every run after: ${warm} s${N}"
note "Summary says 'sent (cold)' first, 'already on the node' second."
note "The environment ships once per machine; after that only the script travels."
