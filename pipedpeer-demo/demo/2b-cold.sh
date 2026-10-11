#!/usr/bin/env bash
# First run vs every run after it (run on PC-2).
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
cold=$((SECONDS - t0))
echo
pause
note "Same command without --cold: only the script travels."
t0=$SECONDS
show "$PIPE" run 06_pool_lambda.py --remote
warm=$((SECONDS - t0))
echo
echo "${B}  first run (cold): ${cold} s     every run after: ${warm} s${N}"
note "Summary says 'sent (cold)' first, 'already on the node' second."
note "The environment ships once per machine; after that only the script travels."
