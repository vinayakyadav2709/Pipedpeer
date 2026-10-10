#!/usr/bin/env bash
# Step 3 - a script stock Python cannot run at all.
source "$(dirname "$0")/_lib.sh"

banner "Step 3 - the one stock Python refuses"
show grep -n "lambda" 06_pool_lambda.py
pause
echo
note "Plain python3 first. This is EXPECTED to fail:"
typed python3 06_pool_lambda.py
python3 06_pool_lambda.py 2>&1 | tail -3
echo
pause
note "Same file, under pipedpeer:"
typed pipedpeer run 06_pool_lambda.py
"$PIPE" run 06_pool_lambda.py 2>&1 | grep -vE '^\s*$' | tail -6
echo
note "multiprocessing sends functions by name; a lambda has none. Pipedpeer sends it by value."
note "The point here is that it runs at all, not the speed."
