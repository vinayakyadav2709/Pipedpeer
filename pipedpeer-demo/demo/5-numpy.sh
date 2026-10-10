#!/usr/bin/env bash
# Step 5 - NumPy: the cost model decides what is worth shipping.
source "$(dirname "$0")/_lib.sh"

banner "Step 5 - NumPy matmul + SVD"
note "Watch the 'matmul decision' line: it may keep the matmul local (local BLAS is fast)"
note "and ship only the SVD. The claim is 'never slower than local', not 'everything moves'."
pause
./compare.sh 02_numpy_heavy.py
