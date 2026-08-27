#!/usr/bin/env python3
"""Heavy linear algebra: fast local BLAS + single-node svd offload.

Plain NumPy. The cost model decides whether np.matmul is worth shipping;
np.linalg.svd offloads the whole matrix to one worker, so a weak orchestrator
never blocks on LAPACK for tens of seconds.

The matmul is also timed against local BLAS here, so the run can be checked on
whether the decision was right rather than on which way it went. Asserting it
never ships is a statement about this rig - three containers sharing one host's
cores, where nothing remote can beat local BLAS - dressed up as a law, and it
would fail the day the model got good enough to ship a shape that should be.
What must always hold is that the choice is not slower than the alternative.
"""
import os
import time

import numpy as np

rng = np.random.RandomState(3)
n = 4096

print(f"building two {n}x{n} float64 matrices ({n * n * 8 / 1e6:.0f} MB each) ...")
A = rng.rand(n, n)
B = rng.rand(n, n)

# The shim's gate checks this marker on every call and falls through to the
# original numpy, so this is untouched local BLAS in the same process, on the
# same operands. Timing a thin slice and scaling it up was tried first and
# over-stated local by 3.4x: 64 rows of a 4096-wide product keep neither the
# cache nor the threads busy the way the whole thing does, so the per-row cost
# is not the same number. The full product costs a second here and is exact.
os.environ["PIPEDPEER_NUMPY_NESTED"] = "1"
t0 = time.monotonic()
np.matmul(A, B)
t_local = time.monotonic() - t0
del os.environ["PIPEDPEER_NUMPY_NESTED"]

print("C = np.matmul(A, B) ...")
t0 = time.monotonic()
C = np.matmul(A, B)
t_matmul = time.monotonic() - t0
print(f"matmul: {t_matmul:.1f}s  (norm check ||C||_F = {np.linalg.norm(C):.2e})")
print(f"matmul decision: paid {t_matmul:.1f}s, local BLAS would be "
      f"~{t_local:.1f}s (same operands, shim bypassed)")

print("U, S, Vh = np.linalg.svd(A) ...")
t0 = time.monotonic()
U, S, Vh = np.linalg.svd(A)
t_svd = time.monotonic() - t0
print(f"svd: {t_svd:.1f}s  (singular values: {S[0]:.4f} ... {S[-1]:.4e})")

print("verifying reconstruction A ~= U @ diag(S) @ Vh ...")
t0 = time.monotonic()
err = np.linalg.norm(A - (U * S) @ Vh) / np.linalg.norm(A)
print(f"reconstruction relative error: {err:.2e}  (checked in {time.monotonic() - t0:.1f}s)")