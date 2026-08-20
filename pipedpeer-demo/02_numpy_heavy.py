#!/usr/bin/env python3
"""Heavy linear algebra: fast local BLAS + single-node svd offload.

Plain NumPy. np.matmul stays local — local BLAS (~200 GFLOP/s) beats the
cluster transport for a square product (D2: never slower). np.linalg.svd
offloads the whole matrix to one worker, so a weak orchestrator never
blocks on LAPACK for tens of seconds.
"""
import time

import numpy as np

rng = np.random.RandomState(3)
n = 4096

print(f"building two {n}x{n} float64 matrices ({n * n * 8 / 1e6:.0f} MB each) ...")
A = rng.rand(n, n)
B = rng.rand(n, n)

print("C = np.matmul(A, B) ...")
t0 = time.monotonic()
C = np.matmul(A, B)
t_matmul = time.monotonic() - t0
print(f"matmul: {t_matmul:.1f}s  (norm check ||C||_F = {np.linalg.norm(C):.2e})")

print("U, S, Vh = np.linalg.svd(A) ...")
t0 = time.monotonic()
U, S, Vh = np.linalg.svd(A)
t_svd = time.monotonic() - t0
print(f"svd: {t_svd:.1f}s  (singular values: {S[0]:.4f} ... {S[-1]:.4e})")

print("verifying reconstruction A ~= U @ diag(S) @ Vh ...")
t0 = time.monotonic()
err = np.linalg.norm(A - (U * S) @ Vh) / np.linalg.norm(A)
print(f"reconstruction relative error: {err:.2e}  (checked in {time.monotonic() - t0:.1f}s)")