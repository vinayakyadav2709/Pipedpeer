"""Two numpy operations that the cost model should treat differently:
matmul has high arithmetic intensity but local BLAS is very fast, while SVD
is slow enough locally that moving the matrix can pay for itself."""
import numpy as np, time

rng = np.random.default_rng(0)
a = rng.standard_normal((2048, 2048))
b = rng.standard_normal((2048, 2048))

t0 = time.perf_counter(); c = a @ b; t_mm = time.perf_counter() - t0
t0 = time.perf_counter(); u, s, vt = np.linalg.svd(a); t_svd = time.perf_counter() - t0
print("NUMPY-OK matmul=%.4f svd=%.4f checksum=%.6f" % (t_mm, t_svd, float(c[0, 0] + s[0])))
