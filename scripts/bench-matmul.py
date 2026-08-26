"""One matmul, timed both ways, so the cost model's choice can be scored.

The shim decides whether A @ B is worth shipping to the cluster. Whether that
decision was right is not something the decision can tell you - only running it
both ways can - so this measures local and cluster for the same operands and
prints both.

  python bench-matmul.py <m> <k> <n>

PIPEDPEER_NUMPY_NESTED is how the local number is taken: the shim's own gate
checks that marker on every call and falls through to the original numpy, so
this is the untouched BLAS path in the same process, on the same arrays, with
the caches in the same state. Timing it in a separate process would compare
two machines' worth of noise instead.
"""
import json
import os
import sys
import time

import numpy as np


def timed(fn, repeats=3):
    fn()  # warm: first call pays for BLAS threads and page faults
    best = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best


def main():
    m, k, n = (int(x) for x in sys.argv[1:4])
    rng = np.random.default_rng(0)
    a = rng.random((m, k))
    b = rng.random((k, n))

    os.environ["PIPEDPEER_NUMPY_NESTED"] = "1"
    local = timed(lambda: np.matmul(a, b))
    del os.environ["PIPEDPEER_NUMPY_NESTED"]

    # One call only. A repeat would find the cluster warm in a way the first
    # call of a real script never is, and the decision is made once.
    t0 = time.perf_counter()
    c = np.matmul(a, b)
    configured = time.perf_counter() - t0

    # Correctness is not the point here, but a wrong answer would make the
    # timings meaningless, so it is checked rather than assumed.
    expect = np.matmul(a[:4], b)
    ok = bool(np.allclose(c[:4], expect, rtol=1e-9, atol=1e-9))

    print("BENCH " + json.dumps({
        "m": m, "k": k, "n": n,
        "a_mb": a.nbytes / 1e6, "b_mb": b.nbytes / 1e6, "c_mb": c.nbytes / 1e6,
        "gflop": 2.0 * m * k * n / 1e9,
        "local_s": local,
        "configured_s": configured,
        "correct": ok,
    }))


main()
