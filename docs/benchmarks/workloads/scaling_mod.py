#!/usr/bin/env python3
"""Same workload as scaling.py, but the kernel lives in an importable module."""
import os
import time
from multiprocessing import Pool

from kernel import work

if __name__ == "__main__":
    n = int(os.environ.get("BENCH_ITEMS", "60"))
    t0 = time.perf_counter()
    with Pool(int(os.environ.get("BENCH_PROCS", "12"))) as p:
        out = p.map(work, range(n))
    dt = time.perf_counter() - t0
    assert len(out) == n
    print(f"SCALING-OK items={n} seconds={dt:.3f} checksum={sum(out):.4f}")
