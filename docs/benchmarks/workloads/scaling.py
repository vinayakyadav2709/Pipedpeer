"""CPU-bound Pool.map used for the scaling measurement.

Each item is a few hundred milliseconds of pure arithmetic with a tiny payload
(one integer in, one float out), so the running time is governed by how many
cores are available rather than by how fast the network is.
"""
import os
import time
from multiprocessing import Pool


def work(i):
    acc = 0.0
    for k in range(1, 3000000):
        acc += (i + k) ** 0.5
    return acc


if __name__ == "__main__":
    n = int(os.environ.get("BENCH_ITEMS", "60"))
    t0 = time.perf_counter()
    with Pool(int(os.environ.get("BENCH_PROCS", "12"))) as p:
        out = p.map(work, range(n))
    dt = time.perf_counter() - t0
    assert len(out) == n
    print("SCALING-OK items=%d seconds=%.3f checksum=%.4f" % (n, dt, sum(out)))
