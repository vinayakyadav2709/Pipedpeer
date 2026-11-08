#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/pool_map.py (W1) to Ray.
# Distributed: the Pool items, as Ray tasks (ray.util.multiprocessing.Pool).
# Local: the checksum reduction and the result line (the same work() bodies).
# Follows: https://docs.ray.io/en/latest/ray-more-libs/multiprocessing.html
# Pool() = all CPUs in the cluster (Ray's default), so BENCH_PROCS is not read;
# on one machine that is the same nprocs BENCH_PROCS would name.
# Original result line, byte-for-byte: POOLMAP-OK items=%d seconds=%.4f checksum=%.6f
import os
import time

import ray
from ray.util.multiprocessing import Pool


def work(i):
    acc = 0.0
    for k in range(1, 60000):
        acc += (i + k) ** 0.5
    return acc


if __name__ == "__main__":
    ray.init(address=os.environ["RAY_ADDRESS"])
    n = int(os.environ.get("BENCH_ITEMS", "48"))
    t0 = time.perf_counter()
    with Pool() as p:
        out = p.map(work, range(n))
    dt = time.perf_counter() - t0
    assert len(out) == n
    # Correctness: the same computation, serially (as the original).
    if os.environ.get("BENCH_VERIFY") == "1":
        assert out == [work(i) for i in range(n)], "results differ from serial"
    print(f"POOLMAP-OK items={n} seconds={dt:.4f} checksum={sum(out):.6f}")
