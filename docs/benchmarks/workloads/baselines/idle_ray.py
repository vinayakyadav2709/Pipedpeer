#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/pool_idle.py (idle) to Ray.
# Distributed: the Pool items, as Ray tasks; each item is ~0.1 ms of arithmetic,
# which is the point (this port trains the idle case of the same map, where the
# dispatch cost is what shows).
# Local: the checksum reduction and the result line.
# Follows: https://docs.ray.io/en/latest/ray-more-libs/multiprocessing.html
# Pool() = all CPUs in the cluster (Ray's default), so BENCH_PROCS is not read.
# Original result line, byte-for-byte: POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f
import os
import socket
import time

import ray
from ray.util.multiprocessing import Pool


def work(i):
    acc = 0.0
    for k in range(1, 1500):
        acc += (i + k) ** 0.5
    return acc


if __name__ == "__main__":
    # Fail fast: ray.init retries an unreachable head for minutes.
    _host, _, _port = os.environ["RAY_ADDRESS"].rpartition(":")
    socket.create_connection((_host, int(_port)), timeout=5).close()
    ray.init(address=os.environ["RAY_ADDRESS"])
    n = int(os.environ.get("BENCH_ITEMS", "16"))
    t0 = time.perf_counter()
    with Pool() as p:
        out = p.map(work, range(n))
    dt = time.perf_counter() - t0
    assert out == [work(i) for i in range(n)], "results differ from serial"
    print(f"POOLIDLE-OK items={n} seconds={dt:.4f} checksum={sum(out):.6f}")
