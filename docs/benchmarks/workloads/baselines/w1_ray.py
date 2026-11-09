#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/pool_map.py (W1) to Ray.
# Distributed: the items as Ray tasks (@ray.remote), default num_cpus=1,
# default retries; results gathered with ray.get in submission order.
# Local: the checksum reduction and the result line (the same work() bodies).
# Follows: https://docs.ray.io/en/latest/ray-more-libs/tasks.html
# Original result line, byte-for-byte: POOLMAP-OK items=%d seconds=%.4f checksum=%.6f
import os
import socket
import time

import ray


def _work(i):
    acc = 0.0
    for k in range(1, 60000):
        acc += (i + k) ** 0.5
    return acc


@ray.remote
def work(i):
    return _work(i)


if __name__ == "__main__":
    # Fail fast: ray.init retries an unreachable head for minutes.
    _host, _, _port = os.environ["RAY_ADDRESS"].rpartition(":")
    socket.create_connection((_host, int(_port)), timeout=5).close()
    ray.init(address=os.environ["RAY_ADDRESS"])
    n = int(os.environ.get("BENCH_ITEMS", "48"))
    t0 = time.perf_counter()
    refs = [work.remote(i) for i in range(n)]
    out = ray.get(refs)
    dt = time.perf_counter() - t0
    assert len(out) == n
    # Correctness: the same computation, serially (as the original).
    if os.environ.get("BENCH_VERIFY") == "1":
        assert out == [_work(i) for i in range(n)], "results differ from serial"
    print(f"POOLMAP-OK items={n} seconds={dt:.4f} checksum={sum(out):.6f}")
