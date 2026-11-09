#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/lat_map.py (E16) to Ray.
# Distributed: one Ray task per repetition, waited on before the next.
# Local: the timing loop, the percentiles and the result line.
# Follows: https://docs.ray.io/en/latest/ray-core/tasks.html
# Fail-fast connect as baselines/w1_ray.py.
# Percentile: sorted(x)[ceil(q * n) - 1].
# Result line, byte-for-byte as systems.sh expects:
#   LAT-OK n=%d p50_ms=%.4f p90_ms=%.4f p99_ms=%.4f
import math
import os
import socket
import sys
import time

import ray


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"lat_ray.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"lat_ray.py: {name} is not an integer: {raw!r}")


def percentile(values, q):
    ordered = sorted(values)
    return ordered[math.ceil(q * len(ordered)) - 1]


@ray.remote
def noop(i):
    return i


def main():
    calls = _env_int("BENCH_CALLS")
    host, _, port = os.environ["RAY_ADDRESS"].rpartition(":")
    socket.create_connection((host, int(port)), timeout=5).close()
    ray.init(address=os.environ["RAY_ADDRESS"])
    latencies = []
    for i in range(calls):
        t0 = time.perf_counter()
        out = ray.get(noop.remote(i))
        latencies.append((time.perf_counter() - t0) * 1000.0)
        assert out == i, f"call {i} returned {out!r}"
    print(
        "LAT-OK n=%d p50_ms=%.4f p90_ms=%.4f p99_ms=%.4f"
        % (
            calls,
            percentile(latencies, 0.50),
            percentile(latencies, 0.90),
            percentile(latencies, 0.99),
        )
    )


if __name__ == "__main__":
    main()
