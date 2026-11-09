#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/lat_map.py (E16) to Dask Distributed.
# Distributed: one Dask task per repetition, .result() waited on before the
# next.
# Local: the timing loop, the percentiles and the result line.
# Follows: https://docs.dask.org/en/stable/scheduling.html
# Percentile: sorted(x)[ceil(q * n) - 1].
# Result line, byte-for-byte as systems.sh expects:
#   LAT-OK n=%d p50_ms=%.4f p90_ms=%.4f p99_ms=%.4f
import math
import os
import sys
import time

from dask.distributed import Client


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"lat_dask.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"lat_dask.py: {name} is not an integer: {raw!r}")


def percentile(values, q):
    ordered = sorted(values)
    return ordered[math.ceil(q * len(ordered)) - 1]


def noop(i):
    return i


def main():
    calls = _env_int("BENCH_CALLS")
    client = Client(os.environ["DASK_SCHEDULER"])
    latencies = []
    for i in range(calls):
        t0 = time.perf_counter()
        out = client.submit(noop, i).result()
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
