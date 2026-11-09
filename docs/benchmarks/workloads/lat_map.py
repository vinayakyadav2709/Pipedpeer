#!/usr/bin/env python3
"""The E16 latency workload: one no-op call, wait for it, repeat BENCH_CALLS
times, through one Pool for the whole run.

Distributed: the calls through the Pool (Pipedpeer's shim may ship chunks; the
Ray and Dask ports submit one remote call per repetition).
Local: the timing loop, the percentiles and the result line.
Percentile: sorted(x)[ceil(q * n) - 1], so with n = 1 all three are that value.
Result line, byte-for-byte as systems.sh expects:
  LAT-OK n=%d p50_ms=%.4f p90_ms=%.4f p99_ms=%.4f
"""
import math
import os
import sys
import time
from multiprocessing import Pool


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"lat_map.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"lat_map.py: {name} is not an integer: {raw!r}")


def noop(i):
    return i


def percentile(values, q):
    ordered = sorted(values)
    return ordered[math.ceil(q * len(ordered)) - 1]


def main():
    calls = _env_int("BENCH_CALLS")
    procs = _env_int("BENCH_PROCS", required=False, default=0)
    latencies = []
    with Pool(procs or None) as p:
        for i in range(calls):
            t0 = time.perf_counter()
            out = p.map(noop, [i])
            latencies.append((time.perf_counter() - t0) * 1000.0)
            assert out == [i], f"call {i} returned {out!r}"
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
