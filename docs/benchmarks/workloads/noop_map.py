#!/usr/bin/env python3
"""The E15 no-op workload: BENCH_ITEMS calls with no work and no payload, one
Pool map. It measures task dispatch, not compute.

Distributed: every call through the Pool (Pipedpeer's shim ships chunks by
design, so its tables label the arm batched; the Ray and Dask ports submit one
remote call per item, unbatched).
Local: the reduction and the result line. seconds covers submit to all results.
Result line, byte-for-byte as systems.sh expects:
  NOOP-OK items=%d seconds=%.4f tasks_per_s=%.4f
"""
import os
import sys
import time
from multiprocessing import Pool


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"noop_map.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"noop_map.py: {name} is not an integer: {raw!r}")


def noop(i):
    return i


def main():
    n = _env_int("BENCH_ITEMS")
    procs = _env_int("BENCH_PROCS", required=False, default=0)
    t0 = time.perf_counter()
    with Pool(procs or None) as p:
        out = p.map(noop, range(n))
    dt = time.perf_counter() - t0
    assert len(out) == n, f"expected {n} results, got {len(out)}"
    rate = n / max(dt, 1e-9)
    print(f"NOOP-OK items={n} seconds={dt:.4f} tasks_per_s={rate:.4f}")


if __name__ == "__main__":
    main()
