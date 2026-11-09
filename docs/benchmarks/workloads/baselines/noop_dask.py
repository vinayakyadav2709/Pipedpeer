#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/noop_map.py (E15) to Dask Distributed.
# Distributed: one Dask task per no-op call (raw, unbatched; Pipedpeer ships
# chunks by design and its tables say so).
# Local: the result collection and the line. seconds covers submit to all
# results.
# Follows: https://docs.dask.org/en/stable/scheduling.html
# Result line, byte-for-byte as systems.sh expects:
#   NOOP-OK items=%d seconds=%.4f tasks_per_s=%.4f
import os
import sys
import time

from dask.distributed import Client


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"noop_dask.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"noop_dask.py: {name} is not an integer: {raw!r}")


def noop(i):
    return i


def main():
    n = _env_int("BENCH_ITEMS")
    client = Client(os.environ["DASK_SCHEDULER"])
    t0 = time.perf_counter()
    out = client.gather(client.map(noop, range(n)))
    dt = time.perf_counter() - t0
    assert out == list(range(n)), f"expected {n} results, got {len(out)}"
    print(f"NOOP-OK items={n} seconds={dt:.4f} tasks_per_s={n / max(dt, 1e-9):.4f}")


if __name__ == "__main__":
    main()
