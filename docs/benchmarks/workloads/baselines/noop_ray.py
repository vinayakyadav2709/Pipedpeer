#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/noop_map.py (E15) to Ray.
# Distributed: one Ray task per no-op call (raw, unbatched; Pipedpeer ships
# chunks by design and its tables say so).
# Local: the result collection and the line. seconds covers submit to all
# results.
# Follows: https://docs.ray.io/en/latest/ray-core/tasks.html
# Fail-fast connect as baselines/w1_ray.py.
# Result line, byte-for-byte as systems.sh expects:
#   NOOP-OK items=%d seconds=%.4f tasks_per_s=%.4f
import os
import socket
import sys
import time

import ray


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"noop_ray.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"noop_ray.py: {name} is not an integer: {raw!r}")


@ray.remote
def noop(i):
    return i


def main():
    n = _env_int("BENCH_ITEMS")
    host, _, port = os.environ["RAY_ADDRESS"].rpartition(":")
    socket.create_connection((host, int(port)), timeout=5).close()
    ray.init(address=os.environ["RAY_ADDRESS"])
    t0 = time.perf_counter()
    out = ray.get([noop.remote(i) for i in range(n)])
    dt = time.perf_counter() - t0
    assert out == list(range(n)), f"expected {n} results, got {len(out)}"
    print(f"NOOP-OK items={n} seconds={dt:.4f} tasks_per_s={n / max(dt, 1e-9):.4f}")


if __name__ == "__main__":
    main()
