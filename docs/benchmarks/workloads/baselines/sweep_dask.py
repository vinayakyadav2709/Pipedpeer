#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/sweep_map.py (SW1) to Dask Distributed.
# Distributed: the sweep items, batched into client.map tasks that each take
#   batch = max(1, ceil(100 / BENCH_TASK_MS)) items.
# Local: building the item list and payloads, the checksum reduction and the
# result line.
# Follows: https://docs.dask.org/en/stable/scheduling.html and
#          https://docs.dask.org/en/stable/best-practices.html
# Dask prefers workers to load large data themselves over client.scatter; this
# workload's payloads are built on the driver (so the transfer is measured),
# so they are sent with the task arguments, not scattered. Disclosed in the
# tables. The ceil(100 / task_ms) batching rule is this project's, not a Dask
# default.
# Fail-fast connect: Client() raises promptly on an unreachable scheduler.
# Result line, byte-for-byte as systems.sh expects:
#   SWEEP-OK items=%d seconds=%.4f checksum=%.6f
import math
import os
import sys
import time
import zlib

from dask.distributed import Client


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"sweep_dask.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"sweep_dask.py: {name} is not an integer: {raw!r}")


def build_items(n, payload_bytes):
    return [
        (i, bytes([i % 251]) * payload_bytes if payload_bytes else b"")
        for i in range(n)
    ]


def run_batch(items, iters):
    out = []
    for i, payload in items:
        acc = 0.0
        for k in range(1, 1 + iters):
            acc += (i + k) ** 0.5
        if payload:
            acc += zlib.crc32(payload)
        out.append(acc)
    return out


def split_batches(items, batch):
    return [items[i : i + batch] for i in range(0, len(items), batch)]


def main():
    n = _env_int("BENCH_ITEMS")
    task_ms = _env_int("BENCH_TASK_MS")
    iters_per_ms = _env_int("BENCH_ITERS_PER_MS")
    payload_bytes = _env_int("BENCH_PAYLOAD_BYTES", required=False, default=0)
    if task_ms < 1:
        sys.exit("sweep_dask.py: BENCH_TASK_MS must be >= 1")
    iters = task_ms * iters_per_ms
    batch = max(1, math.ceil(100 / task_ms))
    items = build_items(n, payload_bytes)
    chunks = split_batches(items, batch)

    client = Client(os.environ["DASK_SCHEDULER"])
    t0 = time.perf_counter()
    parts = client.gather(client.map(run_batch, chunks, [iters] * len(chunks)))
    dt = time.perf_counter() - t0
    out = [value for part in parts for value in part]
    assert len(out) == n, f"expected {n} results, got {len(out)}"
    print(f"SWEEP-OK items={n} seconds={dt:.4f} checksum={sum(out):.6f}")


if __name__ == "__main__":
    main()
