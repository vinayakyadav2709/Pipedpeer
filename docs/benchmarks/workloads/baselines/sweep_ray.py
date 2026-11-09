#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/sweep_map.py (SW1) to Ray.
# Distributed: the sweep items, in remote tasks that each take
#   batch = max(1, ceil(100 / BENCH_TASK_MS)) items.
# Local: building the item list and payloads, the checksum reduction and the
# result line.
# Follows: https://docs.ray.io/en/latest/ray-core/tasks/patterns/too-fine-grained-tasks.html
# That page gives no millisecond threshold; ceil(100 / task_ms) is this
# project's own batching rule, and is stated as such in the write-up.
# Payloads are built per item on the driver (one distinct bytes object each),
# so pickle memoisation cannot hide the transfer.
# Fail-fast connect as baselines/w1_ray.py.
# Result line, byte-for-byte as systems.sh expects:
#   SWEEP-OK items=%d seconds=%.4f checksum=%.6f
import math
import os
import socket
import sys
import time
import zlib

import ray

_TASK_ITERS = 0


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"sweep_ray.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"sweep_ray.py: {name} is not an integer: {raw!r}")


def build_items(n, payload_bytes):
    return [
        (i, bytes([i % 251]) * payload_bytes if payload_bytes else b"")
        for i in range(n)
    ]


def work(item):
    i, payload = item
    acc = 0.0
    for k in range(1, 1 + _TASK_ITERS):
        acc += (i + k) ** 0.5
    if payload:
        acc += zlib.crc32(payload)
    return acc


@ray.remote
def run_batch(items):
    return [work(item) for item in items]


def split_batches(items, batch):
    return [items[i : i + batch] for i in range(0, len(items), batch)]


def submit_batches(remote_run_batch, batches):
    """Submit each batch; kept separate so a test can count .remote calls."""
    return [remote_run_batch.remote(batch) for batch in batches]


def main():
    global _TASK_ITERS
    n = _env_int("BENCH_ITEMS")
    task_ms = _env_int("BENCH_TASK_MS")
    iters_per_ms = _env_int("BENCH_ITERS_PER_MS")
    payload_bytes = _env_int("BENCH_PAYLOAD_BYTES", required=False, default=0)
    if task_ms < 1:
        sys.exit("sweep_ray.py: BENCH_TASK_MS must be >= 1")
    _TASK_ITERS = task_ms * iters_per_ms
    batch = max(1, math.ceil(100 / task_ms))

    host, _, port = os.environ["RAY_ADDRESS"].rpartition(":")
    socket.create_connection((host, int(port)), timeout=5).close()
    ray.init(
        address=os.environ["RAY_ADDRESS"],
        runtime_env={
            "env_vars": {
                "BENCH_TASK_MS": str(task_ms),
                "BENCH_ITERS_PER_MS": str(iters_per_ms),
            }
        },
    )

    items = build_items(n, payload_bytes)
    t0 = time.perf_counter()
    batches = ray.get(submit_batches(run_batch, split_batches(items, batch)))
    dt = time.perf_counter() - t0
    out = [value for part in batches for value in part]
    assert len(out) == n, f"expected {n} results, got {len(out)}"
    print(f"SWEEP-OK items={n} seconds={dt:.4f} checksum={sum(out):.6f}")


if __name__ == "__main__":
    main()
