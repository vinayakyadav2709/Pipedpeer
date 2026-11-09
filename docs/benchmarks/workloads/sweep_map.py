#!/usr/bin/env python3
"""The campaign sweep workload: BENCH_ITEMS Pool.map items, each a fixed
amount of arithmetic and optionally a payload.

Distributed: every item through the Pool (Pipedpeer's shim ships chunks; plain
python runs a multiprocessing.Pool, and the Ray/Dask/ssh ports in baselines/
distribute the same items).
Local: the item list is built on this process, the result reduction and the
result line.
Sizes come from sizes.env through the runner: BENCH_ITEMS, BENCH_TASK_MS,
BENCH_ITERS_PER_MS, BENCH_PROCS, BENCH_PAYLOAD_BYTES.

Payloads are built per item on the driver, one distinct bytes object each, so a
pickle memo cannot hide the transfer. That conflicts with Dask's advice that
workers load large data themselves (docs/dask best practices); the port
discloses it in its header. The rule that a remote task takes
ceil(100 / BENCH_TASK_MS) items is this project's, not a Ray default.

Result line, byte-for-byte as systems.sh expects:
  SWEEP-OK items=%d seconds=%.4f checksum=%.6f
"""
import os
import sys
import time
import zlib
from functools import partial
from multiprocessing import Pool


def _iters_from_env():
    try:
        return int(os.environ.get("BENCH_TASK_MS", "0") or 0) * int(
            os.environ.get("BENCH_ITERS_PER_MS", "0") or 0
        )
    except ValueError:
        sys.stderr.write(
            "sweep_map.py: warning: BENCH_TASK_MS=%r BENCH_ITERS_PER_MS=%r are "
            "not integers; falling back to 0 iterations until main() validates "
            "them\n"
            % (
                os.environ.get("BENCH_TASK_MS"),
                os.environ.get("BENCH_ITERS_PER_MS"),
            )
        )
        return 0


# Set by main(); read again at import so a spawn/forkserver worker starts with
# the same value. Pool.map also carries it in a partial, so a worker whose
# environment lacks BENCH_* still computes the same thing.
_TASK_ITERS = _iters_from_env()


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"sweep_map.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"sweep_map.py: {name} is not an integer: {raw!r}")


def build_items(n, payload_bytes):
    """One (item index, payload) tuple per item; each payload a new object."""
    items = []
    for i in range(n):
        payload = bytes([i % 251]) * payload_bytes if payload_bytes else b""
        items.append((i, payload))
    return items


def work(item, iters=None):
    i, payload = item
    acc = 0.0
    for k in range(1, 1 + (_TASK_ITERS if iters is None else iters)):
        acc += (i + k) ** 0.5
    if payload:
        acc += zlib.crc32(payload)
    return acc


def main():
    global _TASK_ITERS
    n = _env_int("BENCH_ITEMS")
    task_ms = _env_int("BENCH_TASK_MS")
    iters_per_ms = _env_int("BENCH_ITERS_PER_MS")
    procs = _env_int("BENCH_PROCS", required=False, default=0)
    payload_bytes = _env_int("BENCH_PAYLOAD_BYTES", required=False, default=0)
    if task_ms < 0 or iters_per_ms < 0 or payload_bytes < 0:
        sys.exit("sweep_map.py: BENCH_TASK_MS, BENCH_ITERS_PER_MS and BENCH_PAYLOAD_BYTES must be >= 0")
    _TASK_ITERS = task_ms * iters_per_ms

    items = build_items(n, payload_bytes)
    t0 = time.perf_counter()
    with Pool(procs or None) as p:
        out = p.map(partial(work, iters=_TASK_ITERS), items)
    dt = time.perf_counter() - t0
    assert len(out) == n, f"expected {n} results, got {len(out)}"
    print(f"SWEEP-OK items={n} seconds={dt:.4f} checksum={sum(out):.6f}")


if __name__ == "__main__":
    main()
