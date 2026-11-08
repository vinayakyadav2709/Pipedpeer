#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/trivial.py (fixed cost) to Ray.
# Distributed: one no-op task on the cluster (this is the point: the fixed cost
# is the submission hop, not compute).
# Local: the result line (the driver prints it, never the task).
# Follows: https://docs.ray.io/en/latest/ray-core/walkthrough.html (task submit).
# Original result line, byte-for-byte: TRIVIAL-OK
import os

import ray


@ray.remote
def noop():
    return 1


if __name__ == "__main__":
    ray.init(address=os.environ["RAY_ADDRESS"])
    ray.get(noop.remote())
    print("TRIVIAL-OK")
