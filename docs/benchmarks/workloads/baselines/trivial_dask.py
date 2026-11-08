#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/trivial.py (fixed cost) to Dask Distributed.
# Distributed: one no-op task on the cluster (the fixed cost is the submission hop).
# Local: the result line.
# Follows: https://distributed.dask.org/en/stable/quickstart.html (client.submit).
# Original result line, byte-for-byte: TRIVIAL-OK
import os

from dask.distributed import Client


def noop():
    return 1


if __name__ == "__main__":
    client = Client(os.environ["DASK_SCHEDULER"])
    client.submit(noop).result()
    print("TRIVIAL-OK")
