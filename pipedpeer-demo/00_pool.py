#!/usr/bin/env python3
"""Embarrassingly parallel CPU work, written the ordinary way.

Plain Python: multiprocessing.Pool over a named function. There is nothing
about pipedpeer in this file, and running it with `python3` works exactly as
it always did.

Run it both ways to see the difference:

    python3 00_pool.py                # this machine's cores only
    pipedpeer run 00_pool.py          # the cluster's cores
"""
import multiprocessing
import os
import time

ITEMS = 96          # independent pieces of work
SPIN = 15_000_000   # inner iterations each


def score(seed):
    """Deterministic CPU-bound work. No I/O, no randomness, no globals."""
    total = 0
    for i in range(SPIN):
        total += (i ^ seed) % 7
    return total


if __name__ == "__main__":
    t0 = time.monotonic()
    print(f"scoring {ITEMS} items ({os.cpu_count()} local cores) ...")

    with multiprocessing.Pool(os.cpu_count()) as pool:
        results = pool.map(score, range(ITEMS))

    print(f"checksum {sum(results)}")
    print(f"TOTAL {time.monotonic() - t0:.1f}s")
