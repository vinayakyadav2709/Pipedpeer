#!/usr/bin/env python3
"""The same work, written with a lambda instead of a named function.

Worth running BOTH ways, because the two answers differ in kind rather than
in speed:

    python3 06_pool_lambda.py         # raises PicklingError - stock Python
                                      # cannot send a lambda to a worker
                                      # process, because it pickles callables
                                      # by name and a lambda has none
    pipedpeer run 06_pool_lambda.py   # runs, on every machine in the cluster

Nothing here is written for pipedpeer. It is the shape people reach for first
and then rewrite when multiprocessing refuses it.
"""
import multiprocessing
import os
import time

# Smaller than 00_pool.py on purpose. This one is about whether the work can
# leave the machine at all, not how fast it goes: a lambda has to travel by
# value, and the half that stays local runs on threads rather than processes
# (nothing can hand a lambda to a worker process), so local is not the
# interesting number here.
ITEMS = 32
SPIN = 2_000_000

# A lambda, closing over nothing, exactly as somebody would write it before
# discovering that multiprocessing will not take it.
score = lambda seed: sum((i ^ seed) % 7 for i in range(SPIN))

if __name__ == "__main__":
    t0 = time.monotonic()
    print(f"scoring {ITEMS} items with a lambda ({os.cpu_count()} local cores) ...")

    with multiprocessing.Pool(os.cpu_count()) as pool:
        results = pool.map(score, range(ITEMS))

    print(f"checksum {sum(results)}")
    print(f"TOTAL {time.monotonic() - t0:.1f}s")
