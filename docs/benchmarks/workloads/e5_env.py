#!/usr/bin/env python3
"""E5 env-shipping workload: imports numpy/pandas/sklearn/joblib/scipy and
runs a Pool map with --distribute force semantics (4 items per PC so every
PC gets work). Same print lines as the W1 result line for the harness."""
import sys
import time
from multiprocessing import Pool

import joblib  # noqa: F401
import numpy  # noqa: F401
import pandas  # noqa: F401
import scipy  # noqa: F401
import sklearn  # noqa: F401


def work(i):
    acc = 0.0
    for k in range(1, 2000):
        acc += (i + k) ** 0.5
    return acc


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 128
    t0 = time.perf_counter()
    with Pool() as p:
        out = p.map(work, range(n))
    dt = time.perf_counter() - t0
    print(f"E5-OK items={n} seconds={dt:.4f} checksum={sum(out):.6f}")


if __name__ == "__main__":
    main()
