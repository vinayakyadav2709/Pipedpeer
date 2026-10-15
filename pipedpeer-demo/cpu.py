#!/usr/bin/env python3
"""CPU work, written the ordinary way: a Pool, joblib, numpy and pandas.

Nothing here mentions pipedpeer. Run it both ways and compare the TOTAL lines:

    python3 cpu.py                  # this machine's cores
    pipedpeer run cpu.py            # every machine's cores

The functions the Pool and joblib hand out use only pure Python and numpy, so a
phone running Termux (python + python-numpy) can take a share of them.

    --items N   Pool items (default 128). Raise it for the failover drill so
                there is time to switch a machine's Wi-Fi off mid-run.
"""
import argparse
import multiprocessing
import os
import time

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

SPIN = 10_000_000          # inner loop per Pool item (~1 s on one laptop core)
TILES, TILE_ROWS, WIDTH = 64, 40, 1600   # joblib: Mandelbrot tiles
SVD_N = 2304               # numpy: a 42 MB matrix
ROWS = 3_000_000           # pandas: generated frame


def score(seed):
    """Deterministic pure-Python work: no I/O, no globals."""
    total = 0
    for i in range(SPIN):
        total += (i ^ seed) % 7
    return total


def mandel_tile(tile):
    """Escape counts for TILE_ROWS rows of the Mandelbrot set (numpy only)."""
    rows = TILES * TILE_ROWS
    y = np.linspace(-1.2, 1.2, rows)[tile * TILE_ROWS:(tile + 1) * TILE_ROWS]
    x = np.linspace(-2.0, 0.6, WIDTH)
    c = x[None, :] + 1j * y[:, None]
    z = np.zeros_like(c)
    count = np.zeros(c.shape, dtype=np.int32)
    for _ in range(200):
        z = z * z + c
        inside = np.abs(z) <= 2.0
        count += inside
        z[~inside] = 2.0
    return int(count.sum())


def timed(name, fn):
    t0 = time.monotonic()
    fn()
    print(f"  {name} took {time.monotonic() - t0:.1f}s")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--items", type=int, default=128, help="Pool items")
    args = parser.parse_args()
    t0 = time.monotonic()
    print(f"cpu.py on {os.cpu_count()} local cores")

    def pool():
        with multiprocessing.Pool(os.cpu_count()) as p:
            results = p.map(score, range(args.items))
        print(f"Pool: {args.items} items, checksum {sum(results)}")

    def joblib_tiles():
        counts = Parallel(n_jobs=-1)(delayed(mandel_tile)(t) for t in range(TILES))
        print(f"joblib: {TILES} Mandelbrot tiles, {sum(counts)} points inside")

    def numpy_svd():
        a = np.random.RandomState(3).rand(SVD_N, SVD_N)
        s = np.linalg.svd(a, compute_uv=False)
        print(f"numpy: largest singular value {s[0]:.4f}")

    def pandas_groupby():
        rng = np.random.RandomState(5)
        cities = np.array([f"city_{i:02d}" for i in range(50)], dtype=object)
        df = pd.DataFrame({
            "city": cities[rng.randint(0, 50, ROWS)],
            "temp": rng.normal(20, 8, ROWS),
            "rain": rng.exponential(2.0, ROWS),
        })
        out = df.groupby("city").agg({"temp": "mean", "rain": "sum"})
        print(f"pandas: {ROWS:,} rows -> {len(out)} cities, total rain {out['rain'].sum():.1f}")

    timed("Pool", pool)
    timed("joblib", joblib_tiles)
    timed("numpy", numpy_svd)
    timed("pandas", pandas_groupby)
    print(f"TOTAL {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
