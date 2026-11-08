#!/usr/bin/env python3
# Port of pipedpeer-demo/cpu.py (W2) to Dask Distributed.
# Distributed: the Pool items (client.map) and the joblib Mandelbrot tiles
# (joblib.parallel_backend("dask")). Local on pc00: the numpy SVD; the pandas
# part uses dask.dataframe per the Dask docs (from_pandas + groupby/agg + .compute()).
# Follows: https://docs.dask.org/en/stable/scheduling.html
# Original result lines, byte-for-byte:
#   cpu.py on <cores> local cores
#   Pool: <items> items, checksum <sum>
#   joblib: 64 Mandelbrot tiles, <n> points inside
#   numpy: largest singular value <x:.4f>
#   pandas: 3,000,000 rows -> 50 cities, total rain <x:.1f>
#   TOTAL <s:.1f>s
import argparse
import os
import time

import numpy as np
import pandas as pd
from dask import dataframe as dd
from dask.distributed import Client
from joblib import Parallel, delayed, parallel_backend

SPIN = 10_000_000
TILES, TILE_ROWS, WIDTH = 64, 40, 1600
SVD_N = 2304
ROWS = 3_000_000


def score(seed):
    total = 0
    for i in range(SPIN):
        total += (i ^ seed) % 7
    return total


def mandel_tile(tile):
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
    t = time.monotonic()
    fn()
    print(f"  {name} took {time.monotonic() - t:.1f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--items", type=int, default=128)
    args = parser.parse_args()
    client = Client(os.environ["DASK_SCHEDULER"])
    t0 = time.monotonic()
    print(f"cpu.py on {os.cpu_count()} local cores")

    def pool():
        out = client.gather(client.map(score, range(args.items)))
        print(f"Pool: {args.items} items, checksum {sum(out)}")

    def joblib_tiles():
        with parallel_backend("dask"):  # registers the backend on every joblib version
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
        ddf = dd.from_pandas(df, npartitions=2)
        out = ddf.groupby("city").agg({"temp": "mean", "rain": "sum"}).compute()
        print(f"pandas: {ROWS:,} rows -> {len(out)} cities, total rain {out['rain'].sum():.1f}")

    timed("Pool", pool)
    timed("joblib", joblib_tiles)
    timed("numpy", numpy_svd)
    timed("pandas", pandas_groupby)
    print(f"TOTAL {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
