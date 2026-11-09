#!/usr/bin/env python3
# Port of pipedpeer-demo/cpu.py (W2) for the plain-ssh baseline.
# One part of the Pool items and of the Mandelbrot tiles, run on one lab PC by
# w2_ssh.sh (GNU parallel --sshlogin); numpy and pandas stay local on pc00.
# Prints one JSON line:
#   {"part":i,"pool_out":[...],"joblib_count":n,"procs":p,"seconds":s}
# procs is the Pool size used: BENCH_PROCS when positive, else this PC's
# os.cpu_count() (0 and unset both mean "this PC's cores").
import json
import os
import sys
import time

import numpy as np
from joblib import Parallel, delayed

SPIN = 10_000_000
TILES, TILE_ROWS, WIDTH = 64, 40, 1600


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


def main(argv):
    part = parts = None
    for i, a in enumerate(argv):
        if a == "--part":
            part = int(argv[i + 1])
        elif a == "--parts":
            parts = int(argv[i + 1])
    if part is None or parts is None or not 0 <= part < parts:
        sys.stderr.write("usage: w2_ssh_part.py --part i --parts n\n")
        raise SystemExit(2)
    n = int(os.environ.get("BENCH_ITEMS", "128"))
    procs = int(os.environ.get("BENCH_PROCS", "0") or 0) or (os.cpu_count() or 1)
    items = list(range(part, n, parts))
    tiles = list(range(part, TILES, parts))
    t0 = time.perf_counter()
    from multiprocessing import Pool as MPool
    with MPool(procs) as p:
        pool_out = p.map(score, items)
    counts = Parallel(n_jobs=-1)(delayed(mandel_tile)(t) for t in tiles)
    dt = time.perf_counter() - t0
    if os.environ.get("BENCH_VERIFY") == "1":
        assert pool_out == [score(i) for i in items]
        assert counts == [mandel_tile(t) for t in tiles]
    print(json.dumps({"part": part, "pool_out": pool_out, "joblib_count": sum(counts),
                     "procs": procs, "seconds": dt}))


if __name__ == "__main__":
    main(sys.argv[1:])
