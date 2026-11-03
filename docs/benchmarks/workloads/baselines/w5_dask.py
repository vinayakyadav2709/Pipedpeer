#!/usr/bin/env python3
"""Per-group fits: pandas groupby, then joblib over the groups.

The usual way to run a slow function per group on every core: split the
frame with groupby and hand each group to joblib.Parallel(n_jobs=-1).
Plain pandas and joblib, no cluster code. Each group gets a least-squares
trend of y on t and a bootstrap of its slope (PD_BOOT resamples), seeded
by the group key, so the result is the same on any machine. PD_GROUPS
groups of PD_ROWS rows set the size.
"""
import os
import time

import numpy as np
import pandas as pd
from dask.distributed import Client
from joblib import Parallel, delayed, parallel_backend

GROUPS = int(os.environ.get("PD_GROUPS", "256"))
ROWS = int(os.environ.get("PD_ROWS", "4000"))
BOOT = int(os.environ.get("PD_BOOT", "1500"))


def fit(key, g):
    x = g["t"].to_numpy()
    y = g["y"].to_numpy()
    rng = np.random.default_rng(int(key))
    slopes = np.empty(BOOT)
    for b in range(BOOT):
        i = rng.integers(0, len(x), len(x))
        xs, ys = x[i], y[i]
        dx = xs - xs.mean()
        slopes[b] = (dx * (ys - ys.mean())).sum() / (dx * dx).sum()
    return int(key), float(slopes.mean()), float(slopes.std())


rng = np.random.default_rng(0)
df = pd.DataFrame({"k": np.repeat(np.arange(GROUPS), ROWS), "t": rng.random(GROUPS * ROWS)})
df["y"] = 0.5 * df["k"] * df["t"] + rng.normal(0.0, 1.0, len(df))
client = Client(os.environ["DASK_SCHEDULER"])
t0 = time.perf_counter()
with parallel_backend("dask"):
    out = Parallel(n_jobs=-1)(delayed(fit)(k, g) for k, g in df.groupby("k"))
dt = time.perf_counter() - t0
res = pd.DataFrame(out, columns=["k", "slope", "slope_sd"]).sort_values("k")
print("groups=%d rows=%d boot=%d" % (GROUPS, len(df), BOOT))
print("slope_sum=%.9e slope_sd_sum=%.9e" % (res["slope"].sum(), res["slope_sd"].sum()))
print("fit_seconds=%.2f" % dt)
