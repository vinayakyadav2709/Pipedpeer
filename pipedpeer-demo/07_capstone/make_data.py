#!/usr/bin/env python3
"""Write data.csv for 07_capstone.py: rows of four readings, a category, and
a target the forest can actually learn. DEMO_CAPSTONE_ROWS sizes it (2M rows
is ~200 MB). Plain numpy + pandas; written under a temporary name so a
Ctrl+C never leaves a half file that looks finished."""
import os

import numpy as np
import pandas as pd

n = int(os.environ.get("DEMO_CAPSTONE_ROWS", "2000000"))
rng = np.random.RandomState(42)
cat = rng.choice(["alpha", "beta", "gamma", "delta"], size=n)
g = rng.rand(4, n)
offset = pd.Series(cat).map({"alpha": 0.0, "beta": 0.2, "gamma": -0.2, "delta": 0.4}).to_numpy()
y = 2 * g[0] + g[1] - g[2] * g[3] + offset + rng.normal(0, 0.1, n)
pd.DataFrame({"id": np.arange(n), "cat": cat, "g1": g[0], "g2": g[1],
              "g3": g[2], "g4": g[3], "y": y.round(5)}).to_csv("data.csv.tmp", index=False)
os.replace("data.csv.tmp", "data.csv")
print(f"wrote data.csv ({n:,} rows)")
