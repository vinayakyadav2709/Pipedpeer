#!/usr/bin/env python3
"""Out-of-core CSV read + per-chunk distributed groupby mean.

Plain pandas reading a ~1.1 GB CSV that barely fits a weak laptop's RAM.
The read streams the file in bounded 64 MB chunks, parses each chunk on a
cluster node (cached content-addressed there), and the groupby mean
re-combines each chunk's tiny partial aggregates at the origin — so no
machine ever holds the whole file. PIPEDPEER_OOC_MIN is the threshold knob
the demo uses to force the out-of-core path.
"""
import time

import pandas as pd

CSV = "data.csv"

t0 = time.monotonic()
df = pd.read_csv(CSV)
t_read = time.monotonic() - t0
print(f"read {CSV}: {df.shape[0]:,} rows x {df.shape[1]} cols in {t_read:.1f}s")

t0 = time.monotonic()
result = df.groupby("cat").mean()
t_agg = time.monotonic() - t0
print(f"groupby('cat').mean(): {t_agg:.1f}s")
print(result.head(10).to_string())