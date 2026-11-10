"""Client-side cost of the pandas groupby and merge hash shuffles.

usage: uv run --no-project --with pandas python bench_shuffle.py [--repeats N]

_pool_send is replaced by a stub that answers each part with an empty
frame, so the timing is the shim's own work (bucketing, pickling the parts,
the local bucket's aggregate, concatenating) with no network or worker.
"""
import argparse
import os
import pickle
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipedpeer_shim import core, pandas_ops  # noqa: E402


def stub_send(header, globals_pickle, items, timeout):
    import pandas as pd
    return [pickle.dumps(pd.DataFrame()) for _ in items]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=7)
    args = ap.parse_args()
    import numpy as np
    import pandas as pd
    core._NUM_SHARDS = "3"
    core._FORCE = False
    pandas_ops._OFFLOAD_HERE = False
    pandas_ops._log = lambda *a, **k: None
    pandas_ops._pool_send = stub_send
    rng = np.random.default_rng(0)
    n = 2_000_000
    df = pd.DataFrame({"k": rng.integers(0, 1000, n), "a": rng.random(n),
                       "b": rng.random(n), "c": rng.integers(0, 100, n)})
    right = pd.DataFrame({"k": np.arange(1000), "w": rng.random(1000)})
    gb = df.groupby("k")
    cases = {
        "groupby": lambda: pandas_ops._groupby_shuffle_inner(gb, "sum", (), {}),
        "merge": lambda: pandas_ops._merge_shuffle(df, right, {"on": "k"}),
    }
    for name, fn in cases.items():
        fn()
        times = []
        for _ in range(args.repeats):
            t0 = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t0)
        print("%-8s min %.3fs median %.3fs" % (name, min(times), statistics.median(times)))


if __name__ == "__main__":
    main()
