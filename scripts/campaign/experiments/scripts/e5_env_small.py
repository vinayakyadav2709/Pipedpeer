"""E5's job for the container rehearsal: the same shape as e5_env.py with a
set small enough for 2 GB containers (numpy, joblib)."""
import importlib
import os
from multiprocessing import Pool

import joblib
import numpy


def work(i):
    return float(numpy.sqrt(i))


if __name__ == "__main__":
    extra = os.environ.get("E5_EXTRA", "")
    if extra:
        importlib.import_module(extra)
    with Pool(2) as p:
        s = sum(p.map(work, range(16)))
    print("E5-ENV-OK numpy=%s joblib=%s extra=%s sum=%.6f" % (
        numpy.__version__, joblib.__version__, extra or "-", s))
