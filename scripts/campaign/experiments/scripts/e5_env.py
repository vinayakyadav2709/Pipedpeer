"""E5's job: import the campaign's pinned set and do a little work, so the
environment is what is being shipped. The extra package of an update run is
named by E5_EXTRA and imported too."""
import importlib
import os
from multiprocessing import Pool

import joblib
import numpy
import pandas
import sklearn
import torch


def work(i):
    return float(numpy.sqrt(i))


if __name__ == "__main__":
    extra = os.environ.get("E5_EXTRA", "")
    if extra:
        importlib.import_module(extra)
    with Pool(4) as p:
        s = sum(p.map(work, range(64)))
    print("E5-ENV-OK numpy=%s pandas=%s sklearn=%s joblib=%s torch=%s extra=%s sum=%.6f" % (
        numpy.__version__, pandas.__version__, sklearn.__version__, joblib.__version__,
        torch.__version__, extra or "-", s))
