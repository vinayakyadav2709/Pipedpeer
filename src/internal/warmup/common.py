"""The packages a script most likely imports, in one environment.

Never run for its own sake: `pipedpeer connect` builds its environment in the
background (`pipedpeer run common.py --check-only`), so the nix store already
holds these packages and a new script that imports any of them only builds a
small wrapper.
"""

import time

import matplotlib
import numpy
import pandas
import scipy
import seaborn
import sklearn
import torch
import tqdm

t0 = time.monotonic()
for mod in (numpy, pandas, scipy, sklearn, matplotlib, seaborn, tqdm, torch):
    print(f"{mod.__name__:12s} {mod.__version__}")
print(f"TOTAL {time.monotonic() - t0:.1f}s")
