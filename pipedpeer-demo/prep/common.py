#!/usr/bin/env python3
"""The packages a teacher's script most likely imports, in one environment.

Never run for its own sake: demo/prep-4-common.sh builds its environment with
'pipedpeer run prep/common.py --check-only' on every machine, so the nix store
already holds these packages and a new script that imports any of them only
builds a small wrapper. Importing torch makes pipedpeer pick the CUDA build,
the same one any torch script gets, so the paths match.
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
