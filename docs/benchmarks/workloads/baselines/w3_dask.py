#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/w3_rf.py (W3) to Dask Distributed.
# Distributed: the n_jobs=-1 fit, via joblib.parallel_backend("dask").
# Local: data generation, prediction, accuracy, result lines.
# Follows: https://docs.dask.org/en/stable/
# Result line, byte-for-byte: accuracy: <x:.4f>
import os
import time

import numpy as np
from dask.distributed import Client
from joblib import parallel_backend
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

rng = np.random.RandomState(7)
n_samples, n_features = 200_000, 20
X = rng.rand(n_samples, n_features)
y = ((X[:, 0] * 2.0 + X[:, 1] - X[:, 2] * X[:, 3] + rng.normal(0, 0.05, n_samples)) > 0.5).astype(int)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=7)

trees = int(os.environ.get("BENCH_TREES", "150"))
print(f"data: {X.shape}, classes: {np.bincount(y)}")
print(f"training RandomForest(n_estimators={trees}, n_jobs=-1) ...")

client = Client(os.environ["DASK_SCHEDULER"])
t0 = time.monotonic()
with parallel_backend("dask"):
    clf = RandomForestClassifier(n_estimators=trees, max_depth=12, n_jobs=-1, random_state=7)
    clf.fit(X_train, y_train)
t_fit = time.monotonic() - t0

y_pred = clf.predict(X_test)
acc = accuracy_score(y_test, y_pred)
print(f"fit time: {t_fit:.1f}s")
print(f"accuracy: {acc:.4f}")

print(f"TOTAL {time.monotonic() - t0:.1f}s")
