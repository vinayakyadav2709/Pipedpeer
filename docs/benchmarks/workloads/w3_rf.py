#!/usr/bin/env python3
"""Random Forest benchmark on synthetic data (campaign copy).

Same as pipedpeer-demo/01_sklearn_rf.py but n_estimators is configurable via
BENCH_TREES (default 150), so E1 (strong, 150) and the port smoke (20) share
one script. Nothing else changed.
"""
import os
import time

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

_T0 = time.monotonic()

rng = np.random.RandomState(7)

n_samples, n_features = 200_000, 20
X = rng.rand(n_samples, n_features)
y = ((X[:, 0] * 2.0 + X[:, 1] - X[:, 2] * X[:, 3] + rng.normal(0, 0.05, n_samples)) > 0.5).astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=7)

trees = int(os.environ.get("BENCH_TREES", "150"))
print(f"data: {X.shape}, classes: {np.bincount(y)}")
print(f"training RandomForest(n_estimators={trees}, n_jobs=-1) ...")
t0 = time.monotonic()
clf = RandomForestClassifier(n_estimators=trees, max_depth=12, n_jobs=-1, random_state=7)
clf.fit(X_train, y_train)
t_fit = time.monotonic() - t0

y_pred = clf.predict(X_test)
acc = accuracy_score(y_test, y_pred)
print(f"fit time: {t_fit:.1f}s")
print(f"accuracy: {acc:.4f}")

print(f"TOTAL {time.monotonic() - _T0:.1f}s")
