#!/usr/bin/env python3
"""Random Forest benchmark on synthetic data.

Plain scikit-learn. No pipedpeer imports, no SDK, no Pool wrappers —
n_jobs=-1 is routed transparently through the cluster pool.
"""
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

print(f"data: {X.shape}, classes: {np.bincount(y)}")
print("training RandomForest(n_estimators=150, n_jobs=-1) ...")
t0 = time.monotonic()
clf = RandomForestClassifier(n_estimators=150, max_depth=12, n_jobs=-1, random_state=7)
clf.fit(X_train, y_train)
t_fit = time.monotonic() - t0

y_pred = clf.predict(X_test)
acc = accuracy_score(y_test, y_pred)
print(f"fit time: {t_fit:.1f}s")
print(f"accuracy: {acc:.4f}")

print(f"TOTAL {time.monotonic() - _T0:.1f}s")
