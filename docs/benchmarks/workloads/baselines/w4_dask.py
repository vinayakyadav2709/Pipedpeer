#!/usr/bin/env python3
"""Hyperparameter search: scikit-learn GridSearchCV(n_jobs=-1).

Plain scikit-learn, no cluster code. 64 candidate settings of a gradient
boosting classifier x 4 folds = 256 fits on a synthetic dataset; n_jobs=-1
hands the fits to joblib. GS_SAMPLES sets the dataset's rows, and so the
cost of each fit (GS_TREES, default 100, the trees per fit: the smoke size
lowers it). Every fit is single-threaded and seeded, so the best
parameters and score are the same on any machine.
"""
import os
import time

from dask.distributed import Client
from joblib import parallel_backend
from sklearn.datasets import make_classification
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import GridSearchCV, StratifiedKFold

n = int(os.environ.get("GS_SAMPLES", "2000"))
X, y = make_classification(n_samples=n, n_features=20, n_informative=8, random_state=0)
grid = {
    "max_depth": [2, 3, 4, 5],
    "learning_rate": [0.03, 0.1, 0.3, 1.0],
    "subsample": [0.7, 1.0],
    "max_features": [0.5, 1.0],
}
trees = int(os.environ.get("GS_TREES", "100"))
search = GridSearchCV(GradientBoostingClassifier(n_estimators=trees, random_state=0), grid,
                      cv=StratifiedKFold(n_splits=4), n_jobs=-1)
client = Client(os.environ["DASK_SCHEDULER"])
t0 = time.perf_counter()
with parallel_backend("dask"):
    search.fit(X, y)
dt = time.perf_counter() - t0
best = ", ".join("%s=%s" % kv for kv in sorted(search.best_params_.items()))
print("fits=%d samples=%d" % (len(search.cv_results_["params"]) * 4, n))
print("best_params={%s}" % best)
print("best_score=%.6f" % search.best_score_)
print("fit_seconds=%.2f" % dt)
