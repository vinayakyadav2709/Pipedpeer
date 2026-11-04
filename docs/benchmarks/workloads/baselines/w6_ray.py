#!/usr/bin/env python3
"""Model evaluation: scikit-learn cross_val_score(n_jobs=-1).

Plain scikit-learn, no cluster code. A random forest scored with repeated
stratified k-fold cross-validation (CV_SPLITS folds x CV_REPEATS repeats,
so CV_SPLITS x CV_REPEATS fits); n_jobs=-1 hands the fits to joblib. Each
fit is single-threaded and seeded, so the scores are the same on any
machine. CV_SAMPLES rows and CV_TREES trees set the cost of a fit.
"""
import os
import time

import ray
from joblib import parallel_backend
from ray.util.joblib import register_ray
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RepeatedStratifiedKFold, cross_val_score

n = int(os.environ.get("CV_SAMPLES", "4000"))
X, y = make_classification(n_samples=n, n_features=30, n_informative=10, random_state=0)
cv = RepeatedStratifiedKFold(n_splits=int(os.environ.get("CV_SPLITS", "8")),
                             n_repeats=int(os.environ.get("CV_REPEATS", "8")), random_state=0)
clf = RandomForestClassifier(n_estimators=int(os.environ.get("CV_TREES", "100")), random_state=0, n_jobs=1)
ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
register_ray()
t0 = time.perf_counter()
with parallel_backend("ray", ray_remote_args={"num_cpus": 1}):
    scores = cross_val_score(clf, X, y, cv=cv, n_jobs=-1)
dt = time.perf_counter() - t0
print("fits=%d samples=%d" % (len(scores), n))
print("scores_sum=%.6f" % scores.sum())
print("mean_score=%.6f" % scores.mean())
print("fit_seconds=%.2f" % dt)
