#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/w3_rf.py (W3) for the plain-ssh baseline.
# Each PC trains a subset of the trees (random_state=7+part) and returns its
# predict_proba on the test set; pc00 averages them and scores.
# Prints one JSON line: {"part":i,"trees":t,"proba":[[p0,p1],...],"seconds":s}
import json
import os
import sys
import time

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

rng = np.random.RandomState(7)
n_samples, n_features = 200_000, 20
X = rng.rand(n_samples, n_features)
y = ((X[:, 0] * 2.0 + X[:, 1] - X[:, 2] * X[:, 3] + rng.normal(0, 0.05, n_samples)) > 0.5).astype(int)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=7)

trees = int(os.environ.get("BENCH_TREES", "150"))


def main(argv):
    part = parts = None
    for i, a in enumerate(argv):
        if a == "--part":
            part = int(argv[i + 1])
        elif a == "--parts":
            parts = int(argv[i + 1])
    if part is None or parts is None or not 0 <= part < parts:
        sys.stderr.write("usage: w3_ssh_part.py --part i --parts n\n")
        raise SystemExit(2)
    n_trees = trees // parts + (1 if part < trees % parts else 0)
    t0 = time.perf_counter()
    clf = RandomForestClassifier(n_estimators=n_trees, max_depth=12, n_jobs=-1,
                                 random_state=7 + part)
    clf.fit(X_train, y_train)
    proba = clf.predict_proba(X_test)
    dt = time.perf_counter() - t0
    print(json.dumps({"part": part, "trees": n_trees, "proba": proba.tolist(), "seconds": dt}))


if __name__ == "__main__":
    main(sys.argv[1:])
