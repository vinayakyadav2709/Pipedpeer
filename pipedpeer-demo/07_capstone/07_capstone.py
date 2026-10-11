#!/usr/bin/env python3
"""Everything at once: one ordinary script, five kinds of work, one command.

    python3 07_capstone.py            # this machine, the way it runs today
    pipedpeer run 07_capstone.py -e PIPEDPEER_OOC_MIN=5e7

Each stage is plain library code that pipedpeer intercepts on its own:

  1. pandas   read a CSV and groupby().mean() it - out-of-core, chunks parsed
              on the cluster and only the per-chunk partials combined here
  2. numpy    engineer features and a target (the cost model decides whether
              the matmul is worth shipping; at this size it is not)
  3. sklearn  RandomForest(n_jobs=-1) - its trees train across the cluster
  4. Pool     bootstrap the test accuracy with multiprocessing.Pool
  5. files    write results/ - they sync back to wherever the command was typed

Nothing in this file mentions pipedpeer. PIPEDPEER_OOC_MIN only forces stage
1 out-of-core on a machine with enough RAM to skip it; plain python3 ignores
it. Each stage prints its own time, and `pipedpeer job --last --timeline` and
`--receipt` show where those seconds and items went.
"""
import json
import multiprocessing
import os
import random
import time
from functools import partial

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

FEATURES = ["g1", "g2", "g3", "g4"]
PAIRS = [(i, j) for i in range(4) for j in range(i + 1, 4)]
TRAIN_ROWS = 120_000
TASKS, PER_TASK = 48, 100


def resampled_accuracies(hits, seed):
    """Accuracy on PER_TASK bootstrap resamples of the test set."""
    rng, n = random.Random(seed), len(hits)
    return [sum(rng.choices(hits, k=n)) / n for _ in range(PER_TASK)]


def main():
    t_all = time.monotonic()
    timings = {}

    def stage(name, t0):
        timings[name] = round(time.monotonic() - t0, 2)
        print(f"  {name}: {timings[name]:.1f}s")

    print("[1/5] pandas: read data.csv and average each category ...")
    t0 = time.monotonic()
    df = pd.read_csv("data.csv")
    by_cat = df.groupby("cat").mean()
    print(f"  {df.shape[0]:,} rows x {df.shape[1]} cols, {len(by_cat)} categories")
    stage("pandas", t0)

    # A sample, not the whole file: the forest needs a training set, not
    # every row. Taking it pulls the chunks back into one frame here.
    print("[2/5] numpy: features and target ...")
    t0 = time.monotonic()
    sample = df.sample(n=min(TRAIN_ROWS, len(df)), random_state=7)
    X = sample[FEATURES].to_numpy()
    X = np.hstack([X, np.stack([X[:, i] * X[:, j] for i, j in PAIRS], axis=1)])
    gram = np.matmul(X.T, X) / len(X)
    y = (sample["y"].to_numpy() > np.median(sample["y"])).astype(int)
    print(f"  X {X.shape}, gram trace {np.trace(gram):.3f}, positives {y.mean():.2f}")
    stage("numpy", t0)

    print("[3/5] sklearn: RandomForest(n_estimators=80, n_jobs=-1) ...")
    t0 = time.monotonic()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=7)
    clf = RandomForestClassifier(n_estimators=80, max_depth=10, n_jobs=-1, random_state=7)
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)
    acc = float((pred == y_test).mean())
    print(f"  accuracy {acc:.4f}")
    stage("sklearn", t0)

    # How sure is that accuracy? Resample the test set and see how far it moves.
    print(f"[4/5] Pool: bootstrap the accuracy ({TASKS} tasks x {PER_TASK} resamples) ...")
    t0 = time.monotonic()
    hits = (pred == y_test).tolist()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        parts = pool.map(partial(resampled_accuracies, hits), range(TASKS))
    boot = sorted(a for part in parts for a in part)
    lo, hi = boot[int(0.025 * len(boot))], boot[int(0.975 * len(boot)) - 1]
    print(f"  95% of resamples between {lo:.4f} and {hi:.4f}")
    stage("pool", t0)

    print("[5/5] files: write results/ ...")
    t0 = time.monotonic()
    os.makedirs("results", exist_ok=True)
    by_cat.to_csv("results/by_category.csv")
    names = FEATURES + [f"{FEATURES[i]}*{FEATURES[j]}" for i, j in PAIRS]
    (pd.Series(clf.feature_importances_, index=names, name="importance")
     .sort_values(ascending=False).to_csv("results/feature_importance.csv"))
    stage("files", t0)
    with open("results/summary.json", "w") as f:
        json.dump({"rows": int(df.shape[0]), "accuracy": acc, "accuracy_95": [lo, hi],
                   "stages_s": timings}, f, indent=2)
    print("  wrote results/by_category.csv, feature_importance.csv, summary.json")

    print(f"TOTAL {time.monotonic() - t_all:.1f}s")


if __name__ == "__main__":
    main()
