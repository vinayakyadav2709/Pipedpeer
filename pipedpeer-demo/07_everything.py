#!/usr/bin/env python3
"""Every kind of work the earlier demo scripts show, in one ordinary file.

    Pool          a named function over 48 items          (00_pool.py)
    lambda Pool   the same with a lambda                  (06_pool_lambda.py)
    sklearn       RandomForest with n_jobs=-1             (01_sklearn_rf.py)
    numpy         one big np.linalg.svd                   (02_numpy_heavy.py)
    pandas        groupby on a generated 3M-row frame     (03_pandas_ooc)
    torch         a few epochs of a small MLP, tqdm bar   (04_torch_ddp.py)
    files         results/everything.txt comes back       (05_file_sync.py)

Nothing here mentions pipedpeer. Under plain python3 the lambda section fails
(stock multiprocessing cannot pickle a lambda), which is the point of it.

    pipedpeer run 07_everything.py
"""
import multiprocessing
import os
import time

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.ensemble import RandomForestClassifier
from tqdm import tqdm

_T0 = time.monotonic()
TIMES = {}
CHECKS = {}

ITEMS, SPIN = 48, 8_000_000          # Pool
L_ITEMS, L_SPIN = 24, 2_000_000      # lambda Pool
SVD_N = 2304                         # 42 MB matrix, above the 32 MB ship floor
ROWS = 3_000_000                     # pandas frame (~72 MB of columns)


def score(seed):
    total = 0
    for i in range(SPIN):
        total += (i ^ seed) % 7
    return total


lambda_score = lambda seed: sum((i ^ seed) % 7 for i in range(L_SPIN))


def section(name):
    """Print and record how long the block under it took."""
    class _Timer:
        def __enter__(self):
            print(f"--- {name}")
            self.t0 = time.monotonic()

        def __exit__(self, *exc):
            TIMES[name] = time.monotonic() - self.t0
            print(f"section {name} {TIMES[name]:.1f}s")
    return _Timer()


def run_pool():
    with multiprocessing.Pool(os.cpu_count()) as pool:
        results = pool.map(score, range(ITEMS))
    CHECKS["pool"] = sum(results)
    print(f"checksum {CHECKS['pool']}")


def run_lambda_pool():
    with multiprocessing.Pool(os.cpu_count()) as pool:
        results = pool.map(lambda_score, range(L_ITEMS))
    CHECKS["lambda"] = sum(results)
    print(f"checksum {CHECKS['lambda']}")


def run_sklearn():
    rng = np.random.RandomState(7)
    X = rng.rand(40_000, 20)
    y = ((X[:, 0] * 2 + X[:, 1] - X[:, 2] * X[:, 3]) > 0.9).astype(int)
    clf = RandomForestClassifier(n_estimators=60, max_depth=10, n_jobs=-1, random_state=7)
    clf.fit(X[:32_000], y[:32_000])
    CHECKS["sklearn"] = round(clf.score(X[32_000:], y[32_000:]), 4)
    print(f"accuracy {CHECKS['sklearn']}")


def run_numpy():
    A = np.random.RandomState(3).rand(SVD_N, SVD_N)
    _, S, _ = np.linalg.svd(A)
    CHECKS["numpy"] = round(float(S[0]), 4)
    print(f"largest singular value {CHECKS['numpy']}")


def run_pandas():
    rng = np.random.RandomState(5)
    cities = np.array([f"city_{i:02d}" for i in range(50)], dtype=object)
    df = pd.DataFrame({
        "city": cities[rng.randint(0, 50, ROWS)],
        "temp": rng.normal(20, 8, ROWS),
        "rain": rng.exponential(2.0, ROWS),
    })
    out = df.groupby("city").agg({"temp": "mean", "rain": "sum"})
    CHECKS["pandas"] = round(float(out["rain"].sum()), 1)
    print(out.head(3).to_string())


def run_torch():
    torch.manual_seed(11)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print("using GPU:", torch.cuda.get_device_name(0) if device == "cuda" else "none (CPU)")
    n, d, batch, epochs = 40_000, 256, 512, 5
    X = torch.randn(n, d, device=device)
    y = (X[:, 0] * 2 + X[:, 1] - X[:, 2] > 0.5).float().unsqueeze(1)
    model = nn.Sequential(
        nn.Linear(d, 512), nn.ReLU(),
        nn.Linear(512, 512), nn.ReLU(),
        nn.Linear(512, 1),
    ).to(device)
    opt = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
    loss_fn = nn.MSELoss()
    for epoch in range(epochs):
        for step in tqdm(range(n // batch), desc=f"epoch {epoch}", mininterval=0.2):
            xb = X[step * batch:(step + 1) * batch]
            yb = y[step * batch:(step + 1) * batch]
            opt.zero_grad()
            loss = loss_fn(model(xb), yb)
            loss.backward()
            opt.step()
    CHECKS["torch"] = round(loss.item(), 4)
    print(f"final loss {CHECKS['torch']}")


if __name__ == "__main__":
    print(f"everything: {os.cpu_count()} local cores")
    with section("pool"):
        run_pool()
    with section("lambda"):
        run_lambda_pool()
    with section("sklearn"):
        run_sklearn()
    with section("numpy"):
        run_numpy()
    with section("pandas"):
        run_pandas()
    with section("torch"):
        run_torch()

    total = time.monotonic() - _T0
    os.makedirs("results", exist_ok=True)
    with open(os.path.join("results", "everything.txt"), "w") as f:
        f.write(f"written {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        for name, secs in TIMES.items():
            f.write(f"{name:8s} {secs:7.1f}s  check {CHECKS.get(name)}\n")
        f.write(f"TOTAL    {total:7.1f}s\n")
    print("wrote results/everything.txt")
    print(f"TOTAL {total:.1f}s")
