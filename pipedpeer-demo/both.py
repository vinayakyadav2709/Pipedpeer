#!/usr/bin/env python3
"""A CPU phase, then a GPU phase, in one ordinary script.

Feature engineering on a Pool (pure Python + numpy, phone-safe), then training a
model on the features with torch. Nothing here mentions pipedpeer.

    python3 both.py
    pipedpeer run both.py       # the Pool spreads over every machine, the
                                # training goes where the GPU is
"""
import multiprocessing
import os
import time

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

CHUNKS, ROWS_PER_CHUNK, RAW_D, SPIN = 64, 4000, 16, 15_000_000
EPOCHS, BATCH, WIDTH = 4, 2048, 512


def features(chunk):
    """Raw rows -> features + label for one chunk; the spin is the costly part."""
    rng = np.random.RandomState(chunk)
    raw = rng.randn(ROWS_PER_CHUNK, RAW_D)
    feats = np.hstack([raw, raw ** 2, np.sin(raw)]).astype(np.float32)
    label = (raw[:, 0] * 2 + raw[:, 1] - raw[:, 2] * raw[:, 3] > 0.5).astype(np.float32)
    total = 0
    for i in range(SPIN):          # stands in for slow per-row Python logic
        total += (i ^ chunk) % 7
    return feats, label, total


def main():
    t0 = time.monotonic()
    print(f"both.py on {os.cpu_count()} local cores")

    t = time.monotonic()
    with multiprocessing.Pool(os.cpu_count()) as pool:
        parts = pool.map(features, range(CHUNKS))
    x = torch.from_numpy(np.vstack([p[0] for p in parts]))
    y = torch.from_numpy(np.concatenate([p[1] for p in parts])).unsqueeze(1)
    print(f"CPU phase: {len(x):,} rows x {x.shape[1]} features, "
          f"checksum {sum(p[2] for p in parts)}, took {time.monotonic() - t:.1f}s")

    torch.manual_seed(11)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print("GPU phase on:", torch.cuda.get_device_name(0) if device == "cuda" else "CPU (no GPU here)")
    split = len(x) * 9 // 10
    model = nn.Sequential(
        nn.Linear(x.shape[1], WIDTH), nn.ReLU(),
        nn.Linear(WIDTH, WIDTH), nn.ReLU(),
        nn.Linear(WIDTH, 1),
    ).to(device)
    opt = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
    loss_fn = nn.MSELoss()
    loader = DataLoader(TensorDataset(x[:split], y[:split]), batch_size=BATCH,
                        shuffle=True, pin_memory=device == "cuda")
    t = time.monotonic()
    for epoch in range(EPOCHS):
        loss = None
        for xb, yb in loader:
            xb, yb = xb.to(device, non_blocking=True), yb.to(device, non_blocking=True)
            opt.zero_grad()
            loss = loss_fn(model(xb), yb)
            loss.backward()
            opt.step()
        if loss is not None:
            print(f"epoch {epoch}: loss {loss.item():.4f}")
    with torch.no_grad():
        val = loss_fn(model(x[split:].to(device)), y[split:].to(device)).item()
    print(f"GPU phase: validation loss {val:.4f}, took {time.monotonic() - t:.1f}s")
    print(f"TOTAL {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
