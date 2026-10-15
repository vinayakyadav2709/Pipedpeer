#!/usr/bin/env python3
"""GPU training, written the ordinary way: one model, one DataLoader, one loop.

Nothing here mentions pipedpeer. The model is the same on a GPU and on a CPU
(only the device changes), so every machine trains the same network.

    python3 gpu.py                          # this machine
    pipedpeer run gpu.py                    # pipedpeer picks one GPU or a split
    pipedpeer run --ddp 2 gpu.py            # force a 2-machine split, to compare
    pipedpeer run --ddp 2 --ddp-sync fast gpu.py   # split, syncing less often

Sizing: each step is a few milliseconds on a laptop GPU while its ~2.4 MB of
gradients take far longer to cross Wi-Fi, so the automatic choice on two Wi-Fi
laptops should be one GPU. `--ddp 2` shows why. Validation loss is printed at
the end so runs compare on quality, not only on time.

    --epochs N   passes over the data (default 3)
"""
import argparse
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

N_TRAIN, N_VAL, D, WIDTH, BATCH = 200_000, 20_000, 64, 512, 4096


def make_data():
    g = torch.Generator().manual_seed(11)
    x = torch.randn(N_TRAIN + N_VAL, D, generator=g)
    y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] * x[:, 3] > 0.5).float().unsqueeze(1)
    return x[:N_TRAIN], y[:N_TRAIN], x[N_TRAIN:], y[N_TRAIN:]


def make_model():
    return nn.Sequential(
        nn.Linear(D, WIDTH), nn.ReLU(),
        nn.Linear(WIDTH, WIDTH), nn.ReLU(),
        nn.Linear(WIDTH, WIDTH), nn.ReLU(),
        nn.Linear(WIDTH, 1),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--epochs", type=int, default=3)
    args = parser.parse_args()
    t0 = time.monotonic()
    torch.manual_seed(11)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print("using GPU:", torch.cuda.get_device_name(0) if device == "cuda" else "none (CPU)")

    x, y, xv, yv = make_data()
    model = make_model().to(device)
    opt = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
    loss_fn = nn.MSELoss()
    loader = DataLoader(TensorDataset(x, y), batch_size=BATCH, shuffle=True,
                        pin_memory=device == "cuda")

    t_train = time.monotonic()
    for epoch in range(args.epochs):
        steps, loss = 0, None
        for xb, yb in loader:
            xb, yb = xb.to(device, non_blocking=True), yb.to(device, non_blocking=True)
            opt.zero_grad()
            loss = loss_fn(model(xb), yb)
            loss.backward()
            opt.step()
            steps += 1
        # None: an epoch this rank skipped because it joined the split later.
        if loss is not None:
            print(f"epoch {epoch}: {steps} steps, loss {loss.item():.4f}")
    print(f"training took {time.monotonic() - t_train:.1f}s")

    with torch.no_grad():
        val = loss_fn(model(xv.to(device)), yv.to(device)).item()
    print(f"validation loss {val:.4f}")
    print(f"TOTAL {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
