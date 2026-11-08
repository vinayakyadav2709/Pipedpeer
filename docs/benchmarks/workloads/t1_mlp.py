#!/usr/bin/env python3
"""Training with a realistic step size (campaign copy).

Same as scripts/bench-train.py, but the seed and epoch count are configurable
via BENCH_SEED (default 11) and BENCH_EPOCHS (default 30), so E8 can run
several seeds and the port smoke can use 1 epoch.
"""
import os
import time

import torch
from torch import nn

seed = int(os.environ.get("BENCH_SEED", "11"))
epochs = int(os.environ.get("BENCH_EPOCHS", "30"))
torch.manual_seed(seed)
device = "cuda" if torch.cuda.is_available() else "cpu"
print("using", device)

n, d = 122880, 512
X = torch.randn(n, d, device=device)
y = (X[:, 0] * 2 + X[:, 1] - X[:, 2] > 0.5).float().unsqueeze(1)

X_eval, y_eval = X[:2000].clone(), y[:2000].clone()

rank = int(os.environ.get("PIPEDPEER_RANK", "0"))
world = int(os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
if world > 1:
    X, y = X[rank::world], y[rank::world]
    print(f"rank {rank}/{world} has {X.shape[0]} samples")


class MLP(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d, 512), nn.ReLU(),
            nn.Linear(512, 512), nn.ReLU(),
            nn.Linear(512, 512), nn.ReLU(),
            nn.Linear(512, 1),
        )

    def forward(self, x):
        return self.net(x)


model = MLP().to(device)
opt = torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9)
loss_fn = nn.MSELoss()

BATCH = 61440 // world
steps_per_epoch = X.shape[0] // BATCH
print("training ...", steps_per_epoch * epochs, "steps of", BATCH)
t0 = time.monotonic()
for epoch in range(epochs):
    for step in range(steps_per_epoch):
        xb = X[step * BATCH:(step + 1) * BATCH]
        yb = y[step * BATCH:(step + 1) * BATCH]
        opt.zero_grad()
        loss = loss_fn(model(xb), yb)
        loss.backward()
        opt.step()
print(f"training done in {time.monotonic() - t0:.1f}s")
print("final loss:", loss_fn(model(X_eval), y_eval).item())
print("weight checksum: {:.9f}".format(sum(float(p.detach().double().sum())
                                    for p in model.parameters())))
