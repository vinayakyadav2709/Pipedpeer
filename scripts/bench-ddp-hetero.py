"""Training through a DataLoader, so measured shares can actually be applied.

A script that indexes its own tensors (`X[rank::world]`) takes an equal slice
whatever placement measured — there is no sampler to swap. This one uses a
DataLoader, which is the case the shim can reshard: each rank gets a slice
sized by its share and a batch scaled to match, so every rank still runs the
same number of steps and none waits on a slower one.

Prints one line per rank naming how much data it took, which is the receipt
the benchmark checks.
"""
import os
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

torch.manual_seed(11)
device = "cuda" if torch.cuda.is_available() else "cpu"

rank = int(os.environ.get("PIPEDPEER_RANK", 0))
world = int(os.environ.get("PIPEDPEER_WORLD_SIZE", 1))

n, d = 60_000, 512
X = torch.randn(n, d)
y = (X[:, 0] * 2 + X[:, 1] - X[:, 2] > 0.5).float().unsqueeze(1)

# No manual sharding: the loader is what the shim reshards, and doing it by
# hand here would take the equal slice before the shim ever saw the dataset.
loader = DataLoader(TensorDataset(X, y), batch_size=512, shuffle=False)


class MLP(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d, 512), nn.ReLU(),
            nn.Linear(512, 512), nn.ReLU(),
            nn.Linear(512, 1),
        )

    def forward(self, x):
        return self.net(x)


model = MLP().to(device)
opt = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
loss_fn = nn.MSELoss()

EPOCHS = 2
seen = 0
steps = 0
t0 = time.monotonic()
for epoch in range(EPOCHS):
    for xb, yb in loader:
        xb, yb = xb.to(device), yb.to(device)
        opt.zero_grad()
        loss = loss_fn(model(xb), yb)
        loss.backward()
        opt.step()
        seen += xb.shape[0]
        steps += 1
elapsed = time.monotonic() - t0

# The receipt: samples is what this rank actually trained on, and steps must
# match across ranks or the ring would have deadlocked at a barrier.
print(f"RANKSTAT rank={rank} world={world} samples={seen} steps={steps} "
      f"seconds={elapsed:.2f}")
print(f"final loss: {loss.item()}")
