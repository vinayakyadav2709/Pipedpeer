#!/usr/bin/env python3
"""Training with a realistic step size, to test the transport rather than a toy.

The bundled demo syncs 1.5 MiB against 50 ms of compute, so 91% of its wall
clock is gradients and no transport could make distributing it worthwhile.
That ratio is a property of the demo, not of training: a sync costs
parameters, while a step costs parameters x batch. Widening the batch moves
the ratio linearly, and this is what an actual job looks like.

Same model as the demo so the numbers are comparable; the batch is 30x larger.
"""
import os
import time

import torch
import torch.nn as nn

torch.manual_seed(11)
device = "cuda" if torch.cuda.is_available() else "cpu"
print("using", device)

n, d = 122880, 512
X = torch.randn(n, d, device=device)
y = (X[:, 0] * 2 + X[:, 1] - X[:, 2] > 0.5).float().unsqueeze(1)

# Held back before sharding so every rank scores the same data. Without this
# each rank evaluates its own slice, the reported losses differ, and there is
# no way to tell that from the ranks' models having actually diverged.
X_eval, y_eval = X[:2000].clone(), y[:2000].clone()

rank = int(os.environ.get("PIPEDPEER_RANK", 0))
world = int(os.environ.get("PIPEDPEER_WORLD_SIZE", 1))
if world > 1:
    X, y = X[rank::world], y[rank::world]
    print("rank %d/%d has %d samples" % (rank, world, X.shape[0]))


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

BATCH, EPOCHS = 61440 // world, 30
steps_per_epoch = X.shape[0] // BATCH
print("training ...", steps_per_epoch * EPOCHS, "steps of", BATCH)
t0 = time.monotonic()
for epoch in range(EPOCHS):
    for step in range(steps_per_epoch):
        xb = X[step * BATCH:(step + 1) * BATCH]
        yb = y[step * BATCH:(step + 1) * BATCH]
        opt.zero_grad()
        loss = loss_fn(model(xb), yb)
        loss.backward()
        opt.step()
print(f"training done in {time.monotonic() - t0:.1f}s")
print("final loss:", loss_fn(model(X_eval), y_eval).item())
# A checksum of the weights: ranks in exact DDP hold identical models, and a
# difference here is divergence rather than a different evaluation set.
print("weight checksum: %.9f" % sum(float(p.detach().double().sum())
                                    for p in model.parameters()))
