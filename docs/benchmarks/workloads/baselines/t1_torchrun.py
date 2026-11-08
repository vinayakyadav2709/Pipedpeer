#!/usr/bin/env python3
"""T1 MLP under torchrun (campaign copy). Same model as t1_mlp.py but uses
torch.distributed + DistributedDataParallel; rank/world come from torchrun's
RANK/WORLD_SIZE. Only rank 0 prints.
"""
import os
import time

import torch
import torch.distributed as dist
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP

seed = int(os.environ.get("BENCH_SEED", "11"))
epochs = int(os.environ.get("BENCH_EPOCHS", "30"))
torch.manual_seed(seed)
device = "cuda" if torch.cuda.is_available() else "cpu"

rank = int(os.environ.get("RANK", os.environ.get("PIPEDPEER_RANK", "0")))
world = int(os.environ.get("WORLD_SIZE", os.environ.get("PIPEDPEER_WORLD_SIZE", "1")))

if world > 1:
    dist.init_process_group("gloo")
    if rank == 0:
        print("using", device)

n, d = 122880, 512
X = torch.randn(n, d, device=device)
y = (X[:, 0] * 2 + X[:, 1] - X[:, 2] > 0.5).float().unsqueeze(1)

X_eval, y_eval = X[:2000].clone(), y[:2000].clone()

if world > 1:
    X, y = X[rank::world], y[rank::world]
    if rank == 0:
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
if world > 1:
    model = DDP(model)
opt = torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9)
loss_fn = nn.MSELoss()

BATCH = 61440 // world
steps_per_epoch = X.shape[0] // BATCH
if rank == 0:
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
if rank == 0:
    print(f"training done in {time.monotonic() - t0:.1f}s")
    print("final loss:", loss_fn(model.module(X_eval) if hasattr(model, "module") else model(X_eval), y_eval).item())
    print("weight checksum: {:.9f}".format(sum(float(p.detach().double().sum())
                                        for p in (model.module if hasattr(model, "module") else model).parameters())))
if world > 1:
    dist.barrier()
    dist.destroy_process_group()
