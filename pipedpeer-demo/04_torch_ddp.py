#!/usr/bin/env python3
"""Tiny distributed MLP training loop.

Plain PyTorch. No distributed.init_process_group, no DistributedDataParallel
import, no rank logic — `pipedpeer run --ddp 3` makes this transparently a
3-rank DDP run (weight sync after every Optimizer.step via the shim hook).
"""
import time

import torch
import torch.nn as nn

torch.manual_seed(11)

device = "cuda" if torch.cuda.is_available() else "cpu"
if device == "cuda":
    print("using GPU:", torch.cuda.get_device_name(0))
else:
    print("using CPU")

n, d = 60_000, 512
X = torch.randn(n, d, device=device)
y = (X[:, 0] * 2 + X[:, 1] - X[:, 2] > 0.5).float().unsqueeze(1)


class MLP(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d, 2048),
            nn.ReLU(),
            nn.Linear(2048, 2048),
            nn.ReLU(),
            nn.Linear(2048, 2048),
            nn.ReLU(),
            nn.Linear(2048, 1),
        )

    def forward(self, x):
        return self.net(x)


model = MLP().to(device)
opt = torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9)
loss_fn = nn.MSELoss()

BATCH, EPOCHS = 1024, 10
steps_per_epoch = n // BATCH
print("training ...")
t0 = time.monotonic()
for epoch in range(EPOCHS):
    for step in range(steps_per_epoch):
        xb = X[step * BATCH:(step + 1) * BATCH]
        yb = y[step * BATCH:(step + 1) * BATCH]
        opt.zero_grad()
        loss = loss_fn(model(xb), yb)
        loss.backward()
        opt.step()
        if step % (steps_per_epoch // 10) == 0:
            print(f"epoch {epoch} step {step:4d} loss {loss.item():.4f}")
print(f"training done in {time.monotonic() - t0:.1f}s")
print("final loss:", loss_fn(model(X[:1000]), y[:1000]).item())