#!/usr/bin/env python3
"""Distributed MLP training loop, sized to keep one GPU busy for ~15 s.

Plain PyTorch. No distributed.init_process_group, no DistributedDataParallel
import — `pipedpeer run --ddp 3` makes this transparently a 3-rank DDP run
(gradients averaged after every Optimizer.step via the shim hook).

The data comes through a DataLoader, and that is what lets pipedpeer split it:
the shim swaps the loader's sampler so each rank trains on its own share. It
is also what lets the ring change shape mid-run — a rank whose machine dies
hands its share to the others at the next epoch, and a machine that comes
back joins at the next epoch with the ring's model. A script that slices its
own tensors gets neither.

Optional argument: the number of epochs (default 3). Use more to have time
to unplug a machine and plug it back in: `pipedpeer run --ddp 2 04_torch_ddp.py 30`.

Size: on a GPU, 600k samples through a 2048-wide MLP in batches of 8192 -
about 4.5 s an epoch on an RTX 3060 laptop GPU at ~100% utilisation, in
about 650 MB of VRAM. Without CUDA the same run would take minutes, so the
CPU run is scaled down (60k samples, 512 wide) to finish in seconds.
"""
import sys
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

_T0 = time.monotonic()

torch.manual_seed(11)

device = "cuda" if torch.cuda.is_available() else "cpu"
if device == "cuda":
    print("using GPU:", torch.cuda.get_device_name(0))
else:
    print("using CPU")

# Wide layers and big batches give the GPU real work per step, while the
# gradients each DDP step syncs stay ~34 MB.
if device == "cuda":
    n, d, width, BATCH = 600_000, 64, 2048, 8192
else:
    n, d, width, BATCH = 60_000, 64, 512, 2048
X = torch.randn(n, d)
y = (X[:, 0] * 2 + X[:, 1] - X[:, 2] > 0.5).float().unsqueeze(1)


class MLP(nn.Module):
    def __init__(self):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d, width),
            nn.ReLU(),
            nn.Linear(width, width),
            nn.ReLU(),
            nn.Linear(width, width),
            nn.ReLU(),
            nn.Linear(width, 1),
        )

    def forward(self, x):
        return self.net(x)


model = MLP().to(device)
opt = torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9)
loss_fn = nn.MSELoss()

EPOCHS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
# Pinned batches copy to the GPU while it is still busy with the last step.
loader = DataLoader(TensorDataset(X, y), batch_size=BATCH, shuffle=True,
                    pin_memory=device == "cuda")

print("training ...")
t0 = time.monotonic()
for epoch in range(EPOCHS):
    steps = 0
    loss = None
    for xb, yb in loader:
        xb, yb = xb.to(device, non_blocking=True), yb.to(device, non_blocking=True)
        opt.zero_grad()
        loss = loss_fn(model(xb), yb)
        loss.backward()
        opt.step()
        steps += 1
    # None: an epoch this rank skipped because it joined the ring later.
    if loss is not None:
        print(f"epoch {epoch}: {steps} steps, loss {loss.item():.4f}")
print(f"training done in {time.monotonic() - t0:.1f}s")
with torch.no_grad():
    print("final loss:", loss_fn(model(X[:1000].to(device)), y[:1000].to(device)).item())

print(f"TOTAL {time.monotonic() - _T0:.1f}s")
