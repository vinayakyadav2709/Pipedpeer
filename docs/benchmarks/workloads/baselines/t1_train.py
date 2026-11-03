import os, time
# A torch.distributed script as far as `pipedpeer run` can tell: the auto arm
# runs it without --ddp, so the CLI rings it on its own (not a fixed ring).
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

MODEL = os.environ.get("BENCH_MODEL", "gpu")
SEED = int(os.environ.get("BENCH_SEED", "1"))
dev = os.environ.get("BENCH_DEVICE", "cpu")
torch.set_num_threads(int(os.environ.get("BENCH_THREADS", "2")))
g = torch.Generator().manual_seed(11)  # the data is the same for every seed


def mlp(d, w):
    return nn.Sequential(nn.Linear(d, w), nn.ReLU(), nn.Linear(w, w), nn.ReLU(),
                         nn.Linear(w, w), nn.ReLU(), nn.Linear(w, 1))


class Tfm(nn.Module):
    def __init__(self, vocab=16, d=64, seq=32, classes=3):
        super().__init__()
        self.emb = nn.Embedding(vocab, d)
        self.pos = nn.Parameter(torch.randn(seq, d) * 0.02)
        layer = nn.TransformerEncoderLayer(d, 4, 128, dropout=0.0, batch_first=True)
        self.enc = nn.TransformerEncoder(layer, 2, enable_nested_tensor=False)
        self.head = nn.Linear(d, classes)

    def forward(self, x):
        return self.head(self.enc(self.emb(x) + self.pos).mean(1))


if MODEL == "gpu":  # pipedpeer-demo/gpu.py
    x = torch.randn(220_000, 64, generator=g)
    y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] * x[:, 3] > 0.5).float().unsqueeze(1)
    ntr, batch, epochs = 200_000, 4096, 3
    torch.manual_seed(SEED)
    model, loss_fn = mlp(64, 512), nn.MSELoss()
    opt = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
elif MODEL == "ddp04":  # pipedpeer-demo/04_torch_ddp.py, CPU sizing
    x = torch.randn(66_000, 64, generator=g)
    y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] > 0.5).float().unsqueeze(1)
    ntr, batch, epochs = 60_000, 2048, 6
    torch.manual_seed(SEED)
    model, loss_fn = mlp(64, 512), nn.MSELoss()
    opt = torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9)
elif MODEL == "tfm":  # majority token among 1..3 (ties: the lowest), 10% labels
    x = torch.randint(0, 16, (44_000, 32), generator=g)  # noisy so the loss stays off 0
    y = torch.stack([(x == t).sum(1) for t in (1, 2, 3)], 1).argmax(1)
    noisy = torch.rand(len(y), generator=g) < 0.1
    y[noisy] = torch.randint(0, 3, (int(noisy.sum()),), generator=g)
    ntr, batch, epochs = 40_000, 256, 2
    torch.manual_seed(SEED)
    model, loss_fn = Tfm(), nn.CrossEntropyLoss()
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3)
else:
    raise SystemExit("unknown BENCH_MODEL %r" % MODEL)
epochs = int(os.environ.get("BENCH_EPOCHS", epochs))
model = model.to(dev)
loader = DataLoader(TensorDataset(x[:ntr], y[:ntr]), batch_size=batch, shuffle=True)
t0, steps = time.monotonic(), 0
for _ in range(epochs):
    for xb, yb in loader:
        xb, yb = xb.to(dev), yb.to(dev)
        opt.zero_grad()
        loss_fn(model(xb), yb).backward()
        opt.step()
        steps += 1
if dev != "cpu":
    torch.cuda.synchronize()
el = time.monotonic() - t0
with torch.no_grad():
    val = loss_fn(model(x[ntr:].to(dev)), y[ntr:].to(dev)).item()
rank = os.environ.get("PIPEDPEER_RANK", "0")
print("BENCH_RANKLOSS %s %.6f" % (rank, val))
if rank == "0":
    print("BENCH_LOSS %.6f" % val)
print("BENCH_WORLD", os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
print("BENCH_STEPS_DONE", steps)
print("BENCH_STEP_S %.5f" % (el / max(steps, 1)))
print("BENCH_ELAPSED %.3f" % el)
