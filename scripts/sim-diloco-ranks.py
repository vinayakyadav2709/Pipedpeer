"""In-process N-rank DiLoCo simulation (DILOCO-MANY, PHONE-DDP), using the
shim's own schedule helpers: docs/benchmarks/diloco.md "Ranks". Plain outer
step by default, epoch cap on, exact float averaging weighted by samples.

usage: sim-diloco-ranks.py MODEL SEED ARM SCEN SHIMDIR
  MODEL: gpu | ddp04 | tfm
  ARM: single | eq | timed | timedX.Y, then +H<h> (fixed interval),
       +W<n> (warm-up steps), +L<lr> (outer lr), +B<batch> (global batch)
       e.g. eq+H16+W10
  SCEN: N<n> = n equal ranks, equal shares (DILOCO-MANY)
        A = laptop + phone, equal shares, speeds 1/0.39; Ar = A refit
        B = laptop + 10 phones, shares by speed, phones 3x slow w.p. 0.1
  SHIMDIR: src/internal/nixgen/shim
Prints LOSS (validation), modelled TIME, DROPPED %, ROUNDS."""
import sys, math, random
sys.path.insert(0, sys.argv[5])
import torch, torch.nn as nn
from pipedpeer_shim import ddp

MODEL, SEED, ARM, SCEN = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
torch.set_num_threads(2)
g = torch.Generator().manual_seed(11)


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


def build():
    torch.manual_seed(SEED)
    if MODEL == "gpu":
        m = mlp(64, 512); o = torch.optim.SGD(m.parameters(), lr=0.05, momentum=0.9)
    elif MODEL == "ddp04":
        m = mlp(64, 512); o = torch.optim.SGD(m.parameters(), lr=0.1, momentum=0.9)
    else:
        m = Tfm(); o = torch.optim.AdamW(m.parameters(), lr=1e-3)
    return m, o


if MODEL == "gpu":
    x = torch.randn(220_000, 64, generator=g)
    y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] * x[:, 3] > 0.5).float().unsqueeze(1)
    ntr, batch, epochs, lf = 200_000, 4096, 3, nn.MSELoss()
elif MODEL == "ddp04":
    x = torch.randn(66_000, 64, generator=g)
    y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] > 0.5).float().unsqueeze(1)
    ntr, batch, epochs, lf = 60_000, 2048, 6, nn.MSELoss()
else:
    x = torch.randint(0, 16, (44_000, 32), generator=g)
    y = torch.stack([(x == t).sum(1) for t in (1, 2, 3)], 1).argmax(1)
    noisy = torch.rand(len(y), generator=g) < 0.1
    y[noisy] = torch.randint(0, 3, (int(noisy.sum()),), generator=g)
    ntr, batch, epochs, lf = 40_000, 256, 2, nn.CrossEntropyLoss()

for part in ARM.split("+")[1:]:
    if part[0] == "B": batch = int(part[1:])   # DILOCO-MANY: per-rank batch isolation
torch.manual_seed(SEED + 100)
spe = ntr // batch
if ARM.split("+")[0] == "single":
    m, o = build()
    for e in range(epochs):
        perm = torch.randperm(ntr)
        for s in range(spe):
            idx = perm[s * batch:(s + 1) * batch]
            o.zero_grad(); lf(m(x[idx]), y[idx]).backward(); o.step()
    with torch.no_grad():
        print("LOSS", lf(m(x[ntr:]), y[ntr:]).item())
    sys.exit()

FIXED, WARM, OLR = None, None, 1.0
for part in ARM.split("+")[1:]:
    if part[0] == "H": FIXED = int(part[1:])
    if part[0] == "W": WARM = int(part[1:])
    if part[0] == "L": OLR = float(part[1:])
base = ARM.split("+")[0]
slack = 0.0 if base == "eq" else (1.25 if base == "timed" else float(base[5:]))
if SCEN[0] == "N":   # DILOCO-MANY: N equal ranks, equal shares, no jitter
    speeds = [1.0] * int(SCEN[1:]); shares = [1.0 / len(speeds)] * len(speeds); jit_p = 0.0
elif SCEN in ("A", "Ar"):
    speeds, shares, jit_p = [1.0, 0.39], [0.5, 0.5], 0.0
else:
    speeds = [1.0] + [0.39] * 10
    shares = [s / sum(speeds) for s in speeds]
    jit_p = 0.1
R = len(speeds)
bs = ddp._weighted_batches(shares, batch, R)
rng = random.Random(SEED * 7 + 1)
nominal = [b / batch / sp for b, sp in zip(bs, speeds)]   # step time, laptop full batch = 1

ranks = [build() for _ in range(R)]
params = [[p for gr in o.param_groups for p in gr["params"]] for _, o in ranks]
for ps in params[1:]:
    for p, q in zip(ps, params[0]): p.data.copy_(q.data)
n, H, theta, ref, nxt, outer = 0, 1, None, None, 0, 0
refs, warm = [], WARM or ddp._diloco_warmup(None)
pace = {r: 1000 * t for r, t in enumerate(nominal)}
clock, trained, offered, rounds = 0.0, 0, 0, 0


def wavg(ts, ws):
    tot = sum(ws)
    return sum(t * (w / tot) for t, w in zip(ts, ws))


for e in range(epochs):
    perm = torch.randperm(ntr)
    s = 0
    if SCEN == "Ar" and e == 1:
        # the epoch-boundary refit: shares by measured speed from epoch 2 on
        shares = [sp / sum(speeds) for sp in speeds]
        bs = ddp._weighted_batches(shares, batch, R)
        nominal = [b / batch / sp for b, sp in zip(bs, speeds)]
    while s < spe:
        done = n + 1
        if theta is None:
            idx = perm[s * batch:(s + 1) * batch]
            cuts = [0]
            for b in bs: cuts.append(cuts[-1] + b)
            for r, (m, o) in enumerate(ranks):
                o.zero_grad(); lf(m(x[idx[cuts[r]:cuts[r + 1]]]), y[idx[cuts[r]:cuts[r + 1]]]).backward()
            grads = [[p.grad.clone() for p in ps] for ps in params]
            mean = [wavg(t, bs) for t in zip(*grads)]
            mflat = torch.cat([t.flatten() for t in mean])
            drift = (sum(b * ((torch.cat([t.flatten() for t in gr]) - mflat) ** 2).sum()
                         for gr, b in zip(grads, bs)) / sum(bs)).sqrt() / mflat.norm()
            for ps in params:
                for p, mm in zip(ps, mean): p.grad.copy_(mm)
            for _, o in ranks: o.step()
            clock += max(nominal)
            trained += batch; offered += batch
            if done < warm:
                ddp._push_window(refs, drift.item(), 10)
            else:
                ref = sum(refs) / len(refs)
                H = min(FIXED or ddp._DILOCO_H_START, spe - (s + 1)) or 1
                theta = [p.detach().clone() for p in params[0]]
                nxt = done + H
            n += 1; s += 1
            continue
        # one local round of H steps (H already epoch-capped)
        h = min(nxt - n, spe - s)     # a round never runs past the epoch's end
        nxt = n + h
        jit = [3.0 if (r > 0 and rng.random() < jit_p) else 1.0 for r in range(R)]
        t = [nominal[r] * jit[r] for r in range(R)]
        budget = ddp._round_budget(pace, h, slack) if slack > 0 else None
        ks = []
        for r in range(R):
            k = h if budget is None else min(h, max(1, math.ceil(budget / t[r] - 1e-9)))
            ks.append(k)
        clock += max(k * t[r] for r, k in enumerate(ks))
        for i in range(h):
            idx = perm[(s + i) * batch:(s + i + 1) * batch]
            cuts = [0]
            for b in bs: cuts.append(cuts[-1] + b)
            for r, (m, o) in enumerate(ranks):
                if i < ks[r]:
                    o.zero_grad(); lf(m(x[idx[cuts[r]:cuts[r + 1]]]), y[idx[cuts[r]:cuts[r + 1]]]).backward(); o.step()
                    trained += bs[r]
            offered += batch
        samples = [ks[r] * bs[r] for r in range(R)]
        pace = {r: 1000 * ks[r] * t[r] / h for r in range(R)}
        deltas = [[th - p.detach() for th, p in zip(theta, ps)] for ps in params]
        mean = [wavg(tt, samples) for tt in zip(*deltas)]
        mflat = torch.cat([tt.flatten() for tt in mean])
        drift = ((sum(w * ((torch.cat([tt.flatten() for tt in d]) - mflat) ** 2).sum()
                      for d, w in zip(deltas, samples)) / sum(samples)).sqrt() / mflat.norm()).item()
        theta = [th - OLR * d for th, d in zip(theta, mean)]
        for ps in params:
            for p, th in zip(ps, theta): p.data.copy_(th)
        outer += 1
        if outer % ddp._DILOCO_STATE_EVERY == 0:
            for i in range(len(params[0])):
                sts = [o.state[ps[i]] for (_, o), ps in zip(ranks, params)]
                for k in sts[0]:
                    if torch.is_tensor(sts[0][k]) and sts[0][k].shape == params[0][i].shape:
                        avg = wavg([st[k] for st in sts], samples)
                        for st in sts: st[k].copy_(avg)
        n += h; s += h; rounds += 1
        H = FIXED or ddp._diloco_next_interval(H, drift, ref, n, None)
        H = min(H, spe - s) if spe - s > 0 else H
        nxt = n + H
with torch.no_grad():
    ls = [lf(m(x[ntr:]), y[ntr:]).item() for m, _ in ranks[:1]]
print("LOSS", ls[0], "TIME %.1f" % clock, "DROPPED %.2f%%" % (100 * (1 - trained / offered)),
      "ROUNDS", rounds)
