"""In-process N-rank DiLoCo simulation (DILOCO-MANY, PHONE-DDP), using the
shim's own schedule helpers: docs/benchmarks/diloco.md "Ranks". Plain outer
step by default, epoch cap on, exact float averaging weighted by samples.

usage: sim-diloco-ranks.py MODEL SEED ARM SCEN SHIMDIR
  MODEL: gpu | ddp04 | tfm
  ARM: single | eq | exact | timed | timedX.Y, then +H<h> (fixed interval),
       +W<n> (warm-up steps), +L<lr> (outer lr), +B<batch> (global batch)
       e.g. eq+H16+W10
       MANY-SIM (docs/benchmarks/diloco.md "Many ranks"):
       exact = every step synced; +C<codec> compresses it with error
       feedback: int8 | psgd<r> (PowerSGD rank r) | topk<density>;
       +A AdaScale lr gain, +Xlin / +Xsqrt lr x N / x sqrt(N) ramped over
       the first 10% of steps (both change the lr); +E<f> trains f x the
       epochs; +K keeps the
       per-rank batch (global = N x batch); +M<m> outer Nesterov momentum;
       +P<mu> FedProx pull; +S SCAFFOLD; +T<density> SparseLoCo top-k + EF
       on the pseudo-gradient. Also prints BYTES/STEP and MB one rank sends
       (int8 wire: 1 B a value; sparse: 8 B a kept value), SYNCS/EPOCH (per
       epoch of the script) and COMPUTE (samples one rank trains / one
       device's samples: 1/N for a plain split).
  SCEN: N<n> = n equal ranks, equal shares (DILOCO-MANY)
        A = laptop + phone, equal shares, speeds 1/0.39; Ar = A refit
        B = laptop + 10 phones, shares by speed, phones 3x slow w.p. 0.1
  SHIMDIR: src/internal/nixgen/shim
Prints LOSS (validation), modelled TIME, DROPPED %, ROUNDS."""
import sys, math, random
sys.path.insert(0, sys.argv[5])
import torch, torch.nn as nn
import types
from pipedpeer_shim import ddp_diloco, ddp_sampler, ddp_wire   # the helpers since SPLIT-DDP
ddp = types.SimpleNamespace(**{**vars(ddp_sampler), **vars(ddp_diloco)})

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

epochs0 = epochs
for part in ARM.split("+")[1:]:
    if part[0] == "B": batch = int(part[1:])   # DILOCO-MANY: per-rank batch isolation
    if part == "K": batch *= int(SCEN[1:])     # MANY-SIM (1): per-rank batch kept, global grows
    if part[0] == "E": epochs = round(epochs0 * float(part[1:]))   # epochs x f
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
OMOM, PROX, SCAF, SPARSE, CODEC, ADA, XS = 0.0, 0.0, False, 0.0, None, False, None
for part in ARM.split("+")[1:]:
    if part[0] == "H": FIXED = int(part[1:])
    if part[0] == "W": WARM = int(part[1:])
    if part[0] == "L": OLR = float(part[1:])
    if part[0] == "M": OMOM = float(part[1:])     # outer Nesterov momentum
    if part[0] == "P": PROX = float(part[1:])     # FedProx mu
    if part == "S": SCAF = True                   # SCAFFOLD control variates
    if part[0] == "T": SPARSE = float(part[1:])   # SparseLoCo: top-k density + EF on deltas
    if part[0] == "C": CODEC = part[1:]           # exact-sync codec: int8 | psgd<r> | topk<d>
    if part == "A": ADA = True                    # AdaScale lr gain on exact steps
    if part[0] == "X": XS = part[1:]              # lr scaling rule on exact steps: lin | sqrt
base = ARM.split("+")[0]
if base == "exact":                               # MANY-SIM: every step synced
    base, WARM = "eq", 10 ** 9
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
clock, trained, offered, rounds, syncs = 0.0, 0, 0, 0, 0
J = len(params[0])
sent_bytes, qs, obuf = 0, {}, None
ef_x = [[None] * J for _ in range(R)]   # exact-sync codec error feedback
ef_d = [[None] * J for _ in range(R)]   # SparseLoCo delta error feedback
cvar = [torch.zeros_like(p) for p in params[0]]                   # SCAFFOLD c
cvar_r = [[torch.zeros_like(p) for p in params[0]] for _ in range(R)]   # c_i
ada_v, ada_m, ada_gain = None, None, []
base_lrs = [[gr["lr"] for gr in o.param_groups] for _, o in ranks]


def wavg(ts, ws):
    tot = sum(ws)
    return sum(t * (w / tot) for t, w in zip(ts, ws))


# --- MANY-SIM: codecs with error feedback, and the bytes each rank sends ---
def int8_bytes(ts):   # the shim's wire: 1 B a value + a float32 scale per block
    return sum(t.numel() for t in ts) + 4 * len(ddp_wire._int8_blocks([t.numel() for t in ts]))


def topk_keep(t, d):  # (sparse tensor, values kept); a kept value costs 4 B + a 4 B index
    k = max(1, int(d * t.numel()))
    flat = t.flatten()
    out = torch.zeros_like(flat)
    i = flat.abs().topk(k).indices
    out[i] = flat[i]
    return out.view_as(t), k


def ef_mean(rows, ws, codec, err, qs):
    """Each rank's tensors through codec with error feedback (err[r][j]
    carries what was not sent); returns (weighted mean, bytes one rank sent).
    psgd<r> is PowerSGD (Vogels et al. 2019): P = MQ averaged and
    orthonormalised, Q = M^T P averaged, warm-started Q per tensor in qs."""
    R_, J = len(rows), len(rows[0])
    for r in range(R_):
        for j in range(J):
            if err[r][j] is None: err[r][j] = torch.zeros_like(rows[r][j])
    ms = [[rows[r][j] + err[r][j] for j in range(J)] for r in range(R_)]
    out, nbytes = [], 0
    for j in range(J):
        col = [ms[r][j] for r in range(R_)]
        if codec == "int8":
            sent = []
            for mtx in col:
                flat = mtx.detach().flatten().numpy()
                _, _, deq = ddp_wire._int8_encode(flat, ddp_wire._int8_blocks([flat.size]))
                sent.append(torch.from_numpy(deq.copy()).view_as(mtx))
            nbytes += int8_bytes([col[0]])
        elif codec.startswith("topk"):
            sent = []
            for mtx in col:
                s, k = topk_keep(mtx, float(codec[4:]))
                sent.append(s)
            nbytes += 8 * k
        elif codec.startswith("psgd") and col[0].dim() >= 2:
            shp = col[0].shape
            mats = [mtx.reshape(shp[0], -1) for mtx in col]
            rk = min(int(codec[4:]), *mats[0].shape)
            if j not in qs:
                qs[j] = torch.randn(mats[0].shape[1], rk, generator=torch.Generator().manual_seed(j))
            p = torch.linalg.qr(wavg([mm @ qs[j] for mm in mats], ws)).Q
            qs[j] = wavg([mm.T @ p for mm in mats], ws)
            approx = (p @ qs[j].T).reshape(shp)
            sent = [approx] * R_
            nbytes += 4 * rk * sum(mats[0].shape)
        else:   # psgd leaves vectors uncompressed, as the paper does
            sent = col
            nbytes += 4 * col[0].numel()
        for r in range(R_):
            err[r][j] = col[r] - sent[r]
        out.append(wavg(sent, ws))
    return out, nbytes


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
            if ADA:   # AdaScale (Johnson et al. 2020): gain from the ranks' gradient spread
                fl = [torch.cat([t.flatten() for t in gr]) for gr in grads]
                var = sum(((f - mflat) ** 2).sum() for f in fl).item() / (R - 1)
                mu2 = max(mflat.norm().item() ** 2 - var / R, 0.0)
                ada_v = var if ada_v is None else 0.9 * ada_v + 0.1 * var
                ada_m = mu2 if ada_m is None else 0.9 * ada_m + 0.1 * mu2
                gain = min(max((ada_v + ada_m) / (ada_v / R + ada_m + 1e-12), 1.0), R)
                ada_gain.append(gain)
                for (_, o), lrs in zip(ranks, base_lrs):
                    for gr, lr0 in zip(o.param_groups, lrs): gr["lr"] = lr0 * gain
            if XS:    # lr x N (lin) or x sqrt(N), ramped over the first 10% of steps (Goyal et al. 2017)
                f = R if XS == "lin" else math.sqrt(R)
                gain = 1 + (f - 1) * min(1.0, (n + 1) / max(1.0, 0.1 * epochs * spe))
                ada_gain.append(gain)
                for (_, o), lrs in zip(ranks, base_lrs):
                    for gr, lr0 in zip(o.param_groups, lrs): gr["lr"] = lr0 * gain
            if CODEC:
                mean, nb = ef_mean(grads, bs, CODEC, ef_x, qs)
                sent_bytes += nb
            else:
                sent_bytes += int8_bytes(params[0])
            for ps in params:
                for p, mm in zip(ps, mean): p.grad.copy_(mm)
            for _, o in ranks: o.step()
            clock += max(nominal)
            syncs += 1
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
        gsum = [[torch.zeros_like(p) for p in params[0]] for _ in range(R)] if SCAF else None
        for i in range(h):
            idx = perm[(s + i) * batch:(s + i + 1) * batch]
            cuts = [0]
            for b in bs: cuts.append(cuts[-1] + b)
            for r, (m, o) in enumerate(ranks):
                if i < ks[r]:
                    o.zero_grad(); lf(m(x[idx[cuts[r]:cuts[r + 1]]]), y[idx[cuts[r]:cuts[r + 1]]]).backward()
                    for j, p in enumerate(params[r]):
                        if SCAF:   # option II for plain SGD = mean raw local gradient
                            gsum[r][j] += p.grad
                            p.grad += cvar[j] - cvar_r[r][j]
                        if PROX:
                            p.grad += PROX * (p.detach() - theta[j])
                    o.step()
                    trained += bs[r]
            offered += batch
        samples = [ks[r] * bs[r] for r in range(R)]
        pace = {r: 1000 * ks[r] * t[r] / h for r in range(R)}
        deltas = [[th - p.detach() for th, p in zip(theta, ps)] for ps in params]
        mean = [wavg(tt, samples) for tt in zip(*deltas)]
        mflat = torch.cat([tt.flatten() for tt in mean])
        drift = ((sum(w * ((torch.cat([tt.flatten() for tt in d]) - mflat) ** 2).sum()
                      for d, w in zip(deltas, samples)) / sum(samples)).sqrt() / mflat.norm()).item()
        if SPARSE:   # SparseLoCo (Sarfi et al. 2025): top-k + EF on the pseudo-gradient
            mean, nb = ef_mean(deltas, samples, "topk%g" % SPARSE, ef_d, qs)
        else:
            nb = int8_bytes(params[0])
        if SCAF:
            cvar_r = [[g / max(k, 1) for g in gs] for gs, k in zip(gsum, ks)]
            cvar = [wavg(tt, samples) for tt in zip(*cvar_r)]
            nb += int8_bytes(params[0])
        sent_bytes += nb
        if OMOM:     # outer Nesterov, as the shim's _outer_nesterov
            obuf = [d.clone() for d in mean] if obuf is None else [OMOM * b + d for b, d in zip(obuf, mean)]
            mean = [d + OMOM * b for d, b in zip(mean, obuf)]
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
                        sent_bytes += int8_bytes([avg])
        n += h; s += h; rounds += 1; syncs += 1
        H = FIXED or ddp._diloco_next_interval(H, drift, ref, n, None)
        H = min(H, spe - s) if spe - s > 0 else H
        nxt = n + H
with torch.no_grad():
    ls = [lf(m(x[ntr:]), y[ntr:]).item() for m, _ in ranks[:1]]
print("LOSS", ls[0], "TIME %.1f" % clock, "DROPPED %.2f%%" % (100 * (1 - trained / offered)),
      "BYTES/STEP %d" % (sent_bytes / max(n, 1)), "MB %.2f" % (sent_bytes / 1e6),
      "GAIN %.2f" % (sum(ada_gain) / len(ada_gain) if ada_gain else 1.0),
      "SYNCS/EPOCH %.1f" % (syncs / epochs0), "COMPUTE %.3f" % (n * bs[0] / (epochs0 * ntr)),
      "ROUNDS", rounds)
