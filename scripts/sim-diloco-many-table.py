"""MANY-SIM table from sim-diloco-ranks.py lines on stdin
("MODEL SEED ARM SCEN | LOSS ... BYTES/STEP b MB m GAIN g ROUNDS r").
Gap = mean over seeds of the loss against the same seed's one-device run
(single, or single+B<b> for a +B arm). One-sided bar: pass when the mean is
<= +1%; below -5% is flagged "changed" (the training moved, needs a look)."""
import sys, collections, statistics as st

DATA = {"gpu": (200_000, 4096, 3), "ddp04": (60_000, 2048, 6), "tfm": (40_000, 256, 2)}


def speed(m, arm, n, kv):
    """(syncs per script epoch, compute per rank as a share of one device's);
    from the run's own line, or worked out for lines older than those fields."""
    if "SYNCS/EPOCH" in kv:
        return float(kv["SYNCS/EPOCH"]), float(kv["COMPUTE"])
    ntr, batch, ep = DATA[m]
    parts = arm.split("+")
    e = next((float(p[1:]) for p in parts[1:] if p[0] == "E"), 1.0)
    spe = ntr // (batch * n if "K" in parts else batch)
    if parts[0] == "exact":
        syncs = spe * round(ep * e)
    else:
        w = next((int(p[1:]) for p in parts[1:] if p[0] == "W"), 10)
        syncs = min(w, spe * ep) + int(kv.get("ROUNDS", 0))
    return syncs / ep, e / n

single, runs = {}, collections.defaultdict(dict)
for line in sys.stdin:
    head, _, res = line.partition(" | ")
    f = res.split()
    if len(head.split()) != 4 or not f or f[0] != "LOSS":
        print("skip:", line.strip()[:100], file=sys.stderr)
        continue
    m, seed, arm, scen = head.split()
    b = "".join("+" + p for p in arm.split("+")[1:] if p[0] == "B")
    if arm.startswith("single"):
        single[(m + b, seed)] = float(f[1])
    else:
        kv = dict(zip(f[::2], f[1::2]))
        kv["ROUNDS"] = f[-1]
        sy, cp = speed(m, arm, int(scen[1:]), kv)
        runs[(arm, int(scen[1:]))][(m, seed)] = (float(f[1]), float(kv.get("MB", "nan")), sy, b, cp)

models = ("gpu", "ddp04", "tfm")
print("| arm | ranks | " + " | ".join(models) +
      " | all pass | MB one rank sends | syncs per epoch | compute per rank |")
print("|---|---|" + "---|" * len(models) + "---|---|---|---|")
for (arm, n), d in sorted(runs.items(), key=lambda k: (k[0][0], k[0][1])):
    cells, mbs, sys_, verdict = [], [], [], True
    for m in models:
        gaps = [100 * (l / single[(m + b, s)] - 1) for (mm, s), (l, _, _, b, _) in d.items()
                if mm == m and (m + b, s) in single]
        mb = [v[1] for (mm, _), v in d.items() if mm == m]
        mbs.append("%.1f" % st.mean(mb) if mb else "-")
        sy = [v[2] for (mm, _), v in d.items() if mm == m]
        sys_.append("%.0f" % st.mean(sy) if sy else "-")
        if len(gaps) < 3:
            cells.append("-" if not gaps else "%+.2f (%d seeds)" % (st.mean(gaps), len(gaps)))
            verdict = False
            continue
        g = st.mean(gaps)
        ok = g <= 1.0
        verdict = verdict and ok
        tag = "**%+.2f**" % g if ok else "%+.2f" % g
        cells.append(tag + " (worst %+.1f)" % max(gaps) + (" changed" if g < -5 else ""))
    cp = "%.2f" % list(d.values())[0][4]
    print("| %s | %d | %s | %s | %s | %s | %s |" % (arm, n, " | ".join(cells),
          "**PASS**" if verdict else "fail", "/".join(mbs), "/".join(sys_), cp))
