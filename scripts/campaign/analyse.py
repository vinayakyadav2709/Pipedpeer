#!/usr/bin/env python3
"""analyse.py -- summarize campaign runs.jsonl into CSV tables and figures.

S11: per experiment analysis/<exp>.csv, <exp>-ratios.csv, weak-scaling
<exp>-efficiency.csv, E4/E5/E6/E11 tables from analysis-input/, warnings to
stdout and figures (PNG+PDF) under figures/. Usage: analyse.py [--lab DIR] [--spread].
"""
import argparse
import csv
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
from scipy import stats

REPO = Path(__file__).resolve().parents[2]
EXPS = ("E4", "E5", "E6", "E11")
EXP_COLS = ["experiment", "workload", "system", "n", "size", "repeats",
            "median_s", "q1_s", "q3_s", "speedup", "speedup_lo", "speedup_hi",
            "result_ok_all", "ref"]
EFF_COLS = [c if not c.startswith("speedup")
            else "efficiency" + c[len("speedup"):] for c in EXP_COLS]
RATIO_COLS = ["workload", "n", "system", "vs", "ratio", "lo", "hi", "verdict"]


class Group:
    """Repeats of one (experiment, workload, system, n), warm-ups excluded."""

    def __init__(self):
        self.walls, self.rids, self.oks, self.refs = [], [], [], []
        self.sizes = Counter()

    def add(self, rid, wall, ok, ref, size):
        self.walls.append(wall)
        self.rids.append(rid)
        self.oks.append(ok)
        self.refs.append(ref)
        key = ";".join(f"{k}={v}" for k, v in sorted(size.items()))
        self.sizes[key] += 1

    def size(self):
        return self.sizes and self.sizes.most_common(1)[0][0] or ""


def load_runs(path):
    groups = defaultdict(lambda: defaultdict(
        lambda: defaultdict(lambda: defaultdict(Group))))
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r.get("warmup"):
            continue
        if r.get("wall_s") is None or float(r["wall_s"]) <= 0:
            # timed out or lost: no wall time (older records wrote 0.0), so it
            # cannot enter a median
            print(f"WARN {r.get('run_id') or '?'}: wall_s is null or 0 "
                  f"(exit_code {r.get('exit_code')}); run left out of the tables")
            continue
        g = groups[r["experiment"]][r["workload"]][r["system"]][int(r["n"])]
        g.add(r.get("run_id") or "", float(r["wall_s"]),
              bool(r.get("result_ok")), str(r.get("ref") or ""),
              r.get("size") or {})
    return groups


def _boot_ratio(a, b):
    """median(a)/median(b) with the brief's 95% bootstrap CI; degenerate -> point."""
    a = np.asarray(a, float)
    b = np.asarray(b, float)
    if not len(a) or not len(b) or not np.median(b):
        return float("nan"), float("nan"), float("nan")
    point = float(np.median(a) / np.median(b))
    if len(a) < 2 or len(b) < 2:
        return point, point, point
    res = stats.bootstrap(
        (a, b),
        statistic=lambda x, y, axis=0: np.median(x, axis=axis)
        / np.median(y, axis=axis),
        paired=False, vectorized=True, n_resamples=10000,
        method="percentile", confidence_level=0.95,
        rng=np.random.default_rng(0))
    return point, float(res.confidence_interval.low), \
        float(res.confidence_interval.high)


def _verdict(lo, hi):
    if not (math.isfinite(lo) and math.isfinite(hi)):
        return "unresolved"
    if lo > 1.10:
        return "pipedpeer_faster"
    if hi < 1 / 1.10:
        return "other_faster"
    if lo >= 1 / 1.10 and hi <= 1.10:
        return "same_within_10pct"
    return "unresolved"


def _group_row(exp, w, s, n, g):
    med = float(np.median(g.walls))
    ref = Counter(g.refs).most_common(1)[0][0] if g.refs else "none"
    return {
        "experiment": exp, "workload": w, "system": s, "n": n,
        "size": g.size(), "repeats": len(g.walls), "median_s": med,
        "q1_s": float(np.percentile(g.walls, 25)),
        "q3_s": float(np.percentile(g.walls, 75)),
        "result_ok_all": all(g.oks), "ref": ref,
    }


def exp_speed_rows(groups, exp, speedups=None, eff=False):
    """speedup (strong) or efficiency (weak scaling = t(1)/t(N)) vs own n=1."""
    key = "efficiency" if eff else "speedup"
    rows = []
    for w in sorted(groups.get(exp, {})):
        for s in sorted(groups[exp][w]):
            base = groups[exp][w][s].get(1)
            for n in sorted(groups[exp][w][s]):
                g = groups[exp][w][s][n]
                row = _group_row(exp, w, s, n, g)
                if n != 1 and base and base.walls:
                    pt, lo, hi = _boot_ratio(base.walls, g.walls)
                    row.update({key: pt, key + "_lo": lo, key + "_hi": hi})
                    if speedups is not None and math.isfinite(pt):
                        speedups.setdefault((s, w, n), pt)
                rows.append(row)
    return rows


def exp_ratio_rows(groups, exp):
    """ratio of each system vs pipedpeer at the same (workload, n)."""
    rows = []
    for w in sorted(groups.get(exp, {})):
        if "pipedpeer" not in groups[exp][w]:
            continue
        for n in sorted(groups[exp][w]["pipedpeer"]):
            base = groups[exp][w]["pipedpeer"][n]
            for s in sorted(groups[exp][w]):
                if s == "pipedpeer":
                    continue
                g = groups[exp][w][s].get(n)
                if not g or not g.walls:
                    continue
                pt, lo, hi = _boot_ratio(g.walls, base.walls)
                rows.append({"workload": w, "n": n, "system": s,
                             "vs": "pipedpeer", "ratio": pt, "lo": lo,
                             "hi": hi, "verdict": _verdict(lo, hi)})
    return rows


def check_groups(groups, exp, warns):
    """slow-repeat, result_ok-false and missing (system, N) warnings."""
    for w in sorted(groups[exp]):
        ns_all = sorted({n for s in groups[exp][w] for n in groups[exp][w][s]})
        for s in sorted(groups[exp][w]):
            for n, g in sorted(groups[exp][w][s].items()):
                med = float(np.median(g.walls))
                for rid, wall in zip(g.rids, g.walls):
                    if med and wall > 2 * med:
                        warns.append(f"slow-repeat {rid} {wall / med:.2f}× median")
                for rid, ok in zip(g.rids, g.oks):
                    if not ok:
                        warns.append(f"result_ok-false {rid}")
            have = sorted(groups[exp][w][s])
            for n in ns_all:
                if n not in have:
                    warns.append(f"missing {exp} {w} {s} n={n}")


def _is_weak(groups, exp):
    for w in groups[exp].values():
        for s in w.values():
            if len({g.size() for g in s.values()}) > 1:
                return True
    return False


def check_predictions(lab, speedups, warns):
    p = lab / "predictions.md"
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        m = re.match(r"\s*PRED\s+(\S+)\s+(\S+)\s+(\S.*)$", line)
        if not m:
            continue
        system, workload, rest = m.groups()
        params = dict(re.findall(r"(\w+)=([-\w.]+)", rest))
        ns = sorted(n for (s, w, n), pt in speedups.items()
                    if s == system and w == workload)
        if not ns:
            warns.append(f"prediction-missed {system} {workload} {rest} (no data)")
            continue
        beats = [n for n in ns if speedups[(system, workload, n)] > 1.0]
        mine = beats[0] if beats else None
        hit_n = 32 if 32 in ns else ns[-1]
        at = speedups[(system, workload, hit_n)]
        ok_speed = ("speedup" not in params
                    or (math.isfinite(at)
                        and at >= float(params["speedup"]) * 0.9))
        held = ("min_n" not in params or mine == int(params["min_n"])) \
            and ok_speed
        if not held:
            warns.append(f"prediction-missed {system} {workload} {rest}")


def speedup_name(exp):
    """File of an experiment's per-workload speedup table: E4..E11 also have
    their own table under analysis/<EXP>.csv, so theirs is <EXP>-speedup.csv."""
    return f"{exp}-speedup.csv" if exp in EXPS else f"{exp}.csv"


def spread_report(groups):
    max_spread = 0.0
    used = 0  # most repeats any group ran
    for exp in sorted(groups):
        for w in sorted(groups[exp]):
            for s in sorted(groups[exp][w]):
                for n, g in sorted(groups[exp][w][s].items()):
                    used = max(used, len(g.walls))
                    if len(g.walls) < 2:
                        continue
                    med = float(np.median(g.walls))
                    iqr = float(np.percentile(g.walls, 75)
                                - np.percentile(g.walls, 25))
                    sp = iqr / med if med else 0.0
                    max_spread = max(max_spread, sp)
                    print(f"SPREAD {exp} {w} {s} n={n} {sp:.4f}")
    if used < 2:
        print("REPEATS-NONE (no group ran 2 or more repeats: spread unknown)")
    elif max_spread < 0.10:
        print("REPEATS-OK")
    else:
        # Variance scales with 1/k, so k grows with the square of the spread
        # ratio; it must exceed the repeats already run, else it is no advice.
        k = min(15, max(math.ceil(used * (max_spread / 0.10) ** 2), used + 1))
        print(f"RAISE-REPEATS {k}")


def fmt(v):
    if v is None:
        return ""
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, float):
        return f"{v:.10g}" if math.isfinite(v) else ""
    return str(v)


def write_csv(path, cols, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        wr = csv.writer(f, lineterminator="\n")
        wr.writerow(cols)
        for r in rows:
            wr.writerow([fmt(r.get(c, "")) for c in cols])


def _read_csv_rows(path):
    if not path.exists():
        return None
    with path.open(newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def table_e4(data):
    shares = [c for c in data[0] if c.startswith("share_")]
    rows = []
    for s in sorted({r["system"] for r in data}):
        allrs = [r for r in data if r["system"] == s]
        # a failed measurement is an empty cell: excluded and counted, not 0
        rs = [r for r in allrs if r["wall_s"] != "" and r["result_ok"] != ""]
        skipped = len(allrs) - len(rs)
        note = ""
        if skipped:
            note = (f"{skipped} of {len(allrs)} rows excluded "
                    "(empty wall_s or result_ok)")
            print(f"WARN E4 {s}: {note}")
        row = {"system": s, "repeats": len(rs), "note": note,
               "median_wall_s": (float(np.median([float(r["wall_s"])
                                                  for r in rs]))
                                 if rs else None),
               "result_ok_all": (all(float(r["result_ok"]) != 0 for r in rs)
                                 if rs else None)}
        for c in shares:
            vals = [float(r[c]) for r in rs if r[c] != ""]
            row[c] = sum(vals) / len(vals) if vals else None
        rows.append(row)
    cols = ["system", "repeats", "median_wall_s", "result_ok_all"] + shares + ["note"]
    return cols, rows


def table_e5(data):
    rows = []
    for sys_ in sorted({r["system"] for r in data}):
        for phase in sorted({r["phase"] for r in data
                             if r["system"] == sys_}):
            rs = [r for r in data
                  if r["system"] == sys_ and r["phase"] == phase]
            ready = [float(r["ready_s"]) for r in rs if r["ready_s"] != ""]
            reuse = [float(r["reused"]) for r in rs if r["reused"] != ""]
            spread = [float(r["spread"]) for r in rs
                      if r.get("spread", "") != ""]
            rows.append({"system": sys_, "phase": phase,
                         "ready_s_max": max(ready) if ready else None,
                         "ready_s_median": float(np.median(ready)) if ready else None,
                         "rx_total": sum(float(r["rx_bytes"]) for r in rs
                                         if r["rx_bytes"] != ""),
                         "reused_all": (min(reuse) > 0) if reuse else None,
                         "spread_all": ((min(spread) > 0) if spread else None)})
            if spread and min(spread) == 0:
                print(f"WARN E5 {sys_} {phase}: some runs did not spread across PCs "
                      "(spread=0); their ready times do not measure shipping")
    cols = ["system", "phase", "ready_s_max", "ready_s_median", "rx_total",
            "reused_all", "spread_all"]
    return cols, rows


def _med(choice, rs, col):
    vals = [float(r[col]) for r in rs if r[col] != ""]
    return float(np.median(vals)) if vals else None


def _all_ok(rs):
    """False if any recorded result_ok is 0; empty cells (no record) skipped.
    A kill row with no kill_at_s was never disturbed: it proves no recovery."""
    vals = [float(r["result_ok"]) for r in rs if r["result_ok"] != ""
            and not (r.get("kind") == "kill" and r.get("kill_at_s", "") == "")]
    return all(v != 0 for v in vals) if vals else None


def table_e6(data):
    rows = []
    for sys_ in sorted({r["system"] for r in data}):
        for kind in sorted({r["kind"] for r in data if r["system"] == sys_}):
            rs = [r for r in data if r["system"] == sys_ and r["kind"] == kind]
            rows.append({"system": sys_, "kind": kind, "runs": len(rs),
                         "kill_at_s_median": _med(None, rs, "kill_at_s"),
                         "wall_s_median": _med(None, rs, "wall_s"),
                         "result_ok_all": _all_ok(rs),
                         "stop_latency_s_median": _med(None, rs,
                                                       "stop_latency_s")})
    cols = ["system", "kind", "runs", "kill_at_s_median", "wall_s_median",
            "result_ok_all", "stop_latency_s_median"]
    return cols, rows


def table_e11(data):
    rows = []
    for k in sorted({int(r["k"]) for r in data}):
        rs = [r for r in data if int(r["k"]) == k]
        joins = [float(r["join_s"]) for r in rs if r["join_s"] != ""]
        intro = [r["pc"] for r in rs
                 if r.get("is_introducer", "") not in ("", "0", "False", "false")]
        got = [float(r["bytes_per_min"]) for r in rs if r["bytes_per_min"] != ""]
        if len(got) != len(rs):
            print(f"WARN E11 k={k}: {len(rs) - len(got)} of {len(rs)} PCs have no "
                  "bytes_per_min; the total is left empty, not a partial sum")
        rows.append({"k": k, "pcs": len(rs),
                     "join_s_median": float(np.median(joins)) if joins else None,
                     "bytes_per_min_total": sum(got) if got and len(got) == len(rs) else None,
                     "introducer_pc": intro[0] if intro else ""})
    cols = ["k", "pcs", "join_s_median", "bytes_per_min_total", "introducer_pc"]
    return cols, rows


def extras_tables(lab):
    builders = {"E4": ("e4.csv", table_e4), "E5": ("e5.csv", table_e5),
                "E6": ("e6.csv", table_e6), "E11": ("e11.csv", table_e11)}
    for exp in EXPS:
        name, build = builders[exp]
        data = _read_csv_rows(lab / "analysis-input" / name)
        if data is None:
            print(f"SKIP {exp} (no analysis-input/{name})")
            continue
        cols, rows = build(data)
        write_csv(lab / "analysis" / f"{exp}.csv", cols, rows)


def vmstat_util(path):
    """Mean (us+sy) from a vmstat -t log; None when unparseable."""
    idx = idx_sy = None
    vals = []
    for line in path.read_text().splitlines():
        parts = line.split()
        if idx is None:
            if "us" in parts and "sy" in parts:
                idx, idx_sy = parts.index("us"), parts.index("sy")
        elif len(parts) > max(idx, idx_sy):
            try:
                vals.append(float(parts[idx]) + float(parts[idx_sy]))
            except ValueError:
                print(f"SKIP vmstat line in {path}: {line[:40]!r}")
    return sum(vals) / len(vals) if vals else None


def make_figures(lab, groups, weak):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figdir = lab / "figures"
    figdir.mkdir(parents=True, exist_ok=True)
    analysis = lab / "analysis"

    def save(fig, name):
        for ext in ("png", "pdf"):
            fig.savefig(figdir / f"{name}.{ext}", bbox_inches="tight")
        plt.close(fig)

    def curve(name, rows, y, lo, hi, title, ylabel):
        pts = [r for r in rows if r[y] != "" and r[lo] != ""]
        if not pts:
            print(f"SKIP FIGURE {name} (no {y} data)")
            return
        fig, ax = plt.subplots()
        for s in sorted({r["system"] for r in pts}):
            d = sorted((r for r in pts if r["system"] == s),
                       key=lambda r: int(r["n"]))
            x = [int(r["n"]) for r in d]
            yv = [float(r[y]) for r in d]
            lo_e = [yv[i] - float(d[i][lo]) for i in range(len(d))]
            hi_e = [float(d[i][hi]) - yv[i] for i in range(len(d))]
            ax.errorbar(x, yv, yerr=[lo_e, hi_e], label=s, marker="o",
                        capsize=3)
        ax.set_xscale("log", base=2)
        ax.set_xlabel("N (machines)")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend()
        save(fig, name)

    for exp in sorted(groups):
        rows = _read_csv_rows(analysis / speedup_name(exp))
        if not rows:
            continue
        for w in sorted({r["workload"] for r in rows}):
            curve(f"{exp.lower()}-speedup-{w.lower()}",
                  [r for r in rows if r["workload"] == w],
                  "speedup", "speedup_lo", "speedup_hi",
                  f"{exp} strong scaling, {w}", "speedup")
        if exp in weak:
            rows = _read_csv_rows(analysis / f"{exp}-efficiency.csv")
            if rows:
                curve(f"{exp.lower()}-efficiency", rows,
                      "efficiency", "efficiency_lo", "efficiency_hi",
                      f"{exp} efficiency (weak scaling)", "t(1)/t(N)")

    def bars(name, rows, valcol, ylabel):
        rows = [r for r in rows if r[valcol] != ""]
        if not rows:
            print(f"SKIP FIGURE {name} ({valcol} data missing)")
            return
        fig, ax = plt.subplots()
        ax.bar(range(len(rows)), [float(r[valcol]) for r in rows])
        labels = [" ".join(str(r[k]) for k in labels_keys if r.get(k))
                  for r in rows]
        ax.set_xticks(range(len(rows)))
        ax.set_xticklabels(labels, rotation=30, ha="right")
        ax.set_ylabel(ylabel)
        save(fig, name)

    labels_keys = []
    e4 = _read_csv_rows(analysis / "E4.csv")
    if e4:
        labels_keys = ["system"]
        bars("e4-bars", e4, "median_wall_s", "median wall s")
    e5 = _read_csv_rows(analysis / "E5.csv")
    if e5:
        phases = sorted({r["phase"] for r in e5})
        systems = sorted({r["system"] for r in e5})
        fig, ax = plt.subplots()
        width = 0.8 / len(phases)
        for i, ph in enumerate(phases):
            xs = np.arange(len(systems)) + i * width
            vals = [float(r["ready_s_max"])
                    for r in e5 if r["phase"] == ph]
            ax.bar(xs, vals, width, label=ph)
        ax.set_xticks(np.arange(len(systems)) + width * (len(phases) - 1) / 2)
        ax.set_xticklabels(systems)
        ax.set_ylabel("environment ready s (max over PCs)")
        ax.legend()
        save(fig, "e5-bars")
    e11 = _read_csv_rows(analysis / "E11.csv")
    if e11:
        fig, axes = plt.subplots(1, 2, figsize=(10, 4))
        jr = [r for r in e11 if r["join_s_median"] != ""]
        br = [r for r in e11 if r["bytes_per_min_total"] != ""]
        if len(jr) != len(e11) or len(br) != len(e11):
            print("WARN e11-membership figure leaves out k with an empty "
                  "join_s_median or bytes_per_min_total")
        axes[0].plot([int(r["k"]) for r in jr],
                     [float(r["join_s_median"]) for r in jr], "o-")
        axes[0].set(xlabel="cluster size k", ylabel="join s (median)")
        axes[1].bar([int(r["k"]) for r in br],
                    [float(r["bytes_per_min_total"]) for r in br])
        axes[1].set(xlabel="cluster size k", ylabel="idle bytes/min")
        save(fig, "e11-membership")
    util = {}
    hosts = lab / "hosts"
    if hosts.exists():
        for pc in sorted(hosts.glob("pc*")):
            f = pc / "metrics" / "E1-n32.vmstat"
            val = vmstat_util(f) if f.exists() else None
            if val is not None:
                util[pc.name] = val
    if util:
        fig, ax = plt.subplots()
        ax.bar(range(len(util)), list(util.values()))
        ax.set_xticks(range(len(util)))
        ax.set_xticklabels(list(util), rotation=30, ha="right")
        ax.set_ylabel("vmstat us+sy mean %")
        save(fig, "cpu-util-w1-n32")


def main(argv=None):
    ap = argparse.ArgumentParser(description="campaign runs.jsonl analysis")
    ap.add_argument("--lab", default=None, help="lab results dir")
    ap.add_argument("--spread", action="store_true",
                    help="report per-group IQR/median spread")
    args = ap.parse_args(argv)
    lab = Path(args.lab or os.environ.get("LAB_RESULTS")
               or REPO / "results" / "lab")
    if not (lab / "runs.jsonl").exists():
        print(f"analyse: no runs.jsonl under {lab}", file=sys.stderr)
        return 1
    groups = load_runs(lab / "runs.jsonl")
    warns = []
    speedups = {}
    weak = {e for e in groups if _is_weak(groups, e)}
    for exp in sorted(groups):
        check_groups(groups, exp, warns)
        write_csv(lab / "analysis" / speedup_name(exp), EXP_COLS,
                  exp_speed_rows(groups, exp, speedups=speedups))
        if exp in weak:
            write_csv(lab / "analysis" / f"{exp}-efficiency.csv", EFF_COLS,
                      exp_speed_rows(groups, exp, eff=True))
        ratios = exp_ratio_rows(groups, exp)
        if ratios:
            write_csv(lab / "analysis" / f"{exp}-ratios.csv", RATIO_COLS,
                      ratios)
    extras_tables(lab)
    check_predictions(lab, speedups, warns)
    for w in sorted(warns):
        print(f"WARN {w}")
    if args.spread:
        spread_report(groups)
    make_figures(lab, groups, weak)
    return 0


if __name__ == "__main__":
    sys.exit(main())
