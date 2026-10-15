"""Fit log-log growth per step for the M2 scaling sweep (scripts/scaling/sweep.sh).

    python fit.py <results.csv> [plot-dir]     # markdown on stdout, PNGs in plot-dir

cpu.csv beside results.csv (sweep.sh) adds each run's cgroup CPU seconds
(cpu = all containers, cpu_submitter = node 1, cpu_peers = the rest) and the
host load. Only the total CPU series raises superlinear flags: wall-clock steps are
inflated by a loaded host (parallel agents), so their exponents are shown as
hints and re-checked on a quiet rerun.

Reads the m2 rows of the bench CSV, each run's steps.json (C24 run record,
beside the run log as <log>.steps.json), totals every step per run, takes the
median per (variable, value), and fits log(duration) = k*log(value) + c.
A step is superlinear when its exponent beats the expected one by > 0.15,
over the whole range or over the top three points (fixed costs flatten the
whole-range slope, so the tail catches growth that only shows at scale).
Every number is EMULATED (equal cgroup quotas + tc netem on one host).
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict

import numpy as np

MARGIN = 0.15
NUMERIC = ("payload", "items", "cost", "nodes", "spread")
CPU = ("cpu", "cpu_submitter", "cpu_peers")
# Only the cluster-wide total flags: the submitter/peer split moves with
# placement (more nodes or a slower peer shifts work), not with overhead.
FLAG = ("cpu",)
# Fits start here: one node is a different regime (nothing crosses a link),
# so 1 -> 2 is the fixed cost of distributing, reported, not fitted.
MIN_X = {"nodes": 2}
# Expected exponent of each step's total against the variable; anything not
# listed is expected flat (0). "wall" is the outside wall clock.
EXPECTED = {
    "payload": dict.fromkeys(("pickle", "compress", "send", "receive", "unpickle") + CPU, 1.0),
    "items": dict.fromkeys(("pickle", "compress", "send", "queue", "compute", "receive", "unpickle", "wall") + CPU,
                           1.0),
    "cost": dict.fromkeys(("compute", "wall") + CPU, 1.0),
    "nodes": {"wall": -1.0},
    "spread": {},
}


def slope(xs, ys):
    """Least-squares exponent k of y = c * x**k; None with < 2 usable points."""
    pts = [(x, y) for x, y in zip(xs, ys) if x > 0 and y > 0]
    if len(pts) < 2:
        return None
    lx, ly = np.log([p[0] for p in pts]), np.log([p[1] for p in pts])
    if np.ptp(lx) == 0:
        return None
    return float(np.polyfit(lx, ly, 1)[0])


def fit(var, series):
    """series: {step: [(value, seconds), ...]} -> list of fit dicts, flagged ones marked."""
    out = []
    for step, pts in sorted(series.items()):
        pts = sorted(pts)
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        whole, tail = slope(xs, ys), slope(xs[-3:], ys[-3:]) if len(xs) > 3 else None
        exp = EXPECTED.get(var, {}).get(step, 0.0)
        worst = max(k for k in (whole, tail, -1e9) if k is not None)
        out.append({"step": step, "whole": whole, "tail": tail, "expected": exp,
                    "superlinear": worst > exp + MARGIN, "points": len(pts)})
    return out


def run_steps(log):
    """{step: seconds} for one run, from <log>.steps.json; {} when absent."""
    path = log + ".steps.json"
    if not os.path.exists(path) or not os.path.getsize(path):
        print(f"warning: no run record for {log}", file=sys.stderr)
        return {}
    try:
        with open(path) as f:
            rec = json.load(f)
    except ValueError as e:
        print(f"warning: unreadable run record {path}: {e}", file=sys.stderr)
        return {}
    tot = defaultdict(float)
    for s in rec.get("steps") or []:
        tot[s.get("step", "?")] += s.get("dur_ns", 0) / 1e9
    return dict(tot)


def load_cpu(csv_path):
    """{log: {cpu, cpu_submitter, cpu_peers, load}} from cpu.csv beside results.csv."""
    path = os.path.join(os.path.dirname(csv_path), "cpu.csv")
    if not os.path.exists(path):
        print(f"warning: no {path}; CPU series and flags are missing", file=sys.stderr)
        return {}
    with open(path, newline="") as f:
        return {r["log"]: {"cpu": float(r["cpu_s"]), "cpu_submitter": float(r["cpu_submitter_s"]),
                           "cpu_peers": float(r["cpu_peers_s"]),
                           "load": max(float(r["load1_before"]), float(r["load1_after"]))}
                for r in csv.DictReader(f)}


def load(csv_path):
    """{var: {value: [ {step: s, ..., wall: s}, ... ]}} from the ok m2 rows."""
    cells = defaultdict(lambda: defaultdict(list))
    cpu = load_cpu(csv_path)
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        if r["suite"] != "m2" or r["status"] != "ok":
            continue
        steps = run_steps(r["log"])
        steps.update(cpu.get(r["log"], {}))
        steps["wall"] = float(r["wall_s"])
        if r["script_s"]:
            steps["script"] = float(r["script_s"])
        cells[r["case"]][r["variant"]].append(steps)
    return cells


def medians(runs):
    names = sorted({k for run in runs for k in run})
    return {n: statistics.median(run.get(n, 0.0) for run in runs) for n in names}


def fmt(k):
    return "-" if k is None else f"{k:+.2f}"


def report(cells, plot_dir=None):
    lines, flags = [], []
    for var in list(NUMERIC) + sorted(set(cells) - set(NUMERIC)):
        if var not in cells:
            continue
        numeric = var in NUMERIC
        vals = sorted(cells[var], key=lambda v: float(v) if numeric else v)
        med = {v: medians(cells[var][v]) for v in vals}
        steps = sorted({s for m in med.values() for s in m})
        lines += ["", f"### {var} (emulated)", "",
                  "| {} | runs | {} |".format(var, " | ".join(steps)),
                  "|---|---|%s" % ("---|" * len(steps))]
        for v in vals:
            cols = " | ".join(f"{med[v].get(s, 0.0):.3f}" for s in steps)
            lines.append(f"| {v} | {len(cells[var][v])} | {cols} |")
        if not numeric:
            continue
        lo = MIN_X.get(var, 0)
        series = {s: [(float(v), med[v].get(s, 0.0)) for v in vals if float(v) >= lo] for s in steps if s != "load"}
        fits = fit(var, series)
        lines += ["", "| step | exponent (whole) | exponent (top 3) | expected | superlinear |", "|---|---|---|---|---|"]
        for f in fits:
            lines.append("| {} | {} | {} | {:+.1f} | {} |".format(f["step"], fmt(f["whole"]), fmt(f["tail"]),
                                                           f["expected"], verdict(f)))
            if f["superlinear"] and f["step"] in FLAG:
                flags.append("- {} vs {}: exponent {} whole / {} top-3, expected {:+.1f}".format(
                    f["step"], var, fmt(f["whole"]), fmt(f["tail"]), f["expected"]))
        if plot_dir:
            plot(var, series, os.path.join(plot_dir, var + ".png"))
            lines += ["", f"![{var}](scaling/{var}.png)"]
    lines += ["", f"### Superlinear CPU growth (exponent > expected + {MARGIN:.2f})", ""] + (flags or ["- none"])
    return "\n".join(lines)


def verdict(f):
    if not f["superlinear"]:
        return "no"
    if f["step"] in FLAG:
        return "**yes**"
    if f["step"] in CPU:
        return "split only (placement; not flagged)"
    return "wall only (load-inflated; not flagged)"


def plot(var, series, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig, ax = plt.subplots(figsize=(6, 4))
    for step, pts in sorted(series.items()):
        pts = [(x, y) for x, y in pts if x > 0 and y > 0]
        if len(pts) >= 2:
            ax.plot(*zip(*pts), marker="o", label=step)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel(var)
    ax.set_ylabel("seconds (median, summed over chunks)")
    ax.set_title(f"{var} - emulated")
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(path, dpi=90)
    plt.close(fig)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    print(report(load(sys.argv[1]), sys.argv[2] if len(sys.argv) > 2 else None))
