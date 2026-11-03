#!/usr/bin/env python3
"""Turn results/lab/runs.jsonl into one CSV per experiment and the figures.

  analyse.py [--runs results/lab/runs.jsonl] [--out results/lab]

CSV (results/lab/csv/<experiment>.csv): one row per group (workload, system,
N and whatever else tells runs apart: mode, variant, phase, fault), with the
median and interquartile range of wall_s over the repeats. Warm-ups and
failed runs (error set, or result_ok false) are excluded and counted in
every row (warmups_excluded, failed_excluded), and summary.txt totals them.
Lines of an experiment the lab day cut off and ran again are dropped: each
attempts.jsonl entry (beside runs.jsonl) with status "interrupted" names them
as the 0-based line range [runs_from_line, runs_to_line).

Figures (results/lab/figures/, PNG and PDF):
  e1_speedup        strong scaling: speedup vs N, a line per system, a panel
                    per workload; speedup = median wall of plain at N=1 /
                    median wall at N
  e1b_efficiency    weak scaling efficiency: T(1) / T(N) of the same system
  e4_uneven         median wall per system, IQR as error bars
  e5_env            wall and MB per method, cold and update
  e11_membership    membership bytes per PC per minute vs N
and e8_table.md, the training table. A figure whose experiment has no
usable runs is skipped with a note.
"""
import argparse
import csv
import json
import os
import statistics
import sys
from collections import defaultdict

# Colour follows the system, never its rank (the validated default palette,
# categorical slots 1-8 in order).
COLORS = {
    "pipedpeer": "#2a78d6", "ray": "#eb6834", "dask": "#1baf7a", "plain": "#eda100",
    "pipedpeer-headcount": "#e87ba4", "pipedpeer-exact": "#2a78d6", "pipedpeer-fast": "#6250d6",
    "torchrun": "#eb6834", "pip": "#008300", "pipedpeer-shim": "#2a78d6", "pipedpeer-noshim": "#e87ba4",
}
ORDER = ["plain", "pipedpeer", "pipedpeer-headcount", "pipedpeer-exact", "pipedpeer-fast",
         "pipedpeer-shim", "pipedpeer-noshim", "ray", "dask", "torchrun", "pip"]
GROUP_EXTRA = ["mode", "variant", "phase", "fault", "extra_package"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def interrupted(runs_path):
    """0-based runs.jsonl line numbers that belong to cut-off attempts."""
    path = os.path.join(os.path.dirname(os.path.abspath(runs_path)), "attempts.jsonl")
    drop = set()
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                try:
                    a = json.loads(line)
                except ValueError:
                    continue
                if a.get("status") == "interrupted":
                    drop.update(range(int(a["runs_from_line"]), int(a["runs_to_line"])))
    return drop


def load(path):
    runs, drop = [], interrupted(path)
    if drop:
        print("dropped %d line(s) of interrupted attempts (attempts.jsonl)" % len(drop), file=sys.stderr)
    with open(path) as f:
        for i, line in enumerate(f, 1):
            if i - 1 in drop:
                continue
            line = line.strip()
            if not line:
                continue
            try:
                runs.append(json.loads(line))
            except ValueError:
                print("warning: %s:%d is not JSON; skipped" % (path, i), file=sys.stderr)
    return runs


def usable(r):
    return not r.get("warmup") and not r.get("error") and r.get("result_ok") is not False


def stats(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None, None, None
    if len(xs) == 1:
        return xs[0], xs[0], xs[0]
    q = statistics.quantiles(xs, n=4, method="inclusive")
    return statistics.median(xs), q[0], q[2]


def sort_key(system):
    return ORDER.index(system) if system in ORDER else len(ORDER)


def group(runs, value="wall_s"):
    """Rows: one per (workload, system, n, extras), stats of value."""
    groups = defaultdict(list)
    for r in runs:
        key = (r.get("workload"), r.get("system"), r.get("n")) + tuple(
            json.dumps(r.get(k), sort_keys=True) if isinstance(r.get(k), (dict, list)) else r.get(k)
            for k in GROUP_EXTRA)
        groups[key].append(r)
    rows = []
    for key, rs in groups.items():
        ok = [r for r in rs if usable(r)]
        med, q1, q3 = stats([r.get(value) for r in ok])
        row = {"workload": key[0], "system": key[1], "n": key[2]}
        for k, v in zip(GROUP_EXTRA, key[3:]):
            if v is not None:
                row[k] = v
        row.update({"runs_used": len(ok), "warmups_excluded": sum(1 for r in rs if r.get("warmup")),
                    "failed_excluded": sum(1 for r in rs if not r.get("warmup") and not usable(r)),
                    "median_" + value: med, "q1_" + value: q1, "q3_" + value: q3,
                    "iqr_" + value: (q3 - q1) if med is not None else None})
        for extra in ("mb_down", "all_have_s", "bytes_per_rank", "ctrlc_latency_s", "join_s",
                      "bytes_per_node_per_min", "script_s", "minutes", "commands_typed"):
            vals = [r.get(extra) for r in ok if isinstance(r.get(extra), (int, float))]
            if vals:
                row["median_" + extra] = statistics.median(vals)
        losses = []
        for r in ok:
            try:
                losses.append(float(str(r.get("result")).split()[-1]))
            except (ValueError, IndexError):
                pass
        if key[0] == "T1" and losses:
            row["median_loss"] = statistics.median(losses)
        rows.append(row)
    rows.sort(key=lambda r: (str(r["workload"]), sort_key(r["system"]), r["n"] or 0,
                             str(r.get("phase")), str(r.get("variant"))))
    return rows


def add_speedup(rows):
    base = {}
    for r in rows:
        if r["system"] == "plain" and r["n"] == 1 and r.get("median_wall_s") and r.get("mode", "strong") != "weak":
            base[r["workload"]] = r["median_wall_s"]
    for r in rows:
        b, m = base.get(r["workload"]), r.get("median_wall_s")
        r["speedup_vs_plain"] = round(b / m, 4) if b and m else None


def add_efficiency(rows):
    base = {}
    for r in rows:
        if r["n"] == 1 and r.get("median_wall_s"):
            base[(r["workload"], r["system"])] = r["median_wall_s"]
    for r in rows:
        b, m = base.get((r["workload"], r["system"])), r.get("median_wall_s")
        r["weak_efficiency"] = round(b / m, 4) if b and m else None


def write_csv(path, rows):
    cols = []
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# --- figures ----------------------------------------------------------------

def plt_module():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "font.size": 9, "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2,
        "ytick.color": INK2, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
        "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False,
        "figure.dpi": 150, "savefig.bbox": "tight", "axes.axisbelow": True,
    })
    return plt


def save(fig, figdir, name, made):
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(figdir, "%s.%s" % (name, ext)))
    made.append(name)


def line_panels(plt, rows, value, ylabel, title, figdir, name, made, ideal=None):
    wls = sorted({r["workload"] for r in rows if r.get(value)})
    if not wls:
        return False
    fig, axes = plt.subplots(1, len(wls), figsize=(3.4 * len(wls), 3.0), squeeze=False)
    for ax, wl in zip(axes[0], wls):
        mine = [r for r in rows if r["workload"] == wl and r.get(value)]
        ns = sorted({r["n"] for r in mine})
        if ideal == "linear":
            ax.plot(ns, ns, color=INK2, linewidth=1, linestyle=(0, (3, 3)), label="ideal")
        elif ideal == "one":
            ax.axhline(1.0, color=INK2, linewidth=1, linestyle=(0, (3, 3)), label="ideal")
        for system in sorted({r["system"] for r in mine}, key=sort_key):
            pts = sorted((r["n"], r[value]) for r in mine if r["system"] == system)
            if system == "plain" and len(pts) == 1:
                ax.plot(*zip(*pts), marker="o", markersize=5, linestyle="none",
                        color=COLORS.get(system, INK2), label=system)
                continue
            ax.plot(*zip(*pts), marker="o", markersize=4, linewidth=2, color=COLORS.get(system, INK2), label=system)
        ax.set_title(wl, color=INK)
        ax.set_xlabel("N (PCs doing work)")
        ax.set_xscale("log", base=2)
        ax.set_xticks(ns)
        ax.set_xticklabels([str(n) for n in ns])
    axes[0][0].set_ylabel(ylabel)
    axes[0][-1].legend(loc="best", fontsize=8)
    fig.suptitle(title, color=INK, fontsize=10, y=1.03)
    save(fig, figdir, name, made)
    plt.close(fig)
    return True


def bars(plt, rows, label_of, title, ylabel, figdir, name, made, value="median_wall_s"):
    rows = [r for r in rows if r.get(value) is not None]
    if not rows:
        return False
    labels = [label_of(r) for r in rows]
    fig, ax = plt.subplots(figsize=(max(3.2, 0.7 * len(rows) + 1), 3.0))
    meds = [r[value] for r in rows]
    q1 = [r.get(value.replace("median", "q1"), r[value]) or r[value] for r in rows]
    q3 = [r.get(value.replace("median", "q3"), r[value]) or r[value] for r in rows]
    err = [[m - a for m, a in zip(meds, q1)], [b - m for m, b in zip(meds, q3)]]
    ax.bar(range(len(rows)), meds, width=0.6, color=[COLORS.get(r["system"], INK2) for r in rows],
           edgecolor="white", linewidth=2, yerr=err, ecolor=INK2, capsize=3)
    ax.set_xticks(range(len(rows)))
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title, color=INK, fontsize=10)
    ax.grid(axis="x", visible=False)
    save(fig, figdir, name, made)
    plt.close(fig)
    return True


def e8_table(rows, path):
    """One row per (variant, ranks, system); "loss vs 1 rank" is the median
    validation loss over the plain one-rank run's of the same variant."""
    ref = {r.get("variant"): r.get("median_loss") for r in rows if r["system"] == "plain" and r["n"] == 1}
    with open(path, "w") as f:
        f.write("| variant | ranks | system | runs | median wall s | IQR s | median val loss | loss vs 1 rank | bytes/rank |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for r in rows:
            def fmt(v, p="%.2f"):
                return p % v if isinstance(v, (int, float)) else "-"
            one = ref.get(r.get("variant"))
            rel = r.get("median_loss") / one if isinstance(r.get("median_loss"), (int, float)) and one else None
            f.write("| %s | %s | %s | %d | %s | %s | %s | %s | %s |\n" % (
                r.get("variant", "-"), r["n"], r["system"], r["runs_used"], fmt(r.get("median_wall_s")),
                fmt(r.get("iqr_wall_s")), fmt(r.get("median_loss"), "%.4f"), fmt(rel, "%.3f"),
                fmt(r.get("median_bytes_per_rank"), "%d")))


def main(argv=None):
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.normpath(os.path.join(here, "..", ".."))
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--runs", default=os.path.join(repo, "results", "lab", "runs.jsonl"))
    p.add_argument("--out", default=None)
    p.add_argument("--no-figures", action="store_true")
    a = p.parse_args(argv)
    out = a.out or os.path.dirname(os.path.abspath(a.runs))
    csvdir, figdir = os.path.join(out, "csv"), os.path.join(out, "figures")
    os.makedirs(csvdir, exist_ok=True)
    os.makedirs(figdir, exist_ok=True)
    runs = load(a.runs)
    by_exp = defaultdict(list)
    for r in runs:
        by_exp[r.get("experiment", "?")].append(r)

    tables, summary = {}, []
    for exp, rs in sorted(by_exp.items()):
        rows = group(rs)
        if exp == "e1":
            add_speedup(rows)
        if exp == "e1b":
            add_efficiency(rows)
        write_csv(os.path.join(csvdir, "%s.csv" % exp), rows)
        tables[exp] = rows
        warm = sum(1 for r in rs if r.get("warmup"))
        bad = sum(1 for r in rs if not r.get("warmup") and not usable(r))
        summary.append("%s: %d runs, %d used, %d warm-ups excluded, %d failed excluded" % (
            exp, len(rs), len(rs) - warm - bad, warm, bad))
        for r in rs:
            if r.get("error"):
                summary.append("  FAILED %s %s %s n=%s on %s: %s (%s)" % (
                    r.get("run_id"), r.get("workload"), r.get("system"), r.get("n"),
                    r.get("failing_pc"), r.get("error"), r.get("log_path")))
    if "e8" in tables:
        e8_table(tables["e8"], os.path.join(out, "e8_table.md"))

    made, skipped = [], []
    if not a.no_figures:
        plt = plt_module()
        figs = [
            ("e1", lambda rows: line_panels(plt, rows, "speedup_vs_plain", "speedup vs plain (N=1)",
                                            "Strong scaling", figdir, "e1_speedup", made, ideal="linear")),
            ("e1b", lambda rows: line_panels(plt, rows, "weak_efficiency", "efficiency T(1)/T(N)",
                                             "Weak scaling, W1", figdir, "e1b_efficiency", made, ideal="one")),
            ("e4", lambda rows: bars(plt, rows, lambda r: r["system"], "Uneven PCs (cores 8/4/2/1), W1",
                                     "median wall (s)", figdir, "e4_uneven", made)),
            ("e5", lambda rows: bars(plt, rows, lambda r: "%s %s%s" % (r["system"], r.get("phase", ""),
                                     " +" + r["extra_package"] if r.get("extra_package") else ""),
                                     "Shipping the environment", "median wall (s)", figdir, "e5_env", made)
             and bars(plt, rows, lambda r: "%s %s%s" % (r["system"], r.get("phase", ""),
                      " +" + r["extra_package"] if r.get("extra_package") else ""),
                      "Shipping the environment: data received", "MB, all PCs", figdir, "e5_env_mb", made,
                      value="median_mb_down")),
            ("e11", lambda rows: line_panels(plt, [dict(r, workload="membership") for r in rows
                                                   if r["workload"] == "E11-idle"],
                                             "median_bytes_per_node_per_min", "bytes per PC per minute",
                                             "Idle membership traffic", figdir, "e11_membership", made)),
        ]
        for exp, fn in figs:
            if exp in tables and not fn(tables[exp]):
                skipped.append(exp)
    with open(os.path.join(out, "summary.txt"), "w") as f:
        f.write("\n".join(summary) + "\n")
    print("\n".join(summary))
    print("csv: %s (%d files); figures: %s%s" % (csvdir, len(tables), ", ".join(made) or "none",
                                                 "; skipped (no usable runs): " + ", ".join(skipped) if skipped else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
