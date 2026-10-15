"""Results-chapter plots. Every value is read from a measurement file; nothing
here is typed in by hand except the labels.

  python3 docs/diagrams/src/plots_results.py
"""
import json, pathlib, re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = pathlib.Path(__file__).resolve()
OUT = HERE.parents[1]
BENCH = HERE.parents[2] / "benchmarks"
BLUE, GREEN, RED, GOLD = "#1e50a0", "#2f7d4f", "#a33333", "#8a6d1f"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 15,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "figure.dpi": 300, "savefig.dpi": 300, "savefig.bbox": "tight"})

R = json.loads((BENCH / "results.json").read_text())
M = R["measurements"]


def _shipped(label, i):
    """What the run log says was sent: 'whole', 'nothing' or 'diff'."""
    log = (BENCH / "raw" / f"{label}_run{i}.log").read_text()
    if "nothing to send" in log:
        return "nothing"
    if "Sending the whole closure" in log:
        return "whole"
    return "diff"


def fig_closure_cache():
    """5.1 - Cold versus warm submission of the same environment. A run counts
    as warm only if its own log says nothing was sent; a later run that landed
    on a worker without the closure is shown as what it was."""
    s = [x["seconds"] for x in M["n1_trivial_latency"]["samples"]]
    kinds = [_shipped("N1_trivial", i) for i in range(len(s))]
    cold = s[0]
    warm = [v for v, k in zip(s, kinds) if k == "nothing"]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(10.4, 3.9),
                                  gridspec_kw={"width_ratios": [1.15, 1], "wspace": 0.38})
    ax.bar(["first run\n(closure shipped)", "closure present\n(median)"],
           [cold, float(np.median(warm))], color=[RED, GREEN], width=0.55)
    ax.set_ylabel("seconds, end to end")
    for i, v in enumerate([cold, float(np.median(warm))]):
        ax.text(i, v + cold * 0.03, f"{v:.2f} s", ha="center", fontsize=15, bbox=dict(facecolor="white", edgecolor="none", pad=1.6))
    ax.set_ylim(0, cold * 1.22)
    ax.grid(axis="y", alpha=0.25)
    ax.set_axisbelow(True)
    ax.set_title(f"{cold/np.median(warm):.0f}\u00d7 faster start once cached",
                 fontsize=15, loc="left")

    ax2.plot(range(1, len(s) + 1), s, color=BLUE, linewidth=1.4, zorder=1)
    for i, (v, k) in enumerate(zip(s, kinds)):
        ax2.plot(i + 1, v, "o", markersize=8, zorder=2,
                 color=GREEN if k == "nothing" else RED)
    ax2.text(len(s), max(s) * 0.92, "red: whole closure sent\ngreen: nothing sent",
             ha="right", va="top", fontsize=12,
             bbox=dict(facecolor="white", edgecolor="none", pad=1.6))
    ax2.set_xlabel("submission"); ax2.set_ylabel("seconds")
    ax2.set_xticks(range(1, len(s) + 1))
    ax2.set_title("Each run in turn", fontsize=15, loc="left")
    ax2.grid(alpha=0.25)
    fig.savefig(OUT / "fig_5.1_closure_cache.png"); plt.close(fig)


def fig_shim_overhead():
    """5.2 - Cost of leaving interception on when it decides not to distribute.
    Left: the declined 16-item map (N7). Bars are the medians
    results.json records over all five runs; the whiskers leave out the
    enabled side's cold first run (its environment build), which is printed
    instead, as Table tab:shimidle does."""
    offs = [x["seconds"] for x in M["n7_shim_off"]["samples"]]
    ons_all = [x["seconds"] for x in M["n7_shim_declines"]["samples"]]
    cold, ons = ons_all[0], ons_all[1:]
    on, off = M["n7_shim_declines"]["median_s"], M["n7_shim_off"]["median_s"]
    rng = [(min(offs), max(offs)), (min(ons), max(ons))]
    b3 = (BENCH / "raw" / "B3_shim_overhead.txt").read_text()
    before = float(re.search(r"ratio: ([0-9.]+)x", b3).group(1))
    d2 = (BENCH / "raw" / "O2_shim_startup.txt").read_text()
    after = float(re.search(r"ratio: ([0-9.]+)x", d2).group(1))
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(10.4, 3.9), gridspec_kw={"wspace": 0.34})

    ax.bar(["interception off", "interception on"], [off, on],
           color=["#9bb8dd", BLUE], width=0.5)
    for i, (lo, hi) in enumerate(rng):
        ax.plot([i, i], [lo, hi], color="#333333", linewidth=1.2)
    for i, v in enumerate([off, on]):
        ax.text(i, v * 0.5, f"{v:.3f} s", ha="center", va="center", fontsize=15, bbox=dict(facecolor="white", edgecolor="none", pad=1.6))
    ax.text(1, max(on, off) * 1.12, f"+ one cold first run, {cold:.2f} s", ha="center",
            fontsize=12, color="#555555")
    ax.set_ylabel("seconds, end to end"); ax.set_ylim(0, max(on, off) * 1.25)
    ax.grid(axis="y", alpha=0.25); ax.set_axisbelow(True)
    ax.set_title(f"16-item map the cost model declines\n(median of {len(offs)} runs each; line: min-max)",
                 fontsize=15, loc="left")

    # Startup tax with libraries patched at start-up (raw/B3) and on first import (O2).
    ax2.bar(["patched at\nstart-up", "patched on\nfirst import"], [before, after], color=[RED, GREEN], width=0.5)
    ax2.axhline(3.0, color=GOLD, linestyle="--", linewidth=1.5)
    ax2.text(0.5, 5.6, "3x budget", color=GOLD, fontsize=13, ha="center", va="bottom", bbox=dict(facecolor="white", edgecolor="none", pad=1.6))
    for i, v in enumerate([before, after]):
        ax2.text(i, v + 0.9, f"{v:.2f}x", ha="center", fontsize=15, bbox=dict(facecolor="white", edgecolor="none", pad=1.6))
    ax2.set_ylabel("slowdown vs no shim"); ax2.set_ylim(0, 25)
    ax2.grid(axis="y", alpha=0.25); ax2.set_axisbelow(True)
    ax2.set_title("Startup, job that imports nothing heavy",
                  fontsize=15, loc="left")
    fig.savefig(OUT / "fig_5.2_shim_overhead.png"); plt.close(fig)


def fig_sandbox():
    """5.3 - Sandbox start cost, measured on this host. Warmup iterations
    (the first 5 of each series) are excluded, matching bench/main.go."""
    import collections, statistics
    data = json.loads((BENCH / "raw" / "B2_sandbox_bench.json").read_text())
    agg = collections.defaultdict(list)
    for r in data:
        if r["iteration"] >= 5 and r["exit_code"] == 0:
            agg[(r["sandbox"], r["task_name"])].append(r["total_ms"])
    tasks = ["trivial", "medium", "python"]
    bw = [statistics.median(agg[("bwrap", t)]) for t in tasks]
    cr = [statistics.median(agg[("crun", t)]) for t in tasks]

    x = np.arange(len(tasks)); w = 0.36
    fig, ax = plt.subplots(figsize=(7.4, 3.6))
    ax.bar(x - w / 2, bw, w, label="bwrap", color="#9bb8dd")
    ax.bar(x + w / 2, cr, w, label="crun (what PipedPeer uses)", color=BLUE)
    for i in range(len(tasks)):
        ax.text(i - w / 2, bw[i] + 0.9, f"{bw[i]:.1f}", ha="center", fontsize=13, bbox=dict(facecolor="white", edgecolor="none", pad=1.6))
        ax.text(i + w / 2, cr[i] + 0.9, f"{cr[i]:.1f}", ha="center", fontsize=13, bbox=dict(facecolor="white", edgecolor="none", pad=1.6))
    ax.set_xticks(x)
    ax.set_xticklabels(["trivial\n(echo)", "medium\n(shell loop)", "python\n(interpreter start)"])
    ax.set_ylabel("milliseconds to start")
    ax.set_ylim(0, max(cr) * 1.25)
    ax.legend(frameon=True, framealpha=1.0, edgecolor="none",
              fontsize=13, loc="upper left")
    ax.set_title("Sandbox start cost, median of 20 runs",
                 fontsize=15, loc="left")
    ax.grid(axis="y", alpha=0.25); ax.set_axisbelow(True)
    fig.savefig(OUT / "fig_5.3_sandbox_overhead.png"); plt.close(fig)


def _md_rows(text, first_cell):
    """Rows of the markdown table in text whose first cell is first_cell."""
    out = []
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[0] == first_cell:
            out.append(cells)
    return out


def fig_sync_modes():
    """5.4 - Two-rank ring under exact and fast synchronisation against one
    device (diloco.md). Left: wall time on Wi-Fi from the one-seed wall-time
    table. Right: mean validation-loss gap of fast sync over three seeds, from
    the loss table of the plain-averaging defaults."""
    md = (BENCH / "diloco.md").read_text()
    models = [("gpu", "MLP-G"), ("ddp04", "MLP-C"), ("tfm", "Transformer")]
    secs = lambda v: float(v.replace("s", "").strip())
    wall = {}
    for key, _ in models:
        row = [r for r in _md_rows(md, key) if len(r) == 7][-1]   # model | single | exact lan | exact wifi | fast lan | fast wifi | MiB
        wall[key] = (secs(row[1]), secs(row[3]), secs(row[5]))
    gaps = {}
    start = md.index("Every model passes under both profiles") if "Every model passes under both profiles" in md else len(md)
    loss = md[:start]
    for key, _ in models:
        for prof in ("lan", "wifi"):
            r = [x for x in _md_rows(loss, key) if len(x) > 7 and x[1] == "fast" and x[2] == prof][-1]
            gaps[(key, prof)] = float(r[7].replace("%", ""))
    x = np.arange(len(models)); w = 0.26
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(11.6, 4.2), gridspec_kw={"width_ratios": [1.3, 1], "wspace": 0.3})
    for i, (lab, col) in enumerate([("one device", "#9bb8dd"), ("exact, Wi-Fi", RED), ("fast, Wi-Fi", GREEN)]):
        vals = [wall[k][i] for k, _ in models]
        ax.bar(x + (i - 1) * w, vals, w, label=lab, color=col)
        for j, v in enumerate(vals):
            ax.text(j + (i - 1) * w, v + 0.8, f"{v:.0f}", ha="center", fontsize=11)
    ax.set_xticks(x); ax.set_xticklabels([n for _, n in models])
    ax.set_ylabel("seconds, end to end"); ax.set_ylim(0, max(max(v) for v in wall.values()) * 1.2)
    ax.legend(frameon=False, fontsize=12, loc="upper right", ncol=1)
    ax.grid(axis="y", alpha=0.25); ax.set_axisbelow(True)
    ax.set_title("Wall time, two ranks (one seed)", fontsize=15, loc="left")
    for i, (prof, col) in enumerate([("lan", BLUE), ("wifi", GREEN)]):
        vals = [gaps[(k, prof)] for k, _ in models]
        ax2.bar(x + (i - 0.5) * 0.36, vals, 0.36, label={"lan": "LAN", "wifi": "Wi-Fi"}[prof], color=col)
    ax2.axhline(1.0, color=GOLD, linestyle="--", linewidth=1.5)
    ax2.axhline(0.0, color="#333333", linewidth=0.8)
    ax2.text(2.45, 1.1, "1% bar", color=GOLD, fontsize=12, ha="right", va="bottom")
    ax2.set_xticks(x); ax2.set_xticklabels([n for _, n in models])
    ax2.set_ylabel("val. loss vs one device, %"); ax2.set_ylim(-3, 2)
    ax2.legend(frameon=False, fontsize=12, loc="lower right")
    ax2.grid(axis="y", alpha=0.25); ax2.set_axisbelow(True)
    ax2.set_title("Fast sync gap (mean of 3 seeds)", fontsize=15, loc="left")
    fig.savefig(OUT / "fig_5.4_sync_modes.png"); plt.close(fig)


if __name__ == "__main__":
    fig_closure_cache(); fig_shim_overhead(); fig_sandbox(); fig_sync_modes()
    print("wrote results figures")
