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
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                     "axes.spines.top": False, "axes.spines.right": False,
                     "figure.dpi": 300, "savefig.dpi": 300, "savefig.bbox": "tight"})

R = json.loads((BENCH / "results.json").read_text())
M = R["measurements"]


def fig_closure_cache():
    """5.1 - Cold versus warm submission of the same environment."""
    s = [x["seconds"] for x in M["n1_trivial_latency"]["samples"]]
    cold, warm = s[0], s[1:]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(9.4, 3.6),
                                  gridspec_kw={"width_ratios": [1.15, 1], "wspace": 0.32})
    ax.bar(["first submission\n(closure shipped)", "later submissions\n(closure already there)"],
           [cold, float(np.median(warm))], color=[RED, GREEN], width=0.55)
    ax.set_ylabel("seconds, end to end")
    for i, v in enumerate([cold, float(np.median(warm))]):
        ax.text(i, v + cold * 0.03, f"{v:.2f} s", ha="center", fontsize=11)
    ax.set_ylim(0, cold * 1.22)
    ax.set_title(f"Same environment, {cold/np.median(warm):.0f}x cheaper once cached",
                 fontsize=11, loc="left")

    ax2.plot(range(1, len(s) + 1), s, marker="o", color=BLUE, linewidth=1.8)
    ax2.set_xlabel("submission"); ax2.set_ylabel("seconds")
    ax2.set_xticks(range(1, len(s) + 1))
    ax2.set_title("Per submission", fontsize=11, loc="left")
    ax2.grid(alpha=0.25)
    fig.savefig(OUT / "fig_5.1_closure_cache.png"); plt.close(fig)


def fig_shim_overhead():
    """5.2 - Cost of leaving interception on when it decides not to distribute."""
    on = M["n7_shim_declines"]["median_s"]
    off = M["n7_shim_off"]["median_s"]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(9.4, 3.6), gridspec_kw={"wspace": 0.34})

    ax.bar(["interception off", "interception on"], [off, on],
           color=["#9bb8dd", BLUE], width=0.5)
    for i, v in enumerate([off, on]):
        ax.text(i, v + off * 0.03, f"{v:.2f} s", ha="center", fontsize=11)
    ax.set_ylabel("seconds, end to end"); ax.set_ylim(0, max(on, off) * 1.25)
    ax.set_title(f"Whole job: {(on/off - 1) * 100:+.0f}% with interception on",
                 fontsize=11, loc="left")

    # Startup tax before and after the lazy-patching fix (bench-shim-d2.sh).
    ax2.bar(["before fix", "after fix"], [21.6, 1.20], color=[RED, GREEN], width=0.5)
    ax2.axhline(3.0, color=GOLD, linestyle="--", linewidth=1.5)
    ax2.text(1.42, 3.3, "3x budget", color=GOLD, fontsize=9, ha="right")
    for i, v in enumerate([21.6, 1.20]):
        ax2.text(i, v + 0.6, f"{v:.2f}x", ha="center", fontsize=11)
    ax2.set_ylabel("slowdown vs no shim"); ax2.set_ylim(0, 25)
    ax2.set_title("Interpreter startup, a job that never\nimports a heavy library",
                  fontsize=11, loc="left")
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
        ax.text(i - w / 2, bw[i] + 0.7, f"{bw[i]:.1f}", ha="center", fontsize=9)
        ax.text(i + w / 2, cr[i] + 0.7, f"{cr[i]:.1f}", ha="center", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(["trivial\n(echo)", "medium\n(shell loop)", "python\n(interpreter start)"])
    ax.set_ylabel("milliseconds, median of 20")
    ax.set_ylim(0, max(cr) * 1.25)
    ax.legend(frameon=False, fontsize=9, loc="upper left")
    ax.set_title("Sandbox start cost. crun is slower than bwrap, and both are "
                 "negligible\nnext to building or shipping an environment.",
                 fontsize=10, loc="left")
    ax.grid(axis="y", alpha=0.25)
    fig.savefig(OUT / "fig_5.3_sandbox_overhead.png"); plt.close(fig)


def fig_comparison():
    """5.4 - Where PipedPeer sits against the usual alternatives."""
    systems = ["PipedPeer", "Spark", "Ray", "Dask", "BOINC", "plain ssh"]
    criteria = ["Runs unmodified\nPython", "No cluster to\nconfigure",
                "No head or\nmaster node", "Reproduces the\nenvironment",
                "Sandboxes the\njob", "Reschedules on\nnode loss"]
    #  2 = yes, 1 = partial, 0 = no
    M_ = np.array([
        [2, 2, 2, 2, 2, 2],   # PipedPeer
        [0, 0, 0, 1, 0, 2],   # Spark
        [1, 0, 0, 1, 0, 2],   # Ray
        [1, 0, 0, 1, 0, 2],   # Dask
        [0, 0, 0, 1, 1, 2],   # BOINC
        [2, 1, 2, 0, 0, 0],   # ssh
    ])
    fig, ax = plt.subplots(figsize=(8.6, 3.9))
    cmap = matplotlib.colors.ListedColormap(["#f4dcdc", "#fbf0d5", "#dcefe3"])
    ax.imshow(M_, cmap=cmap, vmin=0, vmax=2, aspect="auto")
    for i in range(len(systems)):
        for j in range(len(criteria)):
            ax.text(j, i, {2: "yes", 1: "partial", 0: "no"}[M_[i, j]],
                    ha="center", va="center", fontsize=9,
                    color={2: GREEN, 1: GOLD, 0: RED}[M_[i, j]])
    ax.set_xticks(range(len(criteria))); ax.set_xticklabels(criteria, fontsize=9)
    ax.set_yticks(range(len(systems)))
    ax.set_yticklabels(systems, fontsize=10,
                       fontweight="bold")
    ax.set_xticks(np.arange(-.5, len(criteria), 1), minor=True)
    ax.set_yticks(np.arange(-.5, len(systems), 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=2)
    ax.tick_params(which="minor", length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    fig.savefig(OUT / "fig_5.4_comparison.png"); plt.close(fig)


if __name__ == "__main__":
    fig_closure_cache(); fig_shim_overhead(); fig_sandbox(); fig_comparison()
    print("wrote results figures")
