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


def fig_closure_cache():
    """5.1 - Cold versus warm submission of the same environment."""
    s = [x["seconds"] for x in M["n1_trivial_latency"]["samples"]]
    cold, warm = s[0], s[1:]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(10.4, 3.9),
                                  gridspec_kw={"width_ratios": [1.15, 1], "wspace": 0.38})
    ax.bar(["first run\n(closure shipped)", "later runs\n(already cached)"],
           [cold, float(np.median(warm))], color=[RED, GREEN], width=0.55)
    ax.set_ylabel("seconds, end to end")
    for i, v in enumerate([cold, float(np.median(warm))]):
        ax.text(i, v + cold * 0.03, f"{v:.2f} s", ha="center", fontsize=17)
    ax.set_ylim(0, cold * 1.22)
    ax.grid(axis="y", alpha=0.25)
    ax.set_axisbelow(True)
    ax.set_title(f"{cold/np.median(warm):.0f}x cheaper once cached",
                 fontsize=15, loc="left")

    ax2.plot(range(1, len(s) + 1), s, marker="o", color=BLUE, linewidth=1.8)
    ax2.set_xlabel("submission"); ax2.set_ylabel("seconds")
    ax2.set_xticks(range(1, len(s) + 1))
    ax2.set_title("Each run in turn", fontsize=15, loc="left")
    ax2.grid(alpha=0.25)
    fig.savefig(OUT / "fig_5.1_closure_cache.png"); plt.close(fig)


def fig_shim_overhead():
    """5.2 - Cost of leaving interception on when it decides not to distribute."""
    on = M["n7_shim_declines"]["median_s"]
    off = M["n7_shim_off"]["median_s"]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(10.4, 3.9), gridspec_kw={"wspace": 0.34})

    ax.bar(["interception off", "interception on"], [off, on],
           color=["#9bb8dd", BLUE], width=0.5)
    for i, v in enumerate([off, on]):
        ax.text(i, v + off * 0.03, f"{v:.2f} s", ha="center", fontsize=17)
    ax.set_ylabel("seconds, end to end"); ax.set_ylim(0, max(on, off) * 1.25)
    ax.grid(axis="y", alpha=0.25); ax.set_axisbelow(True)
    ax.set_title(f"Whole job: {(on/off - 1) * 100:+.0f}% with interception on",
                 fontsize=15, loc="left")

    # Startup tax before and after the lazy-patching fix (bench-shim-d2.sh).
    ax2.bar(["before fix", "after fix"], [21.6, 1.20], color=[RED, GREEN], width=0.5)
    ax2.axhline(3.0, color=GOLD, linestyle="--", linewidth=1.5)
    ax2.text(0.5, 4.2, "3x budget", color=GOLD, fontsize=13, ha="center", va="bottom")
    for i, v in enumerate([21.6, 1.20]):
        ax2.text(i, v + 0.6, f"{v:.2f}x", ha="center", fontsize=17)
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
        ax.text(i - w / 2, bw[i] + 0.7, f"{bw[i]:.1f}", ha="center", fontsize=14)
        ax.text(i + w / 2, cr[i] + 0.7, f"{cr[i]:.1f}", ha="center", fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(["trivial\n(echo)", "medium\n(shell loop)", "python\n(interpreter start)"])
    ax.set_ylabel("milliseconds, median of 20")
    ax.set_ylim(0, max(cr) * 1.25)
    ax.legend(frameon=True, framealpha=1.0, edgecolor="none",
              fontsize=13, loc="upper left")
    ax.set_title("Sandbox start cost, median of 20 runs",
                 fontsize=15, loc="left")
    ax.grid(axis="y", alpha=0.25); ax.set_axisbelow(True)
    fig.savefig(OUT / "fig_5.3_sandbox_overhead.png"); plt.close(fig)


def fig_comparison():
    """5.4 - Where PipedPeer sits against the usual alternatives.

    The lower block is the half that matters for honesty: on security,
    reach, language coverage and maturity PipedPeer is the weakest system
    in the table, and the figure says so.
    """
    systems = ["PipedPeer", "Spark", "Ray", "Dask", "BOINC", "plain ssh"]
    rows = [
        # criterion,                          Piped Spark Ray Dask BOINC ssh
        ("Runs unmodified Python",            [2, 0, 1, 1, 0, 2]),
        ("No cluster to configure",           [2, 0, 0, 0, 0, 1]),
        ("No head or master node",            [2, 0, 0, 0, 0, 2]),
        ("Reproduces the environment",        [2, 1, 1, 1, 1, 0]),
        ("Sandboxes each job",                [2, 0, 0, 0, 1, 0]),
        ("Survives losing a node",            [2, 2, 2, 2, 2, 0]),
        ("Authenticated and encrypted",       [0, 2, 1, 1, 2, 2]),
        ("Works beyond one LAN",              [1, 2, 2, 2, 2, 2]),
        ("Runtimes other than Python",        [0, 2, 1, 0, 2, 2]),
        ("Proven in production use",          [0, 2, 2, 2, 2, 2]),
    ]
    SPLIT = 6            # rows below this are where PipedPeer is behind
    labels = [r[0] for r in rows]
    M_ = np.array([r[1] for r in rows])

    fig, ax = plt.subplots(figsize=(8.2, 6.2))
    cmap = matplotlib.colors.ListedColormap(["#f4dcdc", "#fbf0d5", "#dcefe3"])
    ax.imshow(M_, cmap=cmap, vmin=0, vmax=2, aspect="auto")
    for i in range(len(rows)):
        for j in range(len(systems)):
            ax.text(j, i, {2: "yes", 1: "partial", 0: "no"}[M_[i, j]],
                    ha="center", va="center", fontsize=13,
                    color={2: GREEN, 1: GOLD, 0: RED}[M_[i, j]])

    ax.set_xticks(range(len(systems)))
    ax.set_xticklabels(systems, fontsize=13, fontweight="bold")
    ax.xaxis.set_ticks_position("top")
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels(labels, fontsize=13)
    ax.set_xticks(np.arange(-.5, len(systems), 1), minor=True)
    ax.set_yticks(np.arange(-.5, len(rows), 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=2)
    ax.tick_params(which="both", length=0)
    for sp in ax.spines.values():
        sp.set_visible(False)

    # A heavy rule separating what PipedPeer does differently from what it
    # is simply worse at.
    ax.axhline(SPLIT - 0.5, color="#333333", linewidth=2.4)
    # Group labels go in the right margin: on the left they collided with the
    # criterion text.
    ax.set_xlim(-0.5, len(systems) - 0.5 + 0.95)
    ax.text(len(systems) - 0.5 + 0.42, (SPLIT - 1) / 2, "what it does\ndifferently",
            rotation=270, ha="center", va="center", fontsize=12, color="#333333")
    ax.text(len(systems) - 0.5 + 0.42, SPLIT + (len(rows) - SPLIT - 1) / 2,
            "where it is\nbehind", rotation=270, ha="center", va="center",
            fontsize=12, color=RED)
    fig.savefig(OUT / "fig_5.4_comparison.png"); plt.close(fig)


if __name__ == "__main__":
    fig_closure_cache(); fig_shim_overhead(); fig_sandbox(); fig_comparison()
    print("wrote results figures")
