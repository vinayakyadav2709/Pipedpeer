"""Design-chapter plots for the PipedPeer report.

Every constant here is transcribed from the implementation; the source of
each is named in the comment above it so the figure can be re-checked.
Run:  python3 docs/diagrams/src/plots_design.py
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle
import numpy as np
import pathlib

OUT = pathlib.Path(__file__).resolve().parents[1]
BLUE, GREEN, RED, GOLD, GREY = "#1e50a0", "#2f7d4f", "#a33333", "#8a6d1f", "#666666"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 15,
    "axes.spines.top": False, "axes.spines.right": False,
    "figure.dpi": 300, "savefig.dpi": 300, "savefig.bbox": "tight",
})


def fig_sequence():
    """3.5 - UML-style sequence for one job."""
    actors = ["User\n(CLI)", "Submitting\ndaemon", "Chosen\nworker", "Peer\nworkers"]
    x = np.arange(len(actors)) * 3.0
    steps = [
        (0, 1, "run script.py", False),
        (1, 1, "scan imports, build Nix closure", None),
        (1, 1, "estimate memory requirement", None),
        (1, 2, "accept (capacity check)", False),
        (2, 1, "lease granted", True),
        (1, 2, "commit lease", False),
        (1, 2, "upload workspace, closure if absent", False),
        (2, 3, "broadcast closure to peers", False),
        (1, 2, "open execution stream", False),
        (2, 2, "run in crun sandbox", None),
        (2, 3, "distribute parallel work", False),
        (3, 2, "partial results", True),
        (2, 1, "stdout and stderr, line by line", True),
        (2, 1, "changed files, deletion manifest", True),
        (1, 0, "results in the working directory", True),
    ]
    fig, ax = plt.subplots(figsize=(9.2, 9.6))
    top, dy = 0.0, -0.74
    for i, a in enumerate(actors):
        ax.add_patch(FancyBboxPatch((x[i] - 1.32, top), 2.64, 0.66,
                     boxstyle="round,pad=0.04", facecolor="#eef3fb",
                     edgecolor=BLUE, linewidth=1.2))
        ax.text(x[i], top + 0.33, a, ha="center", va="center", fontsize=13)
        ax.plot([x[i], x[i]], [top, top + dy * (len(steps) + 1.2)],
                color=GREY, linewidth=0.9, linestyle=(0, (4, 4)), zorder=0)
    for k, (src, dst, label, back) in enumerate(steps):
        y = top + dy * (k + 1.1)
        if src == dst:                                  # self call
            w = 0.62
            ax.plot([x[src], x[src] + w, x[src] + w, x[src] + 0.04],
                    [y, y, y - 0.20, y - 0.20], color=GOLD, linewidth=1.3)
            ax.annotate("", xy=(x[src] + 0.04, y - 0.20), xytext=(x[src] + 0.22, y - 0.20),
                        arrowprops=dict(arrowstyle="-|>", color=GOLD, linewidth=1.3))
            ax.text(x[src] + w + 0.16, y - 0.10, label, va="center", fontsize=13,
                    color=GOLD, zorder=6,
                    bbox=dict(facecolor="white", edgecolor="none", pad=1.4))
        else:
            colour = GREEN if back else BLUE
            ax.annotate("", xy=(x[dst], y), xytext=(x[src], y),
                        arrowprops=dict(arrowstyle="-|>", color=colour, linewidth=1.4,
                                        linestyle="--" if back else "-"))
            ax.text((x[src] + x[dst]) / 2, y + 0.09, label, ha="center",
                    va="bottom", fontsize=13, color=colour, zorder=6,
                    bbox=dict(facecolor="white", edgecolor="none", pad=1.4))
    ax.set_xlim(-1.8, x[-1] + 3.4); ax.set_ylim(top + dy * (len(steps) + 1.6), 0.85)
    ax.axis("off")
    fig.savefig(OUT / "fig_3.5_sequence.png"); plt.close(fig)


def fig_scoring():
    """4.2 - How each term moves a node's score. Constants: coordinator.scoreNode."""
    fig, (axl, axr) = plt.subplots(1, 2, figsize=(9.6, 3.8), gridspec_kw={"wspace": 0.34})

    # Load penalties and hardware bonuses, at their extremes.
    terms = ["CPU load", "Memory load", "Active jobs\n(per job)",
             "Core count", "Clock speed", "Free memory", "Free core ratio"]
    lo = [-0.50, -0.50, -0.05, -0.05, -0.03, -0.05, -0.05]
    hi = [0.0, 0.0, 0.0, 0.05, 0.03, 0.05, 0.05]
    y = np.arange(len(terms))[::-1]
    for yi, a, b in zip(y, lo, hi):
        axl.barh(yi, b - a, left=a, height=0.55,
                 color=(RED if b <= 0 else GREEN), alpha=0.75,
                 edgecolor="white", linewidth=0.8)
    axl.axvline(0, color="#333333", linewidth=1.0)
    axl.set_yticks(y); axl.set_yticklabels(terms, fontsize=14)
    axl.set_xlabel("contribution to the score")
    axl.set_title("Range of each term", fontsize=17, loc="left")
    axl.set_xlim(-0.58, 0.13)
    axl.spines["left"].set_visible(False)
    axl.grid(axis="x", alpha=0.25)

    # Worked example: an idle strong node against a loaded weak one.
    labels = ["base", "CPU", "memory", "jobs", "hardware", "final"]
    idle = [1.0, -0.02, -0.05, 0.0, +0.11]
    busy = [1.0, -0.42, -0.35, -0.15, -0.06]
    for series, colour, name in ((idle, GREEN, "idle, strong node"),
                                 (busy, RED, "loaded, weak node")):
        run = series[0]
        vals = [series[0]]
        for d in series[1:]:
            run += d; vals.append(run)
        axr.plot(np.arange(len(vals)), vals, marker="o", color=colour,
                 linewidth=1.8, markersize=5)
        axr.annotate(f"{name}\nfinal {vals[-1]:.2f}", (len(vals) - 1, vals[-1]),
                     textcoords="offset points", xytext=(-6, 14 if colour == GREEN else 16),
                     ha="right", fontsize=14, color=colour)
    axr.set_xticks(np.arange(len(labels) - 1)); axr.set_xticklabels(labels[:-1], fontsize=14)
    axr.set_ylabel("running score"); axr.set_ylim(0, 1.45)
    axr.set_title("Two nodes scored", fontsize=17, loc="left")
    axr.grid(axis="y", alpha=0.25)
    fig.savefig(OUT / "fig_4.2_scoring_weights.png"); plt.close(fig)


def fig_race():
    """4.7 - Interleaved ownership and straggler re-execution."""
    fig, ax = plt.subplots(figsize=(9.4, 3.6))
    n = 8
    local_chunks = list(range(0, n, 2))
    remote_chunks = list(range(1, n, 2))
    # remote chunk 5 never returns; local re-runs it once it is idle.
    dead = 5
    for c in local_chunks:
        ax.barh(1, 1.0, left=c * 0.55, height=0.42, color=GREEN, alpha=0.8,
                edgecolor="white")
        ax.text(c * 0.55 + 0.5, 1, f"c{c}", ha="center", va="center",
                color="white", fontsize=14)
    for c in remote_chunks:
        ok = c != dead
        ax.barh(0, 1.0, left=c * 0.55, height=0.42,
                color=(BLUE if ok else "#cccccc"), alpha=0.85, edgecolor="white")
        ax.text(c * 0.55 + 0.5, 0, f"c{c}" if ok else f"c{c} lost",
                ha="center", va="center", color=("white" if ok else "#555555"), fontsize=14)
    redo_x = (n - 1) * 0.55 + 1.15
    ax.barh(1, 1.0, left=redo_x, height=0.42, color=GOLD, alpha=0.9, edgecolor="white")
    ax.text(redo_x + 0.5, 1, f"c{dead} re-run", ha="center", va="center",
            color="white", fontsize=14)
    ax.annotate("", xy=(redo_x, 0.78), xytext=(dead * 0.55 + 0.5, 0.22),
                arrowprops=dict(arrowstyle="-|>", color=GOLD, linewidth=1.4,
                                linestyle="--", connectionstyle="arc3,rad=-0.25"))
    ax.text(redo_x + 0.55, 0.55, "idle local cores pick up\nany chunk still missing",
            fontsize=14, color=GOLD, va="center")
    ax.set_yticks([0, 1]); ax.set_yticklabels(["cluster", "local"])
    ax.tick_params(which="both", length=0)
    ax.set_xlabel("time")
    ax.set_xticks([])
    ax.set_ylim(-0.45, 1.55); ax.set_xlim(-0.12, redo_x + 2.7)
    ax.set_title("Alternate chunks are owned by each side; the local side is "
                 "always able to finish alone", fontsize=16, loc="left")
    ax.spines["left"].set_visible(False); ax.spines["bottom"].set_visible(False)
    fig.savefig(OUT / "fig_4.7_race_timeline.png"); plt.close(fig)


def fig_cost_model():
    """4.9 - Decision regions of the cost model. Source: shim _should_spill."""
    K, BW = 3, 500e6          # 3 nodes, 500 MB/s link
    MIN_BYTES = 32 * 1024**2  # hard floor
    LOW_F, LOW_CAP = 8, 512 * 1024**2   # low-intensity carve-out

    mb = np.logspace(np.log10(4), np.log10(4000), 600)      # payload, MB
    f = np.logspace(np.log10(0.5), np.log10(2000), 600)     # flop per byte
    MB, F = np.meshgrid(mb, f)
    nb = MB * 1024**2

    est_local = nb * F / 1e9
    est_transfer = nb / BW
    est_remote = est_local / K * 1.3
    spill = est_local > est_transfer + est_remote
    spill &= nb >= MIN_BYTES
    spill &= ~((F < LOW_F) & (nb <= LOW_CAP))

    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    ax.contourf(MB, F, spill.astype(float), levels=[-0.5, 0.5, 1.5],
                colors=["#f6e7e7", "#e2f0e8"])
    ax.contour(MB, F, spill.astype(float), levels=[0.5], colors=[GREEN], linewidths=1.8)

    ax.text(8, 500, "kept local", color=RED, fontsize=19, weight="bold")
    ax.text(700, 700, "distributed", color=GREEN, fontsize=19, weight="bold")
    ax.annotate("payload below 32 MB", xy=(20, 60), fontsize=13, color=RED, rotation=90,
                ha="center", va="center")
    ax.annotate("under 8 flop/byte and up to 512 MB:\ntoo little work per byte moved",
                xy=(4.6, 0.78), fontsize=13, color=RED, va="center", ha="left")

    for name, fv, mbv in (("pandas join", 2, 900), ("pandas groupby", 5.5, 900),
                          ("dense matmul", 200, 300), ("SVD", 1.5, 300)):
        ax.plot(mbv, fv, "o", color="#1e50a0", markersize=7, zorder=5)
        off = {"pandas join": (12, -5), "pandas groupby": (12, -5),
               "dense matmul": (12, -5), "SVD": (12, -5)}[name]
        ax.annotate(name, (mbv, fv), textcoords="offset points", xytext=off,
                    ha="left", fontsize=13, color="#1e50a0")

    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("payload size (MB, log scale)")
    ax.set_ylabel("arithmetic intensity (flop per byte, log scale)")
    ax.set_xlim(4, 9000); ax.set_ylim(0.5, 2000)
    ax.grid(alpha=0.2, which="both")
    ax.set_title("Decision regions at three nodes on a 500 MB/s link",
                 fontsize=17, loc="left")
    fig.savefig(OUT / "fig_4.9_cost_model.png"); plt.close(fig)


if __name__ == "__main__":
    fig_sequence(); fig_scoring(); fig_race(); fig_cost_model()
    print("wrote design figures")
