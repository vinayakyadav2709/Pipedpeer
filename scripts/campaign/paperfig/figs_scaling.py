"""E1 and E1b: the frontier, strong and weak scaling, wall times and
parallel efficiency."""
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

from . import style as st
from .data import n_reps
from .registry import fig, need

SEC_E2E = "Evaluation > End-to-end runs (Q3)"


def e1_rows(lab):
    return [r for r in lab.rows("e1") if r.get("median_wall_s") and r.get("mode", "strong") != "weak"]


def base_wall(rows, wl):
    return next((r["median_wall_s"] for r in rows if r["workload"] == wl and r["system"] == "plain"
                 and r["n"] == 1), None)


def speedup_band(r, base):
    """(median, low, high) speedup over plain at N = 1; the band is the
    wall's IQR carried through base / wall."""
    m = base / r["median_wall_s"]
    return m, base / (r.get("q3_wall_s") or r["median_wall_s"]), base / (r.get("q1_wall_s") or r["median_wall_s"])


def workloads(rows):
    return sorted({r["workload"] for r in rows})


def ns_of(rows):
    return sorted({r["n"] for r in rows if r.get("n")})


@fig("eval-frontier", "single", 2.15, SEC_E2E,
     "Lines changed to distribute each workload against the speedup it bought at the largest N.",
     "Porting cost against payoff. Each point is one workload on one system at the largest N; "
     "the vertical bar is the interquartile range over repeats. PipedPeer runs the unmodified script "
     "(zero lines changed); Ray and Dask need a port.")
def frontier(lab, ctx):
    rows = e1_rows(lab)
    need(rows and lab.port_lines, "needs e1 runs and port-lines.csv")
    nmax = max(r["n"] for r in rows)
    pts = []
    for r in rows:
        b = base_wall(rows, r["workload"])
        key = (r["workload"], r["system"])
        if r["n"] == nmax and r["system"] in st.DIST and b and key in lab.port_lines:
            pts.append((lab.port_lines[key], speedup_band(r, b), r))
    need(pts, "no speedup at N=%d with port lines" % nmax)
    f, ax = st.figure("single", 2.15)
    ax = ax[0][0]
    xs_max = max(p[0] for p in pts)
    ax.axvspan(-0.8, 0.8, color=st.tint(st.SYS["pipedpeer"].color, 0.08), lw=0, zorder=0)
    ax.text(0, 1.0, "unmodified\nscript", transform=ax.get_xaxis_transform(), ha="center", va="top",
            fontsize=6.5, color=st.SYS["pipedpeer"].color, linespacing=0.95)
    ax.axhline(1.0, color=st.RULE, lw=0.6, zorder=1)
    ax.axhline(nmax, color=st.MUTED, lw=0.6, ls=(0, (1, 1.5)), zorder=1)
    ax.text(xs_max + 1.6, nmax, "ideal %d×" % nmax, ha="right", va="bottom", fontsize=6.5, color=st.INK2)
    ax.text(xs_max + 1.6, 1.0, "plain, 1 PC", ha="right", va="bottom", fontsize=6.5, color=st.INK2)
    by_x = {}
    for x, band, r in pts:
        by_x.setdefault(x, []).append((band, r))
    labels = []
    for x, grp in by_x.items():
        grp.sort(key=lambda g: (st.order_key(g[1]["system"]), g[1]["workload"]))
        for i, ((m, lo, hi), r) in enumerate(grp):
            px = x + (i - (len(grp) - 1) / 2) * 0.5
            ax.plot([px] * 2, [lo, hi], color=st.SYS[r["system"]].color, lw=0.8, zorder=2)
            ax.plot([px], [m], zorder=3, **st.point_kw(r["system"], 4.2))
            labels.append((px, m, r["workload"]))
    ax.set_xlim(-1, xs_max + 1.8)
    top = max(nmax, max(p[1][2] for p in pts))
    ax.set_ylim(0, top * 1.22)
    st.int_y(ax)
    ax.set_xlabel("lines of the script changed")
    ax.set_ylabel("speedup at N = %d (×)" % nmax)
    st.ygrid(ax)
    st.legend_top(f, st.DIST, top=0.9)
    st.place_labels(ax, sorted(labels, key=lambda p: -p[1]))
    return f


@fig("eval-strong-scaling", "double", 1.72, SEC_E2E,
     "Strong scaling of W1-W4: speedup over plain Python on one PC against N, log2 x axis, shared y.",
     "Strong scaling. Speedup over plain Python on one PC (median; bars are the IQR over repeats) "
     "against the number of PCs N. The dotted line is ideal linear speedup; the grey line is parity "
     "with plain Python. Panels share the y axis.")
def strong(lab, ctx):
    rows = e1_rows(lab)
    wls = workloads(rows)
    need(wls, "needs e1 runs")
    ns = ns_of(rows)
    f, axes = st.figure("double", 1.72, 1, len(wls), sharey=True)
    ymax = max(max(ns), max(speedup_band(r, base_wall(rows, r["workload"]))[2] for r in rows
                            if base_wall(rows, r["workload"]) and r["system"] in st.DIST))
    for j, (ax, wl) in enumerate(zip(axes[0], wls)):
        b = base_wall(rows, wl)
        ax.plot(ns, ns, color=st.MUTED, lw=0.6, ls=(0, (1, 1.5)), zorder=1)
        ax.axhline(1.0, color=st.RULE, lw=0.6, zorder=1)
        ends = []
        for s in st.DIST:
            rs = sorted((r for r in rows if r["workload"] == wl and r["system"] == s), key=lambda r: r["n"])
            if not rs or not b:
                continue
            band = [speedup_band(r, b) for r in rs]
            x = [r["n"] for r in rs]
            ax.errorbar(x, [m for m, _, _ in band], yerr=[[m - lo for m, lo, _ in band], [hi - m for m, _, hi in band]],
                        elinewidth=0.6, capsize=1.5, capthick=0.6, zorder=3, **st.line_kw(s))
            ends.append((x[-1], band[-1][0], st.label(s), st.SYS[s].color))
        st.log2_x(ax, ns)
        ax.set_xlim(min(ns) / 1.3, max(ns) * (2.4 if j == len(wls) - 1 else 1.3))
        st.ygrid(ax)
        st.panel_tag(ax, "(%s) %s" % ("abcdefgh"[j], wl))
        ax.set_xlabel("PCs (N)")
        if j == len(wls) - 1:
            st.end_labels(ax, ends)
            ax.text(max(ns) * 1.06, max(ns), "ideal", fontsize=6.5, color=st.INK2, va="center")
    axes[0][0].set_ylabel("speedup (×)")
    axes[0][0].set_ylim(0, ymax * 1.15)
    st.int_y(axes[0][0])
    st.note(axes[0][0], 0.03, 0.97, n_reps(rows), va="top", fontsize=6.5)
    return f


@fig("eval-weak-scaling", "single", 1.75, SEC_E2E,
     "Weak scaling of W1: efficiency T(1)/T(N) when the work grows with N.",
     "Weak scaling of W1: the work grows with N, so a perfect system holds efficiency T(1)/T(N) at 1.0 "
     "(dotted). Median and IQR over repeats.")
def weak(lab, ctx):
    rows = [r for r in lab.rows("e1b") if r.get("weak_efficiency") and r["system"] in st.DIST]
    need(rows, "needs e1b runs")
    ns = ns_of(rows)
    wl = sorted({r["workload"] for r in rows})[0]
    f, ax = st.figure("single", 1.75)
    ax = ax[0][0]
    ax.axhline(1.0, color=st.MUTED, lw=0.6, ls=(0, (1, 1.5)))
    ends = []
    for s in st.DIST:
        rs = sorted((r for r in rows if r["system"] == s and r["workload"] == wl), key=lambda r: r["n"])
        if not rs:
            continue
        t1 = rs[0]["median_wall_s"] / rs[0]["weak_efficiency"]
        m = [r["weak_efficiency"] for r in rs]
        lo = [t1 / (r.get("q3_wall_s") or r["median_wall_s"]) for r in rs]
        hi = [t1 / (r.get("q1_wall_s") or r["median_wall_s"]) for r in rs]
        x = [r["n"] for r in rs]
        ax.errorbar(x, m, yerr=[np.subtract(m, lo), np.subtract(hi, m)], elinewidth=0.6, capsize=1.5,
                    capthick=0.6, **st.line_kw(s))
        ends.append((x[-1], m[-1], st.label(s), st.SYS[s].color))
    st.log2_x(ax, ns)
    ax.set_xlim(min(ns) / 1.3, max(ns) * 2.2)
    ax.set_ylim(0, 1.15)
    st.end_labels(ax, ends)
    st.ygrid(ax)
    ax.set_xlabel("PCs (N), work per PC fixed")
    ax.set_ylabel("efficiency T(1)/T(N)")
    st.note(ax, 0.02, 0.04, "%s, %s" % (wl, n_reps(rows)), fontsize=6.5)
    return f


@fig("eval-wall-time", "double", 1.85, SEC_E2E,
     "Wall time of every workload, system and N: median, IQR and each repeat.",
     "Wall time per workload, system and N. The marker is the median, the bar the IQR, and the small "
     "dots are the individual repeats; the dotted line is plain Python on one PC. Lower is better; "
     "log y axis, each panel with its own range.")
def wall(lab, ctx):
    rows = [r for r in e1_rows(lab) if r["system"] in st.DIST]
    wls = workloads(rows)
    need(wls, "needs e1 runs")
    allrows = e1_rows(lab)
    ns = ns_of(rows)
    f, axes = st.figure("double", 1.85, 1, len(wls))
    runs = lab.exp("e1")
    off = {s: (i - 1) * 0.24 for i, s in enumerate(st.DIST)}
    for j, (ax, wl) in enumerate(zip(axes[0], wls)):
        b = base_wall(allrows, wl)
        if b:
            ax.axhline(b, color=st.MUTED, lw=0.6, ls=(0, (1, 1.5)), zorder=1)
        top = b or 0
        for s in st.DIST:
            for r in (r for r in rows if r["workload"] == wl and r["system"] == s):
                x = ns.index(r["n"]) + off[s]
                reps = [x2["wall_s"] for x2 in runs if x2["workload"] == wl and x2["system"] == s
                        and x2["n"] == r["n"] and x2.get("wall_s")]
                ax.plot([x + 0.07] * len(reps), reps, ls="none", marker=".", ms=1.6, color=st.SYS[s].color,
                        alpha=0.55, zorder=2)
                ax.plot([x, x], [r["q1_wall_s"], r["q3_wall_s"]], color=st.SYS[s].color, lw=1.0, zorder=3)
                ax.plot([x], [r["median_wall_s"]], zorder=4, **st.point_kw(s, 3.8))
                top = max([top, r["q3_wall_s"]] + reps)
        ax.set_xticks(range(len(ns)))
        ax.set_xticklabels([str(n) for n in ns])
        ax.set_xlim(-0.6, len(ns) - 0.4)
        lo = min(x2["wall_s"] for x2 in runs if x2["workload"] == wl and x2.get("wall_s"))
        ax.set_yscale("log")
        ax.set_ylim(lo / 1.5, top * 1.5)
        st.plain_log(ax, "y")
        st.ygrid(ax)
        st.panel_tag(ax, "(%s) %s" % ("abcdefgh"[j], wl))
        ax.set_xlabel("PCs (N)")
        if b and j == 0:
            ax.annotate("plain, 1 PC", (len(ns) - 0.45, b), xytext=(0, 1.5), textcoords="offset points",
                        ha="right", va="bottom", fontsize=6, color=st.INK2)
    axes[0][0].set_ylabel("wall time (s)")
    st.legend_top(f, st.DIST, top=0.9)
    return f


@fig("eval-efficiency-heatmap", "single", 1.55, SEC_E2E,
     "Parallel efficiency speedup/N for each system and workload at each N.",
     "Parallel efficiency (speedup over plain Python divided by N) for every system, workload and N. "
     "1.00 is linear scaling. Darker is better; values are printed so the figure reads in greyscale.")
def efficiency_heatmap(lab, ctx):
    rows = [r for r in e1_rows(lab) if r["system"] in st.DIST and r.get("speedup_vs_plain")]
    need(rows, "needs e1 speedups")
    ns, wls = ns_of(rows), workloads(rows)
    keys = [(s, w) for s in st.DIST for w in wls if any(r["system"] == s and r["workload"] == w for r in rows)]
    grid = np.full((len(keys), len(ns)), np.nan)
    for r in rows:
        grid[keys.index((r["system"], r["workload"])), ns.index(r["n"])] = r["speedup_vs_plain"] / r["n"]
    h = 0.4 + 0.125 * len(keys)
    f, ax = st.figure("single", max(1.3, h))
    ax = ax[0][0]
    cmap = LinearSegmentedColormap.from_list("eff", ["#F4F8FB", "#9CC3DE", "#3E86BD", "#0B4A7A"])
    im = ax.imshow(grid, cmap=cmap, vmin=0, vmax=1.0, aspect="auto")
    for i in range(len(keys)):
        for k in range(len(ns)):
            v = grid[i, k]
            if not np.isnan(v):
                ax.text(k, i, "%.2f" % v, ha="center", va="center", fontsize=6.5,
                        color="white" if v > 0.55 else st.INK)
    ax.set_xticks(range(len(ns)))
    ax.set_xticklabels(["N = %d" % n for n in ns])
    ax.xaxis.tick_top()
    ax.set_yticks(range(len(keys)))
    ax.set_yticklabels(["%s  %s" % (st.label(s), w) for s, w in keys])
    ax.tick_params(length=0)
    for sp in ax.spines.values():
        sp.set_visible(False)
    for i in range(1, len(keys)):
        if keys[i][0] != keys[i - 1][0]:
            ax.axhline(i - 0.5, color="white", lw=1.6)
    cb = f.colorbar(im, ax=ax, fraction=0.05, pad=0.02, aspect=12)
    cb.outline.set_visible(False)
    cb.ax.tick_params(labelsize=6, length=1.5, width=0.4)
    cb.set_label("speedup / N", fontsize=6.5)
    return f
