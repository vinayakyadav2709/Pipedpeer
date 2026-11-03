"""Print-ready paper figures for sample_figures.py (PNG at 300 dpi and PDF).

Every function takes the model sample_figures.build_model() returns and
returns True when it drew, or a reason string when its data is missing.
Widths: SINGLE (3.5 in, one column) or DOUBLE (7.16 in, both columns).
Colour follows the system (sample_figures.LIGHT); each system also has its
own marker, so identity never rests on colour alone.
"""
import json
import os

from sample_figures import (AXIS, BLUES, DOUBLE, GRID, INK, INK2, LABEL, LIGHT, MARKER, MUTED, SINGLE,
                            family)

DIST = ["pipedpeer", "ray", "dask"]


def plt_module():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Liberation Sans", "Arial", "Helvetica", "DejaVu Sans"],
        "font.size": 7.5, "axes.titlesize": 8, "axes.labelsize": 7.5, "xtick.labelsize": 7,
        "ytick.labelsize": 7, "legend.fontsize": 7, "axes.edgecolor": AXIS, "axes.linewidth": 0.6,
        "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK2,
        "ytick.labelcolor": INK2, "xtick.major.width": 0.6, "ytick.major.width": 0.6,
        "xtick.major.size": 2.5, "ytick.major.size": 2.5, "axes.grid": True, "grid.color": GRID,
        "grid.linewidth": 0.5, "grid.linestyle": "-", "axes.spines.top": False, "axes.spines.right": False,
        "axes.axisbelow": True, "legend.frameon": False, "figure.facecolor": "white",
        "axes.facecolor": "white", "savefig.dpi": 300, "savefig.bbox": "tight", "savefig.pad_inches": 0.03,
        "pdf.fonttype": 42, "ps.fonttype": 42, "axes.titlelocation": "left", "axes.titlecolor": INK,
        "axes.titleweight": "bold", "lines.solid_capstyle": "round", "lines.solid_joinstyle": "round",
    })
    return plt


PLT = None


def save(fig, out, name, made, label):
    if label:
        # Below everything already drawn (x labels included), right-aligned.
        fig.canvas.draw()
        bb = fig.get_tightbbox(fig.canvas.get_renderer())
        h = fig.get_size_inches()[1]
        fig.text(bb.x1 / fig.get_size_inches()[0], (bb.y0 - 0.03) / h, label, ha="right", va="top",
                 fontsize=5.5, color=MUTED)
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(out, "%s.%s" % (name, ext)))
    PLT.close(fig)
    made.append(name)


def series_kw(system, ms=4.5):
    c = LIGHT[family(system)]
    hollow = family(system) == "plain"
    return dict(color=c, marker=MARKER[family(system)], markersize=ms, linewidth=1.4,
                markerfacecolor="white" if hollow else c, markeredgecolor=c if hollow else "white",
                markeredgewidth=1.0 if hollow else 0.8)


def legend_handles(systems):
    from matplotlib.lines import Line2D
    return [Line2D([], [], label=LABEL.get(s, s), **series_kw(s)) for s in systems]


def e1_rows(model):
    return [r for r in model["groups"].get("e1", []) if r.get("median_wall_s")]


def workloads_of(rows):
    return sorted({r["workload"] for r in rows})


# --- E1 ----------------------------------------------------------------------------

def frontier(model, out, made):
    """Hero: lines changed vs speedup at the largest N, one marker per
    (system, workload)."""
    rows = e1_rows(model)
    lines = {(p["workload"], p["system"]): p["lines"] for p in model["port_lines"]}
    if not rows or not lines:
        return "needs e1 runs and port-lines.csv"
    nmax = max(r["n"] for r in rows)
    pts = [(lines.get((r["workload"], r["system"])), r["speedup_vs_plain"], r) for r in rows
           if r["n"] == nmax and r["system"] in DIST and r.get("speedup_vs_plain")
           and (r["workload"], r["system"]) in lines]
    if not pts:
        return "no speedup at N=%d" % nmax
    fig, ax = PLT.subplots(figsize=(SINGLE, 2.5))
    ax.axhline(1.0, color=AXIS, linewidth=0.8, zorder=1)
    ax.text(0.99, 1.0, "plain Python, 1 PC", fontsize=6, color=MUTED, va="bottom", ha="right",
            transform=ax.get_yaxis_transform())
    ax.axhline(nmax, color=AXIS, linewidth=0.8, linestyle=(0, (1, 2)), zorder=1)
    ax.text(0.99, nmax, "ideal: %d\u00d7" % nmax, fontsize=6, color=MUTED, va="bottom", ha="right",
            transform=ax.get_yaxis_transform())
    jitter = {}
    for x, y, r in sorted(pts, key=lambda p: (p[0], p[2]["workload"])):
        k = (x, r["system"])
        dx = jitter.get(k, 0)
        jitter[k] = dx + 0.35
        lo = model_base(model, r["workload"]) / r["q3_wall_s"] if r.get("q3_wall_s") else y
        hi = model_base(model, r["workload"]) / r["q1_wall_s"] if r.get("q1_wall_s") else y
        ax.plot([x + dx, x + dx], [lo, hi], color=LIGHT[r["system"]], linewidth=1.0, alpha=0.6, zorder=2)
        ax.plot([x + dx], [y], linestyle="none", zorder=3, **series_kw(r["system"], ms=6))
        ax.annotate(r["workload"], (x + dx, y), xytext=(4, 0), textcoords="offset points", fontsize=6,
                    color=INK2, va="center")
    ax.set_xlabel("lines of the script changed to run distributed (fewer is better)")
    ax.set_ylabel("speedup at N = %d (× plain)" % nmax)
    ax.set_xlim(-1, max(p[0] for p in pts) + 3)
    ax.set_ylim(0, max(nmax, max(p[1] for p in pts)) * 1.18)
    ax.legend(handles=legend_handles(DIST), loc="upper right", ncol=3, handletextpad=0.3,
              columnspacing=0.8, bbox_to_anchor=(1, 1.13))
    save(fig, out, "fig_frontier", made, model["label"])
    return True


def end_labels(ax, ends, gap_pt=8):
    """Direct labels at line ends, pushed apart where lines converge, each
    tied to its end by a hairline leader."""
    if not ends:
        return
    fig = ax.figure
    fig.canvas.draw()
    to_px = ax.transData.transform
    px = sorted(((to_px((x, y))[1], x, y, s) for x, y, s in ends))
    gap = gap_pt * fig.dpi / 72.0
    placed = []
    for yp, x, y, s in px:
        yp2 = max(yp, placed[-1] + gap) if placed else yp
        placed.append(yp2)
        dy = (yp2 - yp) * 72.0 / fig.dpi
        ax.annotate(s, (x, y), xytext=(6, dy), textcoords="offset points", fontsize=6, color=INK2, va="center",
                    arrowprops=dict(arrowstyle="-", color=AXIS, linewidth=0.5, shrinkA=0, shrinkB=2) if dy else None)


def model_base(model, wl):
    for r in model["groups"].get("e1", []):
        if r["workload"] == wl and r["system"] == "plain" and r["n"] == 1:
            return r["median_wall_s"]
    return None


def speedup(model, out, made):
    rows = e1_rows(model)
    wls = workloads_of(rows)
    if not rows or not any(r.get("speedup_vs_plain") for r in rows):
        return "needs e1 with a plain N=1 baseline"
    fig, axes = PLT.subplots(1, len(wls), figsize=(DOUBLE, 2.2), sharey=True, squeeze=False)
    nall = sorted({r["n"] for r in rows})
    for ax, wl in zip(axes[0], wls):
        base = model_base(model, wl)
        ax.plot(nall, nall, color=AXIS, linewidth=0.8, linestyle=(0, (1, 2)))
        ends = []
        for s in DIST:
            pts = sorted((r["n"], r["speedup_vs_plain"], base / r["q3_wall_s"], base / r["q1_wall_s"])
                         for r in rows if r["workload"] == wl and r["system"] == s and r.get("speedup_vs_plain"))
            if not pts:
                continue
            n, y, lo, hi = zip(*pts)
            ax.fill_between(n, lo, hi, color=LIGHT[s], alpha=0.12, linewidth=0)
            ax.plot(n, y, **series_kw(s))
            ends.append((n[-1], y[-1], LABEL[s]))
        end_labels(ax, ends)
        ax.axhline(1.0, color=AXIS, linewidth=0.8)
        ax.set_title(wl)
        ax.set_xscale("log", base=2)
        ax.set_xticks(nall)
        ax.set_xticklabels([str(n) for n in nall])
        ax.set_xlim(nall[0] / 1.25, nall[-1] * 1.9)
        ax.set_xlabel("N (PCs doing work)")
    axes[0][0].set_ylabel("speedup (× plain at N = 1)")
    fig.legend(handles=legend_handles(DIST), loc="upper right", ncol=3, bbox_to_anchor=(1, 1.06))
    fig.tight_layout(w_pad=1.2)
    save(fig, out, "fig_speedup", made, model["label"])
    return True


def wall_bars(model, out, made):
    rows = e1_rows(model)
    if not rows:
        return "needs e1"
    wls = workloads_of(rows)
    fig, axes = PLT.subplots(1, len(wls), figsize=(DOUBLE, 2.2), squeeze=False)
    for ax, wl in zip(axes[0], wls):
        mine = [r for r in rows if r["workload"] == wl]
        ns = sorted({r["n"] for r in mine})
        systems = [s for s in ["plain"] + DIST if any(r["system"] == s for r in mine)]
        w = 0.8 / len(systems)
        for i, s in enumerate(systems):
            xs, ys, err = [], [], [[], []]
            for j, n in enumerate(ns):
                r = next((r for r in mine if r["system"] == s and r["n"] == n), None)
                if not r:
                    continue
                xs.append(j + (i - (len(systems) - 1) / 2) * w)
                ys.append(r["median_wall_s"])
                err[0].append(r["median_wall_s"] - r["q1_wall_s"])
                err[1].append(r["q3_wall_s"] - r["median_wall_s"])
            ax.bar(xs, ys, width=w * 0.9, color=LIGHT[s] if s != "plain" else "white",
                   edgecolor=LIGHT[s] if s == "plain" else "white", linewidth=0.8 if s == "plain" else 0.5,
                   yerr=err, ecolor=INK2, error_kw=dict(linewidth=0.6, capsize=1.5), label=LABEL[s],
                   hatch="////" if s == "plain" else None)
        ax.set_xticks(range(len(ns)))
        ax.set_xticklabels(["N = %d" % n for n in ns])
        ax.grid(axis="x", visible=False)
        ax.set_title(wl)
    axes[0][0].set_ylabel("wall time (s), median and IQR")
    PLT.rcParams["hatch.color"] = LIGHT["plain"]
    fig.legend(*axes[0][0].get_legend_handles_labels(), loc="upper right", ncol=4, bbox_to_anchor=(1, 1.06))
    fig.tight_layout()
    save(fig, out, "fig_wall", made, model["label"])
    return True


def efficiency_heatmap(model, out, made):
    rows = [r for r in e1_rows(model) if r.get("speedup_vs_plain") and r["system"] in DIST]
    if not rows:
        return "needs e1 speedups"
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("blues", BLUES)
    wls, ns = workloads_of(rows), sorted({r["n"] for r in rows})
    systems = [s for s in DIST if any(r["system"] == s for r in rows)]
    fig, axes = PLT.subplots(1, len(systems), figsize=(DOUBLE, 0.45 * len(wls) + 0.9), sharey=True, squeeze=False)
    im = None
    for ax, s in zip(axes[0], systems):
        grid = [[None] * len(ns) for _ in wls]
        for r in rows:
            if r["system"] == s:
                grid[wls.index(r["workload"])][ns.index(r["n"])] = r["speedup_vs_plain"] / r["n"]
        data = [[v if v is not None else float("nan") for v in row] for row in grid]
        im = ax.imshow(data, cmap=cmap, vmin=0, vmax=1, aspect="auto")
        for i, row in enumerate(grid):
            for j, v in enumerate(row):
                if v is not None:
                    ax.text(j, i, "%.2f" % v, ha="center", va="center", fontsize=6.5,
                            color="white" if v > 0.55 else INK)
                else:
                    ax.text(j, i, "–", ha="center", va="center", fontsize=6.5, color=MUTED)
        ax.set_xticks(range(len(ns)))
        ax.set_xticklabels([str(n) for n in ns])
        ax.set_yticks(range(len(wls)))
        ax.set_yticklabels(wls)
        ax.set_xlabel("N")
        ax.set_title(LABEL[s])
        ax.grid(False)
        for sp in ax.spines.values():
            sp.set_visible(False)
        ax.tick_params(length=0)
    cb = fig.colorbar(im, ax=axes[0].tolist(), fraction=0.025, pad=0.02)
    cb.set_label("parallel efficiency (speedup / N)", fontsize=6.5)
    cb.outline.set_visible(False)
    cb.ax.tick_params(labelsize=6, length=0)
    save(fig, out, "fig_efficiency_heatmap", made, model["label"])
    return True


def weak(model, out, made):
    rows = [r for r in model["groups"].get("e1b", []) if r.get("weak_efficiency")]
    if not rows:
        return "needs e1b"
    fig, ax = PLT.subplots(figsize=(SINGLE, 2.1))
    ns = sorted({r["n"] for r in rows})
    ax.axhline(1.0, color=AXIS, linewidth=0.8, linestyle=(0, (1, 2)))
    for s in DIST:
        pts = sorted((r["n"], r["weak_efficiency"]) for r in rows if r["system"] == s)
        if pts:
            ax.plot(*zip(*pts), **series_kw(s))
            ax.annotate(LABEL[s], pts[-1], xytext=(4, 0), textcoords="offset points", fontsize=6, color=INK2,
                        va="center")
    ax.set_xscale("log", base=2)
    ax.set_xticks(ns)
    ax.set_xticklabels([str(n) for n in ns])
    ax.set_xlim(ns[0] / 1.25, ns[-1] * 1.9)
    ax.set_ylim(0, 1.15)
    ax.set_xlabel("N (work per PC fixed)")
    ax.set_ylabel("efficiency T(1) / T(N)")
    ax.legend(handles=legend_handles(DIST), loc="lower left", ncol=3)
    save(fig, out, "fig_weak", made, model["label"])
    return True


# --- E4, E6, E7, E8 ---------------------------------------------------------------

def e4(model, out, made):
    rows = [r for r in model["groups"].get("e4", []) if r.get("median_wall_s")]
    if not rows:
        return "needs e4"
    order = ["pipedpeer", "pipedpeer-headcount", "ray", "dask"]
    rows.sort(key=lambda r: order.index(r["system"]) if r["system"] in order else 9)
    fig, ax = PLT.subplots(figsize=(SINGLE, 1.7))
    for i, r in enumerate(rows):
        c = LIGHT[family(r["system"])]
        head = r["system"] == "pipedpeer-headcount"
        ax.barh(i, r["median_wall_s"], height=0.55, color="white" if head else c, edgecolor=c,
                linewidth=0.8, hatch="////" if head else None,
                xerr=[[r["median_wall_s"] - r["q1_wall_s"]], [r["q3_wall_s"] - r["median_wall_s"]]],
                ecolor=INK2, error_kw=dict(linewidth=0.6, capsize=1.5))
        ax.text(r["q3_wall_s"], i, "  %.1f s" % r["median_wall_s"], va="center", fontsize=6, color=INK2)
    PLT.rcParams["hatch.color"] = LIGHT["pipedpeer"]
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([{"pipedpeer": "PipedPeer (measured split)", "pipedpeer-headcount":
                         "PipedPeer (even split)"}.get(r["system"], LABEL.get(r["system"], r["system"]))
                        for r in rows])
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    caps = next((x.get("caps") for x in model["runs"] if x.get("experiment") == "e4" and x.get("caps")), None)
    ax.set_xlabel("wall time (s), median and IQR; cores %s" % "/".join(str(c) for c in caps or []))
    ax.set_xlim(0, max(r["q3_wall_s"] for r in rows) * 1.25)
    save(fig, out, "fig_e4", made, model["label"])
    return True


def e6(model, out, made):
    runs = [r for r in model["runs"] if r.get("experiment") == "e6" and not r.get("warmup")]
    pairs = []
    for s in DIST:
        ref = [r for r in runs if r["system"] == s and r.get("fault") == "none" and r.get("wall_s")]
        kills = [r for r in runs if r["system"] == s and r.get("fault") == "kill -9"]
        if ref or kills:
            pairs.append((s, ref, kills))
    if not pairs:
        return "needs e6"
    fig, ax = PLT.subplots(figsize=(SINGLE, 1.6))
    for i, (s, ref, kills) in enumerate(pairs):
        c = LIGHT[s]
        r0 = ref[0]["wall_s"] if ref else None
        ok = [k for k in kills if k.get("wall_s") and k.get("result_ok")]
        bad = [k for k in kills if not (k.get("wall_s") and k.get("result_ok"))]
        if r0:
            ax.plot([r0], [i], linestyle="none", **dict(series_kw(s, 6), markerfacecolor="white", markeredgecolor=c))
        for k in ok:
            if r0:
                ax.plot([r0, k["wall_s"]], [i, i], color=c, linewidth=1.4, zorder=1)
            ax.plot([k["wall_s"]], [i], linestyle="none", **series_kw(s, 6))
            ax.annotate("%+.1f s" % (k["wall_s"] - r0) if r0 else "", (k["wall_s"], i), xytext=(5, 0),
                        textcoords="offset points", fontsize=6, color=INK2, va="center")
        for k in bad:
            ax.annotate("kill: run failed" if not k.get("wall_s") else "kill: wrong result",
                        (r0 or 0, i), xytext=(8, -7), textcoords="offset points", fontsize=6, color=INK2)
    ax.set_yticks(range(len(pairs)))
    ax.set_yticklabels([LABEL[p[0]] for p in pairs])
    ax.set_ylim(len(pairs) - 0.5, -0.7)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("wall time (s): hollow = no fault, filled = one worker killed at 30%")
    lat = [r["ctrlc_latency_s"] for r in model["runs"] if r.get("ctrlc_latency_s") is not None]
    if lat:
        ax.set_title("PipedPeer Ctrl-C to all stopped: %s s" % ", ".join("%.2f" % x for x in lat),
                     fontweight="normal", fontsize=7, color=INK2)
    xs = [r.get("wall_s") or 0 for p in pairs for r in p[1] + p[2]]
    ax.set_xlim(0, max(xs) * 1.3 if xs else 1)
    save(fig, out, "fig_e6", made, model["label"])
    return True


def e7(model, out, made):
    runs = [r for r in model["runs"] if r.get("experiment") == "e7" and not r.get("warmup") and r.get("wall_s")
            and not r.get("error")]
    if not runs:
        return "needs e7"
    fig, ax = PLT.subplots(figsize=(SINGLE, 1.3))
    meds = {}
    for i, (s, lab) in enumerate([("pipedpeer-noshim", "shim off"), ("pipedpeer-shim", "shim on")]):
        xs = sorted(r["wall_s"] for r in runs if r["system"] == s)
        if not xs:
            continue
        c = LIGHT["pipedpeer"] if s.endswith("-shim") else LIGHT["plain"]
        ax.plot(xs, [i] * len(xs), linestyle="none", marker="o", markersize=5, markerfacecolor=c,
                markeredgecolor="white", markeredgewidth=0.8, alpha=0.85)
        med = sorted(xs)[len(xs) // 2] if len(xs) % 2 else sum(sorted(xs)[len(xs) // 2 - 1:len(xs) // 2 + 1]) / 2
        meds[s] = med
        ax.plot([med, med], [i - 0.28, i + 0.28], color=INK, linewidth=1.2)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["shim off", "shim on"])
    ax.set_ylim(-0.6, 1.6)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("whole-job wall time of trivial.py (s); bar = median")
    if len(meds) == 2:
        d = meds["pipedpeer-shim"] - meds["pipedpeer-noshim"]
        ax.set_title("interception costs %+.0f ms per job" % (d * 1000), fontweight="normal", fontsize=7,
                     color=INK2)
    save(fig, out, "fig_e7", made, model["label"])
    return True


def e8_table(model, results_dir):
    rows = [r for r in model["groups"].get("e8", [])]
    if not rows:
        return None
    ref = {}
    p = os.path.join(results_dir, "result-cache.json")
    if os.path.exists(p):
        for k, v in json.load(open(p)).items():
            if k.startswith("T1") and isinstance(v, str):
                try:
                    ref[k] = float(v.split()[-1])
                except ValueError:
                    pass
    out = ["| variant | ranks | system | runs used | wall s, median [IQR] | val loss | loss vs 1 rank | "
           "bytes sent per rank |", "|---|---|---|---|---|---|---|---|"]
    for r in rows:
        loss = r.get("median_loss")
        one = next(iter(ref.values()), None)
        out.append("| %s | %s | %s | %d | %s | %s | %s | %s |" % (
            r.get("variant", "-"), r["n"], r["system"], r["runs_used"],
            "%.2f [%.2f-%.2f]" % (r["median_wall_s"], r["q1_wall_s"], r["q3_wall_s"]) if r.get("median_wall_s")
            else "-", "%.4f" % loss if loss is not None else "-",
            "%+.4f" % (loss - one) if loss is not None and one is not None else "-",
            "{:,}".format(int(r["median_bytes_per_rank"])) if r.get("median_bytes_per_rank") else "-"))
    return "\n".join(out)


# --- timelines: CPU heatmap, swimlanes, network, replay -------------------------------

def pick_trio(model, workload=None):
    """The representative e1 runs of one workload at its largest N, one per
    distributed system: the same work on each system."""
    tl = model["timelines"]
    cands = [r for r in model["runs"] if r["run_id"] in tl and r.get("experiment") == "e1" and r["system"] in DIST]
    if workload:
        cands = [r for r in cands if r["workload"] == workload]
    if not cands:
        return None, []
    wl = workload or sorted({r["workload"] for r in cands})[0]
    n = max(r["n"] for r in cands if r["workload"] == wl)
    trio = [next((r for r in cands if r["workload"] == wl and r["n"] == n and r["system"] == s), None) for s in DIST]
    return (wl, n), [r for r in trio if r]


def cpu_heatmap(model, out, made):
    (key, trio) = pick_trio(model)
    if not trio or not any(model["timelines"][r["run_id"]]["cpu"] for r in trio):
        return "needs OTel CPU samples during e1 runs"
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("blues", ["#f4f8fd"] + BLUES)
    pcs = model["pcs"]
    vmax = max(1.0, max(v for r in trio for s in model["timelines"][r["run_id"]]["cpu"].values() for _, v in s))
    fig, axes = PLT.subplots(len(trio), 1, figsize=(DOUBLE, 0.32 * len(pcs) * len(trio) + 0.8), sharex=True,
                             squeeze=False)
    tmax = max(model["timelines"][r["run_id"]]["wall_s"] + 2 * model["timelines"][r["run_id"]]["pad_s"]
               for r in trio)
    for ax, r in zip(axes[:, 0], trio):
        t = model["timelines"][r["run_id"]]
        pad = t["pad_s"]
        for i, pc in enumerate(pcs):
            for (tb, v), prev in zip(t["cpu"].get(pc, []), [(None, None)] + t["cpu"].get(pc, [])):
                ta = prev[0] if prev[0] is not None else tb - 1
                ax.barh(i, tb - ta, left=ta, height=0.92, color=cmap(min(v / vmax, 1)), linewidth=0)
        ax.axvline(pad, color=INK, linewidth=0.6)
        ax.axvline(pad + t["wall_s"], color=INK, linewidth=0.6)
        ax.set_yticks(range(len(pcs)))
        ax.set_yticklabels([pc + (" *" if pc in t["hosts"] else "") for pc in pcs])
        ax.invert_yaxis()
        ax.grid(False)
        srcs = sorted({v for v in t.get("cpu_source", {}).values() if v})
        ax.set_title("%s, %s, N = %d (wall %.1f s; CPU from %s)" % (
            LABEL[r["system"]], r["workload"], r["n"], t["wall_s"], ", ".join(srcs) or "?"), fontsize=7)
        ax.tick_params(axis="y", length=0)
        for sp in ("left", "bottom"):
            ax.spines[sp].set_visible(False)
    axes[-1, 0].set_xlim(0, tmax)
    axes[-1, 0].set_xlabel("seconds from %.0f s before the run starts (lines: start, end); * = PC in the run" % pad)
    import matplotlib.cm as cm
    from matplotlib.colors import Normalize
    sm = cm.ScalarMappable(norm=Normalize(0, vmax), cmap=cmap)
    cb = fig.colorbar(sm, ax=axes[:, 0].tolist(), fraction=0.02, pad=0.01)
    cb.set_label("busy cores", fontsize=6.5)
    cb.outline.set_visible(False)
    cb.ax.tick_params(labelsize=6, length=0)
    save(fig, out, "fig_cpu_heatmap", made, model["label"])
    return True


def swimlanes(model, out, made):
    key, trio = pick_trio(model)
    if not trio:
        return "needs e1 timelines"
    pcs = model["pcs"]
    fig, axes = PLT.subplots(len(trio), 1, figsize=(DOUBLE, 0.3 * len(pcs) * len(trio) + 0.9), sharex=True,
                             squeeze=False)
    for ax, r in zip(axes[:, 0], trio):
        t = model["timelines"][r["run_id"]]
        c = LIGHT[r["system"]]
        steps = t["steps"]
        if steps:
            for s in steps:
                if s["pc"] not in pcs:
                    continue
                i = pcs.index(s["pc"])
                if s["step"] == "script":
                    ax.barh(i, s["d"], left=s["t"], height=0.84, color=c, alpha=0.10, linewidth=0)
                elif s["step"] == "compute":
                    k = max(1, t["lane_rows"].get(s["pc"], 1))
                    h = 0.8 / k
                    ax.barh(i - 0.4 + h * (s.get("row", 0) + 0.5), max(s["d"], 0.02), left=s["t"],
                            height=h * 0.82, color=c, linewidth=0)
            note = "each bar is a chunk computing (receipt), one sub-row per concurrent chunk; wash: the script"
        else:
            for i, pc in enumerate(pcs):
                for (tb, v), prev in zip(t["cpu"].get(pc, []), [(None, None)] + t["cpu"].get(pc, [])):
                    ta = prev[0] if prev[0] is not None else tb - 1
                    if v >= 0.5 and 0 <= ta - t["pad_s"] <= t["wall_s"]:
                        ax.barh(i, tb - ta, left=ta - t["pad_s"], height=0.6, color=c, alpha=min(1, 0.25 + v / 2),
                                linewidth=0)
            note = "no task timeline recorded: CPU busy ≥ 0.5 core (from host metrics) instead"
        ax.set_yticks(range(len(pcs)))
        ax.set_yticklabels(pcs)
        ax.set_ylim(len(pcs) - 0.5, -0.5)
        ax.grid(axis="y", visible=False)
        ax.set_xlim(0, max(model["timelines"][x["run_id"]]["wall_s"] for x in trio) * 1.02)
        ax.set_title("%s: %s" % (LABEL[r["system"]], note), fontsize=7)
        ax.tick_params(axis="y", length=0)
    axes[-1, 0].set_xlabel("seconds since the run started (%s, N = %d)" % key)
    save(fig, out, "fig_swimlanes", made, model["label"])
    return True


def net_bytes(model, out, made):
    rows = []
    for r in model["runs"]:
        t = model["timelines"].get(r["run_id"])
        if t and t["net"] and r.get("experiment") == "e1" and r["system"] in DIST:
            rows.append((r, t))
    if not rows:
        return "needs OTel network samples during e1 runs"
    nmax = max(r["n"] for r, _ in rows)
    rows = [(r, t) for r, t in rows if r["n"] == nmax]
    wls = sorted({r["workload"] for r, _ in rows})
    pcs = model["pcs"]
    fig, axes = PLT.subplots(1, len(wls), figsize=(DOUBLE, 1.9), sharey=True, squeeze=False)
    w = 0.8 / len(DIST)
    for ax, wl in zip(axes[0], wls):
        for i, s in enumerate(DIST):
            m = next(((r, t) for r, t in rows if r["workload"] == wl and r["system"] == s), None)
            if not m:
                continue
            ys = [(m[1]["net"].get(pc, {}).get("tx", 0) + m[1]["net"].get(pc, {}).get("rx", 0)) / 1e6 for pc in pcs]
            ax.bar([j + (i - 1) * w for j in range(len(pcs))], ys, width=w * 0.9, color=LIGHT[s], edgecolor="white",
                   linewidth=0.5, label=LABEL[s])
        ax.set_xticks(range(len(pcs)))
        ax.set_xticklabels(pcs)
        ax.grid(axis="x", visible=False)
        ax.set_title("%s, N = %d" % (wl, nmax))
    axes[0][0].set_ylabel("MB sent + received")
    fig.legend(*axes[0][0].get_legend_handles_labels(), loc="upper right", ncol=3, bbox_to_anchor=(1, 1.08))
    fig.tight_layout()
    save(fig, out, "fig_net_bytes", made, model["label"])
    return True


def replay(model, out, made):
    """Cluster replay: each PC's busy cores over one run, three systems side
    by side, as a GIF (and MP4 when ffmpeg exists)."""
    key, trio = pick_trio(model)
    if not trio or not any(model["timelines"][r["run_id"]]["cpu"] for r in trio):
        return "needs OTel CPU samples during e1 runs"
    import shutil
    from matplotlib import animation
    pcs = model["pcs"]
    tls = [model["timelines"][r["run_id"]] for r in trio]
    tmax = max(t["wall_s"] + 2 * t["pad_s"] for t in tls)
    vmax = max(1.0, max(v for t in tls for s in t["cpu"].values() for _, v in s))
    step = 0.25
    frames = int(tmax / step) + 1
    fig, axes = PLT.subplots(1, len(trio), figsize=(DOUBLE, 0.28 * len(pcs) + 1.1), sharey=True, squeeze=False)
    bars, clocks = [], []
    for ax, r, t in zip(axes[0], trio, tls):
        b = ax.barh(range(len(pcs)), [0] * len(pcs), height=0.6, color=LIGHT[r["system"]])
        bars.append(b)
        ax.set_xlim(0, vmax * 1.05)
        ax.set_yticks(range(len(pcs)))
        ax.set_yticklabels(pcs)
        ax.invert_yaxis()
        ax.grid(axis="y", visible=False)
        ax.set_title(LABEL[r["system"]])
        ax.set_xlabel("busy cores")
        clocks.append(ax.text(0.98, 0.04, "", transform=ax.transAxes, ha="right", fontsize=6.5, color=INK2))
    fig.suptitle("%s at N = %d: every PC's CPU through one run" % key, x=0.01, ha="left", fontsize=8,
                 fontweight="bold", color=INK)
    if model["label"]:
        fig.text(0.995, 0.005, model["label"], ha="right", va="bottom", fontsize=5.5, color=MUTED)

    def value(series, ts):
        for tb, v in series:
            if tb >= ts:
                return v
        return 0.0

    def draw(k):
        ts = k * step
        for b, t, clk in zip(bars, tls, clocks):
            for i, pc in enumerate(pcs):
                b[i].set_width(value(t["cpu"].get(pc, []), ts))
            rel = ts - t["pad_s"]
            state = "before start" if rel < 0 else ("done" if rel > t["wall_s"] else "running")
            clk.set_text("t = %+.1f s  %s" % (rel, state))
        return [x for b in bars for x in b] + clocks
    fig.tight_layout()
    anim = animation.FuncAnimation(fig, draw, frames=frames, interval=1000 * step, blit=False)
    anim.save(os.path.join(out, "anim_cluster_replay.gif"), writer=animation.PillowWriter(fps=int(1 / step)), dpi=150)
    if shutil.which("ffmpeg"):
        anim.save(os.path.join(out, "anim_cluster_replay.mp4"), writer=animation.FFMpegWriter(fps=int(1 / step)),
                  dpi=200)
    PLT.close(fig)
    made.append("anim_cluster_replay")
    return True


def all_figures(model, out, anim=True):
    global PLT
    PLT = plt_module()
    made, skipped = [], []
    figs = [frontier, speedup, wall_bars, efficiency_heatmap, weak, e4, e6, e7, cpu_heatmap, swimlanes, net_bytes]
    if anim:
        figs.append(replay)
    for fn in figs:
        try:
            res = fn(model, out, made)
        except Exception as e:  # one broken figure must not cost the others
            res = "error: %s: %s" % (type(e).__name__, e)
        if res is not True:
            skipped.append((fn.__name__, res))
    results_dir = os.path.dirname(os.path.abspath(out))
    t8 = e8_table(model, results_dir)
    if t8:
        with open(os.path.join(out, "e8_table.md"), "w") as f:
            f.write(t8 + "\n")
        made.append("e8_table.md")
    if model["port_lines"]:
        with open(os.path.join(out, "port_lines.md"), "w") as f:
            f.write("| workload | plain | PipedPeer | Ray | Dask | torchrun |\n|---|---|---|---|---|---|\n")
            for wl in sorted({p["workload"] for p in model["port_lines"]}):
                d = {p["system"]: p["lines"] for p in model["port_lines"] if p["workload"] == wl}
                f.write("| %s | %s |\n" % (wl, " | ".join(str(d.get(s, "-")) for s in
                                                       ["plain", "pipedpeer", "ray", "dask", "torchrun"])))
        made.append("port_lines.md")
    return made, skipped
