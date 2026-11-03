"""Cluster-level views from telemetry and receipts: per-PC CPU, work
placement, network bytes, where PipedPeer's time goes, and how much work
left the submitting PC."""
from collections import defaultdict

import numpy as np
from matplotlib.collections import PolyCollection
from matplotlib.colors import LinearSegmentedColormap

import sample_figures as sf
import story_model as sm

from . import style as st
from .data import median
from .registry import fig, need

SEC_E2E = "Evaluation > End-to-end runs (Q3)"
SEC_MICRO = "Evaluation > Microbenchmarks (Q1, Q2)"

UTIL = LinearSegmentedColormap.from_list("util", ["#FFFFFF", "#C9DDEC", "#6FA3CC", "#2B6FA6", "#0B3D66"])


def trio(lab, wl=None):
    """The representative E1 run per distributed system with a timeline,
    for one workload at its largest N: the same work on every system."""
    runs = [r for r in lab.runs if r["run_id"] in lab.timelines and r.get("experiment") == "e1"
            and r["system"] in st.DIST]
    if wl:
        runs = [r for r in runs if r["workload"] == wl]
    if not runs:
        return None, None, []
    wl = wl or ("W1" if any(r["workload"] == "W1" for r in runs) else sorted({r["workload"] for r in runs})[0])
    n = max(r["n"] for r in runs if r["workload"] == wl)
    out = [next((r for r in runs if r["workload"] == wl and r["n"] == n and r["system"] == s), None)
           for s in st.DIST]
    return wl, n, [r for r in out if r]


def row_h(npcs):
    return min(0.15, max(0.045, 0.8 / max(1, npcs)))


def pc_ticks(ax, pcs):
    """Every PC's row, labelled; past 8 PCs, every fourth label."""
    step = 1 if len(pcs) <= 8 else 4
    ax.set_yticks(range(len(pcs)))
    ax.set_yticklabels([pc if i % step == 0 else "" for i, pc in enumerate(pcs)], fontsize=7 if step == 1 else 6)


@fig("eval-cpu-heatmap", "double", 0, SEC_E2E,
     "Per-PC CPU utilisation over one W1 run at the largest N, one panel per system.",
     "CPU utilisation of every PC during one W1 run at the largest N (OTel host or cgroup CPU time; "
     "busy cores over logical cores, 0.5 s bins). The vertical lines mark the job's start and end; "
     "PCs the run did not use are labelled in grey.")
def cpu_heatmap(lab, ctx):
    wl, n, runs = trio(lab)
    need(runs and any(lab.timelines[r["run_id"]]["cpu"] for r in runs), "needs OTel CPU samples during e1 runs")
    pcs = lab.pcs
    rh = row_h(len(pcs))
    h = 0.42 + len(runs) * (len(pcs) * rh + 0.17)
    f, axes = st.figure("double", h, len(runs), 1, sharex=True)
    tmax = max(lab.timelines[r["run_id"]]["wall_s"] + 2 * lab.timelines[r["run_id"]]["pad_s"] for r in runs)
    dt = 0.5
    edges = np.arange(0, tmax + dt, dt)
    im = None
    for ax, r in zip(axes[:, 0], runs):
        t = lab.timelines[r["run_id"]]
        pad = t["pad_s"]
        grid = np.full((len(pcs), len(edges) - 1), np.nan)
        for i, pc in enumerate(pcs):
            s = t["cpu"].get(pc) or []
            cores = lab.cores.get(pc) or 1
            prev = None
            for tb, v in s:
                ta = prev if prev is not None else tb - 1
                prev = tb
                a, b = int(max(0, ta) / dt), int(min(tmax, tb) / dt)
                if b > a:
                    grid[i, a:b] = min(1.0, v / cores)
        im = ax.imshow(grid, aspect="auto", cmap=UTIL, vmin=0, vmax=1, interpolation="nearest",
                       extent=(-pad, edges[-1] - pad, len(pcs) - 0.5, -0.5))
        for x in (0, t["wall_s"]):
            ax.axvline(x, color=st.INK, lw=0.6)
        pc_ticks(ax, pcs)
        used = set(r.get("hosts") or [])
        for tl, pc in zip(ax.get_yticklabels(), pcs):
            tl.set_color(st.INK if pc in used else st.MUTED)
        ax.tick_params(axis="y", length=0, pad=2)
        if ax is not axes[-1, 0]:
            ax.tick_params(axis="x", length=0)
        for sp in ax.spines.values():
            sp.set_visible(False)
        ax.text(0.0, 1.0, "%s, wall %.1f s" % (st.label(r["system"]), t["wall_s"]), transform=ax.transAxes,
                ha="left", va="bottom", fontsize=7, color=st.INK)
    ax = axes[-1, 0]
    pad0 = lab.timelines[runs[0]["run_id"]]["pad_s"]
    ax.set_xlim(-pad0, tmax - pad0)
    ax.set_xlabel("seconds since the job started (%s, N = %d)" % (wl, n))
    cb = f.colorbar(im, ax=axes[:, 0].tolist(), fraction=0.018, pad=0.008, aspect=18)
    cb.outline.set_visible(False)
    cb.set_ticks([0, 0.5, 1])
    cb.set_ticklabels(["0", "50", "100%"])
    cb.ax.tick_params(labelsize=6, length=1.5, width=0.4)
    cb.set_label("CPU busy", fontsize=6.5)
    return f


def chunk_runs(lab):
    """One run per distributed system that has per-chunk records
    (chunks.json): E18 without the hog, else E12, else E13."""
    for exp, match in (("e18", {"fault": "none"}), ("e12", {}), ("e13", {})):
        runs = [r for r in lab.exp(exp, ok=False, **match) if r.get("chunks_path")]
        got = {}
        for r in runs:
            ch = sm.chunks(r, lab.dir_of(r))
            if ch and r["system"] in st.DIST:
                got[r["system"]] = (r, ch)
        if got:
            return exp, [got[s] for s in st.DIST if s in got]
    return None, []


@fig("eval-swimlanes", "double", 0, SEC_E2E,
     "Work placement: each chunk of work as a bar on the PC that ran it, one panel per system.",
     "Where the work ran. Each bar is one chunk of items on the PC that computed it, from each system's "
     "own records (PipedPeer's receipt, Ray's timeline, Dask's scheduler). Dask records tasks per "
     "second, drawn as one-second cells shaded by count. "
     "Rows within a PC are concurrent chunks.")
def swimlanes(lab, ctx):
    exp, got = chunk_runs(lab)
    need(got, "needs chunks.json (e18, e12 or e13)")
    pcs = sorted({c[0] for _, ch in got for c in ch} | set(h for r, _ in got for h in r.get("hosts") or []),
                 key=sf.pc_sort)
    rh = row_h(len(pcs))
    h = 0.42 + len(got) * (len(pcs) * rh * 1.15 + 0.17)
    f, axes = st.figure("double", h, len(got), 1, sharex=True)
    tmax = max(r.get("wall_s") or max(c[2] for c in ch) for r, ch in got)
    for ax, (r, ch) in zip(axes[:, 0], got):
        col = st.SYS[r["system"]].color
        rows = defaultdict(list)  # pc -> end time per sub-row
        polys, bins = [], []
        for pc, t0, t1, items in sorted(ch, key=lambda c: c[1]):
            i = pcs.index(pc)
            if t1 - t0 < 1e-3:  # Dask: tasks finished in one second, one record per second
                bins.append((i, t0, items or 0))
                continue
            ends = rows[pc]
            k = next((j for j, e in enumerate(ends) if e <= t0 + 1e-6), len(ends))
            if k == len(ends):
                ends.append(0.0)
            ends[k] = t1
            polys.append((i, k, t0, t1))
        for i, k, t0, t1 in polys:
            nk = max(1, len(rows[pcs[i]]))
            hh = 0.8 / nk
            y0 = i - 0.4 + hh * k
            ax.add_collection(PolyCollection([[(t0, y0 + 0.08 * hh), (t1, y0 + 0.08 * hh), (t1, y0 + 0.92 * hh),
                                               (t0, y0 + 0.92 * hh)]], facecolors=col, edgecolors="none"))
        if bins:
            top = max(b[2] for b in bins) or 1
            for i, t0, items in bins:
                ax.add_collection(PolyCollection([[(t0, i - 0.32), (t0 + 1, i - 0.32), (t0 + 1, i + 0.32),
                                                   (t0, i + 0.32)]], facecolors=st.tint(col, 0.15 + 0.85 * items / top),
                                                 edgecolors="white", linewidths=0.3))
        pc_ticks(ax, pcs)
        ax.set_ylim(len(pcs) - 0.5, -0.5)
        ax.tick_params(axis="y", length=0)
        ax.spines["left"].set_visible(False)
        st.xgrid(ax)
        ok = "" if r.get("result_ok") else ", failed"
        ax.text(0.0, 1.0, "%s, wall %.1f s%s" % (st.label(r["system"]), r.get("wall_s") or 0, ok),
                transform=ax.transAxes, ha="left", va="bottom", fontsize=7)
        if r.get("wall_s"):
            ax.axvline(r["wall_s"], color=st.INK, lw=0.6)
    axes[-1, 0].set_xlim(0, tmax * 1.01)
    axes[-1, 0].set_xlabel("seconds since the job started (%s, W1, N = %s)" % (exp.upper(), got[0][0].get("n")))
    return f


@fig("eval-net-bytes", "single", 1.55, SEC_E2E,
     "Network bytes each PC sent and received during one W1 run per system at the largest N.",
     "Network traffic per PC: bytes sent plus received on the lab interface during one W1 run per "
     "system at the largest N (OTel system.network.io). Log y axis; pc00 submits the job and hosts "
     "the Ray head and the Dask scheduler.")
def net_bytes(lab, ctx):
    runs = [r for r in lab.runs if r["run_id"] in lab.timelines and r.get("experiment") == "e1"
            and r["system"] in st.DIST and lab.timelines[r["run_id"]]["net"]]
    need(runs, "needs OTel network samples during e1 runs")
    wl = "W1" if any(r["workload"] == "W1" for r in runs) else sorted({r["workload"] for r in runs})[0]
    runs = [r for r in runs if r["workload"] == wl]
    nmax = max(r["n"] for r in runs)
    runs = [r for r in runs if r["n"] == nmax]
    pcs = lab.pcs
    f, ax = st.figure("single", 1.55)
    ax = ax[0][0]
    off = {s: (i - 1) * 0.24 for i, s in enumerate(st.DIST)}
    ys_all = []
    for s in st.DIST:
        r = next((r for r in runs if r["system"] == s), None)
        if not r:
            continue
        net = lab.timelines[r["run_id"]]["net"]
        xs, ys = [], []
        for k, pc in enumerate(pcs):
            v = net.get(pc)
            if v:
                xs.append(k + off[s])
                ys.append(max(1e-3, (v["tx"] + v["rx"]) / 1e6))
        ys_all += ys
        ax.plot(xs, ys, **st.point_kw(s, 3.4))
    need(ys_all, "no network deltas")
    ax.set_yscale("log")
    ax.set_xticks(range(len(pcs)))
    ax.set_xticklabels([p if len(pcs) <= 6 else p.replace("pc", "") for p in pcs], fontsize=6.5 if len(pcs) > 8 else 7)
    for k in range(1, len(pcs)):
        ax.axvline(k - 0.5, color=st.GRID, lw=0.4)
    ax.set_xlim(-0.5, len(pcs) - 0.5)
    ax.set_ylim(min(ys_all) / 2.5, max(ys_all) * 4)
    ax.set_xlabel("PC" + ("" if len(pcs) <= 6 else " (pc00 to pc%02d)" % (len(pcs) - 1)))
    ax.set_ylabel("MB sent + received")
    st.ygrid(ax)
    st.legend_top(f, st.DIST, top=0.86, handlelength=1.4)
    ax.set_xlabel(ax.get_xlabel() + ", %s at N = %d" % (wl, nmax))
    return f


PHASES = [("env_build", "build environment"), ("env_send", "ship closure + workspace"), ("script", "run")]


@fig("eval-time-breakdown", "single", 0, SEC_MICRO,
     "Where PipedPeer's job time goes: environment build and ship (ms) against the run (s), per workload and N.",
     "Where a PipedPeer job's time goes, from its own step records. (a) Building the Nix environment "
     "and shipping closure and workspace, in milliseconds; (b) running the script, in seconds. "
     "Median over repeats, warm environment cache.")
def time_breakdown(lab, ctx):
    acc = defaultdict(lambda: defaultdict(list))
    for r in lab.exp("e1", system="pipedpeer"):
        rec = lab.record(r)
        if not rec.get("steps"):
            continue
        d = defaultdict(float)
        for s in rec["steps"]:
            if s.get("step") in dict(PHASES) and s.get("chunk", -1) == -1:
                d[s["step"]] += (s.get("dur_ns") or 0) / 1e9
        for k, _ in PHASES:
            acc[(r["workload"], r["n"])][k].append(d.get(k, 0.0))
    need(acc, "needs record.json for pipedpeer e1 runs")
    keys = sorted(acc)
    h = 0.62 + 0.13 * len(keys)
    f, axes = st.figure("single", h, 1, 2, sharey=True, width_ratios=[1, 1.25])
    a, b = axes[0]
    dark, mid = st.SYS["pipedpeer"].color, st.tint(st.SYS["pipedpeer"].color, 0.35)
    for i, k in enumerate(keys):
        bld, snd = (median(acc[k]["env_build"]) or 0) * 1e3, (median(acc[k]["env_send"]) or 0) * 1e3
        a.barh(i, bld, height=0.62, facecolor=dark, edgecolor=dark, linewidth=0.4)
        a.barh(i, snd, left=bld, height=0.62, facecolor=mid, edgecolor=dark, hatch="////", linewidth=0.4)
        a.text(bld + snd, i, " %.0f" % (bld + snd), va="center", fontsize=6.2, color=st.INK2)
        run = median(acc[k]["script"]) or 0
        b.barh(i, run, height=0.62, facecolor=st.tint(dark, 0.15), edgecolor=dark, linewidth=0.5)
        b.text(run, i, " %.1f" % run, va="center", fontsize=6.2, color=st.INK2)
    a.set_yticks(range(len(keys)))
    a.set_yticklabels(["%s, N = %d" % k for k in keys])
    a.invert_yaxis()
    for ax in (a, b):
        ax.tick_params(axis="y", length=0)
        st.xgrid(ax)
    a.set_xlim(0, max(((median(acc[k]["env_build"]) or 0) + (median(acc[k]["env_send"]) or 0)) * 1e3
                      for k in keys) * 1.35)
    b.set_xlim(0, max(median(acc[k]["script"]) or 0 for k in keys) * 1.3)
    a.set_xlabel("build + ship (ms)")
    b.set_xlabel("run (s)")
    st.panel_tag(a, "(a)")
    st.panel_tag(b, "(b)")
    from matplotlib.patches import Patch
    hs = [Patch(facecolor=dark, edgecolor=dark, label="build environment", linewidth=0.4),
          Patch(facecolor=mid, edgecolor=dark, hatch="////", label="ship closure + workspace", linewidth=0.4),
          Patch(facecolor=st.tint(dark, 0.15), edgecolor=dark, label="run script", linewidth=0.5)]
    st.legend_top(f, hs, top=1 - 0.2 / h, ncol=3, handlelength=1.2)
    return f


@fig("eval-offload-share", "single", 1.6, SEC_E2E,
     "Share of PipedPeer's items computed off the submitting PC, per workload, against N.",
     "How much work PipedPeer moved off the submitting PC: the share of mapped items computed by peers "
     "(from the shim's receipt), median and range over repeats. The dotted line is an even share "
     "(N-1)/N.")
def offload(lab, ctx):
    acc = defaultdict(list)
    for r in lab.exp("e1", system="pipedpeer"):
        rc = r.get("receipt") or {}
        loc, rem = rc.get("local_items"), rc.get("remote_items")
        if loc is not None and rem is not None and loc + rem:
            acc[(r["workload"], r["n"])].append(rem / (loc + rem))
    need(acc, "needs pipedpeer e1 receipts")
    ns = sorted({k[1] for k in acc})
    wls = sorted({k[0] for k in acc})
    f, ax = st.figure("single", 1.6)
    ax = ax[0][0]
    ax.plot(ns, [(n - 1) / n for n in ns], color=st.MUTED, lw=0.6, ls=(0, (1, 1.5)))
    markers = ["o", "s", "^", "D", "v", "P"]
    ls = ["-", (0, (4, 1.6)), (0, (5, 1.4, 1, 1.4)), (0, (1, 1.3))]
    ends = []
    for j, wl in enumerate(wls):
        pts = [(n, acc[(wl, n)]) for n in ns if acc.get((wl, n))]
        x = [p[0] for p in pts]
        m = [median(p[1]) for p in pts]
        c = st.tint(st.SYS["pipedpeer"].color, 1.0 - 0.13 * j)
        ax.errorbar(x, m, yerr=[[a - min(p[1]) for a, p in zip(m, pts)], [max(p[1]) - a for a, p in zip(m, pts)]],
                    color=c, marker=markers[j % 6], ls=ls[j % 4], ms=3.4, mfc=c if j == 0 else "white", mec=c,
                    lw=0.9, elinewidth=0.6, capsize=1.4)
        ends.append((x[-1], m[-1], wl, c))
    st.log2_x(ax, ns)
    ax.set_xlim(min(ns) / 1.3, max(ns) * 1.9)
    ax.set_ylim(0, 1)
    ax.set_yticks([0, 0.25, 0.5, 0.75, 1])
    ax.set_yticklabels(["0", "25", "50", "75", "100%"])
    st.end_labels(ax, ends)
    st.ygrid(ax)
    ax.set_xlabel("PCs (N)")
    ax.set_ylabel("items computed by peers")
    return f
