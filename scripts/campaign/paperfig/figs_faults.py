"""E4 heterogeneous split, E6 single kill, E7 interception overhead, E8
training."""
import numpy as np

from . import style as st
from .data import median
from .registry import fig, need

SEC_MICRO = "Evaluation > Microbenchmarks (Q1, Q2)"
SEC_E2E = "Evaluation > End-to-end runs (Q3)"
SEC_TRAIN = "Evaluation > Training (Q4)"

E4_ORDER = ["pipedpeer", "pipedpeer-headcount", "ray", "dask"]


@fig("eval-e4-heterogeneous", "single", 1.95, SEC_E2E,
     "E4: W1 on PCs of unequal speed: wall time per system and the share of items each PC computed.",
     "Unequal PCs (E4). (a) Wall time, median and IQR. (b) Share of the items each PC computed in one "
     "representative run; the black tick is the first PC's share of the cores. The even split is "
     "PipedPeer with placement by head count instead of measured speed.")
def e4(lab, ctx):
    rows = [r for r in lab.rows("e4") if r.get("median_wall_s")]
    need(rows, "needs e4 runs")
    rows.sort(key=lambda r: E4_ORDER.index(r["system"]) if r["system"] in E4_ORDER else 9)
    runs = lab.exp("e4")
    caps = next((r.get("caps") for r in runs if r.get("caps")), None)
    hosts = next((r.get("hosts") for r in runs if r.get("hosts")), [])
    f, axes = st.figure("single", 1.95, 1, 2, sharey=True)
    axw, axs = axes[0]
    y = np.arange(len(rows))
    for i, r in enumerate(rows):
        s = r["system"]
        kw = st.bar_kw(s, hollow=s == "pipedpeer-headcount")
        axw.barh(i, r["median_wall_s"], height=0.62, **kw)
        axw.plot([r["q1_wall_s"], r["q3_wall_s"]], [i, i], color=st.INK, lw=0.7)
        axw.text(r["q3_wall_s"], i, " %.1f" % r["median_wall_s"], va="center", fontsize=6.5, color=st.INK2)
    axw.set_yticks(y)
    axw.set_yticklabels([st.label(r["system"]).replace("PipedPeer, even split", "PipedPeer,\neven split")
                         for r in rows])
    axw.invert_yaxis()
    axw.set_xlim(0, max(r["q3_wall_s"] for r in rows) * 1.3)
    axw.set_xlabel("wall time (s)")
    st.xgrid(axw)
    axw.tick_params(axis="y", length=0)
    st.panel_tag(axw, "(a)")
    pcs = list(hosts)
    ticked = False
    for i, r in enumerate(rows):
        rep = next((x for x in runs if x["system"] == r["system"] and lab.items_per_pc(x)), None)
        per = lab.items_per_pc(rep) if rep else {}
        tot = sum(per.values())
        if not tot:
            axs.text(0.5, i, "no per-PC record", ha="center", va="center", fontsize=6.5, color=st.MUTED)
            continue
        left = 0.0
        for k, pc in enumerate(pcs):
            w = per.get(pc, 0) / tot
            col = st.sys_of(r["system"]).color
            axs.barh(i, w, left=left, height=0.62, facecolor=st.tint(col, 1.0 - 0.55 * k),
                     edgecolor="white", linewidth=0.6)
            if w > 0.14:
                first = k == 0
                axs.text(left + 0.03 if first else left + w - 0.03, i, "%s %.0f%%" % (pc, 100 * w),
                         ha="left" if first else "right", va="center", fontsize=6,
                         color="white" if first else st.INK)
            left += w
        if caps and len(caps) == len(pcs):
            c = caps[0] / sum(caps)
            axs.plot([c, c], [i - 0.42, i + 0.42], color=st.INK, lw=1.0, solid_capstyle="butt")
            ticked = True
    if ticked:
        axs.annotate("tick: %s's core share (%s)" % (pcs[0], ":".join(str(x) for x in caps)), (1.0, -0.5),
                     xytext=(0, 1), textcoords="offset points", ha="right", va="bottom", fontsize=6,
                     color=st.INK2, annotation_clip=False)
    axs.set_xlim(0, 1)
    axs.set_xticks([0, 0.5, 1])
    axs.set_xticklabels(["0", "50", "100%"])
    axs.set_xlabel("share of items")
    axs.tick_params(axis="y", length=0)
    st.panel_tag(axs, "(b)")
    return f


@fig("eval-e6-kill", "single", 1.45, SEC_E2E,
     "E6: wall time with and without one worker killed (kill -9) mid-run, per system.",
     "One worker killed with kill -9 mid-run (E6). Hollow: no fault; filled: killed. The label is the "
     "added wall time; every run returned the correct result unless marked.")
def e6(lab, ctx):
    runs = [r for r in lab.exp("e6", ok=False) if not r.get("warmup")]
    rows = []
    for s in st.DIST:
        ref = [r["wall_s"] for r in runs if r["system"] == s and r.get("fault") == "none" and analyse_ok(r)]
        kill = [r for r in runs if r["system"] == s and r.get("fault") == "kill -9"]
        if ref or kill:
            rows.append((s, median(ref), median([r["wall_s"] for r in kill if analyse_ok(r)]),
                         sum(1 for r in kill if not analyse_ok(r)), len(kill),
                         median([r.get("kill_at_s") for r in kill])))
    need(rows, "needs e6 runs")
    f, ax = st.figure("single", 1.45)
    ax = ax[0][0]
    for i, (s, r0, k, bad, nk, at) in enumerate(rows):
        c = st.SYS[s].color
        if r0 and k:
            ax.plot([r0, k], [i, i], color=c, lw=1.2, zorder=1)
        if r0:
            ax.plot([r0], [i], ls="none", marker=st.SYS[s].marker, ms=4.6, mfc="white", mec=c, mew=0.9, zorder=2)
        if k:
            ax.plot([k], [i], ls="none", marker=st.SYS[s].marker, ms=4.6, mfc=c, mec=c, zorder=3)
            ax.annotate("%+.1f s" % (k - r0) if r0 else "", (max(k, r0 or 0), i), xytext=(5, 0),
                        textcoords="offset points", va="center", fontsize=6.5, color=st.INK2)
        if bad:
            ax.annotate("%d/%d killed runs failed" % (bad, nk), ((r0 or 0), i), xytext=(0, -6.5),
                        textcoords="offset points", va="center", fontsize=6, color=st.INK2)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([st.label(r[0]) for r in rows])
    ax.set_ylim(len(rows) - 0.4, -0.6)
    xs = [v for r in rows for v in (r[1], r[2]) if v]
    ax.set_xlim(0, max(xs) * 1.25)
    st.xgrid(ax)
    ax.tick_params(axis="y", length=0)
    ax.set_xlabel("wall time (s), median")
    lat = [r["ctrlc_latency_s"] for r in lab.exp("e6") if r.get("ctrlc_latency_s") is not None]
    if lat:
        st.note(ax, 1.0, 1.0, "Ctrl-C to all stopped: %.2f s" % median(lat), ha="right", va="bottom", fontsize=6.5)
    return f


def analyse_ok(r):
    return not r.get("error") and r.get("result_ok") is not False and r.get("wall_s")


@fig("eval-e7-overhead-cdf", "single", 1.5, SEC_MICRO,
     "E7: CDF of whole-job wall time for a trivial script with the shim on and off.",
     "Cost of transparent interception (E7): CDF of the whole-job wall time of a script that does no "
     "parallel work, with the shim injected and without. The gap between the medians is the per-job "
     "overhead.")
def e7(lab, ctx):
    runs = lab.exp("e7")
    on = sorted(r["wall_s"] for r in runs if r["system"] == "pipedpeer-shim")
    off = sorted(r["wall_s"] for r in runs if r["system"] == "pipedpeer-noshim")
    need(on and off, "needs e7 shim and no-shim runs")
    f, ax = st.figure("single", 1.5)
    ax = ax[0][0]
    for xs, s, lab_ in ((off, "plain", "shim off"), (on, "pipedpeer", "shim on")):
        y = np.arange(1, len(xs) + 1) / len(xs)
        k = st.line_kw(s, ms=3.2)
        ax.step([xs[0]] + xs, [0] + list(y), where="post", color=k["color"], ls=k["linestyle"], lw=1.0)
        ax.plot(xs, y, ls="none", marker=k["marker"], ms=3.2, mfc=k["markerfacecolor"], mec=k["color"], mew=0.7)
        ax.annotate("%s (n = %d)" % (lab_, len(xs)), (xs[-1], 1.0), xytext=(4, -2), textcoords="offset points",
                    ha="left", va="top", fontsize=6.5, color=k["color"])
    d = (median(on) - median(off)) * 1000
    ax.axhline(0.5, color=st.RULE, lw=0.5, ls=(0, (1, 1.5)))
    lo, hi = min(on + off), max(on + off)
    span = hi - lo or 0.1
    ax.set_xlim(lo - 0.08 * span, hi + 0.55 * span)
    ax.set_ylim(0, 1.05)
    ax.set_yticks([0, 0.5, 1])
    ax.set_xlabel("whole-job wall time (s)")
    ax.set_ylabel("fraction of runs")
    st.ygrid(ax)
    st.note(ax, 0.98, 0.08, "median difference %+.0f ms" % d, ha="right", fontsize=6.5)
    return f


T1 = ["torchrun", "pipedpeer-exact", "pipedpeer-fast"]


def loss(r):
    try:
        return float(str(r.get("result")).split()[-1])
    except (ValueError, IndexError):
        return None


@fig("eval-e8-training", "double", 1.5, SEC_TRAIN,
     "E8: DDP training of T1: wall time, bytes sent per rank, and final loss, per launcher.",
     "Data-parallel training (E8). (a) Wall time, median and IQR. (b) Bytes each rank sent. (c) Final "
     "training loss of each repeat. Exact averages gradients every step; fast averages weights every few "
     "steps.")
def e8(lab, ctx):
    rows = {r["system"]: r for r in lab.rows("e8") if r.get("median_wall_s")}
    need(rows, "needs e8 runs")
    systems = [s for s in T1 if s in rows]
    runs = lab.exp("e8")
    f, axes = st.figure("double", 1.5, 1, 3)
    names = [st.label(s) for s in systems]
    y = np.arange(len(systems))

    def bars(ax, vals, lo=None, hi=None, fmt="%.1f"):
        for i, s in enumerate(systems):
            ax.barh(i, vals[i], height=0.6, **st.bar_kw(s, hollow=s == "pipedpeer-fast"))
            if lo is not None:
                ax.plot([lo[i], hi[i]], [i, i], color=st.INK, lw=0.7)
            ax.text((hi[i] if hi is not None else vals[i]), i, " " + fmt % vals[i], va="center", fontsize=6.5,
                    color=st.INK2)
        ax.set_yticks(y)
        ax.invert_yaxis()
        ax.tick_params(axis="y", length=0)
        st.xgrid(ax)

    a, b, c = axes[0]
    bars(a, [rows[s]["median_wall_s"] for s in systems], [rows[s]["q1_wall_s"] for s in systems],
         [rows[s]["q3_wall_s"] for s in systems])
    a.set_yticklabels(names)
    a.set_xlim(0, max(rows[s]["q3_wall_s"] for s in systems) * 1.25)
    a.set_xlabel("wall time (s)")
    st.panel_tag(a, "(a)")
    mb = [median([r.get("bytes_per_rank") for r in runs if r["system"] == s]) or 0 for s in systems]
    bars(b, [m / 1e6 for m in mb], fmt="%.0f MB")
    b.set_yticklabels([])
    b.set_xlim(0, max(mb) / 1e6 * 1.3)
    b.set_xlabel("bytes sent per rank (MB)")
    st.panel_tag(b, "(b)")
    for i, s in enumerate(systems):
        ls = [loss(r) for r in runs if r["system"] == s and loss(r) is not None]
        c.plot(ls, [i + (k - (len(ls) - 1) / 2) * 0.12 for k in range(len(ls))], **st.point_kw(s, 3.6))
    c.set_yticks(y)
    c.set_yticklabels([])
    c.set_ylim(len(systems) - 0.5, -0.5)
    c.tick_params(axis="y", length=0)
    st.xgrid(c)
    c.set_xlabel("final training loss (each repeat)")
    c.ticklabel_format(axis="x", useOffset=False)
    st.panel_tag(c, "(c)")
    return f
