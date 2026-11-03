"""The headline experiments: E12 elastic join, E13 churn, E14 curl to
cluster, E15 network sensitivity (scripts/campaign/FIGURES.md)."""
from collections import defaultdict

import numpy as np
from matplotlib.patches import Patch

import analyse
import analyse_headline as hl
import sample_figures as sf
import story_model as sm

from . import style as st
from .registry import fig, need

SEC_E2E = "Evaluation > End-to-end runs (Q3)"
SEC_SETUP = "Evaluation > Setup"

COHORT = [(1.0, ""), (0.5, "////"), (0.3, "\\\\\\\\"), (0.18, "...."), (0.1, "xx")]


def rel(r, t_ns):
    return (t_ns - r["start_ns"]) / 1e9 if t_ns and r.get("start_ns") else None


def status(r):
    if r.get("error"):
        return "failed"
    return "correct result" if r.get("result_ok") else "wrong result"


def panel_head(ax, text):
    ax.text(0.0, 1.0, text, transform=ax.transAxes, ha="left", va="bottom", fontsize=7, color=st.INK)


def by_system(runs):
    return sorted(hl.latest(runs), key=lambda r: st.order_key(r["system"]))


@fig("eval-e12-join", "double", 1.75, SEC_E2E,
     "E12: throughput while PCs join mid-run, stacked by the cohort of PCs that did the work.",
     "Elastic join (E12). Items finished per second, stacked by cohort: the PCs the job started on, "
     "then each group that joined (dashed line, labelled with the PCs added). A cohort that stays at "
     "zero is a system that never used the PCs that joined. Panels share the y axis.")
def e12(lab, ctx):
    runs = by_system([r for r in lab.exp("e12", ok=False) if (r.get("timeline") or {}).get("per_pc")])
    need(runs, "needs e12 runs with a timeline")
    f, axes = st.figure("double", 1.75, 1, len(runs), sharey=True)
    ymax, ncoh = 0, 0
    for j, (ax, r) in enumerate(zip(axes[0], runs)):
        tl = r["timeline"]
        bin_s = tl.get("bin_s", 1)
        coh = hl.cohorts(r)
        ncoh = max(ncoh, max(coh.values()) + 1)
        n = max(len(v) for v in tl["per_pc"].values())
        layers = np.zeros((max(coh.values()) + 1, n))
        for pc, vals in tl["per_pc"].items():
            k = coh.get(pc, 0)
            layers[k, :len(vals)] += vals
        x = np.arange(n + 1) * bin_s
        base = np.zeros(n)
        col = st.SYS[r["system"]].color
        for k in range(layers.shape[0]):
            a, hatch = COHORT[min(k, len(COHORT) - 1)]
            top = base + layers[k]
            ax.fill_between(x, np.append(top, top[-1]), np.append(base, base[-1]), step="post",
                            facecolor=st.tint(col, a), edgecolor=col, hatch=hatch, linewidth=0.3)
            base = top
        ax.step(x, np.append(base, base[-1]), where="post", color=col, lw=0.8)
        ymax = max(ymax, base.max())
        for e in (e for e in r.get("events") or [] if e.get("kind") == "join"):
            t = rel(r, e.get("t_ns"))
            if t is None:
                continue
            ax.axvline(t, color=st.INK, lw=0.6, ls=(0, (3, 2)))
            ax.annotate("+%d" % len(e.get("pcs") or []), (t, 1.0), xycoords=("data", "axes fraction"),
                        xytext=(2, -1), textcoords="offset points", ha="left", va="top", fontsize=6.5)
            seen = rel(r, e.get("seen_ns"))
            if seen is not None:
                ax.plot([seen], [0], marker="|", ms=4, mew=0.8, color=st.INK, clip_on=False, zorder=5)
        ax.set_xlim(0, (r.get("wall_s") or x[-1]) * 1.02)
        panel_head(ax, "(%s) %s: %.0f s, %s" % ("abc"[j], st.label(r["system"]), r.get("wall_s") or 0, status(r)))
        ax.set_xlabel("seconds since the job started")
        st.ygrid(ax)
    axes[0][0].set_ylim(0, ymax * 1.15)
    axes[0][0].set_ylabel("items finished / s")
    hs = [Patch(facecolor=st.tint(st.MUTED, a), edgecolor=st.MUTED, hatch=h, linewidth=0.3,
                label="first PCs" if k == 0 else "join %d" % k) for k, (a, h) in enumerate(COHORT[:ncoh])]
    st.legend_top(f, hs, top=0.86, handlelength=1.4)
    return f


@fig("eval-e13-churn", "double", 1.75, SEC_E2E,
     "E13: throughput under churn, a worker killed and restarted periodically, per system.",
     "Churn (E13): a worker is killed with kill -9 periodically (solid line, filled triangle) and "
     "restarted a few seconds later (dotted line, hollow triangle). Items finished per second over all "
     "PCs; each panel gives the outcome, the wall time against the same job without churn, and the "
     "items computed twice.")
def e13(lab, ctx):
    runs = by_system([r for r in lab.exp("e13", ok=False)
                      if (r.get("timeline") or {}).get("per_pc") or r.get("events")])
    need(runs, "needs e13 runs")
    f, axes = st.figure("double", 1.75, 1, len(runs), sharey=True)
    ymax = 0
    for j, (ax, r) in enumerate(zip(axes[0], runs)):
        tot = hl._total(r.get("timeline"))
        bin_s = (r.get("timeline") or {}).get("bin_s", 1)
        col = st.SYS[r["system"]].color
        x = np.arange(len(tot) + 1) * bin_s
        y = np.append(tot, tot[-1] if tot else 0)
        ax.fill_between(x, y, step="post", color=st.tint(col, 0.15), lw=0)
        ax.step(x, y, where="post", color=col, lw=0.8)
        ymax = max([ymax] + list(tot))
        xmax = max(x[-1], r.get("wall_s") or 0)
        evs = [e for e in r.get("events") or [] if rel(r, e.get("t_ns")) is not None and not e.get("after_job")
               and rel(r, e.get("t_ns")) <= xmax]
        dense = len(evs) > 16
        for e in evs:
            t = rel(r, e["t_ns"])
            kill = e.get("kind") == "kill"
            ax.axvline(t, color=st.INK if kill else st.INK2, lw=0.35 if dense else 0.5,
                       ls="-" if kill else (0, (1, 1.2)), alpha=0.35 if dense else 0.8)
            ax.plot([t], [1.0], transform=ax.get_xaxis_transform(), marker="v" if kill else "^",
                    ms=2 if dense else 3, mfc=st.INK if kill else "white", mec=st.INK, mew=0.5, clip_on=False,
                    zorder=6)
        kills = sum(1 for e in evs if e.get("kind") == "kill")
        ref = hl.no_churn_wall(lab.runs, r)
        rr = r.get("items_rerun")
        line2 = "%.0f s%s, %d kills" % (r.get("wall_s") or 0, " vs %.0f s without churn" % ref if ref else "", kills)
        if rr is not None:
            line2 += ", %s items re-run" % format(int(rr), ",")
        panel_head(ax, "(%s) %s: %s\n%s" % ("abc"[j], st.label(r["system"]), status(r), line2))
        ax.set_xlim(0, xmax)
        ax.set_xlabel("seconds since the job started")
        st.ygrid(ax)
    axes[0][0].set_ylim(0, ymax * 1.12)
    axes[0][0].set_ylabel("items finished / s")
    return f


E14_EVENTS = [("labpc", "o", "LAB-PC line"), ("ssh_ok", "s", "answers SSH"), ("install_end", "^", "installed"),
              ("join_end", "D", "joined"), ("first_work", "*", "first work")]


@fig("eval-e14-staircase", "single", 0, SEC_SETUP,
     "E14: from the first command on the head PC to every PC running work, one row per PC.",
     "Curl to cluster (E14): for every PC, when it printed its LAB-PC line, answered SSH, had "
     "PipedPeer installed, joined, and first ran work of the day's first distributed job, relative to "
     "the head command on pc00. Dashed lines: cluster ready and first job.")
def e14(lab, ctx):
    stc = sm.staircase(lab.inputs.timeline, lab.runs)
    need(stc and stc["rows"], "needs timeline.jsonl (E14)")
    rows = [r for r in stc["rows"] if any(r.get(ev + "_s") is not None for ev, _, _ in E14_EVENTS)]
    key = "labpc_s" if any(r.get("labpc_s") is not None for r in rows) else "first_work_s"
    rows.sort(key=lambda r: (r.get(key) if r.get(key) is not None else 1e9, sf.pc_sort(r["pc"])))
    allv = [r[ev + "_s"] for r in rows for ev, _, _ in E14_EVENTS if r.get(ev + "_s") is not None]
    minutes = max(allv) - min(min(allv), 0) > 180
    u = 60.0 if minutes else 1.0
    h = 0.62 + 0.105 * len(rows)
    f, ax = st.figure("single", max(1.35, h))
    ax = ax[0][0]
    present = []
    for i, r in enumerate(rows):
        vs = [r[ev + "_s"] / u for ev, _, _ in E14_EVENTS if r.get(ev + "_s") is not None]
        ax.plot([min(vs), max(vs)], [i, i], color=st.RULE, lw=0.6, zorder=1)
        for ev, m, _ in E14_EVENTS:
            v = r.get(ev + "_s")
            if v is None:
                continue
            first = ev == "first_work"
            ax.plot([v / u], [i], ls="none", marker=m, ms=5.2 if first else 3.2, zorder=3,
                    mfc=st.SYS["pipedpeer"].color if first else "white",
                    mec=st.SYS["pipedpeer"].color if first else st.INK, mew=0.6)
            if ev not in present:
                present.append(ev)
    s = stc["summary"]
    marks = [("cluster ready", s.get("head_to_cluster_ready_s")), ("first job", s.get("head_to_first_job_start_s"))]
    for k, (name, v) in enumerate(m for m in marks if m[1] is not None):
        ax.axvline(v / u, color=st.INK2, lw=0.6, ls=(0, (3, 2)))
        ax.annotate("%s %.1f" % (name, v / u), (v / u, 1.0), xycoords=("data", "axes fraction"),
                    xytext=(-2, -1 - 8 * k), textcoords="offset points", fontsize=6, ha="right", va="top",
                    color=st.INK2, bbox=dict(facecolor="white", edgecolor="none", pad=0.4))
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([r["pc"] for r in rows], fontsize=6 if len(rows) > 8 else 7)
    ax.set_ylim(len(rows) - 0.5, -0.7)
    ax.tick_params(axis="y", length=0)
    st.xgrid(ax)
    anchor = "the head command on pc00" if stc["anchor"] == "head_start" else "the first job's start"
    ax.set_xlabel("%s since %s" % ("minutes" if minutes else "seconds", anchor))
    from matplotlib.lines import Line2D
    hs = [Line2D([], [], ls="none", marker=m, ms=5.2 if ev == "first_work" else 3.2, label=lab_,
                 mfc=st.SYS["pipedpeer"].color if ev == "first_work" else "white",
                 mec=st.SYS["pipedpeer"].color if ev == "first_work" else st.INK, mew=0.6)
          for ev, m, lab_ in E14_EVENTS if ev in present]
    st.legend_top(f, hs, top=1 - 0.2 / max(1.35, h), handlelength=0.8, columnspacing=0.8)
    return f


@fig("eval-e15-network", "single", 1.55, SEC_E2E,
     "E15: wall-time slowdown as round-trip time is added between every pair of PCs.",
     "Network sensitivity (E15): W1 wall time with netem delay added between every pair of PCs, "
     "relative to the same system with no added delay. Median; bars span the IQR over repeats.")
def e15(lab, ctx):
    walls = defaultdict(list)
    n = None
    for r in lab.exp("e15"):
        if r.get("rtt_added_ms") is not None and r.get("wall_s"):
            walls[(r["system"], r["rtt_added_ms"])].append(r["wall_s"])
            n = r.get("n")
    need(walls, "needs e15 runs")
    rtts = sorted({k[1] for k in walls})
    f, ax = st.figure("single", 1.55)
    ax = ax[0][0]
    ax.axhline(1.0, color=st.MUTED, lw=0.6, ls=(0, (3, 2)))
    ends = []
    for s in st.DIST:
        b = analyse.stats(walls.get((s, 0), []))[0]
        if not b:
            continue
        pts = [(rt, analyse.stats(walls[(s, rt)])) for rt in rtts if walls.get((s, rt))]
        x = [p[0] for p in pts]
        m = [p[1][0] / b for p in pts]
        lo = [p[1][1] / b for p in pts]
        hi = [p[1][2] / b for p in pts]
        ax.errorbar(x, m, yerr=[np.subtract(m, lo), np.subtract(hi, m)], elinewidth=0.6, capsize=1.5,
                    capthick=0.6, **st.line_kw(s))
        ends.append((x[-1], m[-1], st.label(s), st.SYS[s].color))
    ax.set_xticks(rtts)
    ax.set_xlim(-0.04 * max(rtts), max(rtts) * 1.3)
    st.end_labels(ax, ends)
    ax.set_ylim(bottom=0)
    st.ygrid(ax)
    ax.set_xlabel("added round-trip time (ms)")
    ax.set_ylabel("slowdown vs 0 ms (×)")
    st.note(ax, 0.02, 0.05, "W1, N = %s" % n, fontsize=6.5)
    return f
