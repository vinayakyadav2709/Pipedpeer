"""The more-bench experiments: E16 energy, E17 idle footprint and cluster
start, E18 noisy neighbour, E19 cold vs warm ship, E20 unmodified pandas
and scikit-learn (scripts/campaign/FIGURES.md, "More benchmarks")."""
from collections import defaultdict

import numpy as np
from matplotlib.patches import Patch
from matplotlib.ticker import MaxNLocator

import analyse_headline as hl
import analyse_more as more

from . import style as st
from .data import median
from .registry import fig, need

SEC_E2E = "Evaluation > End-to-end runs (Q3)"
SEC_MICRO = "Evaluation > Microbenchmarks (Q1, Q2)"
SEC_SETUP = "Evaluation > Setup"


def latest(runs):
    return sorted(hl.latest(runs), key=lambda r: st.order_key(r["system"]))


# --- E16 ------------------------------------------------------------------------

def e16_groups(lab):
    rows = more.e16_rows(lab.runs, lab.energy)
    summ = more.e16_summary(lab.runs, rows)
    return [g for g in summ["groups"] if g["workload"] == "W1" and g.get("median_energy_all_pcs_j")], summ


def synthetic_e16():
    """A made-up example in the shape of e16_summary's groups, so the draft
    preview shows the layout before the lab day has joules."""
    out = [{"system": "plain", "n": 1, "median_wall_s": 640.0, "median_energy_all_pcs_j": 17 * 640 * 22.0}]
    for s, eff, w in (("pipedpeer", 0.86, 58.0), ("ray", 0.62, 61.0), ("dask", 0.55, 60.0)):
        for n in (1, 4, 16):
            wall = 640.0 / (n * eff if n > 1 else 0.97)
            out.append({"system": s, "n": n, "median_wall_s": wall,
                        "median_energy_all_pcs_j": wall * (17 * 22.0 + n * (w - 22.0))})
    return out


@fig("eval-e16-energy", "double", 1.7, SEC_E2E,
     "E16: package energy to solution for W1, against N and against wall time.",
     "Energy to solution (E16), W1, package energy from RAPL summed over all 17 PCs, idle ones "
     "included. (a) Energy against N; the dashed line is plain Python on one PC. (b) Energy against "
     "wall time, both log: lower left is faster and cheaper.")
def e16(lab, ctx):
    groups, summ = e16_groups(lab)
    mark = None
    if not groups:
        why = sorted(set((summ.get("unavailable") or {}).values()))
        if not ctx.draft:
            need(False, "no joules (%s)" % ("; ".join(why) or "no e16 or energy archive"))
        groups, mark = synthetic_e16(), "synthetic"
    f, axes = st.figure("double", 1.7, 1, 2, width_ratios=[1, 1.15])
    a, b = axes[0]
    plain = next((g for g in groups if g["system"] == "plain"), None)
    if plain:
        a.axhline(plain["median_energy_all_pcs_j"] / 1e3, color=st.MUTED, lw=0.6, ls=(0, (3, 2)))
        a.annotate("plain, 1 PC", (1.0, plain["median_energy_all_pcs_j"] / 1e3), xycoords=("axes fraction", "data"),
                   xytext=(0, 1.5), textcoords="offset points", ha="right", va="bottom", fontsize=6.5,
                   color=st.INK2)
    ns = sorted({g["n"] for g in groups if g["system"] in st.DIST})
    ends = []
    for s in st.DIST:
        gs = sorted((g for g in groups if g["system"] == s), key=lambda g: g["n"])
        if not gs:
            continue
        a.plot([g["n"] for g in gs], [g["median_energy_all_pcs_j"] / 1e3 for g in gs], **st.line_kw(s))
        ends.append((gs[-1]["n"], gs[-1]["median_energy_all_pcs_j"] / 1e3, st.label(s), st.SYS[s].color))
        b.plot([g["median_wall_s"] for g in gs], [g["median_energy_all_pcs_j"] / 1e3 for g in gs],
               **dict(st.line_kw(s), linewidth=0.6))
        for g in gs:
            b.annotate("%d" % g["n"], (g["median_wall_s"], g["median_energy_all_pcs_j"] / 1e3), xytext=(3, 2),
                       textcoords="offset points", fontsize=6, color=st.SYS[s].color)
    if plain:
        b.plot([plain["median_wall_s"]], [plain["median_energy_all_pcs_j"] / 1e3], **st.point_kw("plain", 4))
        b.annotate("plain", (plain["median_wall_s"], plain["median_energy_all_pcs_j"] / 1e3), xytext=(-4, -2),
                   textcoords="offset points", ha="right", va="top", fontsize=6, color=st.INK2)
    st.log2_x(a, ns)
    a.set_xlim(min(ns) / 1.3, max(ns) * 2.3)
    a.set_ylim(bottom=0)
    st.end_labels(a, ends)
    a.set_xlabel("PCs (N)")
    a.set_ylabel("energy, all PCs (kJ)")
    st.ygrid(a)
    st.panel_tag(a, "(a)")
    b.set_xscale("log")
    b.set_yscale("log")
    b.set_xlabel("wall time (s)")
    b.set_ylabel("energy (kJ)")
    st.plain_log(b)
    b.grid(which="major", color=st.GRID, lw=0.4)
    b.legend(handles=st.handles(st.DIST + (["plain"] if plain else [])), loc="lower right", ncol=1,
             handlelength=1.6)
    st.panel_tag(b, "(b)")
    st.note(b, 0.02, 0.97, "point labels: N", va="top", fontsize=6.5)
    return (f, mark) if mark else f


# --- E17 ------------------------------------------------------------------------

@fig("eval-e17-footprint", "single", 1.5, SEC_MICRO,
     "E17: idle cost per PC of each system's daemons over 60 s: CPU and memory.",
     "Idle cost of an installed cluster (E17), per PC, averaged over a quiet minute with no job: "
     "(a) CPU as a percentage of one core; (b) memory, proportional set size (bar) and resident set "
     "size (outline), of every process the system leaves running.")
def e17_footprint(lab, ctx):
    runs = latest([r for r in lab.exp("e17", ok=False) if r.get("workload") == "E17-idle" and r.get("footprint")])
    need(runs, "needs e17 runs")
    f, axes = st.figure("single", 1.5, 1, 2, sharey=True)
    a, b = axes[0]
    y = np.arange(len(runs))
    for i, r in enumerate(runs):
        s = r["system"]
        pcs = (r.get("n") or 0) + 1
        cpu = r.get("cpu_pct_mean_per_pc") or 0
        pss, rss = (r.get("pss_mb_total") or 0) / pcs, (r.get("rss_mb_total") or 0) / pcs
        a.barh(i, cpu, height=0.6, **st.bar_kw(s))
        a.text(cpu, i, " %.2g%%" % cpu, va="center", fontsize=6.5, color=st.INK2)
        b.barh(i, rss, height=0.6, facecolor="none", edgecolor=st.SYS[s].color, lw=0.6, ls=(0, (2, 1)))
        b.barh(i, pss, height=0.6, **st.bar_kw(s))
        b.text(rss, i, " %.0f MB" % pss, va="center", fontsize=6.5, color=st.INK2)
    a.set_yticks(y)
    a.set_yticklabels([st.label(r["system"]) for r in runs])
    a.invert_yaxis()
    a.set_xlim(0, max(r.get("cpu_pct_mean_per_pc") or 0 for r in runs) * 1.45)
    b.set_xlim(0, max((r.get("rss_mb_total") or 0) / ((r.get("n") or 0) + 1) for r in runs) * 1.5)
    for ax in (a, b):
        ax.tick_params(axis="y", length=0)
        st.xgrid(ax)
    a.set_xlabel("idle CPU (% of a core)")
    b.set_xlabel("memory per PC (MB)")
    st.panel_tag(a, "(a)")
    st.panel_tag(b, "(b)")
    return f


@fig("eval-e17-ready", "single", 1.15, SEC_SETUP,
     "E17: time from nothing running to a usable cluster, and the commands a user types.",
     "From nothing running to a cluster that accepts a job (E17): seconds from the first start command "
     "until the readiness check passed; the whisker reaches back to the last failed check. The number is "
     "the commands a user types after every boot.")
def e17_ready(lab, ctx):
    runs = [r for r in latest([r for r in lab.exp("e17", ok=False) if r.get("workload") == "E17-idle"])
            if r.get("ready_s") is not None]
    need(runs, "needs e17 runs with ready_s")
    f, ax = st.figure("single", 1.15)
    ax = ax[0][0]
    for i, r in enumerate(runs):
        s = r["system"]
        ax.barh(i, r["ready_s"], height=0.58, **st.bar_kw(s))
        if r.get("ready_after_s") is not None:
            ax.plot([r["ready_after_s"], r["ready_s"]], [i, i], color=st.INK, lw=0.7)
            ax.plot([r["ready_after_s"]], [i], marker="|", ms=4, color=st.INK, mew=0.7)
        ax.text(r["ready_s"], i, "  %.1f s, %d command%s" % (r["ready_s"], r.get("commands_typed") or 0,
                                                             "" if r.get("commands_typed") == 1 else "s"),
                va="center", fontsize=6.5, color=st.INK2)
    ax.set_yticks(range(len(runs)))
    ax.set_yticklabels([st.label(r["system"]) for r in runs])
    ax.invert_yaxis()
    ax.tick_params(axis="y", length=0)
    ax.set_xlim(0, max(r["ready_s"] for r in runs) * 1.75)
    st.xgrid(ax)
    ax.set_xlabel("seconds to a usable cluster of %d PCs" % ((runs[0].get("n") or 0) + 1))
    return f


# --- E18 ------------------------------------------------------------------------

@fig("eval-e18-straggler", "double", 1.75, SEC_E2E,
     "E18: a noisy neighbour on one PC: wall time with and without it, and each system's throughput per PC.",
     "Noisy neighbour (E18): busy loops on half the cores of one PC, started a few seconds into W1. "
     "(a) Wall time without (hollow) and with (filled) the hog; the label is the slowdown. (b-d) Items "
     "finished per second in the hog run, stacked per PC, the hogged PC hatched; the dashed line is the "
     "hog's start. Panels (b-d) share the y axis.")
def e18(lab, ctx):
    runs = [r for r in lab.exp("e18", ok=False) if not r.get("warmup")]
    need(runs, "needs e18 runs")
    _, summ = more.e18_tables(lab.runs)
    systems = [s for s in st.DIST if s in summ]
    need(systems, "no usable e18 pairs")
    st.apply()
    f = st.plt.figure(figsize=(st.DOUBLE, 1.75), layout="constrained")
    gs = f.add_gridspec(1, 4, width_ratios=[1.05, 1, 1, 1])
    a = f.add_subplot(gs[0])
    y = np.arange(len(systems))
    for i, s in enumerate(systems):
        m = summ[s]
        if m["wall_none_s"]:
            a.barh(i - 0.18, m["wall_none_s"], height=0.34, **st.bar_kw(s, hollow=True))
        if m["wall_hog_s"]:
            a.barh(i + 0.18, m["wall_hog_s"], height=0.34, **st.bar_kw(s))
        if m["slowdown"]:
            a.text(max(m["wall_hog_s"], m["wall_none_s"]), i, "  %.2f×" % m["slowdown"], va="center",
                   fontsize=6.5, color=st.INK2)
    a.set_yticks(y)
    a.set_yticklabels([st.label(s) for s in systems])
    a.invert_yaxis()
    a.tick_params(axis="y", length=0)
    a.set_xlim(0, max(max(summ[s]["wall_hog_s"] or 0, summ[s]["wall_none_s"] or 0) for s in systems) * 1.75)
    st.xgrid(a)
    a.set_xlabel("wall time (s)")
    st.panel_tag(a, "(a)")
    a.legend(handles=[Patch(facecolor="white", edgecolor=st.INK2, lw=0.6, label="no hog"),
                      Patch(facecolor=st.INK2, edgecolor=st.INK2, label="hog")], loc="upper right",
             handlelength=1.0, fontsize=6.5)
    hog = {}
    for r in runs:
        if r.get("fault") == "hog" and (r.get("timeline") or {}).get("per_pc"):
            hog[r["system"]] = r
    share = None
    ymax = 0
    for j, s in enumerate(systems):
        ax = f.add_subplot(gs[j + 1], sharey=share)
        share = share or ax
        r = hog.get(s)
        st.panel_tag(ax, "(%s) %s" % ("bcd"[j], st.label(s)))
        if not r:
            st.note(ax, 0.5, 0.5, "no hog timeline", ha="center")
            continue
        tl = r["timeline"]
        pcs = sorted(tl["per_pc"], key=lambda pc: (pc not in (r.get("hog_pcs") or []), pc))
        n = max(len(v) for v in tl["per_pc"].values())
        x = np.arange(n + 1) * tl.get("bin_s", 1)
        base = np.zeros(n)
        col = st.SYS[s].color
        for k, pc in enumerate(pcs):
            v = np.zeros(n)
            v[:len(tl["per_pc"][pc])] = tl["per_pc"][pc]
            hogged = pc in (r.get("hog_pcs") or [])
            top = base + v
            ax.fill_between(x, np.append(top, top[-1]), np.append(base, base[-1]), step="post",
                            facecolor=st.tint(col, 0.35 if hogged else 1.0 - 0.3 * (k % 3)), edgecolor=col,
                            hatch="////" if hogged else "", lw=0.3)
            base = top
        ymax = max(ymax, base.max())
        for e in r.get("events") or []:
            if e.get("kind") == "hog" and e.get("t_ns"):
                ax.axvline((e["t_ns"] - r["start_ns"]) / 1e9, color=st.INK, lw=0.6, ls=(0, (3, 2)))
        ax.set_xlim(0, max(x[-1], r.get("wall_s") or 0))
        if r.get("hog_share") is not None and r.get("fair_share"):
            st.note(ax, 0.98, 0.97, "%s did %.0f%%\n(fair %.0f%%)" % (", ".join(r.get("hog_pcs") or []),
                                                                     100 * r["hog_share"], 100 * r["fair_share"]),
                    ha="right", va="top", fontsize=6)
        ax.set_xlabel("seconds")
        ax.xaxis.set_major_locator(MaxNLocator(nbins=4, integer=True))
        st.ygrid(ax)
        if j:
            ax.tick_params(axis="y", labelleft=False)
        else:
            ax.set_ylabel("items / s")
    share.set_ylim(0, ymax * 1.3)
    return f


# --- E19 ------------------------------------------------------------------------

@fig("eval-e19-ship", "single", 1.65, SEC_MICRO,
     "E19: time to first result while iterating: cold, warm, edited script, new dependency.",
     "Time to first result while iterating on a script (E19), 16 PCs: the first run of a new "
     "environment (cold), the same again (warm), one constant edited, and one new dependency. "
     "PipedPeer's bar is split into environment build, ship, and the rest of the job. Median over "
     "repeats.")
def e19(lab, ctx):
    runs = lab.exp("e19")
    need(runs, "needs usable e19 runs")
    systems = [s for s in ("pipedpeer", "ray") if any(r["system"] == s for r in runs)]
    acc = defaultdict(lambda: defaultdict(list))
    for r in runs:
        for k in ("ttfr_s", "env_build_s", "env_send_s"):
            if r.get(k) is not None:
                acc[(r["system"], r.get("phase"))][k].append(r[k])
    phases = [p for p in more.PHASES if any((s, p) in acc for s in systems)]
    f, ax = st.figure("single", 1.65)
    ax = ax[0][0]
    w = 0.8 / len(systems)
    top = 0
    pp = st.SYS["pipedpeer"].color
    for j, s in enumerate(systems):
        for i, p in enumerate(phases):
            d = acc.get((s, p))
            if not d:
                continue
            x = i + (j - (len(systems) - 1) / 2) * w
            t = median(d["ttfr_s"]) or 0
            top = max(top, t)
            if s == "pipedpeer" and d.get("env_build_s"):
                bld, snd = median(d["env_build_s"]), median(d.get("env_send_s") or [0])
                ax.bar(x, bld, width=w * 0.9, facecolor="#0B3D66", edgecolor="#0B3D66", lw=0.4)
                ax.bar(x, snd, bottom=bld, width=w * 0.9, facecolor=st.tint(pp, 0.4), edgecolor=pp, hatch="////",
                       lw=0.4)
                ax.bar(x, max(0, t - bld - snd), bottom=bld + snd, width=w * 0.9, facecolor=pp, edgecolor=pp, lw=0.4)
            else:
                ax.bar(x, t, width=w * 0.9, **st.bar_kw(s))
            ax.text(x, t, "%.1f" % t, ha="center", va="bottom", fontsize=6, color=st.INK2)
    ax.set_xticks(range(len(phases)))
    ax.set_xticklabels({"cold": "cold", "warm": "warm", "edit": "edit script", "newdep": "new dependency"}.get(p, p)
                       for p in phases)
    ax.tick_params(axis="x", length=0)
    ax.set_ylim(0, top * 1.18)
    ax.set_ylabel("time to first result (s)")
    st.ygrid(ax)
    hs = [Patch(facecolor="#0B3D66", edgecolor="#0B3D66", label="PipedPeer: build env"),
          Patch(facecolor=st.tint(pp, 0.4), edgecolor=pp, hatch="////", label="ship"),
          Patch(facecolor=pp, edgecolor=pp, label="run")]
    hs += [Patch(label=st.label(s), **st.bar_kw(s)) for s in systems if s != "pipedpeer"]
    st.legend_top(f, hs, top=0.86, ncol=len(hs), handlelength=1.1, columnspacing=0.7)
    return f


# --- E20 ------------------------------------------------------------------------

@fig("eval-e20-unmodified", "single", 1.6, SEC_E2E,
     "E20: speedup of unmodified pandas groupby (W5) and scikit-learn cross-validation (W6) at the largest N.",
     "Unmodified pandas and scikit-learn (E20). W5 fits a model per pandas group through joblib; W6 is "
     "cross_val_score with n_jobs=-1. Speedup over plain Python on one PC; the tick above each bar "
     "marks that every run matched plain Python, and the line under each name is the "
     "number of script lines changed.")
def e20(lab, ctx):
    rows = [r for r in more.e20_summary(lab.runs) if r["system"] in st.DIST]
    need(any(r.get("speedup_vs_plain") for r in rows), "needs e20 runs")
    wls = sorted({r["workload"] for r in rows})
    f, axes = st.figure("single", 1.6, 1, len(wls), sharey=True)
    ymax = max([r["speedup_vs_plain"] for r in rows if r.get("speedup_vs_plain")] + [1.0])
    for j, (ax, wl) in enumerate(zip(axes[0], wls)):
        rs = sorted((r for r in rows if r["workload"] == wl), key=lambda r: st.order_key(r["system"]))
        nmax = max(r["n"] for r in rs)
        rs = [r for r in rs if r["n"] == nmax]
        ax.axhline(1.0, color=st.MUTED, lw=0.6, ls=(0, (3, 2)))
        for i, r in enumerate(rs):
            if not r.get("speedup_vs_plain"):
                ax.text(i, max(1.0, 0.04 * ymax) * 1.12, "no correct\nresult", ha="center", va="bottom", fontsize=6,
                        color="#B03A2E", linespacing=1.0)
                continue
            ax.bar(i, r["speedup_vs_plain"], width=0.62, **st.bar_kw(r["system"]))
            ok = r.get("result_ok_all")
            ax.text(i, r["speedup_vs_plain"], "%.1f× %s" % (r["speedup_vs_plain"], "✓" if ok else "(wrong)"),
                    ha="center", va="bottom", fontsize=6.3, color=st.INK if ok else "#B03A2E")
        ax.set_xticks(range(len(rs)))
        ax.set_xticklabels(["%s\n%s line%s" % (st.label(r["system"]), r.get("script_lines_changed") or 0,
                                               "" if r.get("script_lines_changed") == 1 else "s") for r in rs],
                           fontsize=6.5)
        ax.tick_params(axis="x", length=0)
        ax.set_xlim(-0.6, len(rs) - 0.4)
        st.ygrid(ax)
        st.panel_tag(ax, "(%s) %s, N = %d" % ("ab"[j], wl, nmax))
    axes[0][0].set_ylim(0, ymax * 1.25)
    axes[0][0].set_ylabel("speedup (×)")
    return f
