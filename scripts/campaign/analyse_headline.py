"""The headline figures: E12 elastic join, E13 churn, E14 curl to cluster,
E15 network sensitivity. analyse.py calls run(); FIGURES.md describes each
figure and the fields it reads.

  csv/e12_timeline.csv   run, system, t_s, pc, cohort, items_per_s
  csv/e13_summary.csv    one row per E13 run, with E1b's no-churn wall
  csv/e14_pcs.csv        one row per PC: seconds from head_start to each event
  e14_summary.json       head line -> cluster ready -> first distributed job
  csv/e15_network.csv    system, rtt_added_ms, median wall, slowdown vs 0 ms
  figures/e12_join, e13_churn, e14_staircase, e15_network (PNG and PDF)
"""
import csv
import json
import os
import statistics
from collections import defaultdict

TL_EVENTS = ["setup_start", "labpc", "ssh_ok", "install_end", "join_end", "first_work"]
TL_MARK = {"labpc": "o", "ssh_ok": "s", "install_end": "^", "join_end": "D", "first_work": "*"}
TL_LABEL = {"labpc": "LAB-PC line", "ssh_ok": "answers SSH", "install_end": "pipedpeer installed",
            "join_end": "joined", "first_work": "first work"}


def _ok(r):
    return not r.get("warmup") and not r.get("error") and r.get("result_ok") is not False


def _write(path, rows):
    cols = []
    for r in rows:
        cols += [k for k in r if k not in cols]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def _total(tl):
    per = (tl or {}).get("per_pc") or {}
    n = max((len(v) for v in per.values()), default=0)
    return [sum(v[i] for v in per.values() if i < len(v)) for i in range(n)]


def _rel(r, t_ns):
    return (t_ns - r["start_ns"]) / 1e9 if t_ns and r.get("start_ns") else None


# --- E12 ----------------------------------------------------------------------

def cohorts(r):
    """pc -> 0 for the PCs the run started on, k for the k-th join."""
    c = {pc: 0 for pc in r.get("hosts") or []}
    for k, e in enumerate([e for e in r.get("events") or [] if e.get("kind") == "join"], 1):
        for pc in e.get("pcs") or []:
            c.setdefault(pc, k)
    return c


def e12_rows(runs):
    rows = []
    for r in runs:
        tl = r.get("timeline") or {}
        c = cohorts(r)
        for pc, vals in sorted((tl.get("per_pc") or {}).items()):
            for i, v in enumerate(vals):
                rows.append({"run_id": r["run_id"], "system": r["system"], "t_s": i * tl.get("bin_s", 1),
                             "pc": pc, "cohort": c.get(pc, -1), "items_per_s": v})
    return rows


def latest(runs):
    """The last run per system: one panel each (a resumed day may repeat one)."""
    return list({r["system"]: r for r in runs}.values())


def e12_figure(plt, runs, figdir, save, made):
    runs = latest([r for r in runs if (r.get("timeline") or {}).get("per_pc")])
    if not runs:
        return False
    from analyse import COLORS, INK, INK2, sort_key
    runs.sort(key=lambda r: sort_key(r["system"]))
    fig, axes = plt.subplots(1, len(runs), figsize=(3.6 * len(runs), 3.0), sharey=True, squeeze=False)
    for ax, r in zip(axes[0], runs):
        tl, c = r["timeline"], cohorts(r)
        k_max = max(c.values(), default=0)
        n = max(len(v) for v in tl["per_pc"].values())
        xs = [i * tl.get("bin_s", 1) for i in range(n)]
        layers, labels = [], []
        for k in range(k_max + 1):
            pcs = [pc for pc in tl["per_pc"] if c.get(pc, -1) == k]
            size = sum(1 for v in c.values() if v == k)
            layers.append([sum(tl["per_pc"][pc][i] if i < len(tl["per_pc"][pc]) else 0 for pc in pcs) for i in range(n)])
            labels.append("start (%d PC%s)" % (size, "s" * (size != 1)) if k == 0 else "join %d (+%d)" % (k, size))
        other = [pc for pc in tl["per_pc"] if pc not in c]
        if other:  # placed on a machine the node map could not name
            layers.append([sum(tl["per_pc"][pc][i] if i < len(tl["per_pc"][pc]) else 0 for pc in other) for i in range(n)])
            labels.append("unmapped")
        base = COLORS.get(r["system"], INK2)
        alphas = [1.0 - 0.7 * k / max(1, len(layers) - 1) for k in range(len(layers))]
        ax.stackplot(xs, *layers, labels=labels, colors=[base] * len(layers), step="post")
        for poly, a in zip(ax.collections, alphas):
            poly.set_alpha(a)
            poly.set_edgecolor("white")
            poly.set_linewidth(0.6)
        for e in r.get("events") or []:
            if e.get("kind") != "join":
                continue
            t = _rel(r, e["t_ns"])
            ax.axvline(t, color=INK, linewidth=0.8, linestyle=(0, (3, 3)))
            ax.annotate("+%d" % len(e.get("pcs") or []), (t, 1.0), xycoords=("data", "axes fraction"),
                        xytext=(2, -10), textcoords="offset points", fontsize=7, color=INK)
            seen = _rel(r, e.get("seen_ns"))
            if seen is not None:
                ax.plot([seen], [0], marker="|", markersize=8, color=INK, clip_on=False)
        ax.set_title("%s (%.0f s)" % (r["system"], r.get("wall_s") or 0), color=INK)
        ax.set_xlabel("seconds since the job started")
        ax.set_ylim(top=ax.get_ylim()[1] * 1.12)
        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.22), ncol=3, fontsize=7)
    axes[0][0].set_ylabel("items finished per second")
    fig.suptitle("Elastic join: W1 while PCs join every %g s" % (runs[0].get("join_every_s") or 0),
                 color=INK, fontsize=10, y=1.03)
    save(fig, figdir, "e12_join", made)
    plt.close(fig)
    return True


# --- E13 ----------------------------------------------------------------------

def no_churn_wall(all_runs, r):
    """Median wall of E1b's usable runs at the same workload, system, N and size."""
    w = [x["wall_s"] for x in all_runs if x.get("experiment") == "e1b" and _ok(x)
         and x.get("workload") == r.get("workload") and x.get("system") == r.get("system")
         and x.get("n") == r.get("n") and x.get("size") == r.get("size") and x.get("wall_s")]
    return statistics.median(w) if w else None


def e13_rows(all_runs, runs):
    rows = []
    for r in runs:
        ref = no_churn_wall(all_runs, r)
        rows.append({"run_id": r["run_id"], "system": r["system"], "n": r.get("n"), "size": json.dumps(r.get("size")),
                     "wall_s": r.get("wall_s"), "no_churn_wall_s": ref,
                     "slowdown": round(r["wall_s"] / ref, 3) if ref and r.get("wall_s") else None,
                     "result_ok": r.get("result_ok"), "error": r.get("error"), "kills": r.get("kills"),
                     "items_rerun": r.get("items_rerun"), "rerun_source": r.get("rerun_source"),
                     "items_counted": r.get("items_counted"), "timeline_source": r.get("timeline_source")})
    return rows


def e13_figure(plt, all_runs, runs, figdir, save, made):
    runs = latest([r for r in runs if (r.get("timeline") or {}).get("per_pc") or r.get("events")])
    if not runs:
        return False
    from analyse import COLORS, INK, INK2, sort_key
    runs.sort(key=lambda r: sort_key(r["system"]))
    fig, axes = plt.subplots(1, len(runs), figsize=(3.6 * len(runs), 3.0), sharey=True, squeeze=False)
    for ax, r in zip(axes[0], runs):
        tot = _total(r.get("timeline"))
        bin_s = (r.get("timeline") or {}).get("bin_s", 1)
        ax.step([i * bin_s for i in range(len(tot))], tot, where="post", color=COLORS.get(r["system"], INK2),
                linewidth=1.6)
        for e in r.get("events") or []:
            t = _rel(r, e["t_ns"])
            if t is None:
                continue
            kill = e.get("kind") == "kill"
            ax.axvline(t, color=INK if kill else INK2, linewidth=0.8, linestyle="-" if kill else (0, (2, 2)))
            ax.plot([t], [1.0], transform=ax.get_xaxis_transform(), marker="v" if kill else "^", markersize=5,
                    color=INK, markerfacecolor=INK if kill else "white", clip_on=False)
        ref = no_churn_wall(all_runs, r)
        ok = "result ok" if r.get("result_ok") else ("FAILED" if r.get("error") else "result WRONG")
        sub = "%.0f s vs %s no churn" % (r.get("wall_s") or 0, "%.0f s" % ref if ref else "?")
        ax.set_title("%s: %s\n%s, %s items re-run" % (r["system"], ok, sub, r.get("items_rerun", "?")),
                     color=INK, fontsize=8)
        ax.set_xlabel("seconds since the job started")
    axes[0][0].set_ylabel("items finished per second (all PCs)")
    fig.suptitle("Churn: kill -9 a worker every %g s, restart it %g s later (v kill, ^ restart)" % (
        runs[0].get("churn_every_s") or 0, runs[0].get("churn_down_s") or 0), color=INK, fontsize=10, y=1.06)
    save(fig, figdir, "e13_churn", made)
    plt.close(fig)
    return True


# --- E14 ----------------------------------------------------------------------

def load_timeline(path):
    if not os.path.exists(path):
        return []
    out = []
    for line in open(path):
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def e14_table(events):
    """(per-PC rows of seconds since head_start, summary), first of each
    (machine, event); ssh_ok is keyed by machine_id, mapped through pc_map."""
    by_mid = {e["machine_id"]: e["pc"] for e in events if e["event"] == "pc_map" and e.get("machine_id")}
    first = {}
    for e in events:
        if e["event"] == "pc_map" or not e.get("t_ns"):
            continue
        pc = by_mid.get(e.get("machine_id")) if e["event"] == "ssh_ok" else e["pc"]
        pc = pc or e["pc"]
        k = (pc, e["event"])
        if k not in first or e["t_ns"] < first[k]:
            first[k] = e["t_ns"]
    head = first.get(("pc00", "head_start"))
    if head is None:
        return [], {}
    pcs = sorted({pc for pc, ev in first if ev in TL_EVENTS})
    rows = []
    for pc in pcs:
        row = {"pc": pc}
        for ev in TL_EVENTS:
            t = first.get((pc, ev))
            row[ev + "_s"] = round((t - head) / 1e9, 1) if t else None
        rows.append(row)
    fw = [r["first_work_s"] for r in rows if r["first_work_s"] is not None]
    lp = [r["labpc_s"] for r in rows if r["labpc_s"] is not None]

    def at(ev):
        t = first.get(("pc00", ev))
        return round((t - head) / 1e9, 1) if t else None
    summary = {"pcs": len(rows), "pcs_with_first_work": len(fw),
               "head_to_last_labpc_s": max(lp) if lp else None,
               "head_to_cluster_ready_s": at("cluster_ready"),
               "head_to_first_job_start_s": at("first_job_start"),
               "head_to_all_pcs_working_s": max(fw) if len(fw) == len(rows) and fw else None,
               "head_to_first_job_end_s": at("first_job_end"),
               "clock": "each PC's own clock for lab.sh, install and join; pc00's for ssh_ok, cluster_ready "
                        "and first_work (clocks.txt has the offsets)"}
    return rows, summary


def e14_figure(plt, rows, summary, figdir, save, made):
    rows = [r for r in rows if any(r.get(ev + "_s") is not None for ev in TL_EVENTS)]
    if not rows:
        return False
    from analyse import COLORS, INK, INK2
    rows.sort(key=lambda r: (r.get("labpc_s") is None, r.get("labpc_s") or 0, r["pc"]))
    fig, ax = plt.subplots(figsize=(6.4, 0.22 * len(rows) + 1.4))
    labelled = set()
    for y, r in enumerate(rows):
        ts = [r[ev + "_s"] / 60 for ev in TL_EVENTS if r.get(ev + "_s") is not None]
        ax.plot([min(ts), max(ts)], [y, y], color=INK2, linewidth=0.8)
        for ev, m in TL_MARK.items():
            t = r.get(ev + "_s")
            if t is not None:
                ax.plot([t / 60], [y], marker=m, linestyle="none", markersize=6 if m == "*" else 4,
                        color=COLORS["pipedpeer"] if ev == "first_work" else INK,
                        label=None if ev in labelled else TL_LABEL[ev])
                labelled.add(ev)
    for key, text in (("head_to_cluster_ready_s", "cluster ready"), ("head_to_first_job_start_s", "first job")):
        t = summary.get(key)
        if t is not None:
            ax.axvline(t / 60, color=INK, linewidth=0.8, linestyle=(0, (3, 3)))
            ax.annotate("%s %.1f min" % (text, t / 60), (t / 60, 1.0), xycoords=("data", "axes fraction"),
                        xytext=(3, -10), textcoords="offset points", fontsize=7, color=INK)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([r["pc"] for r in rows], fontsize=7)
    ax.invert_yaxis()
    ax.set_xlabel("minutes since the head line on pc00")
    ax.legend(loc="lower right", fontsize=7)
    ax.set_title("Curl to cluster: %d PCs, first distributed job at %s" % (
        summary.get("pcs", len(rows)),
        "%.1f min" % (summary["head_to_first_job_start_s"] / 60) if summary.get("head_to_first_job_start_s") else "?"),
        color=INK, fontsize=10)
    save(fig, figdir, "e14_staircase", made)
    plt.close(fig)
    return True


# --- E15 ----------------------------------------------------------------------

def e15_rows(runs):
    walls, ns = defaultdict(list), {}
    for r in runs:
        if _ok(r) and r.get("rtt_added_ms") is not None and r.get("wall_s"):
            walls[(r["system"], r["rtt_added_ms"])].append(r["wall_s"])
            ns[r["system"]] = r.get("n")
    rows = []
    for (system, rtt), w in sorted(walls.items()):
        base = walls.get((system, 0))
        b = statistics.median(base) if base else None
        m = statistics.median(w)
        rows.append({"system": system, "n": ns.get(system), "rtt_added_ms": rtt, "runs": len(w), "median_wall_s": round(m, 3),
                     "slowdown_vs_0ms": round(m / b, 3) if b else None})
    return rows


def e15_figure(plt, rows, figdir, save, made):
    rows = [r for r in rows if r.get("slowdown_vs_0ms")]
    if not rows:
        return False
    from analyse import COLORS, INK, INK2, sort_key
    fig, ax = plt.subplots(figsize=(3.6, 3.0))
    ax.axhline(1.0, color=INK2, linewidth=1, linestyle=(0, (3, 3)))
    for system in sorted({r["system"] for r in rows}, key=sort_key):
        pts = sorted((r["rtt_added_ms"], r["slowdown_vs_0ms"]) for r in rows if r["system"] == system)
        ax.plot(*zip(*pts), marker="o", markersize=4, linewidth=2, color=COLORS.get(system, INK2), label=system)
    ax.set_xticks(sorted({r["rtt_added_ms"] for r in rows}))
    ax.set_xlabel("round-trip time added between PCs (ms)")
    ax.set_ylabel("median wall / wall at 0 ms")
    ax.set_title("Network sensitivity, W1 at N=%s" % "/".join(sorted({str(r["n"]) for r in rows})),
                 color=INK, fontsize=10)
    ax.legend(loc="best", fontsize=8)
    save(fig, figdir, "e15_network", made)
    plt.close(fig)
    return True


def run(all_runs, out, csvdir, figdir, plt=None, save=None):
    """Write the headline CSVs and summaries; the figures too when plt is
    given. Returns (made, skipped) figure names."""
    by = defaultdict(list)
    for r in all_runs:
        by[r.get("experiment")].append(r)
    made, skipped = [], []
    e12 = [r for r in by["e12"] if not r.get("warmup")]
    e13 = [r for r in by["e13"] if not r.get("warmup")]
    if e12:
        _write(os.path.join(csvdir, "e12_timeline.csv"), e12_rows(e12) or [{"run_id": None}])
    if e13:
        _write(os.path.join(csvdir, "e13_summary.csv"), e13_rows(all_runs, e13))
    rows14, sum14 = e14_table(load_timeline(os.path.join(out, "timeline.jsonl")))
    if rows14:
        _write(os.path.join(csvdir, "e14_pcs.csv"), rows14)
        with open(os.path.join(out, "e14_summary.json"), "w") as f:
            json.dump(sum14, f, indent=1)
    rows15 = e15_rows(by["e15"])
    if rows15:
        _write(os.path.join(csvdir, "e15_network.csv"), rows15)
    if plt is None:
        return made, skipped
    for name, have, fn in (
            ("e12", e12, lambda: e12_figure(plt, e12, figdir, save, made)),
            ("e13", e13, lambda: e13_figure(plt, all_runs, e13, figdir, save, made)),
            ("e14", rows14, lambda: e14_figure(plt, rows14, sum14, figdir, save, made)),
            ("e15", rows15, lambda: e15_figure(plt, rows15, figdir, save, made))):
        if have and not fn():
            skipped.append(name)
    return made, skipped
