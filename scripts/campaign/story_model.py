"""The data model behind results/lab/story/index.html: everything the page
draws, in one JSON-able dict. story.py inlines it; nothing here renders.

The loading is sample_figures.build_model's (runs.jsonl, OTel CPU and
network, receipts, port-lines.csv, clocks); this module adds the headline
experiments (E12 join, E13 churn, E14 curl to cluster, E15 network delay)
from the same runs, plus any extra runs files (the sample keeps its
rehearsal lines in a second file). A section whose data is missing comes
out as None or an empty list; the page draws a placeholder for it.
"""
import json
import os
from collections import defaultdict

import analyse
import analyse_headline as hl
import sample_figures as sf

SYSTEMS = ["pipedpeer", "ray", "dask"]


def _r(x, k=3):
    return round(x, k) if isinstance(x, float) else x


def _rel(r, t_ns):
    return _r((t_ns - r["start_ns"]) / 1e9, 2) if t_ns and r.get("start_ns") else None


def load_extra(paths):
    """Runs from extra runs files, each tagged with its own directory so
    log_path and chunks_path resolve against the file they came from."""
    out = []
    for p in paths:
        for r in analyse.load(p):
            r["_dir"] = os.path.dirname(os.path.abspath(p))
            out.append(r)
    return out


def chunks(r, results):
    """[[pc, t0, t1, items]] from logs/<run>/chunks.json (timeline.py), or None."""
    p = r.get("chunks_path")
    if not p:
        return None
    p = os.path.join(r.get("_dir") or results, p)
    if not os.path.exists(p):
        return None
    try:
        doc = json.load(open(p))
    except ValueError:
        return None
    return [[c["pc"], _r(c["t0"]), _r(c["t1"]), c.get("items")] for c in doc.get("chunks") or []]


def regroup(runs, extra):
    """Experiment -> analyse.group rows over every runs file; E1 gets speedup
    against plain at N = 1, E1b weak-scaling efficiency."""
    by_exp = defaultdict(list)
    for r in runs + extra:
        by_exp[r.get("experiment")].append(r)
    groups = {}
    for exp, rs in by_exp.items():
        rows = analyse.group(rs)
        if exp == "e1":
            analyse.add_speedup(rows)
        if exp == "e1b":
            analyse.add_efficiency(rows)
        groups[exp] = [{k: _r(v, 4) for k, v in row.items()} for row in rows]
    return groups


# --- the hero: one run per system on the same workload and N ------------------

def hero(model, results, runs_by_id):
    """The E1 workload and N where every system has a timeline with CPU,
    largest N first, W1 preferred: per system the busy cores per PC and the
    chunks (chunks.json, else the receipt's compute steps, else none)."""
    cells = defaultdict(dict)
    for rid, tl in model["timelines"].items():
        r = runs_by_id[rid]
        if r.get("experiment") != "e1" or r.get("system") not in SYSTEMS:
            continue
        if not any(tl["cpu"].values()):
            continue
        cells[(r["workload"], r.get("n") or 1)][r["system"]] = (r, tl)
    full = [k for k, v in cells.items() if len(v) == len(SYSTEMS)] or list(cells)
    if not full:
        return None
    wl, n = max(full, key=lambda k: (len(cells[k]), k[1], k[0] == "W1"))
    out = {"workload": wl, "n": n, "systems": {}}
    for system, (r, tl) in cells[(wl, n)].items():
        pad = tl.get("pad_s", 0)
        cpu = {pc: [[_r(t - pad, 2), _r(b, 2)] for t, b in s] for pc, s in tl["cpu"].items()}
        ch = chunks(r, results)
        source = "chunks"
        if ch is None:
            ch = [[s["pc"], s["t"], _r(s["t"] + s["d"]), None] for s in tl["steps"] if s["step"] == "compute"]
            source = "receipt-steps" if ch else "cpu"
        out["systems"][system] = {
            "run_id": r["run_id"], "wall": r.get("wall_s"), "hosts": r.get("hosts") or [], "cpu": cpu,
            "cpu_source": sorted(set(v for v in tl["cpu_source"].values() if v)), "chunks": ch, "source": source,
            "receipt": (r.get("receipt") or {}) if isinstance(r.get("receipt"), dict) else {}}
    return out


# --- swimlanes and network bytes: every representative run ---------------------

def lanes(model, runs_by_id):
    out = []
    for rid, tl in model["timelines"].items():
        r = runs_by_id[rid]
        comp = [[s["pc"], s.get("row", 0), s["t"], s["d"]] for s in tl["steps"] if s["step"] == "compute"]
        pad = tl.get("pad_s", 0)
        cpu = {pc: [[_r(t - pad, 2), _r(b, 2)] for t, b in s] for pc, s in tl["cpu"].items() if s}
        if not comp and not cpu:
            continue
        out.append({"run_id": rid, "experiment": r.get("experiment"), "workload": r.get("workload"),
                    "system": r.get("system"), "n": r.get("n"), "fault": r.get("fault"), "wall": r.get("wall_s"),
                    "hosts": r.get("hosts") or [], "compute": comp, "rows": tl["lane_rows"], "cpu": cpu,
                    "net": tl["net"]})
    out.sort(key=lambda x: (x["experiment"] or "", x["workload"] or "", x["n"] or 0, analyse.sort_key(x["system"])))
    return out


# --- headline experiments ------------------------------------------------------

def _timeline(r):
    tl = r.get("timeline") or {}
    return tl.get("bin_s", 1), {pc: [_r(v, 1) for v in vals] for pc, vals in (tl.get("per_pc") or {}).items()}


def join(all_runs, results):
    out = []
    for r in hl.latest([r for r in all_runs if r.get("experiment") == "e12"]):
        bin_s, per = _timeline(r)
        out.append({
            "system": r["system"], "wall": r.get("wall_s"), "result_ok": r.get("result_ok"), "error": r.get("error"),
            "hosts": r.get("hosts") or [], "hosts_final": r.get("hosts_final") or [], "n_start": r.get("n_start"),
            "n_final": r.get("n_final"), "bin_s": bin_s, "per_pc": per, "cohorts": hl.cohorts(r),
            "events": [{"t": _rel(r, e.get("t_ns")), "seen": _rel(r, e.get("seen_ns")), "pcs": e.get("pcs") or []}
                       for e in r.get("events") or [] if e.get("kind") == "join"],
            "items_total": r.get("items_total"), "items_counted": r.get("items_counted"),
            "source": r.get("timeline_source"), "chunks": chunks(r, results)})
    return sorted(out, key=lambda x: analyse.sort_key(x["system"]))


def churn(all_runs):
    out = []
    for r in hl.latest([r for r in all_runs if r.get("experiment") == "e13"]):
        bin_s, per = _timeline(r)
        out.append({
            "system": r["system"], "wall": r.get("wall_s"), "result_ok": r.get("result_ok"),
            "error": (r.get("error") or "")[:160] or None, "n": r.get("n"), "hosts": r.get("hosts") or [],
            "kills": r.get("kills"), "items_rerun": r.get("items_rerun"), "items_total": r.get("items_total"),
            "items_counted": r.get("items_counted"), "every_s": r.get("churn_every_s"),
            "down_s": r.get("churn_down_s"), "no_churn_wall": hl.no_churn_wall(all_runs, r),
            "bin_s": bin_s, "per_pc": per, "source": r.get("timeline_source"),
            "events": [{"kind": e.get("kind"), "t": _rel(r, e.get("t_ns")), "done": _rel(r, e.get("done_ns")),
                        "pc": e.get("pc"), "after_job": bool(e.get("after_job"))} for e in r.get("events") or []]})
    return sorted(out, key=lambda x: analyse.sort_key(x["system"]))


def staircase(timeline_paths, all_runs):
    """E14 per PC: seconds since the head line (lab day) or, when the file
    has no head_start (a rehearsal), since the last first_job_start."""
    events = []
    for p in timeline_paths:
        if p and os.path.exists(p):
            events += hl.load_timeline(p)
    e14 = [r for r in all_runs if r.get("experiment") == "e14"]
    if not events and not e14:
        return None
    rows, summary = hl.e14_table(events)
    if rows:
        return {"anchor": "head_start", "rows": rows, "summary": summary, "missing": []}
    starts = sorted(e["t_ns"] for e in events if e.get("event") == "first_job_start" and e.get("t_ns"))
    if not starts:
        return {"anchor": None, "rows": [], "summary": {}, "missing": hl.TL_EVENTS}
    t0 = starts[-1]
    first = {}
    for e in events:
        if e.get("t_ns") and e["t_ns"] >= t0 - 1e9 and e["event"] != "pc_map":
            k = (e["pc"], e["event"])
            first[k] = min(first.get(k, e["t_ns"]), e["t_ns"])
    pcs = sorted({pc for pc, ev in first if ev in hl.TL_EVENTS}, key=sf.pc_sort)
    rows = [{"pc": pc, **{ev + "_s": _rel({"start_ns": t0}, first.get((pc, ev))) for ev in hl.TL_EVENTS}}
            for pc in pcs]
    end = first.get(("pc00", "first_job_end"))
    fw = [r["first_work_s"] for r in rows if r["first_work_s"] is not None]
    run = max(e14, key=lambda r: r.get("start_ns") or 0) if e14 else {}
    return {"anchor": "first_job_start", "rows": rows,
            "summary": {"pcs": len(rows), "pcs_with_first_work": len(fw),
                        "first_job_to_all_pcs_working_s": max(fw) if fw and len(fw) == len(rows) else None,
                        "first_job_end_s": _rel({"start_ns": t0}, end), "wall": run.get("wall_s")},
            "missing": [ev for ev in hl.TL_EVENTS if not any(r.get(ev + "_s") is not None for r in rows)]}


def network(all_runs):
    rows = hl.e15_rows([r for r in all_runs if r.get("experiment") == "e15"])
    return [{k: _r(v, 4) for k, v in row.items()} for row in rows]


def e6(all_runs):
    keep = ("system", "fault", "wall_s", "result_ok", "killed_pc", "kill_at_s", "no_kill_wall_s", "ctrlc_latency_s", "n")
    return [{k: r.get(k) for k in keep} for r in all_runs if r.get("experiment") == "e6" and analyse.usable(r)]


def e7(all_runs):
    return [{"system": r["system"], "wall": r.get("wall_s"), "repeat": r.get("repeat")}
            for r in all_runs if r.get("experiment") == "e7" and analyse.usable(r)]


# --- the whole model -------------------------------------------------------------

def build(a):
    """a: runs (primary file), extra_runs, otel, clocks, port_lines, timeline,
    label, emulated. Returns the page's model."""
    ns = type("A", (), {"runs": a.runs, "otel": a.otel, "clocks": a.clocks, "port_lines": a.port_lines,
                        "label": a.label, "cpu_note": ""})
    model, _, runs, results = sf.build_model(ns)
    extra = load_extra(a.extra_runs)
    all_runs = runs + extra
    runs_by_id = {r["run_id"]: r for r in all_runs}
    timelines = a.timeline or [os.path.join(results, "timeline.jsonl")] + [
        os.path.join(os.path.dirname(os.path.abspath(p)), "timeline.jsonl") for p in a.extra_runs]
    pcs = model["pcs"]
    return {
        "meta": {"label": a.label, "emulated": a.emulated, "pcs": pcs, "cores": model["cores"],
                 "cpu_source": model["cpu_source"], "runs": len(all_runs),
                 "failed": sum(1 for r in all_runs if not r.get("warmup") and not analyse.usable(r)),
                 "experiments": sorted({r.get("experiment") for r in all_runs if r.get("experiment")}),
                 "span_min": _r(_span(all_runs) / 60, 1), "submitter": pcs[0] if pcs else "pc00"},
        "groups": regroup(runs, extra),
        "port_lines": model["port_lines"],
        "hero": hero(model, results, runs_by_id),
        "lanes": lanes(model, runs_by_id),
        "join": join(all_runs, results),
        "churn": churn(all_runs),
        "staircase": staircase(timelines, all_runs),
        "network": network(all_runs),
        "e6": e6(all_runs),
        "e7": e7(all_runs),
        "energy": None,
    }


def _span(runs):
    t = [r for r in runs if r.get("start_ns") and r.get("end_ns")]
    if not t:
        return 0
    return sum((r["end_ns"] - r["start_ns"]) / 1e9 for r in t)

