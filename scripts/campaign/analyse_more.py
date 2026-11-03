"""Data for the E16-E20 figures (lab/more-bench): CSVs and JSON summaries,
no figures. analyse.py calls run(); FIGURES.md describes every field.

  csv/e16_energy.csv    one row per run with joules: E16's own lines, and
                        every other run (E1 first) that results/lab/energy/
                        <pc>.jsonl covers
  e16_summary.json      idle power per PC, and per (experiment, workload,
                        system, N) the median wall, joules and joules above idle
  csv/e17_footprint.csv one row per (system, PC): idle CPU and memory
  e17_summary.json      per system: ready time, commands, idle totals
  csv/e18_hog.csv       one row per E18 run: wall, hogged PCs' share of items
  e18_summary.json      per system: wall without and with the hog, shares
  csv/e19_ship.csv      one row per E19 run: phase, time to first result,
                        pipedpeer's env build and send
  e19_summary.json      per system: ttfr_s per phase
  e20_summary.json      per (workload, system, N): median wall, speedup vs
                        plain, result_ok, script_lines_changed
"""
import json
import os
import statistics
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "lib"))

from campaign import energy  # noqa: E402


def _ok(r):
    return not r.get("warmup") and not r.get("error") and r.get("result_ok") is not False


def _med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 3) if xs else None


def _write(path, rows):
    import csv
    cols = []
    for r in rows:
        cols += [k for k in r if k not in cols]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols or ["none"])
        w.writeheader()
        w.writerows(rows)


def _dump(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True)


# --- E16 ----------------------------------------------------------------------

def load_energy(out):
    d = os.path.join(out, "energy")
    if not os.path.isdir(d):
        return {}
    res = {}
    for name in sorted(os.listdir(d)):
        if name.endswith(".jsonl"):
            with open(os.path.join(d, name)) as f:
                res[name[:-len(".jsonl")]] = energy.parse(f.read())
    return res


def idle_power(runs):
    """{pc: W}, the latest E16 idle-power line."""
    lines = [r for r in runs if r.get("experiment") == "e16" and r.get("phase") == "idle-power"]
    return (lines[-1].get("power_w") or {}) if lines else {}


def e16_rows(runs, samples):
    rows = []
    for r in runs:
        if r.get("error") or not r.get("start_ns") or not r.get("end_ns") or r.get("phase") == "idle-power":
            continue
        if "energy_j" in r:
            j, allj, src = r.get("energy_j"), r.get("energy_all_pcs_j"), "inline"
        elif samples and r.get("hosts"):
            f = energy.fields(samples, r["hosts"], r["start_ns"], r["end_ns"], sorted(samples))
            j, allj, src = f.get("energy_j"), f.get("energy_all_pcs_j"), "archive"
        else:
            continue
        if j is None and allj is None:
            continue
        rows.append({"run_id": r.get("run_id"), "experiment": r.get("experiment"), "workload": r.get("workload"),
                     "system": r.get("system"), "n": r.get("n"), "repeat": r.get("repeat"),
                     "warmup": bool(r.get("warmup")), "result_ok": r.get("result_ok"), "wall_s": r.get("wall_s"),
                     "hosts": len(r.get("hosts") or []), "host_ids": " ".join(r.get("hosts") or []),
                     "energy_j": j, "energy_all_pcs_j": allj, "energy_from": src})
    return rows


def e16_summary(runs, rows):
    idle = idle_power(runs)
    groups = defaultdict(list)
    for x in rows:
        if not x["warmup"] and x["result_ok"] is not False:
            groups[(x["experiment"], x["workload"], x["system"], x["n"])].append(x)
    out = []
    for (exp, wl, system, n), xs in sorted(groups.items(), key=lambda kv: [str(k) for k in kv[0]]):
        dyn = []
        for x in xs:
            w = [idle.get(pc) for pc in x["host_ids"].split()]
            if x["energy_j"] is not None and x["wall_s"] and w and all(v is not None for v in w):
                dyn.append(x["energy_j"] - sum(w) * x["wall_s"])
        out.append({"experiment": exp, "workload": wl, "system": system, "n": n, "runs": len(xs),
                    "median_wall_s": _med([x["wall_s"] for x in xs]),
                    "median_energy_j": _med([x["energy_j"] for x in xs]),
                    "median_energy_all_pcs_j": _med([x["energy_all_pcs_j"] for x in xs]),
                    "median_energy_above_idle_j": _med(dyn)})
    unavailable = {}
    for r in runs:
        if r.get("experiment") == "e16":
            for pc, why in (r.get("energy_unavailable") or {}).items():
                unavailable[pc] = why
    return {"available": bool(rows), "idle_power_w": idle, "unavailable": unavailable, "groups": out}


# --- E17 ----------------------------------------------------------------------

def e17_tables(runs):
    rows, summary = [], {}
    for r in runs:
        if r.get("workload") != "E17-idle":
            continue
        for pc, v in sorted((r.get("footprint") or {}).items()):
            if v:
                rows.append({"run_id": r["run_id"], "system": r["system"], "pc": pc, "cpu_s": v["cpu_s"],
                             "cpu_pct": v["cpu_pct"], "rss_mb_avg": v["rss_mb_avg"], "pss_mb_avg": v["pss_mb_avg"],
                             "rss_mb_max": v["rss_mb_max"], "procs": v["procs"], "seconds": v["seconds"]})
        summary[r["system"]] = {k: r.get(k) for k in (
            "n", "ready_s", "ready_after_s", "start_call_s", "commands_typed", "commands_typed_note", "idle_s",
            "cpu_s_total", "cpu_pct_mean_per_pc", "rss_mb_total", "pss_mb_total", "procs_total",
            "idle_power_w", "error")}
        summary[r["system"]]["commands"] = [c["command"] for c in r.get("commands") or []]
    return rows, summary


# --- E18 ----------------------------------------------------------------------

def e18_tables(runs):
    rows, by = [], defaultdict(lambda: defaultdict(list))
    for r in runs:
        if r.get("experiment") != "e18" or r.get("warmup"):
            continue
        rows.append({"run_id": r["run_id"], "system": r["system"], "n": r.get("n"), "repeat": r.get("repeat"),
                     "fault": r.get("fault"), "hog_pcs": " ".join(r.get("hog_pcs") or []),
                     "hog_cores": r.get("hog_cores"), "wall_s": r.get("wall_s"), "result_ok": r.get("result_ok"),
                     "hog_share": r.get("hog_share"), "fair_share": r.get("fair_share"),
                     "items_counted": r.get("items_counted"), "error": r.get("error")})
        if _ok(r):
            by[r["system"]][r.get("fault")].append(r)
    summary = {}
    for system, f in sorted(by.items()):
        none, hog = _med([x["wall_s"] for x in f["none"]]), _med([x["wall_s"] for x in f["hog"]])
        summary[system] = {"wall_none_s": none, "wall_hog_s": hog,
                           "slowdown": round(hog / none, 3) if none and hog else None,
                           "hog_share_none": _med([x.get("hog_share") for x in f["none"]]),
                           "hog_share_hog": _med([x.get("hog_share") for x in f["hog"]]),
                           "fair_share": _med([x.get("fair_share") for x in f["none"] + f["hog"]])}
    return rows, summary


# --- E19 ----------------------------------------------------------------------

PHASES = ["cold", "warm", "edit", "newdep"]


def e19_tables(runs):
    rows, summary = [], defaultdict(dict)
    for r in runs:
        if r.get("experiment") != "e19":
            continue
        rows.append({k: r.get(k) for k in ("run_id", "system", "n", "phase", "ttfr_s", "wall_s", "script_s",
                                           "env_build_s", "env_send_s", "env_send_bytes", "script_step_s",
                                           "result", "result_ok", "error")})
        if _ok(r) and r.get("phase") in PHASES:
            summary[r["system"]][r["phase"]] = r.get("ttfr_s")
    return rows, dict(summary)


# --- E20 ----------------------------------------------------------------------

def e20_summary(runs):
    groups = defaultdict(list)
    for r in runs:
        if r.get("experiment") == "e20" and not r.get("warmup"):
            groups[(r.get("workload"), r.get("system"), r.get("n"))].append(r)
    base = {wl: _med([r["wall_s"] for r in rs if _ok(r)]) for (wl, s, n), rs in groups.items() if s == "plain"}
    out = []
    for (wl, system, n), rs in sorted(groups.items(), key=lambda kv: [str(k) for k in kv[0]]):
        m = _med([r["wall_s"] for r in rs if _ok(r)])
        b = base.get(wl)
        out.append({"workload": wl, "system": system, "n": n, "runs": len(rs),
                    "ok_runs": sum(1 for r in rs if _ok(r)), "median_wall_s": m,
                    "speedup_vs_plain": round(b / m, 3) if b and m else None,
                    "result_ok_all": all(r.get("result_ok") is True for r in rs),
                    "script_lines_changed": rs[0].get("script_lines_changed"),
                    "result": rs[0].get("result")})
    return out


def run(all_runs, out, csvdir):
    """Write every E16-E20 CSV and summary that has data; returns their names."""
    wrote = []
    rows16 = e16_rows(all_runs, load_energy(out))
    if rows16 or any(r.get("experiment") == "e16" for r in all_runs):
        _write(os.path.join(csvdir, "e16_energy.csv"), rows16)
        _dump(os.path.join(out, "e16_summary.json"), e16_summary(all_runs, rows16))
        wrote += ["e16_energy.csv", "e16_summary.json"]
    for name, fn in (("e17", e17_tables), ("e18", e18_tables), ("e19", e19_tables)):
        rows, summary = fn(all_runs)
        if rows or summary:
            csvname = {"e17": "e17_footprint.csv", "e18": "e18_hog.csv", "e19": "e19_ship.csv"}[name]
            _write(os.path.join(csvdir, csvname), rows)
            _dump(os.path.join(out, "%s_summary.json" % name), summary)
            wrote += [csvname, "%s_summary.json" % name]
    s20 = e20_summary(all_runs)
    if s20:
        _dump(os.path.join(out, "e20_summary.json"), s20)
        wrote.append("e20_summary.json")
    return wrote
