"""analyse_sweeps.py -- E13-E20 sweep, microbenchmark, fault and sandbox analysis tables."""
import csv
import json
import math
import os
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
from scipy import stats


def fmt(v):
    if v is None:
        return ""
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, float):
        return f"{v:.10g}" if math.isfinite(v) else ""
    return str(v)


def write_csv(path, cols, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        wr = csv.writer(f, lineterminator="\n")
        wr.writerow(cols)
        for r in rows:
            wr.writerow([fmt(r.get(c, "")) for c in cols])


def _read_csv_rows(path):
    if not path.exists():
        return None
    with path.open(newline="") as f:
        return [dict(r) for r in csv.DictReader(f)]


def _load_run_records(lab_or_path):
    if isinstance(lab_or_path, (str, Path)):
        p = Path(lab_or_path)
        if p.is_dir():
            p = p / "runs.jsonl"
        if not p.exists():
            print(f"WARN {p}: no such file; its tables are skipped")
            return []
        lines = p.read_text().splitlines()
    else:
        return lab_or_path

    recs = []
    for i, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"WARN runs.jsonl line {i}: unreadable, left out ({e})")
            continue
        if r.get("warmup"):
            continue
        recs.append(r)
    return recs


def _valid_runs(runs):
    valid = []
    for r in runs:
        if r.get("wall_s") is None or float(r.get("wall_s", 0)) <= 0:
            continue
        if r.get("exit_code") not in (0, "0", None):
            continue
        ok = r.get("result_ok")
        if ok is not None and ok is not True and ok != 1 and ok != "1" and ok != "true":
            continue
        valid.append(r)
    return valid


def _sweep_row(sys_runs, valid, task_ms):
    """The columns E13 and E14 share. A system with no receipt has no remote
    share: an empty cell, never an assumed value."""
    walls = [float(r["wall_s"]) for r in valid]
    med_wall = float(np.median(walls))
    size = valid[0].get("size") or {}
    items = size.get("items")
    if not items or not task_ms:
        print(f"WARN {valid[0].get('run_id')}: size has no items or task_ms; "
              "ratio to the 1-core ideal left empty")
    ideal = items * task_ms / 1000.0 if items and task_ms else None
    remote = []
    for r in valid:
        rec = r.get("receipt")
        if items and isinstance(rec, dict) and rec.get("remote_items") is not None:
            remote.append(float(rec["remote_items"]) / items)
    return {
        "runs": len(valid), "dropped": len(sys_runs) - len(valid),
        "median_wall_s": med_wall,
        "iqr_s": float(np.percentile(walls, 75) - np.percentile(walls, 25)),
        "ratio_to_1core_ideal": ideal / med_wall if ideal and med_wall else None,
        "remote_items_ratio": float(np.median(remote)) if remote else None,
    }


# --- E13: Task-size sweeps ---
def table_e13(lab_or_runs):
    runs = _load_run_records(lab_or_runs)
    task_order = [("TS1", 1), ("TS10", 10), ("TS100", 100), ("TS1000", 1000)]
    systems = ["pipedpeer", "ray", "dask", "ssh"]

    cols = ["task_ms", "system", "runs", "dropped", "median_wall_s", "iqr_s",
            "ratio_to_1core_ideal", "remote_items_ratio"]
    rows = []

    for w, ms in task_order:
        w_runs = [r for r in runs if r.get("workload") == w]
        for sys_ in systems:
            sys_runs = [r for r in w_runs if r.get("system") == sys_]
            valid = _valid_runs(sys_runs)
            if not valid:
                continue

            rows.append({"task_ms": ms, "system": sys_,
                         **_sweep_row(sys_runs, valid, ms)})
    return cols, rows


# --- E14: Data-size sweeps ---
def table_e14(lab_or_runs):
    runs = _load_run_records(lab_or_runs)
    payload_order = [("DS10K", 10000), ("DS1M", 1000000), ("DS20M", 20000000)]
    systems = ["pipedpeer", "ray", "dask", "ssh"]

    cols = ["payload_bytes", "system", "runs", "dropped", "median_wall_s", "iqr_s",
            "ratio_to_1core_ideal", "remote_items_ratio", "added_s_vs_ds10k"]
    rows = []

    ds10k_medians = {}
    # First pass: compute DS10K medians per system
    for r in _valid_runs([x for x in runs if x.get("workload") == "DS10K"]):
        s = r.get("system")
        if s not in ds10k_medians:
            s_runs = [x for x in runs if x.get("workload") == "DS10K" and x.get("system") == s]
            v = _valid_runs(s_runs)
            if v:
                ds10k_medians[s] = float(np.median([float(x["wall_s"]) for x in v]))

    for w, payload_b in payload_order:
        w_runs = [r for r in runs if r.get("workload") == w]
        for sys_ in systems:
            sys_runs = [r for r in w_runs if r.get("system") == sys_]
            valid = _valid_runs(sys_runs)
            if not valid:
                continue

            task_ms = (valid[0].get("size") or {}).get("task_ms")
            row = _sweep_row(sys_runs, valid, task_ms)
            base_10k = ds10k_medians.get(sys_)
            added_s = row["median_wall_s"] - base_10k if base_10k is not None else None
            rows.append({"payload_bytes": payload_b, "system": sys_, **row,
                         "added_s_vs_ds10k": added_s})
    return cols, rows


# --- E15: No-op throughput ---
def table_e15(lab_or_runs):
    runs = _load_run_records(lab_or_runs)
    noop_runs = [r for r in runs if r.get("workload") == "noop"]
    systems = ["pipedpeer", "ray", "dask"]
    ns_all = sorted({int(r["n"]) for r in noop_runs} or [8, 16, 32])

    cols = ["n", "system", "runs", "dropped", "median_tasks_per_s"]
    rows = []

    # Check for missing systems
    systems_seen = {r.get("system") for r in noop_runs}
    for s in systems:
        if s not in systems_seen:
            print(f"WARN E15 noop: system {s} missing from runs")

    for n in ns_all:
        for sys_ in systems:
            sys_runs = [r for r in noop_runs if int(r.get("n", 0)) == n and r.get("system") == sys_]
            valid = _valid_runs(sys_runs)
            if not valid:
                continue

            rates = []
            for r in valid:
                # Parse NOOP-OK items=X seconds=Y tasks_per_s=Z
                parsed_rate = None
                for line in (r.get("result_lines") or []):
                    m = re.search(r"tasks_per_s=([0-9.]+)", line)
                    if m:
                        parsed_rate = float(m.group(1))
                        break
                if parsed_rate is None:
                    print(f"WARN {r.get('run_id')}: no tasks_per_s line; run left out of E15")
                    continue
                rates.append(parsed_rate)

            if not rates:
                continue

            label = f"{sys_} (batched)" if sys_ == "pipedpeer" else sys_
            rows.append({
                "n": n, "system": label, "runs": len(rates),
                "dropped": len(sys_runs) - len(rates),
                "median_tasks_per_s": float(np.median(rates)),
            })
    return cols, rows


# --- E16: Task dispatch latency ---
def parse_lat_line(line):
    # ^LAT-OK n=[0-9]+ p50_ms=[0-9.]+ p90_ms=[0-9.]+ p99_ms=[0-9.]+$
    m = re.search(r"p50_ms=([0-9.]+)\s+p90_ms=([0-9.]+)\s+p99_ms=([0-9.]+)", line)
    if not m:
        return {}
    return {
        "p50_ms": float(m.group(1)),
        "p90_ms": float(m.group(2)),
        "p99_ms": float(m.group(3)),
    }


def table_e16(lab_or_runs):
    runs = _load_run_records(lab_or_runs)
    lat_runs = [r for r in runs if r.get("workload") == "lat"]
    systems = ["pipedpeer", "ray", "dask"]

    cols = ["system", "runs", "dropped", "median_p50_ms", "median_p90_ms", "median_p99_ms",
            "p50_min_ms", "p50_max_ms", "p90_min_ms", "p90_max_ms", "p99_min_ms", "p99_max_ms"]
    rows = []

    for sys_ in systems:
        sys_runs = [r for r in lat_runs if r.get("system") == sys_]
        valid = _valid_runs(sys_runs)
        if not valid:
            continue

        p50s, p90s, p99s = [], [], []
        for r in valid:
            for line in (r.get("result_lines") or []):
                p = parse_lat_line(line)
                if p:
                    p50s.append(p["p50_ms"])
                    p90s.append(p["p90_ms"])
                    p99s.append(p["p99_ms"])
                    break

        if not p50s:
            continue

        rows.append({
            "system": sys_, "runs": len(p50s), "dropped": len(sys_runs) - len(p50s),
            "median_p50_ms": float(np.median(p50s)),
            "median_p90_ms": float(np.median(p90s)),
            "median_p99_ms": float(np.median(p99s)),
            "p50_min_ms": min(p50s), "p50_max_ms": max(p50s),
            "p90_min_ms": min(p90s), "p90_max_ms": max(p90s),
            "p99_min_ms": min(p99s), "p99_max_ms": max(p99s),
        })
    return cols, rows


# --- E17: Introducer loss ---
def table_e17_from_csv(csv_path):
    rows_data = _read_csv_rows(Path(csv_path))
    if not rows_data:
        return None, []

    cols = ["runs", "failures", "job_wall_s_median", "new_join_s_median",
            "new_join_ok_all", "old_invite_joined_any", "new_remote_items_median"]

    job_walls, new_joins, new_remotes = [], [], []
    failures = 0
    all_join_ok = True
    any_old_invite = False

    for r in rows_data:
        job_ok = str(r.get("job_result_ok", "")).strip() in ("1", "true", "True")
        if not job_ok:
            failures += 1
        elif r.get("job_wall_s"):
            job_walls.append(float(r["job_wall_s"]))

        join_ok = str(r.get("new_join_ok", "")).strip() in ("1", "true", "True")
        if not join_ok:
            all_join_ok = False
        elif r.get("new_join_s"):
            new_joins.append(float(r["new_join_s"]))

        if r.get("new_remote_items"):
            new_remotes.append(float(r["new_remote_items"]))

        if str(r.get("old_invite_joined", "")).strip() in ("1", "true", "True"):
            any_old_invite = True

    summary = {
        "runs": len(rows_data),
        "failures": failures,
        "job_wall_s_median": float(np.median(job_walls)) if job_walls else None,
        "new_join_s_median": float(np.median(new_joins)) if new_joins else None,
        "new_join_ok_all": all_join_ok if rows_data else None,
        "old_invite_joined_any": any_old_invite,
        "new_remote_items_median": float(np.median(new_remotes)) if new_remotes else None,
    }
    return cols, [summary]


def build_e17_table(lab):
    p = Path(lab) / "analysis-input" / "e17.csv"
    if not p.exists():
        print(f"SKIP E17 (no analysis-input/e17.csv)")
        return None
    cols, rows = table_e17_from_csv(p)
    write_csv(Path(lab) / "analysis" / "E17.csv", cols, rows)
    return cols, rows


# --- E18: Failure detection ---
def table_e18_from_csv(csv_path):
    rows_data = _read_csv_rows(Path(csv_path))
    if not rows_data:
        return None, []

    cols = ["system", "runs", "detect_s_median", "timed_out_count"]
    rows = []

    systems = sorted({r.get("system") for r in rows_data if r.get("system")})
    for s in systems:
        s_rows = [r for r in rows_data if r.get("system") == s]
        detect_times = []
        timed_out_cnt = 0
        for r in s_rows:
            to = str(r.get("timed_out", "")).strip() in ("1", "true", "True")
            if to:
                timed_out_cnt += 1
            elif r.get("detect_s") != "":
                detect_times.append(float(r["detect_s"]))

        rows.append({
            "system": s, "runs": len(s_rows),
            "detect_s_median": float(np.median(detect_times)) if detect_times else None,
            "timed_out_count": timed_out_cnt,
        })
    return cols, rows


def build_e18_table(lab):
    p = Path(lab) / "analysis-input" / "e18.csv"
    if not p.exists():
        print(f"SKIP E18 (no analysis-input/e18.csv)")
        return None
    cols, rows = table_e18_from_csv(p)
    write_csv(Path(lab) / "analysis" / "E18.csv", cols, rows)
    return cols, rows


# --- E19: Cluster elasticity ---
def table_e19_from_csv(csv_path):
    rows_data = _read_csv_rows(Path(csv_path))
    if not rows_data:
        return None, []

    cols = ["system", "runs_baseline", "runs_join", "join_s_median",
            "first_work_s_median", "wall_baseline_s_median", "wall_join_s_median",
            "result_ok_all"]
    rows = []

    systems = sorted({r.get("system") for r in rows_data if r.get("system")})
    for s in systems:
        s_rows = [r for r in rows_data if r.get("system") == s]
        base_rows = [r for r in s_rows if r.get("kind") == "baseline"]
        join_rows = [r for r in s_rows if r.get("kind") == "join"]

        # Baseline rows have empty join columns and must NOT be averaged into join_s or first_work_s
        join_times = [float(r["join_s"]) for r in join_rows if r.get("join_s") not in ("", None)]
        work_times = [float(r["first_work_s"]) for r in join_rows if r.get("first_work_s") not in ("", None)]
        # An empty result_ok is a run with no reference, not a failure.
        marks = [str(r.get("result_ok") or "").strip() for r in s_rows]
        all_oks = [m in ("1", "true", "True") for m in marks if m]

        def walls(rows_):
            return [float(r["wall_s"]) for r in rows_ if r.get("wall_s") not in ("", None)
                    and str(r.get("result_ok") or "").strip() in ("", "1", "true", "True")]
        base_walls, join_walls = walls(base_rows), walls(join_rows)

        rows.append({
            "system": s,
            "runs_baseline": len(base_rows),
            "runs_join": len(join_rows),
            "join_s_median": float(np.median(join_times)) if join_times else None,
            "first_work_s_median": float(np.median(work_times)) if work_times else None,
            "wall_baseline_s_median": float(np.median(base_walls)) if base_walls else None,
            "wall_join_s_median": float(np.median(join_walls)) if join_walls else None,
            "result_ok_all": all(all_oks) if all_oks else None,
        })
    return cols, rows


def build_e19_table(lab):
    p = Path(lab) / "analysis-input" / "e19.csv"
    if not p.exists():
        print(f"SKIP E19 (no analysis-input/e19.csv)")
        return None
    cols, rows = table_e19_from_csv(p)
    write_csv(Path(lab) / "analysis" / "E19.csv", cols, rows)
    return cols, rows


# --- E20: Sandbox overhead ---
def table_e20(lab_or_runs):
    runs = _load_run_records(lab_or_runs)
    e20_runs = [r for r in runs if r.get("experiment") == "E20"]
    workloads = sorted({r.get("workload") for r in e20_runs if r.get("workload")})

    cols = ["workload", "runs_with", "runs_without", "median_with_s", "median_without_s",
            "overhead_pct", "overhead_lo", "overhead_hi"]
    rows = []

    for w in workloads:
        w_runs = [r for r in e20_runs if r.get("workload") == w]
        with_runs = _valid_runs([r for r in w_runs if r.get("system") == "pipedpeer"])
        without_runs = _valid_runs([r for r in w_runs if r.get("system") == "pipedpeer-bare"])

        if not with_runs or not without_runs:
            continue

        w_walls = np.asarray([float(r["wall_s"]) for r in with_runs], float)
        wo_walls = np.asarray([float(r["wall_s"]) for r in without_runs], float)

        med_with = float(np.median(w_walls))
        med_wo = float(np.median(wo_walls))

        # Overhead %: bare faster -> positive overhead: (with - without) / without * 100
        point = float((med_with - med_wo) / med_wo * 100.0) if med_wo > 0 else 0.0

        if len(w_walls) >= 2 and len(wo_walls) >= 2:
            try:
                res = stats.bootstrap(
                    (w_walls, wo_walls),
                    statistic=lambda x, y, axis=0: ((np.median(x, axis=axis) - np.median(y, axis=axis))
                                                    / np.median(y, axis=axis) * 100.0),
                    paired=False, vectorized=True, n_resamples=5000,
                    method="percentile", confidence_level=0.95,
                    rng=np.random.default_rng(0))
                lo = float(res.confidence_interval.low)
                hi = float(res.confidence_interval.high)
            except ValueError as e:
                print(f"WARN E20 {w}: no interval, bootstrap failed ({e})")
                lo, hi = None, None
        else:
            print(f"WARN E20 {w}: under 2 runs in an arm, no interval")
            lo, hi = None, None

        rows.append({
            "workload": w, "runs_with": len(with_runs), "runs_without": len(without_runs),
            "median_with_s": med_with, "median_without_s": med_wo,
            "overhead_pct": point, "overhead_lo": lo, "overhead_hi": hi,
        })
    return cols, rows


# --- Master runner for sweeps ---
def run_sweeps(lab):
    lab = Path(lab)
    # E13
    cols13, rows13 = table_e13(lab)
    if rows13:
        write_csv(lab / "analysis" / "E13.csv", cols13, rows13)
    # E14
    cols14, rows14 = table_e14(lab)
    if rows14:
        write_csv(lab / "analysis" / "E14.csv", cols14, rows14)
    # E15
    cols15, rows15 = table_e15(lab)
    if rows15:
        write_csv(lab / "analysis" / "E15.csv", cols15, rows15)
    # E16
    cols16, rows16 = table_e16(lab)
    if rows16:
        write_csv(lab / "analysis" / "E16.csv", cols16, rows16)
    # E17..E19
    build_e17_table(lab)
    build_e18_table(lab)
    build_e19_table(lab)
    # E20
    cols20, rows20 = table_e20(lab)
    if rows20:
        write_csv(lab / "analysis" / "E20.csv", cols20, rows20)
