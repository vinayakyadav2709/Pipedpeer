#!/usr/bin/env python3
"""Paper figures, the explore dashboard and the data-volume table from a lab
campaign's runs.jsonl and OTel file output. Works on the container sample
(results/lab/sample) and on the real lab day (results/lab) alike.

  sample_figures.py [--results results/lab] [--runs FILE] [--otel DIR ...]
                    [--out DIR] [--label TEXT] [--html] [--no-anim]

Inputs, all optional except runs.jsonl:
  runs.jsonl            one line per run (scripts/campaign/README.md)
  logs/<run_id>/record.json   pipedpeer's `jobs last --export json`: steps per
                        machine and chunk, the swimlane source
  otel dirs             OTLP JSON lines (metrics*.jsonl, logs*.jsonl) from
                        telemetry.sh's file exporter; system.cpu.time and
                        system.network.io per lab.pc_id. A resource with
                        lab.cpu_source=cgroup (the container sample) wins over
                        the host scraper for that PC's CPU.
  port-lines.csv        lines changed per (workload, system)
  clocks.txt            per-PC offsets, applied to that PC's samples
  profile-minimal.tsv   for the data-volume projection

Outputs in --out (default <results>/figures-paper):
  PNG and PDF per figure, at single-column (3.5 in) or double-column (7.16 in)
  width; tables.md (median [IQR] per workload, system, N); volume.md; with
  --html, dashboard.html beside --out (one file, data inlined). A figure
  whose data is missing is skipped and listed in skipped.txt.
"""
import argparse
import bisect
import csv
import glob
import gzip
import json
import os
import re
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import analyse  # noqa: E402  (group(), usable(), stats(): one definition of median/IQR)

# --- one palette, validated (dataviz validate_palette.js, --pairs all): the
# first three categorical slots for the three distributed systems, both
# modes; plain is the neutral reference, always drawn hollow and labelled.
SYSTEMS = ["plain", "pipedpeer", "ray", "dask"]
LIGHT = {"plain": "#898781", "pipedpeer": "#2a78d6", "ray": "#eb6834", "dask": "#1baf7a"}
DARK = {"plain": "#898781", "pipedpeer": "#3987e5", "ray": "#d95926", "dask": "#199e70"}
MARKER = {"plain": "D", "pipedpeer": "o", "ray": "s", "dask": "^"}
LABEL = {"plain": "plain Python", "pipedpeer": "PipedPeer", "ray": "Ray", "dask": "Dask"}
INK, INK2, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
BLUES = ["#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7", "#3987e5",
         "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b"]
SINGLE, DOUBLE = 3.5, 7.16  # inches: one column / both columns of a two-column paper
BASE = {"pipedpeer-shim": "pipedpeer", "pipedpeer-noshim": "plain", "pipedpeer-exact": "pipedpeer",
        "pipedpeer-fast": "pipedpeer", "pipedpeer-headcount": "pipedpeer", "torchrun": "ray"}


def family(system):
    return system if system in LIGHT else BASE.get(system, "plain")


# --- loading -----------------------------------------------------------------

def load_runs(path):
    return analyse.load(path)


def attrs(lst):
    return {a["key"]: next(iter(a["value"].values())) for a in lst or []}


def otel_files(dirs, kind):
    out = []
    for d in dirs:
        out += sorted(glob.glob(os.path.join(d, "**", kind + "*.json*"), recursive=True))
    return out


def open_any(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def load_clocks(path):
    """pc -> offset in ns (PC clock minus driver clock), from clocks.sh's
    table: `<pc> <offset_ms> <+-ms> <status> ...`, comment lines '#'."""
    off = {}
    if not path or not os.path.exists(path):
        return off
    for line in open(path):
        f = line.split()
        if len(f) >= 2 and not line.startswith("#") and f[0] != "pc":
            try:
                off[f[0]] = int(float(f[1]) * 1e6)
            except ValueError:
                pass
    return off


class Metrics:
    """Per-PC series from OTel metrics: CPU busy cores and network bytes.

    cpu[pc] = sorted [(t_ns, cumulative busy seconds)], one source per PC
    (cgroup when present, else host non-idle time); net[pc] = sorted
    [(t_ns, cumulative tx bytes, cumulative rx bytes)] over non-loopback
    devices; cores[pc] = logical CPUs. Clock offsets are subtracted so every
    timestamp is on the driver's clock, like runs.jsonl's start_ns/end_ns.
    """

    def __init__(self):
        self.cpu, self.net, self.cores, self.source = {}, {}, {}, {}
        self.series = {"host": {}, "cgroup": {}}
        self.points = defaultdict(int)
        self.bytes_in = 0

    @classmethod
    def load(cls, files, window=None, clocks=None):
        self = cls()
        clocks = clocks or {}
        cpu = {"host": defaultdict(dict), "cgroup": defaultdict(dict)}
        net = defaultdict(dict)
        for path in files:
            self.bytes_in += os.path.getsize(path)
            with open_any(path) as f:
                for line in f:
                    if "system.cpu" not in line and "system.network.io" not in line:
                        continue
                    try:
                        doc = json.loads(line)
                    except ValueError:
                        continue
                    for rm in doc.get("resourceMetrics", []):
                        ra = attrs(rm.get("resource", {}).get("attributes"))
                        pc = ra.get("lab.pc_id")
                        if not pc:
                            continue
                        src = ra.get("lab.cpu_source", "host")
                        off = clocks.get(pc, 0)
                        for sm in rm.get("scopeMetrics", []):
                            for m in sm.get("metrics", []):
                                self._metric(pc, src, off, m, cpu, net, window)
        for src in ("host", "cgroup"):
            for pc, d in cpu[src].items():
                self.series[src][pc] = sorted(d.items())
        for pc in set(cpu["host"]) | set(cpu["cgroup"]):
            self.cpu[pc] = self.series["cgroup"].get(pc) or self.series["host"][pc]
            self.source[pc] = "cgroup" if self.series["cgroup"].get(pc) else "host"
        for pc, d in net.items():
            self.net[pc] = sorted((t, v[0], v[1]) for t, v in d.items())
        return self

    def _metric(self, pc, src, off, m, cpu, net, window):
        name = m.get("name")
        body = m.get("sum") or m.get("gauge") or {}
        for dp in body.get("dataPoints", []):
            t = int(dp.get("timeUnixNano", 0)) - off
            if window and not (window[0] <= t <= window[1]):
                continue
            v = float(dp.get("asDouble", dp.get("asInt", 0)))
            a = attrs(dp.get("attributes"))
            self.points[name] += 1
            if name == "system.cpu.logical.count":
                self.cores[pc] = int(v)
            elif name == "system.cpu.time" and a.get("state") != "idle" and a.get("state") != "wait":
                cpu[src][pc][t] = cpu[src][pc].get(t, 0.0) + v
            elif name == "system.network.io" and a.get("device", "lo") != "lo":
                cur = net[pc].setdefault(t, [0.0, 0.0])
                cur[0 if a.get("direction") == "transmit" else 1] += v

    def source_for(self, pc, t0, t1):
        """cgroup when its samples span [t0, t1] for this PC, else host: one
        source per run and PC, never a mix within a run."""
        c = self.series["cgroup"].get(pc)
        if c and c[0][0] <= t0 and c[-1][0] >= t1:
            return "cgroup"
        return "host" if self.series["host"].get(pc) else None

    def busy(self, pc, t0, t1):
        """[(t_s relative to t0, busy cores)] between t0 and t1 (ns)."""
        src = self.source_for(pc, t0, t1)
        s = self.series[src].get(pc, []) if src else []
        i = max(0, bisect.bisect_left(s, (t0,)) - 1)
        out = []
        for (ta, va), (tb, vb) in zip(s[i:], s[i + 1:]):
            if ta > t1:
                break
            if tb <= ta:
                continue
            out.append((round((tb - t0) / 1e9, 2), round(max(0.0, (vb - va) / ((tb - ta) / 1e9)), 3)))
        return out

    def net_delta(self, pc, t0, t1):
        """(tx, rx) bytes between the samples nearest t0 and t1, or None."""
        s = self.net.get(pc)
        if not s:
            return None

        def at(t):
            i = bisect.bisect_left(s, (t,))
            i = min(max(i, 0), len(s) - 1)
            return s[i]
        a, b = at(t0), at(t1)
        if b[0] <= a[0]:
            return None
        return int(b[1] - a[1]), int(b[2] - a[2])


# Per-item steps (one pickle and one unpickle per item) say nothing a lane
# can show; the per-chunk ones do.
LANE_STEPS = {"env_build", "env_send", "script", "queue", "compute", "send", "receive"}


def load_steps(results, run):
    """A pipedpeer run's steps: record.json's job steps (env build/send,
    script) plus the receipt's per-chunk steps (queue, compute, send,
    receive per machine), and record.json's node id -> hostname names."""
    steps, names = [], {}
    p = os.path.join(results, run.get("log_path") or "", "record.json")
    if run.get("log_path") and os.path.exists(p):
        try:
            rec = json.load(open(p))
            steps, names = list(rec.get("steps") or []), rec.get("names") or {}
        except ValueError:
            pass
    # The receipt's steps: in logs/<run>/receipt.json.gz since runs.jsonl
    # holds only a summary (lib/campaign/receipts.py); inline in older runs.
    rp = os.path.join(results, run.get("log_path") or "", "receipt.json.gz")
    if run.get("log_path") and os.path.exists(rp):
        try:
            steps += json.load(gzip.open(rp)).get("steps") or []
        except (ValueError, OSError):
            pass
    else:
        steps += (run.get("receipt") or {}).get("steps") or []
    return [s for s in steps if s.get("step") in LANE_STEPS], names


NODE_ID = re.compile(r"node_id=([0-9a-f]{8,})")


def load_node_ids(files):
    """node id prefix -> pc, from the daemon's `daemon starting ... node_id=`
    line in the OTel logs (resource lab.pc_id)."""
    out = {}
    for path in files:
        with open_any(path) as f:
            for line in f:
                if "node_id=" not in line:
                    continue
                try:
                    doc = json.loads(line)
                except ValueError:
                    continue
                for rl in doc.get("resourceLogs", []):
                    pc = attrs(rl.get("resource", {}).get("attributes")).get("lab.pc_id")
                    for sl in rl.get("scopeLogs", []):
                        for rec in sl.get("logRecords", []):
                            m = NODE_ID.search(str((rec.get("body") or {}).get("stringValue", "")))
                            if m and pc:
                                out[m.group(1)[:8]] = pc
    return out


def load_port_lines(path):
    out = {}
    if path and os.path.exists(path):
        for r in csv.DictReader(open(path)):
            out[(r["workload"], r["system"])] = int(r["changed_lines"])
    return out


# --- the model: everything a figure or the dashboard reads -------------------

def build_model(a):
    runs = load_runs(a.runs)
    results = os.path.dirname(os.path.abspath(a.runs))
    timed = [r for r in runs if r.get("start_ns") and r.get("end_ns")]
    window = (min(r["start_ns"] for r in timed) - 30e9, max(r["end_ns"] for r in timed) + 30e9) if timed else None
    clocks = load_clocks(a.clocks or os.path.join(results, "clocks.txt"))
    metrics = Metrics.load(otel_files(a.otel, "metrics"), window, clocks)
    by_exp = defaultdict(list)
    for r in runs:
        by_exp[r.get("experiment")].append(r)
    groups = {}
    for exp, rs in by_exp.items():
        rows = analyse.group(rs)
        if exp == "e1":
            analyse.add_speedup(rows)
        if exp == "e1b":
            analyse.add_efficiency(rows)
        groups[exp] = rows
    pc_ids = sorted({h for r in runs for h in (r.get("hosts") or [])} | set(metrics.cpu), key=pc_sort)
    pc_of_host, pc_of_node = load_hosts(results), load_node_ids(otel_files(a.otel, "logs"))
    # The lab day records the join directly (day/prepare.sh node_ids).
    nid = os.path.join(results, "node-ids.csv")
    if os.path.exists(nid):
        pc_of_node.update({r["node_id"][:8]: r["pc_id"] for r in csv.DictReader(open(nid)) if r.get("node_id")})
    timelines = {}
    def per_pc_cpu(r):
        # Where some PCs have per-PC (cgroup) CPU, prefer runs it fully covers.
        return all(metrics.source_for(pc, r["start_ns"] - 3e9, r["end_ns"] + 3e9) == "cgroup"
                   for pc in metrics.series["cgroup"])
    for r in representative(runs, per_pc_cpu if metrics.series["cgroup"] else None):
        timelines[r["run_id"]] = timeline(results, r, metrics, pc_ids, pc_of_host, pc_of_node)
    return {
        "label": a.label, "cpu_source_note": a.cpu_note, "runs": [slim(r) for r in runs], "groups": groups,
        "port_lines": [{"workload": k[0], "system": k[1], "lines": v} for k, v in sorted(load_port_lines(
            a.port_lines or os.path.join(results, "port-lines.csv")).items())],
        "pcs": pc_ids, "cores": metrics.cores, "cpu_source": metrics.source, "timelines": timelines,
        "colors": {"light": LIGHT, "dark": DARK}, "labels": LABEL,
        "clock_offsets_applied": bool(clocks),
    }, metrics, runs, results


def pc_sort(pc):
    m = re.search(r"(\d+)$", pc)
    return (int(m.group(1)) if m else 1 << 30, pc)


SLIM = ["run_id", "experiment", "workload", "system", "n", "repeat", "warmup", "wall_s", "result_ok", "error",
        "start_ns", "end_ns", "hosts", "variant", "fault", "mode", "killed_pc", "kill_at_s", "no_kill_wall_s",
        "ctrlc_latency_s", "bytes_per_rank", "result", "caps", "failing_pc"]


def slim(r):
    out = {k: r[k] for k in SLIM if k in r}
    rc = r.get("receipt") or {}
    if rc:
        out["receipt"] = {k: rc.get(k) for k in ("local_items", "remote_items", "by_where", "remote_failures")}
    return out


def representative(runs, prefer=None):
    """One run per (experiment, workload, system, n, variant, fault): the
    successful non-warm-up repeat with the median wall, among the repeats
    prefer() accepts when any does. Bounds the dashboard at one timeline
    per cell however many repeats the lab ran."""
    cells = defaultdict(list)
    for r in runs:
        if analyse.usable(r) and r.get("start_ns") and r.get("wall_s"):
            cells[(r.get("experiment"), r.get("workload"), r.get("system"), r.get("n"),
                   r.get("variant"), r.get("fault"))].append(r)
    for rs in cells.values():
        if prefer:
            rs = [r for r in rs if prefer(r)] or rs
        rs.sort(key=lambda r: r["wall_s"])
        yield rs[(len(rs) - 1) // 2]


def timeline(results, r, metrics, pc_ids, pc_of_host, pc_of_node):
    pad = 3e9
    t0, t1 = r["start_ns"], r["end_ns"]
    steps, names = load_steps(results, r)
    pc_of = node_to_pc(names, pc_of_host, pc_of_node)
    lanes = []
    for s in steps:
        if not s.get("start_ns"):
            continue
        lanes.append({"pc": pc_of(s.get("node") or s.get("machine")),
                      "chunk": s.get("chunk", -1), "step": s.get("step"),
                      "t": round((s["start_ns"] - t0) / 1e9, 3), "d": round(s.get("dur_ns", 0) / 1e9, 3),
                      "bytes": s.get("bytes")})
    lane_rows = pack(lanes)
    net = {}
    for pc in pc_ids:
        d = metrics.net_delta(pc, t0, t1)
        if d:
            net[pc] = {"tx": d[0], "rx": d[1]}
    return {"wall_s": r.get("wall_s"), "hosts": r.get("hosts") or [],
            "cpu": {pc: metrics.busy(pc, t0 - pad, t1 + pad) for pc in pc_ids if metrics.cpu.get(pc)},
            "cpu_source": {pc: metrics.source_for(pc, t0 - pad, t1 + pad) for pc in pc_ids if metrics.cpu.get(pc)},
            "pad_s": pad / 1e9, "steps": lanes, "lane_rows": lane_rows, "net": net}


def pack(lanes):
    """Give each compute step a sub-row within its PC so concurrent chunks
    never overdraw: greedy interval packing, earliest start first. Returns
    pc -> number of sub-rows."""
    rows = defaultdict(list)  # pc -> end time of each sub-row
    for s in sorted((x for x in lanes if x["step"] == "compute"), key=lambda x: x["t"]):
        ends = rows[s["pc"]]
        i = next((k for k, e in enumerate(ends) if e <= s["t"] + 1e-6), len(ends))
        if i == len(ends):
            ends.append(0.0)
        ends[i] = s["t"] + s["d"]
        s["row"] = i
    return {pc: len(e) for pc, e in rows.items()}


def node_to_pc(names, pc_of_host, pc_of_node):
    """A function: pipedpeer node id -> pc id. Steps name machines by node
    id; runs.jsonl and OTel name PCs pc00.. . Joined through record.json's
    names and machines.csv (hostname -> pc_id), else through the node id each
    daemon logs at start (pc_of_node, from the OTel daemon logs)."""
    def pc(nid):
        if nid in names and names[nid] in pc_of_host:
            return pc_of_host[names[nid]]
        return pc_of_node.get((nid or "")[:8], names.get(nid, (nid or "?")[:8]))
    return pc


def load_hosts(results):
    p = os.path.join(results, "machines.csv")
    if not os.path.exists(p):
        return {}
    return {r["hostname"]: r["pc_id"] for r in csv.DictReader(open(p)) if r.get("hostname")}


# --- tables --------------------------------------------------------------------

def fmt_iqr(row):
    m, q1, q3 = row.get("median_wall_s"), row.get("q1_wall_s"), row.get("q3_wall_s")
    if m is None:
        return "-"
    return "%.2f [%.2f-%.2f]" % (m, q1, q3)


def tables_md(model):
    out = ["Median wall s [IQR] per group. Warm-ups and failed runs excluded and counted.", ""]
    for exp in sorted(model["groups"]):
        rows = model["groups"][exp]
        extras = [k for k in analyse.GROUP_EXTRA if any(k in r for r in rows)]
        out += ["### %s" % exp, "",
                "| workload | system | N | %s | wall s, median [IQR] | speedup | used | warm-ups | failed |"
                % " | ".join(extras) if extras else
                "| workload | system | N | wall s, median [IQR] | speedup | used | warm-ups | failed |"]
        out.append("|" + "---|" * (8 + len(extras)))
        for r in rows:
            sp = r.get("speedup_vs_plain") or r.get("weak_efficiency")
            out.append("| %s | %s | %s | %s%s | %s | %d | %d | %d |" % (
                r["workload"], r["system"], r["n"], "".join("%s | " % r.get(k, "") for k in extras),
                fmt_iqr(r), "%.2f" % sp if sp else "-", r["runs_used"], r["warmups_excluded"], r["failed_excluded"]))
        out.append("")
    return "\n".join(out)


# --- data volume -------------------------------------------------------------------

def du(paths):
    n = b = 0
    for p in paths:
        if os.path.isfile(p):
            n, b = n + 1, b + os.path.getsize(p)
    return n, b


def gz_ratio(paths, cap=8 << 20):
    raw = comp = 0
    for p in paths:
        if not os.path.isfile(p) or p.endswith(".gz"):
            continue
        with open(p, "rb") as f:
            data = f.read(cap)
        raw += len(data)
        comp += len(gzip.compress(data, 6))
    return comp / raw if raw else 1.0


def count_records(paths, key):
    """(records, first t_ns, last t_ns) over OTLP JSON files: datapoints for
    metrics, log records for logs."""
    n, lo, hi = 0, None, None
    inner = ("scopeMetrics", "metrics") if key == "resourceMetrics" else ("scopeLogs", "logRecords")
    for p in paths:
        with open_any(p) as f:
            for line in f:
                try:
                    doc = json.loads(line)
                except ValueError:
                    continue
                for rr in doc.get(key, []):
                    for s in rr.get(inner[0], []):
                        for x in s.get(inner[1], []):
                            pts = (x.get("sum") or x.get("gauge") or {}).get("dataPoints", []) \
                                if key == "resourceMetrics" else [x]
                            n += len(pts)
                            for dp in pts:
                                t = int(dp.get("timeUnixNano") or dp.get("observedTimeUnixNano") or 0)
                                if t:
                                    lo = t if lo is None or t < lo else lo
                                    hi = t if hi is None or t > hi else hi
    return n, lo, hi


def profile_runs(profile):
    """Planned runs and minutes per experiment from a lab-day profile."""
    plan, minutes = {}, 0
    if not profile or not os.path.exists(profile):
        return plan, minutes
    for line in open(profile):
        if line.startswith("#") or not line.strip():
            continue
        step, est, args = (line.rstrip("\n").split("\t") + ["", ""])[:3]
        minutes += float(est or 0)
        ns = re.search(r"--n ([\d,]+)", args)
        nn = len(ns.group(1).split(",")) if ns else 1
        reps = int((re.search(r"--repeats (\d+)", args) or [0, 5])[1])
        per = reps + 1
        plan[step] = {"e1": 3 * (per + 3 * nn * per), "e1b": 3 * nn * per, "e4": 4 * nn * per,
                      "e5": 3 * (1 + reps), "e6": 3 * (2 + reps) + reps, "e7": 2 * per,
                      "e8": 2 * nn * per * 3}.get(step, per * nn)
    return plan, minutes


def volume(model, runs, results, otel_dirs, profile, pcs_real=17, setup_min=60):
    timed = [r for r in runs if r.get("start_ns")]
    span_s = (max(r["end_ns"] for r in timed) - min(r["start_ns"] for r in timed)) / 1e9 if timed else 1
    pcs = max(1, len(model["pcs"]))
    nruns = max(1, len(runs))
    mfiles, lfiles = otel_files(otel_dirs, "metrics"), otel_files(otel_dirs, "logs")
    logs_dir = glob.glob(os.path.join(results, "logs", "*", "*"))
    receipts = [p for p in logs_dir if p.endswith("record.json")]
    receipt_files = [p for p in logs_dir if p.endswith("receipt.json.gz")]
    outputs = [p for p in logs_dir if not p.endswith(("record.json", "receipt.json.gz"))]
    exp_logs = glob.glob(os.path.join(results, "logs", "*.log"))
    plan, minutes = profile_runs(profile)
    day_s = (minutes + setup_min) * 60
    day_runs = sum(plan.values()) or nruns
    rows = []

    def add(name, files, records, unit, note="", span=None):
        n, b = du(files)
        ratio = gz_ratio(files)
        if unit == "once":
            proj = b * pcs_real / pcs
        elif unit == "run":
            proj = b / nruns * day_runs
        else:  # per PC-second of telemetry, over the telemetry's own time span
            proj = b / (pcs * max(1.0, span or span_s)) * pcs_real * day_s
        rows.append((name, n, records, b, b / nruns, proj, proj * ratio, note))

    def otel(name, files, key, note):
        n, lo, hi = count_records(files, key)
        add(name, files, n, "pcs", note, (hi - lo) / 1e9 if lo and hi else None)
    runs_path = os.path.join(results, "runs.jsonl")
    add("runs.jsonl (one line per run; receipt summaries inline)", [runs_path], len(runs), "run")
    add("pipedpeer record.json (steps per machine, chunk)", receipts, len(receipts), "run")
    if receipt_files:
        add("pipedpeer receipt.json.gz (every shim step)", receipt_files, len(receipt_files), "run")
    add("run output.log", outputs, len(outputs), "run")
    add("runner logs (logs/<exp>.log)", exp_logs, sum(1 for p in exp_logs for _ in open(p)), "run", "lines")
    host = [p for p in mfiles if "cgroup" not in p]
    otel("OTel host metrics (cpu, memory, network, load; 1 s)", host, "resourceMetrics", "datapoints")
    cg = [p for p in mfiles if "cgroup" in p]
    if cg:
        otel("cgroup CPU (sample only; not on the lab)", cg, "resourceMetrics", "datapoints")
    otel("OTel logs (daemon log + runs.jsonl copy)", lfiles, "resourceLogs", "log records")
    for name in ("clocks.txt", "machines.csv", "network.csv"):
        p = os.path.join(results, name)
        add(name, [p], sum(1 for _ in open(p)) if os.path.exists(p) else 0, "once", "lines")
    total = sum(r[5] for r in rows)
    total_gz = sum(r[6] for r in rows)
    out = ["Sample: %d runs, %d PCs, %.0f s of runs. Projection: %d PCs, %.0f min (%d min of experiments "
           "from the profile + %d min setup and cleanup), %d runs." % (
               nruns, pcs, span_s, pcs_real, day_s / 60, minutes, setup_min, day_runs), "",
           "| source | files | records | bytes (sample) | bytes per run | projected day, raw | projected, gzip |",
           "|---|---|---|---|---|---|---|"]
    for name, n, rec, b, per, proj, projz, note in rows:
        out.append("| %s | %d | %s%s | %s | %s | %s | %s |" % (
            name, n, rec, " " + note if note else "", hb(b), hb(per), hb(proj), hb(projz)))
    out += ["", "**Projected day: %s raw, %s gzipped, against the 8 GB R2 cap (%.0f%% / %.0f%%).**" % (
        hb(total), hb(total_gz), 100 * total / 8e9, 100 * total_gz / 8e9)]
    return "\n".join(out), {"total": total, "total_gz": total_gz, "rows": rows}


def hb(b):
    for unit in ("B", "kB", "MB", "GB"):
        if abs(b) < 1000 or unit == "GB":
            return ("%.0f %s" if unit == "B" else "%.1f %s") % (b, unit)
        b /= 1000.0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--results", default=os.path.join(HERE, "..", "..", "results", "lab"))
    p.add_argument("--runs")
    p.add_argument("--otel", action="append", default=[])
    p.add_argument("--out")
    p.add_argument("--port-lines")
    p.add_argument("--clocks")
    p.add_argument("--profile", default=os.path.join(HERE, "profile-minimal.tsv"))
    p.add_argument("--label", default="")
    p.add_argument("--cpu-note", default="", help="shown where a run's CPU comes from the host scraper")
    p.add_argument("--html", action="store_true", help="also write dashboard.html")
    p.add_argument("--no-anim", action="store_true", help="skip the cluster-replay GIF/MP4")
    p.add_argument("--no-figures", action="store_true")
    a = p.parse_args(argv)
    a.runs = a.runs or os.path.join(a.results, "runs.jsonl")
    a.otel = a.otel or [os.path.join(os.path.dirname(os.path.abspath(a.runs)), "otel")]
    model, metrics, runs, results = build_model(a)
    out = a.out or os.path.join(results, "figures-paper")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "tables.md"), "w") as f:
        f.write(tables_md(model))
    vol, model["volume"] = volume(model, runs, results, a.otel, a.profile)
    with open(os.path.join(out, "volume.md"), "w") as f:
        f.write(vol + "\n")
    made, skipped = [], []
    if not a.no_figures:
        import sample_paper
        made, skipped = sample_paper.all_figures(model, out, anim=not a.no_anim)
    if a.html:
        import sample_dashboard
        path = os.path.join(os.path.dirname(out), "dashboard.html")
        sample_dashboard.write(model, path)
        made.append(os.path.relpath(path, out))
    with open(os.path.join(out, "skipped.txt"), "w") as f:
        f.write("".join("%s: %s\n" % s for s in skipped))
    print("figures: %s" % ", ".join(made))
    for s in skipped:
        print("skipped %s: %s" % s)
    print(vol)
    return 0


if __name__ == "__main__":
    sys.exit(main())
