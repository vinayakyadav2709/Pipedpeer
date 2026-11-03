"""Everything the paper figures read, loaded once: the runs of every runs
file, analyse.py's groups, the OTel CPU and network series, receipts,
port-lines.csv, E14's timeline and E16's energy samples.

The loading is sample_figures.build_model's (one definition of median,
IQR, node id -> pc, clock offsets); this module only gathers the extra
runs files, the headline and more-bench tables, and per-PC item counts.
"""
import csv
import gzip
import json
import os
import statistics
from collections import defaultdict
from dataclasses import dataclass, field

import analyse
import analyse_headline as hl
import analyse_more as more
import sample_figures as sf
import story_model as sm

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


@dataclass
class Inputs:
    results: str
    runs: str
    extra_runs: list
    otel: list
    timeline: list
    port_lines: str
    clocks: str = None
    label: str = ""
    sample: bool = False


def lab_inputs(results=None):
    results = results or os.path.join(ROOT, "results", "lab")
    return Inputs(results=results, runs=os.path.join(results, "runs.jsonl"), extra_runs=[],
                  otel=[os.path.join(results, "otel")], timeline=[os.path.join(results, "timeline.jsonl")],
                  port_lines=os.path.join(results, "port-lines.csv"), label="lab day")


def sample_inputs():
    d = os.path.join(ROOT, "results", "lab", "sample", "data")
    return Inputs(results=d, runs=os.path.join(d, "runs.jsonl"),
                  extra_runs=[p for p in (os.path.join(d, "rehearsal", "runs.jsonl"),
                                          os.path.join(d, "more", "runs.jsonl")) if os.path.exists(p)],
                  otel=[os.path.join(d, "otel"), os.path.join(d, "otel-cgroup")],
                  timeline=[os.path.join(d, "rehearsal", "timeline.jsonl")],
                  port_lines=os.path.join(ROOT, "results", "lab", "port-lines.csv"),
                  label="container sample", sample=True)


@dataclass
class Lab:
    inputs: Inputs
    runs: list
    groups: dict
    port_lines: dict
    pcs: list
    cores: dict
    timelines: dict
    ip_pc: dict
    events14: list
    energy: dict
    model: dict = field(repr=False, default=None)

    def exp(self, e, ok=True, **match):
        out = [r for r in self.runs if r.get("experiment") == e and (not ok or analyse.usable(r))]
        for k, v in match.items():
            out = [r for r in out if r.get(k) == v]
        return out

    def rows(self, e, **match):
        out = list(self.groups.get(e, []))
        for k, v in match.items():
            out = [r for r in out if r.get(k) == v]
        return out

    def dir_of(self, r):
        return r.get("_dir") or self.inputs.results

    def items_per_pc(self, r):
        """pc -> items computed, from the best record the run has: the
        runner's own count, the binned timeline, chunks.json, or the receipt's
        by_where (addresses mapped through machines.csv; "local" is the
        submitter)."""
        if r.get("items_per_pc"):
            return dict(r["items_per_pc"])
        per = (r.get("timeline") or {}).get("per_pc")
        if per:
            return {pc: sum(v) for pc, v in per.items()}
        ch = sm.chunks(r, self.dir_of(r))
        if ch:
            out = defaultdict(float)
            for pc, _t0, _t1, items in ch:
                out[pc] += items or 0
            return dict(out)
        bw = (r.get("receipt") or {}).get("by_where")
        if bw:
            out = defaultdict(float)
            origin = (r.get("hosts") or ["pc00"])[0]
            for where, n in bw.items():
                if where == "local":
                    out[origin] += n
                else:
                    ip = where.split(":", 1)[1].rsplit(":", 1)[0] if ":" in where else where
                    out[self.ip_pc.get(ip, ip)] += n
            return dict(out)
        return {}

    def record(self, r):
        """record.json (pipedpeer jobs last --export json), or {}."""
        p = os.path.join(self.dir_of(r), r.get("log_path") or "", "record.json")
        if r.get("log_path") and os.path.exists(p):
            try:
                return json.load(open(p))
            except ValueError:
                return {}
        return {}

    def receipt_steps(self, r):
        p = os.path.join(self.dir_of(r), r.get("log_path") or "", "receipt.json.gz")
        if r.get("log_path") and os.path.exists(p):
            try:
                return json.load(gzip.open(p)).get("steps") or []
            except (ValueError, OSError):
                return []
        st = (r.get("receipt") or {}).get("steps")
        if isinstance(st, str):
            try:
                st = json.loads(st)
            except ValueError:
                st = []
        return st or []


def load(inp):
    ns = type("A", (), {"runs": inp.runs, "otel": inp.otel, "clocks": inp.clocks, "port_lines": inp.port_lines,
                        "label": inp.label, "cpu_note": ""})
    model, _metrics, runs, results = sf.build_model(ns)
    extra = sm.load_extra(inp.extra_runs)
    all_runs = runs + extra
    groups = sm.regroup(runs, extra)
    ip_pc = {}
    p = os.path.join(results, "machines.csv")
    if os.path.exists(p):
        ip_pc = {r["ip"]: r["pc_id"] for r in csv.DictReader(open(p)) if r.get("ip")}
    events = []
    for t in inp.timeline:
        if t and os.path.exists(t):
            events += hl.load_timeline(t)
    return Lab(inputs=inp, runs=all_runs, groups=groups,
               port_lines={(p["workload"], p["system"]): p["lines"] for p in model["port_lines"]},
               pcs=model["pcs"], cores=model["cores"], timelines=model["timelines"], ip_pc=ip_pc,
               events14=events, energy=more.load_energy(results), model=model)


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def iqr(xs):
    return analyse.stats(xs)


def n_reps(rows):
    """'n = 5' style text for a caption or axis note: the repeats behind
    the medians, as a range when groups differ."""
    ns = sorted({r.get("runs_used") for r in rows if r.get("runs_used")})
    if not ns:
        return "n = 0"
    return "n = %d" % ns[0] if len(ns) == 1 else "n = %d-%d" % (ns[0], ns[-1])
