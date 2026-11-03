"""scripts/campaign/workloads.json (lab/baselines owns it), schema 1. Per
workload the runner reads:

  id, script, args       the unmodified script and its arguments
  ports                  {"ray"|"dask"|"torchrun": path}: the baseline ports
  strong                 {ENV: value}: strong-scaling size, the same at every N
  weak_per_pc            {ENV: value}: per-PC size; numbers are multiplied by N
  smoke                  {ENV: value}: the small size --rehearsal uses
  variants               {name: {ENV: value}}: e.g. T1's mlp-c and transformer
  system_env             {system: {ENV: value}}, values may name
                         {nproc_submitter} (pc00's cores) and {nproc_workers}
                         (the cores of the N PCs that do the work)
  result_regex           every match (group 1 if it has one) joined by " | "
  result_compare         "equal", or {"rel_tol": x} for numbers within x
  timing_regex           group 1 is the script's own timing (script_s)
  files                  optional extra repo files shipped beside the script

The size env keys the result cache; system_env does not (it changes how a
system runs the work, never what the work is).
"""
import json
import os
import re

FALLBACK = [{
    # Used only while workloads.json does not exist yet (lab/baselines not
    # merged), so the runner can rehearse pipedpeer and plain on W1.
    "id": "W1",
    "script": "docs/benchmarks/workloads/pool_map.py",
    "ports": {},
    "strong": {"BENCH_ITEMS": "960"},
    "weak_per_pc": {"BENCH_ITEMS": "240"},
    "smoke": {"BENCH_ITEMS": "48"},
    "system_env": {"plain": {"BENCH_PROCS": "{nproc_submitter}"},
                   "pipedpeer": {"BENCH_PROCS": "{nproc_submitter}"}},
    "result_regex": r"items=\d+|checksum=[-0-9.e+]+",
    "result_compare": "equal",
    "timing_regex": r"seconds=([0-9.]+)",
}]

NUM = re.compile(r"^-?\d+(\.\d+)?$")
FLOAT = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")


class Workload:
    def __init__(self, d: dict):
        self.d = d
        self.id = d["id"]
        self.script = d["script"]
        self.ports = d.get("ports") or {}
        self.strong = d.get("strong") or {}
        self.weak = d.get("weak_per_pc", d.get("weak")) or {}
        self.smoke = d.get("smoke") or {}
        self.variants = d.get("variants") or {}
        self.system_env_spec = d.get("system_env") or {}
        self.result_regex = d.get("result_regex", "")
        self.result_compare = d.get("result_compare", "equal")
        self.timing_regex = d.get("timing_regex", "")
        self.files = d.get("files") or []
        self.args = d.get("args") or []

    def script_for(self, system: str) -> str:
        """The original for plain and pipedpeer; the port for the others."""
        kind = system.split("-")[0]
        if kind in ("ray", "dask", "torchrun"):
            port = self.ports.get(kind)
            if not port:
                raise LookupError("workload %s has no %s port in workloads.json" % (self.id, kind))
            return port
        return self.script

    def size(self, mode: str, n: int, rehearsal: bool = False, override: dict = None) -> dict:
        """The size env of a run: mode "strong" or "weak", at N PCs."""
        if override:
            return {k: str(v) for k, v in override.items()}
        if rehearsal and self.smoke:
            base = self.smoke
        elif mode == "weak":
            base = self.weak
        else:
            base = self.strong
        out = {}
        for k, v in (base or {}).items():
            s = str(v).replace("{n}", str(n))
            if mode == "weak" and NUM.match(s):
                s = str(int(float(s) * n)) if "." not in s else repr(float(s) * n)
            out[k] = s
        return out

    def system_env(self, system: str, nproc_submitter: int, nproc_workers: int) -> dict:
        spec = self.system_env_spec.get(system) or self.system_env_spec.get(system.split("-")[0]) or {}
        return {k: str(v).format(nproc_submitter=nproc_submitter, nproc_workers=nproc_workers)
                for k, v in spec.items()}

    def compare(self, a: str, b: str) -> bool:
        """result_ok: a matches the reference b under result_compare."""
        if a is None or b is None:
            return False
        spec = self.result_compare
        if isinstance(spec, dict) and "rel_tol" in spec:
            fa, fb = FLOAT.findall(a), FLOAT.findall(b)
            if len(fa) != len(fb) or not fa:
                return False
            tol = float(spec["rel_tol"])
            return all(abs(float(x) - float(y)) <= tol * max(abs(float(y)), 1e-12) for x, y in zip(fa, fb))
        return a == b


def extract(regex: str, text: str):
    """Every match of regex in text (group 1 if the regex has a group),
    joined by " | "; None when nothing matches."""
    if not regex:
        return None
    found = [m.group(1) if m.re.groups else m.group(0) for m in re.finditer(regex, text, re.M)]
    return " | ".join(found) if found else None


def load(repo: str, path: str = None):
    path = path or os.path.join(repo, "scripts", "campaign", "workloads.json")
    if not os.path.exists(path):
        return {w["id"]: Workload(w) for w in FALLBACK}, False
    with open(path) as f:
        doc = json.load(f)
    entries = doc.get("workloads", doc) if isinstance(doc, dict) else doc
    if isinstance(entries, dict):
        entries = [dict(v, id=k) for k, v in entries.items()]
    return {e["id"]: Workload(e) for e in entries}, True
