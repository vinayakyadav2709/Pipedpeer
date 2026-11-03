"""E20: real one-liner workloads, run unmodified. Two scripts that use every
core the ordinary way, with no cluster code:

  W5  docs/benchmarks/workloads/group_fits.py: a pandas groupby whose groups
      go to joblib.Parallel(n_jobs=-1), a bootstrap fit per group
  W6  docs/benchmarks/workloads/cross_val.py: scikit-learn cross_val_score
      (n_jobs=-1) of a random forest, 8 x 8 repeated stratified folds

plain runs each at N=1 (the reference result and the speedup's base), then
pipedpeer, ray and dask at each N of --n other than 1 (default 16), one
warm-up and --repeats (default 2) each. Ray and Dask run the joblib-backend
ports in docs/benchmarks/workloads/baselines/ (w5_*, w6_*: register_ray or
Client, and a parallel_backend block, as W2-W4's ports); every line has
script_lines_changed, the lines of the port that differ from the original
(0 for plain and pipedpeer, which run the original file). result_ok is the
result equality check against plain at the same size: W5 within a relative
1e-9 (float sums), W6 exactly.

The sizes are fixed here (about 30 s on one 8-core lab PC, untested on
lab hardware; --size W5:PD_GROUPS=.. overrides) unless workloads.json has
a W5 or W6 entry, which then wins.

Plain pandas groupby(...).apply(f) is not here: the shim does not spread
it (it spreads groupby.agg and merge when its cost model says moving the
rows pays, and Pool and joblib work), so it would only show pipedpeer
running it locally (docs/capabilities.md).
"""
import difflib
import os

from campaign import runner
from campaign.workloads import Workload

SYSTEMS = ["pipedpeer", "ray", "dask"]
BASE = "docs/benchmarks/workloads/"
WORKLOADS = [
    {"id": "W5", "script": BASE + "group_fits.py",
     "ports": {"ray": BASE + "baselines/w5_ray.py", "dask": BASE + "baselines/w5_dask.py"},
     "size_var": "PD_GROUPS",
     "strong": {"PD_GROUPS": "256", "PD_ROWS": "4000", "PD_BOOT": "1500"},
     "smoke": {"PD_GROUPS": "16", "PD_ROWS": "1000", "PD_BOOT": "200", "LOKY_MAX_CPU_COUNT": "2"},
     "system_env": {}, "result_regex": r"groups=\d+ rows=\d+ boot=\d+|slope_sum=\S+ slope_sd_sum=\S+",
     "result_compare": {"rel_tol": 1e-9}, "timing_regex": r"fit_seconds=([0-9.]+)"},
    {"id": "W6", "script": BASE + "cross_val.py",
     "ports": {"ray": BASE + "baselines/w6_ray.py", "dask": BASE + "baselines/w6_dask.py"},
     "size_var": "CV_SAMPLES",
     "strong": {"CV_SAMPLES": "4000", "CV_TREES": "100", "CV_SPLITS": "8", "CV_REPEATS": "8"},
     "smoke": {"CV_SAMPLES": "300", "CV_TREES": "10", "CV_SPLITS": "4", "CV_REPEATS": "2",
               "LOKY_MAX_CPU_COUNT": "2"},
     "system_env": {}, "result_regex": r"fits=\d+ samples=\d+|scores_sum=[0-9.]+|mean_score=[0-9.]+",
     "result_compare": "equal", "timing_regex": r"fit_seconds=([0-9.]+)"},
]


def lines_changed(wl, system):
    """Lines of the script a system runs that differ from the original."""
    path = wl.script_for(system)
    if path == wl.script:
        return 0
    with open(os.path.join(runner.REPO, wl.script)) as f:
        a = f.read().splitlines()
    with open(os.path.join(runner.REPO, path)) as f:
        b = f.read().splitlines()
    return sum(1 for d in difflib.unified_diff(a, b, lineterm="", n=0)
               if d[:1] in "+-" and not d.startswith(("+++", "---")))


def main(ctx):
    for d in WORKLOADS:
        ctx.workloads.setdefault(d["id"], Workload(d))
    ns = [n for n in ctx.ns([1, 16]) if n > 1] or [len(ctx.inv.workers)]
    reps = ctx.repeats(2)
    chosen = [s for s in SYSTEMS if ctx.enabled(s)]
    for wid in ctx.wl_ids(["W5", "W6"]):
        wl = ctx.workloads[wid]
        ctx.one_run(wl, "plain", 1, 1, extra={"script_lines_changed": 0})
        for n in ns:
            for s in ctx.rng.sample(chosen, len(chosen)):
                ctx.one_run(wl, s, n, 0, warmup=True, extra={"script_lines_changed": lines_changed(wl, s)})
            for r in range(1, reps + 1):
                for s in ctx.rng.sample(chosen, len(chosen)):
                    ctx.one_run(wl, s, n, r, extra={"script_lines_changed": lines_changed(wl, s)})
