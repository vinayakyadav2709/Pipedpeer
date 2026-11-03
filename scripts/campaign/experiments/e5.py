"""E5: shipping the environment to N PCs (default 32).

Cold, once per method, then an update three times:
  pipedpeer  the first job (scripts/e5_env.py, which imports the pinned set)
             from pc00 with N daemons up: its wall, and the time until every
             one of the N PCs holds the closure (/v1/store?runnable=1);
  pip        `pip install` of the same pins into a fresh venv on all N PCs at
             once (one Ansible fan-out), the wall from the first start to the
             last finish;
  ray        runtime_env={"pip": pins} on a fresh Ray cluster (pc00 head plus
             N-1 workers, so N PCs in all), one task pinned to every worker.
MB is the bytes every PC received during the measurement (/proc/net/dev,
all interfaces but lo), summed.

Update: one extra small package each time: tqdm, then tabulate, then toolz
(each a new environment for all three methods; repeating tqdm would measure
a warm cache from the second time on). pipedpeer adds it with --pkg; pip into
the existing venv; ray with a new runtime_env.

Cold is cold only on PCs that have never run this environment: run E5 before
the other experiments in a session (OPERATOR.md). The pipedpeer line records
whether pc00 had to build the env ("closure_on_pc00": built / already built).
"""
import os
import re
import shlex
import time

from campaign import probes, systems
from campaign.workloads import Workload

EXTRAS = ["tqdm", "tabulate", "toolz"]
FULL = ["numpy", "pandas", "scikit-learn", "joblib", "torch"]
SMALL = ["numpy", "joblib"]
TORCH_CPU = "https://download.pytorch.org/whl/cpu"
VENV = "$HOME/.local/share/pipedpeer-lab/e5-venv"
HERE = "scripts/campaign/experiments/scripts/"


def pins(ctx, names):
    """name==version for each of names, read from pc00's campaign env."""
    code = ("import importlib.metadata as m\n"
            "for p in %r:\n    print('PLPIN %%s==%%s' %% (p, m.version(p)))" % names)
    rc, out = ctx.remote.sh(ctx.inv.head.id, "%s/bin/python3 -c %s" % (ctx.env_dir, shlex.quote(code)), timeout=60)
    got = [ln.split()[1] for ln in out.splitlines() if ln.startswith("PLPIN ")]
    if rc != 0 or len(got) != len(names):
        raise systems.StepError("reading the pins from the campaign env failed: %s" % out.strip()[-300:],
                                host=ctx.inv.head.id)
    return got


def record(ctx, method, phase, repeat, n, hosts, start_ns, end_ns, nets, ok, extra, logdir, **more):
    rx = sum(v[0] for v in nets.values())
    line = {"run_id": os.path.basename(logdir), "workload": "E5-env", "system": method, "n": n,
            "repeat": repeat, "phase": phase, "extra_package": extra, "hosts": [h.id for h in hosts],
            "start_ns": start_ns, "end_ns": end_ns, "result_ok": ok, "mb_down": round(rx / 1e6, 2),
            "rx_bytes": {h: v[0] for h, v in nets.items()},
            "log_path": os.path.relpath(logdir, ctx.out_dir)}
    line.update(more)
    if not ok and "error" not in line:
        line["error"] = "%s %s did not complete on every PC" % (method, phase)
        line.setdefault("failing_pc", ctx.inv.head.id)
    ctx.log("E5 %s %s%s: %s s, %.1f MB, ok=%s" % (method, phase, " +" + extra if extra else "",
                                                 line.get("wall_s") or round((end_ns - start_ns) / 1e9, 2),
                                                 rx / 1e6, ok))
    return ctx.recorder.write(line)


def run_pipedpeer(ctx, n, phase, repeat, extra, script):
    b = ctx.backend("pipedpeer")
    logdir = ctx.recorder.log_dir(ctx.recorder.new_run_id())
    try:
        hosts = b.activate(n)
    except systems.StepError as e:
        return ctx.recorder.write({"workload": "E5-env", "system": "pipedpeer", "n": n, "phase": phase,
                                   "repeat": repeat, "error": str(e), "failing_pc": e.host, "result_ok": False})
    wl = Workload({"id": "E5-env", "script": script, "result_regex": r"E5-ENV-OK.*"})
    flags = ["-v"] + (["--pkg", extra] if extra else [])
    before = probes.netdev(ctx, hosts)
    res = b.run(wl, {"E5_EXTRA": extra or ""}, logdir, n, extra_flags=flags)
    with open(os.path.join(logdir, "output.log"), "w") as f:
        f.write(res["out"])
    m = re.search(r"Store path: (\S+)", res["out"])
    store = m.group(1) if m else None
    all_have_ns, missing = None, [h.id for h in hosts]
    if store and res["rc"] == 0:
        deadline = time.time() + 900
        while time.time() < deadline and missing:
            rc, out = ctx.remote.sh(":".join(missing), "%s/v1/store?path=%s\\&runnable=1" % (systems.API, store),
                                    timeout=60)
            lines = out.splitlines() if len(missing) > 1 else ["%s: %s" % (missing[0], out.strip())]
            have = {ln.split(": ", 1)[0] for ln in lines if '"cached":true' in ln.replace(" ", "")}
            missing = [h for h in missing if h not in have]
            if not missing:
                all_have_ns = time.time_ns()
                break
            time.sleep(2)
    after = probes.netdev(ctx, hosts)
    # The job runs on pc00, which builds the env (cold) or finds it built.
    on_pc00 = "built" if "Built successfully" in res["out"] else "already built"
    ok = res["rc"] == 0 and not missing and "E5-ENV-OK" in res["out"]
    more = {"store_path": store, "closure_on_pc00": on_pc00, "receipt": res.get("receipt"),
            "record_path": res.get("record_path"), "rc": res["rc"],
            "all_have_s": round((all_have_ns - res["start_ns"]) / 1e9, 3) if all_have_ns else None,
            "missing_pcs": missing or None}
    if missing:
        more["failing_pc"] = missing[0]
    return record(ctx, "pipedpeer", phase, repeat, n, hosts, res["start_ns"], res["end_ns"],
                  probes.delta(before, after), ok, extra, logdir, **more)


def run_pip(ctx, n, phase, repeat, extra, pinlist, names):
    ctx.only(ctx.backend("pipedpeer"))  # no Ray or Dask cluster up meanwhile
    hosts = ctx.inv.all[:n]
    logdir = ctx.recorder.log_dir(ctx.recorder.new_run_id())
    imports = ",".join(x.replace("scikit-learn", "sklearn") for x in names + ([extra] if extra else []))
    pip = "%s/bin/python -m pip install -q --disable-pip-version-check --no-cache-dir" % VENV
    if extra:
        cmd = "%s %s" % (pip, extra)
    else:
        cmd = ("rm -rf %s && %s/bin/python3 -m venv %s && %s --extra-index-url %s %s" % (
            VENV, ctx.env_dir, VENV, pip, TORCH_CPU, " ".join(shlex.quote(p) for p in pinlist)))
    cmd += " && %s/bin/python -c 'import %s; print(\"E5-PIP-OK\")'" % (VENV, imports)
    before = probes.netdev(ctx, hosts)
    timed = ctx.remote.timed_all(hosts, cmd, timeout=3600)
    after = probes.netdev(ctx, hosts)
    with open(os.path.join(logdir, "output.log"), "w") as f:
        for h, t in timed.items():
            f.write("== %s rc=%d %.2fs\n%s\n" % (h, t.rc, t.wall_s, t.out))
    bad = [h for h, t in timed.items() if t.rc != 0 or "E5-PIP-OK" not in t.out]
    more = {"per_pc_s": {h: round(t.wall_s, 3) for h, t in timed.items()}, "pins": pinlist}
    if bad:
        more.update({"failing_pc": bad[0], "error": "pip failed on %s" % ",".join(bad)})
    return record(ctx, "pip", phase, repeat, n, hosts, min(t.start_ns for t in timed.values()),
                  max(t.end_ns for t in timed.values()), probes.delta(before, after), not bad, extra, logdir, **more)


def run_ray(ctx, n, phase, repeat, extra, pinlist):
    b = ctx.backend("ray")
    logdir = ctx.recorder.log_dir(ctx.recorder.new_run_id())
    workers = max(1, n - 1)
    b.up_n = None  # a fresh cluster: runtime_env caches live in the Ray session
    try:
        hosts = b.activate(workers)
    except systems.StepError as e:
        return ctx.recorder.write({"workload": "E5-env", "system": "ray", "n": n, "phase": phase,
                                   "repeat": repeat, "error": str(e), "failing_pc": e.host, "result_ok": False})
    wl = Workload({"id": "E5-env", "script": HERE + "e5_ray_env.py", "ports": {"ray": HERE + "e5_ray_env.py"}})
    pl = pinlist + ([extra] if extra else [])
    before = probes.netdev(ctx, hosts)
    res = b.run(wl, {"E5_PIPS": ",".join(pl), "E5_EXTRA": extra or ""}, logdir, workers)
    after = probes.netdev(ctx, hosts)
    with open(os.path.join(logdir, "output.log"), "w") as f:
        f.write(res["out"])
    ok = res["rc"] == 0 and ("E5-RAY-OK nodes=%d " % workers) in res["out"]
    return record(ctx, "ray", phase, repeat, n, hosts, res["start_ns"], res["end_ns"],
                  probes.delta(before, after), ok, extra, logdir, rc=res["rc"])


def main(ctx):
    n = ctx.ns([32])[-1]
    names = SMALL if ctx.rehearsal else FULL
    script = HERE + ("e5_env_small.py" if ctx.rehearsal else "e5_env.py")
    try:
        pinlist = pins(ctx, names)
    except systems.StepError as e:
        ctx.log("FAILED: %s; pip and ray need the pins, running pipedpeer only" % e)
        pinlist = None
    ctx.log("E5 at N=%d, pins %s" % (n, pinlist))
    methods = ["pipedpeer", "pip", "ray"] if pinlist else ["pipedpeer"]
    methods = [m for m in methods if m == "pip" or ctx.enabled(m)]

    def one(method, phase, repeat, extra):
        if method == "pipedpeer":
            run_pipedpeer(ctx, n, phase, repeat, extra, script)
        elif method == "pip":
            if ctx.rehearsal:
                # The lab image is Nix-only: a manylinux wheel with C code
                # (numpy) installs but cannot load libstdc++. Lab PCs have it.
                pure = [p for p in pinlist if p.startswith("joblib==")]
                run_pip(ctx, n, phase, repeat, extra, pure, ["joblib"])
            else:
                run_pip(ctx, n, phase, repeat, extra, pinlist, names)
        else:
            run_ray(ctx, n, phase, repeat, extra, pinlist)

    for m in methods:  # cold in a fixed order: pipedpeer's cold must come first on fresh PCs
        one(m, "cold", 0, None)
    for r in range(1, min(ctx.repeats(3), len(EXTRAS)) + 1):
        for m in ctx.rng.sample(methods, len(methods)):
            one(m, "update", r, EXTRAS[r - 1])
    ctx.remote.sh(":".join(h.id for h in ctx.inv.all[:n]), "rm -rf %s" % VENV, timeout=300)
