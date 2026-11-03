"""E17: idle footprint, and nothing to "cluster ready". Per system, in a
random order, on the same PCs (pc00 plus the first --n workers, default
all of them):

  1. Everything stopped: every pipedpeer daemon, Ray and Dask.
  2. Start, timed from the runner on pc00. The commands are the ones the
     benchmarks use (`commands` on the line), one Ansible fan-out per
     step: pipedpeer, `pipedpeer start` on every PC at once; ray, `ray
     start --head` on pc00, then `ray start --address` on the workers;
     dask, `dask scheduler` on pc00, then `dask worker` on the workers.
     Ready is the runner's own check (pc00's /v1/nodes lists every peer
     healthy; Ray sees every worker with CPUs; Dask sees every host and
     process). ready_s is the end of the first check that passed;
     ready_after_s the end of the last that failed, so the truth lies
     between the two. start_call_s is the time of the start calls alone
     (mostly Ansible).
  3. Idle: PL_IDLE_S (60 s; rehearsal 20) after a 5 s settle, with
     experiments/scripts/footprint_probe.py on every PC: CPU seconds and
     RSS/PSS of that system's processes. And the energy over the same
     window where otel/energy.sh can read RAPL (E16).

pipedpeer's start is a daemon start on PCs that joined before (E14 times
the first join). What a user types: pipedpeer, nothing once
`pipedpeer setup --boot` made the daemon a user service; Ray and Dask,
the head and one start per worker, every time the PCs boot.
"""
import base64
import json
import os
import time

from campaign import energy, systems

SYSTEMS = ["pipedpeer", "ray", "dask"]
RAY_MARK = "PL_CAMPAIGN_RAY=1"
PROBE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts", "footprint_probe.py")
REMOTE_PROBE = systems.STATE + "/footprint_probe.py"


def stop_everything(ctx):
    pp, ray, dask = ctx.backend("pipedpeer"), ctx.backend("ray"), ctx.backend("dask")
    pp.sh_ok(systems.pattern(ctx.inv.all), "pipedpeer stop >/dev/null 2>&1 || true; "
             + systems.proc_kill(systems.DAEMON) + "; true", "pipedpeer stop")
    for h in ctx.inv.all:
        ctx.daemons[h.id] = None
    ray.stop_all()
    dask.stop_all()


def nworkers(ctx):
    return str(ctx.nproc_override) if ctx.nproc_override else "$(nproc)"


def plan(ctx, kind, workers):
    """[(hosts, command, how)] in order; how is "sh" or "bg"."""
    head = ctx.inv.head
    b = ctx.backend(kind)
    if kind == "pipedpeer":
        return [([head] + workers, b._start_cmd(None, {}), "sh")]
    if kind == "ray":
        return [([head], "env %s %s start --head --port=%d --num-cpus=0 --disable-usage-stats "
                 "--include-dashboard=false %s" % (RAY_MARK, b.env_bin("ray"), systems.RAY_PORT, ctx.ray_start_args),
                 "sh"),
                (workers, "env %s %s start --address=%s:%d --num-cpus=%s --disable-usage-stats %s" % (
                    RAY_MARK, b.env_bin("ray"), head.ip, systems.RAY_PORT, nworkers(ctx), ctx.ray_start_args), "sh")]
    addr = "tcp://%s:%d" % (head.ip, systems.DASK_PORT)
    return [([head], "env %s %s scheduler --port %d --dashboard-address :8787" % (
                systems.DASK_MARK, b.env_bin("dask"), systems.DASK_PORT), "bg"),
            (workers, "env %s %s worker %s --nworkers %s --nthreads 1 --no-dashboard %s" % (
                systems.DASK_MARK, b.env_bin("dask"), addr, nworkers(ctx), ctx.dask_worker_args), "bg")]


def ready(ctx, kind, workers):
    """True when the system sees every PC (the runner's own checks)."""
    head = ctx.inv.head
    if kind == "pipedpeer":
        got, own = ctx.backend("pipedpeer").healthy_others()
        return bool(own) and got == len(workers)
    if kind == "ray":
        probe = ("RAY_ADDRESS=%s:%d %s/bin/python3 -c \"import ray; ray.init(address='auto', logging_level='ERROR'); "
                 "print('PLN', sum(1 for x in ray.nodes() if x['Alive'] and x['Resources'].get('CPU', 0) > 0))\""
                 % (head.ip, systems.RAY_PORT, ctx.env_dir))
        rc, out = ctx.remote.sh(head.id, probe, timeout=90)
        return any(ln.split()[1:2] == [str(len(workers))] for ln in out.splitlines() if ln.startswith("PLN "))
    want = sum(ctx.nproc(w) for w in workers)
    probe = ("%s/bin/python3 -c \"from distributed import Client; c = Client('tcp://%s:%d', timeout=10); "
             "w = c.scheduler_info()['workers'].values(); print('PLN', len({x['host'] for x in w}), len(w))\""
             % (ctx.env_dir, head.ip, systems.DASK_PORT))
    rc, out = ctx.remote.sh(head.id, probe, timeout=90)
    for ln in out.splitlines():
        p = ln.split()
        if ln.startswith("PLN ") and len(p) == 3:
            return int(p[1]) == len(workers) and int(p[2]) >= want
    return False


def start_and_wait(ctx, kind, workers, timeout):
    t0 = time.time()
    calls = 0.0
    commands = []
    for hosts, cmd, how in plan(ctx, kind, workers):
        # What a user types: no runner mark, no env path; pipedpeer's start
        # without the runner's systemd drop-in handling.
        shown = "pipedpeer start" if kind == "pipedpeer" else cmd.replace(
            "env %s " % (RAY_MARK if kind == "ray" else systems.DASK_MARK), "").replace(ctx.env_dir + "/bin/", "")
        commands.append({"pcs": [h.id for h in hosts], "command": shown.strip(), "runner_command": cmd})
        c0 = time.time()
        if how == "bg":
            ctx.remote.bg(systems.pattern(hosts), systems.STATE + "/logs/e17-%s.log" % kind, cmd)
        else:
            ctx.backend(kind).sh_ok(systems.pattern(hosts), cmd, "%s start" % kind, timeout=180)
        calls += time.time() - c0
    if kind == "pipedpeer":
        for h in [ctx.inv.head] + workers:
            ctx.daemons[h.id] = (None, ())
    after = None
    while time.time() - t0 < timeout:
        if ready(ctx, kind, workers):
            return round(time.time() - t0, 2), after, round(calls, 2), commands
        after = round(time.time() - t0, 2)
        time.sleep(0.5)
    raise systems.StepError("%s not ready after %d s" % (kind, timeout), host=ctx.inv.head.id)


def footprint(ctx, kind, hosts, seconds):
    rc, out = ctx.remote.sh(systems.pattern(hosts), "%s/bin/python3 %s %s %d" % (
        ctx.env_dir, REMOTE_PROBE, kind, seconds), timeout=seconds + 120)
    res = {}
    for pc, text in energy.per_host(out, hosts).items():
        line = next((ln for ln in text.splitlines() if ln.startswith("PLFP ")), None)
        res[pc] = json.loads(line[5:]) if line else None
    return res


def totals(fp):
    got = [v for v in fp.values() if v]
    return {"cpu_s_total": round(sum(v["cpu_s"] for v in got), 3),
            "cpu_pct_mean_per_pc": round(sum(v["cpu_pct"] or 0 for v in got) / len(got), 2) if got else None,
            "rss_mb_total": round(sum(v["rss_mb_avg"] for v in got), 1),
            "pss_mb_total": round(sum(v["pss_mb_avg"] for v in got), 1),
            "procs_total": sum(v["procs"] for v in got)}


def one(ctx, kind, workers, idle_s, timeout):
    hosts = [ctx.inv.head] + workers
    rid = ctx.recorder.new_run_id()
    line = {"run_id": rid, "workload": "E17-idle", "system": kind, "n": len(workers), "repeat": 1,
            "hosts": [h.id for h in hosts], "idle_s": idle_s,
            "commands_typed": {"pipedpeer": 0, "ray": 1 + len(workers), "dask": 1 + len(workers)}[kind],
            "commands_typed_note": {"pipedpeer": "0 with `pipedpeer setup --boot`, else 1 per PC",
                                    "ray": "1 head + 1 per worker, after every boot",
                                    "dask": "1 scheduler + 1 per worker, each kept running, after every boot"}[kind]}
    try:
        stop_everything(ctx)
        time.sleep(3)
        start_ns = time.time_ns()
        line["ready_s"], line["ready_after_s"], line["start_call_s"], line["commands"] = start_and_wait(
            ctx, kind, workers, timeout)
        line["start_ns"] = start_ns
        line["ready_ns"] = start_ns + int(line["ready_s"] * 1e9)
        time.sleep(5)
        t0 = time.time_ns()
        fp = footprint(ctx, kind, hosts, idle_s)
        t1 = time.time_ns()
        line.update({"idle_start_ns": t0, "idle_end_ns": t1, "footprint": fp})
        line.update(totals(fp))
        e = energy.measure(ctx, hosts, t0, t1)
        line["idle_energy_j"] = e.get("energy_j")
        line["idle_power_w"] = round(e["energy_j"] / ((t1 - t0) / 1e9), 2) if e.get("energy_j") else None
        line["energy_available"] = e.get("energy_available")
        line["end_ns"] = t1
        missing = [pc for pc, v in fp.items() if not v]
        line["result_ok"] = not missing
        if missing:
            line["error"] = "no footprint from %s" % ", ".join(missing)
            line["failing_pc"] = missing[0]
    except systems.StepError as e:
        line.update({"error": str(e), "failing_pc": e.host or ctx.inv.head.id, "result_ok": False})
        ctx.log("FAILED: %s" % e)
    finally:
        if kind in ("ray", "dask"):
            ctx.backend(kind).stop_all()
    ctx.recorder.write(line)
    ctx.log("%s: ready %.1f s, idle %s" % (kind, line.get("ready_s") or -1,
                                            {k: line.get(k) for k in ("cpu_s_total", "rss_mb_total", "pss_mb_total")}))


def main(ctx):
    n = min(ctx.ns([len(ctx.inv.workers)])[-1], len(ctx.inv.workers))
    workers = ctx.inv.workers[:n]
    idle_s = int(float(os.environ.get("PL_IDLE_S", "20" if ctx.rehearsal else "60")))
    timeout = float(os.environ.get("PL_READY_TIMEOUT", "300"))
    with open(PROBE, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    ctx.remote.sh(systems.pattern(ctx.inv.all), "mkdir -p %s && printf %%s %s | base64 -d > %s" % (
        systems.STATE + "/logs", data, REMOTE_PROBE), timeout=60)
    energy.ensure(ctx, ctx.inv.all)
    chosen = [s for s in SYSTEMS if ctx.enabled(s)]
    # The daemons this stops are restarted by run.sh's release_all at the end.
    for kind in ctx.rng.sample(chosen, len(chosen)):
        one(ctx, kind, workers, idle_s, timeout)
