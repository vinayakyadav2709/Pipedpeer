"""E13: churn. W1 at N=16 (rehearsal: 2) on pipedpeer, dask and ray. Every
PL_CHURN_EVERY_S (20 s) one random worker that is up is killed with SIGKILL,
and restarted PL_CHURN_DOWN_S (10 s) later:

  pipedpeer  the daemon (`pipedpeer __daemon__`) on one of pc01..pc(N-1);
             restarted with `pipedpeer start`, unless systemd already did
             (restarted_by says which). pc00 submits and is never killed.
  ray        the raylet on one of pc01..pcN (its workers die with it);
             restarted with `ray stop --force; ray start --address`.
  dask       every Dask process on one of pc01..pcN; a new `dask worker`.

Size: W1's weak size at N (E1b's N=16 runs, about 60 s at N=16), so the
no-churn wall is E1b's median at the same size and N (analyse.py joins
them), and the reference result is E1b's precomputed plain one (the day's
weakrefs step). One run per system. Each line has fault "churn", events
(kill and restart: t_ns, pc, restarted_by), kills, items_rerun and
rerun_source, and the throughput timeline of lib/campaign/timeline.py.
A run that never ends is stopped after PL_CHURN_TIMEOUT (360 s, 6x the
no-churn wall) and fails: Dask under churn can recompute without end, since
a killed worker takes the finished results it held with it.
"""
import os

from campaign import systems, timeline

SYSTEMS = ["pipedpeer", "dask", "ray"]
KILL = {
    "pipedpeer": "pkill -9 -f '[p]ipedpeer __daemon__'; true",
    "ray": "pkill -9 -f '[r]aylet'; true",
    "dask": systems.kill_marked("KILL"),
}


def restart_cmd(ctx, b, h):
    head = ctx.inv.head
    if b.kind == "pipedpeer":
        return ("if pgrep -f '[p]ipedpeer __daemon__' >/dev/null; then echo PLBY systemd; "
                "else %s >/dev/null 2>&1; echo PLBY runner; fi") % b._start_cmd(None, {})
    if b.kind == "ray":
        return "%s stop --force >/dev/null 2>&1; %s start --address=%s:%d --num-cpus=%d --disable-usage-stats %s" % (
            b.env_bin("ray"), b.env_bin("ray"), head.ip, systems.RAY_PORT, ctx.nproc(h), ctx.ray_start_args)
    return None  # dask: a background start, below


def churn_plan(ctx, b, victims, every, down):
    """Kill one up victim every `every` s while the job runs; restart it
    `down` s later (always, so the cluster is whole again afterwards)."""
    up = list(victims)
    nxt = [every]

    def restart(h):
        def fn():
            if b.kind == "dask":
                b.remote.bg(h.id, systems.STATE + "/logs/dask-worker.log",
                            "env %s %s worker tcp://%s:%d --nworkers %d --nthreads 1 --no-dashboard %s" % (
                                systems.DASK_MARK, b.env_bin("dask"), ctx.inv.head.ip, systems.DASK_PORT,
                                ctx.nproc(h), ctx.dask_worker_args))
                by = "runner"
            else:
                out = b.sh_ok(h.id, restart_cmd(ctx, b, h), "restart after the kill", timeout=180)
                by = "systemd" if "PLBY systemd" in out else "runner"
            up.append(h)
            return {"pc": h.id, "restarted_by": by}
        return fn

    def kill(h, at):
        def fn():
            b.sh_ok(h.id, KILL[b.kind], "kill -9", timeout=60)
            return {"pc": h.id}
        return fn

    def more(now, events):
        out = []
        if now >= nxt[0] and up:
            h = ctx.rng.choice(up)
            up.remove(h)
            out.append(timeline.Action(nxt[0], "kill", kill(h, nxt[0])))
            out.append(timeline.Action(nxt[0] + down, "restart", restart(h), always=True))
            nxt[0] += every
        return out
    return more


def one(ctx, wl, system, n, every, down, timeout):
    b = ctx.backend(system)
    hosts = b.hosts_for(n)
    victims = [h for h in hosts if h is not ctx.inv.head]
    sz = ctx.size_for(wl, "weak", n)
    items = int(sz.get(wl.d.get("size_var", "BENCH_ITEMS"), 0))

    def run(b_, wl_, env, logdir, n_):
        if b.kind == "dask":
            timeline.probe(ctx, "dask-start", "tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
        res, events = timeline.run_with_events(ctx, b, wl, env, logdir, churn_plan(ctx, b, victims, every, down),
                                               timeout)
        # After the run: every daemon is up again, so each can name itself.
        nodes = timeline.node_map(ctx, hosts) if b.kind == "pipedpeer" else {}
        dump = None
        if b.kind == "dask":
            dump = timeline.probe(ctx, "dask-dump", "tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
        actors = int(env.get("BENCH_PROCS", "0") or 0)
        res.update(timeline.collect(ctx, b, res, nodes, items, actors, logdir, dask_dump=dump))
        res["items_rerun"], res["rerun_source"] = timeline.rerun_items(
            b, res, items, dask_dump=dump, counted=res.get("items_counted"))
        res["events"] = events
        res["kills"] = sum(1 for e in events if e["kind"] == "kill" and not e.get("error"))
        if b.kind == "pipedpeer":
            for e in events:
                if e["kind"] == "restart":
                    ctx.daemons[e["pc"]] = (None, ())
        return res

    try:
        ctx.one_run(wl, system, n, 1, mode="weak", reference="first", run=run,
                    extra={"fault": "churn", "churn_every_s": every, "churn_down_s": down})
    finally:
        if b.kind in ("ray", "dask"):
            b.up_n = None  # rebuilt before the next run


def main(ctx):
    n = ctx.ns([16])[-1]
    every = float(os.environ.get("PL_CHURN_EVERY_S", "10" if ctx.rehearsal else "20"))
    down = float(os.environ.get("PL_CHURN_DOWN_S", "5" if ctx.rehearsal else "10"))
    timeout = float(os.environ.get("PL_CHURN_TIMEOUT", "360"))
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    timeline.put_probe(ctx)
    chosen = [s for s in SYSTEMS if ctx.enabled(s)]
    for system in ctx.rng.sample(chosen, len(chosen)):
        one(ctx, wl, system, n, every, down, timeout)
