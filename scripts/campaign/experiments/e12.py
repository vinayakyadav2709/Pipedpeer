"""E12: elastic join. One long W1 starts on the first N0 PCs; every
PL_JOIN_EVERY_S (20 s) the runner brings STEP more PCs in, up to N1, while
the job runs. `--n 4,16` means N0=4, N1=16, STEP=N0 (rehearsal: 1 -> 2 for
Ray and Dask, 2 -> 3 for pipedpeer, whose N counts pc00: a map that starts
with no peer stays on pc00, so 1 -> 2 would show nothing).

  pipedpeer  daemons run on pc00..pc(N0-1), the rest stopped; a join is
             `pipedpeer start` on the next PCs (they keep their cluster
             config). The job's Pool spreads over whoever is healthy when
             each chunk is placed.
  dask       workers on pc01..pcN0; a join starts `dask worker` on the next
             PCs. The scheduler hands new workers queued tasks.
  ray        workers on pc01..pcN0; a join is `ray start --address` on the
             next PCs. W1's Ray port makes Pool(BENCH_PROCS) with the cores
             there were at the start (what a user would write), so the new
             nodes may get nothing: that is the result, not a fault.

One run per system (no repeats; the timeline is the result). Size: W1's
strong size x PL_JOIN_SCALE (13: about 80 s while PCs join), or --size.
Each line has n_start, n (the final N), join_every_s, events (one per join:
t_ns, done_ns, pcs, seen_ns when the system first saw them), and the
throughput timeline of lib/campaign/timeline.py.
"""
import os
import time

from campaign import systems, timeline

SYSTEMS = ["pipedpeer", "dask", "ray"]


def size(ctx, wl):
    if wl.id in ctx.size_override:
        return ctx.size_for(wl, "strong", 1)
    base = ctx.size_for(wl, "strong", 1)  # the smoke size in a rehearsal
    scale = float(os.environ.get("PL_JOIN_SCALE", "13"))
    return {k: str(int(int(v) * scale)) for k, v in base.items()}


def joiner(ctx, b, group):
    """The action that brings group (hosts) into b's cluster."""
    head = ctx.inv.head
    if b.kind == "pipedpeer":
        def fn():
            b.sh_ok(systems.pattern(group), b._start_cmd(None, {}), "pipedpeer start (join)", timeout=120)
            for h in group:
                ctx.daemons[h.id] = (None, ())
            want = len([h for h in ctx.inv.all if ctx.daemons.get(h.id) == (None, ())]) - 1
            deadline, got = time.time() + 15, None
            while time.time() < deadline:
                got, _ = b.healthy_others()
                if got >= want:
                    return {"pcs": [h.id for h in group], "seen_ns": time.time_ns(), "healthy_peers": got}
                time.sleep(1)
            return {"pcs": [h.id for h in group], "seen_ns": None, "healthy_peers": got}
        return fn
    if b.kind == "dask":
        addr = "tcp://%s:%d" % (head.ip, systems.DASK_PORT)

        def fn():
            for w in group:
                b.remote.bg(w.id, systems.STATE + "/logs/dask-worker.log",
                            "env %s %s worker %s --nworkers %d --nthreads 1 --no-dashboard %s" % (
                                systems.DASK_MARK, b.env_bin("dask"), addr, ctx.nproc(w), ctx.dask_worker_args))
            return {"pcs": [h.id for h in group]}
        return fn

    def fn():
        for w in group:
            b.sh_ok(w.id, "%s start --address=%s:%d --num-cpus=%d --disable-usage-stats %s" % (
                b.env_bin("ray"), head.ip, systems.RAY_PORT, ctx.nproc(w), ctx.ray_start_args),
                "ray start (join)", timeout=180)
        return {"pcs": [h.id for h in group]}
    return fn


def one(ctx, wl, system, n0, n1, every, timeout):
    b = ctx.backend(system)
    if system == "pipedpeer" and n0 == 1 and len(ctx.inv.all) > n1:
        # pipedpeer's N counts pc00, and a map that starts with no peer stays
        # on pc00: start with one peer (the rehearsal's 1 -> 2 becomes 2 -> 3).
        n0, n1 = 2, n1 + 1
    pool = b.hosts_for(n1)
    first = b.hosts_for(n0)
    rest = [h for h in pool if h not in first]
    groups = [rest[i:i + n0] for i in range(0, len(rest), n0)]
    sz = size(ctx, wl)
    items = int(sz.get(wl.d.get("size_var", "BENCH_ITEMS"), 0))
    state = {}

    def run(b_, wl_, env, logdir, n):
        if b.kind == "dask":
            timeline.probe(ctx, "dask-start", "tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
        actions = [timeline.Action(every * (i + 1), "join", joiner(ctx, b, g)) for i, g in enumerate(groups)]
        res, events = timeline.run_with_events(ctx, b, wl, env, logdir, actions, timeout)
        # After the run: every daemon is up again, so each can name itself.
        nodes = timeline.node_map(ctx, pool) if b.kind == "pipedpeer" else {}
        dump = None
        if b.kind == "dask":
            dump = timeline.probe(ctx, "dask-dump", "tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
            for e in events:  # when the scheduler saw each join's workers
                ips = {ctx.inv.host(p).ip for p in e.get("pcs", [])}
                seen = [t for t, what, addr in (dump or {}).get("workers", [])
                        if what == "add" and addr.split("://")[-1].rsplit(":", 1)[0] in ips and t * 1e9 >= e["t_ns"]]
                e["seen_ns"] = int(min(seen) * 1e9) if seen else None
        actors = int(env.get("BENCH_PROCS", "0") or 0)
        res.update(timeline.collect(ctx, b, res, nodes, items, actors, logdir, dask_dump=dump))
        res["events"] = events
        res["hosts_final"] = [h.id for h in pool]
        state["joined"] = True
        return res

    try:
        with timeline.sized(ctx, wl, sz):
            ctx.one_run(wl, system, n0, 1, reference="first", run=run,
                        extra={"n_start": n0, "n_final": n1, "join_step": n0, "join_every_s": every})
    finally:
        if b.kind in ("ray", "dask"):
            b.up_n = None  # the cluster is now N1 wide; the next activate rebuilds it


def main(ctx):
    ns = ctx.ns([4, 16])
    n0, n1 = ns[0], ns[-1]
    every = float(os.environ.get("PL_JOIN_EVERY_S", "10" if ctx.rehearsal else "20"))
    timeout = float(os.environ.get("PL_JOIN_TIMEOUT", "300"))
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    timeline.put_probe(ctx)
    chosen = [s for s in SYSTEMS if ctx.enabled(s)]
    for system in ctx.rng.sample(chosen, len(chosen)):
        one(ctx, wl, system, n0, n1, every, timeout)
