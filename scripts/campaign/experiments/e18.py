"""E18: noisy neighbour. W1 at N=4 (rehearsal: 2), PL_HOG_SCALE (2) times
its strong size, on pipedpeer, ray and dask. Per system, in a random
order: one run with nothing else on the PCs, then one where
PL_HOG_AT_S (5 s; rehearsal 3) after the job starts a CPU hog lands on
PL_HOG_PCS (1) worker PCs: one busy python loop per core on half their
cores (taskset -c 0..k-1), until the job ends. PL_HOG_REPEATS (1) pairs.

The hogged PCs are pc01.. (in every system's set at N). Each line has
fault "hog" or "none", hog_pcs, hog_cores, hog_at_s, events, the
throughput timeline of lib/campaign/timeline.py, items_per_pc (from the
chunks), hog_share (the hogged PCs' part of the items) and fair_share
(their part of the PCs that do work: pipedpeer counts pc00, Ray's and
Dask's head does no work). A system that moves work off a loaded PC gives
hog_share below fair_share in the hog run; Ray's Pool, whose actors take
fixed batches, is expected not to.

The hogs carry PL_CAMPAIGN_HOG=1 in their environment; the experiment
stops them (pid file, then that mark) after every run and before it
starts, and each one ends itself after PL_HOG_TIMEOUT (300 s), so a
cut-off attempt leaves none behind for long.
"""
import os
import time

from campaign import systems, timeline

SYSTEMS = ["pipedpeer", "ray", "dask"]
HOG_MARK = "PL_CAMPAIGN_HOG=1"
PIDS = systems.STATE + "/hog.pids"


def hog_cmd(ctx, cores, max_s):
    """One busy loop per core 0..cores-1, each ended by timeout after max_s
    at the latest, so a cut-off run cannot leave a hog for the next one."""
    pin = 'pin="taskset -c $c"; command -v taskset >/dev/null || pin='
    return ('%s; pids=; c=0; while [ $c -lt %d ]; do env %s timeout %d $pin %s/bin/python3 -c "while True: pass" & '
            'pids="$pids $!"; c=$((c + 1)); done; echo $pids > %s; wait') % (
        pin, cores, HOG_MARK, int(max_s), ctx.env_dir, PIDS)


def stop_cmd():
    return ('[ -f %(p)s ] && kill $(cat %(p)s) 2>/dev/null; rm -f %(p)s; '
            'for d in /proc/[0-9]*; do grep -qa \'%(m)s\' $d/environ 2>/dev/null && kill -9 ${d#/proc/} 2>/dev/null; '
            'done; true') % {"p": PIDS, "m": HOG_MARK}


def stop_hogs(ctx, hosts):
    ctx.remote.sh(systems.pattern(hosts), stop_cmd(), timeout=60)


def work_pcs(b, hosts):
    return hosts if b.kind == "pipedpeer" else [h for h in hosts if h.id != b.inv.head.id]


def shares(chunks_path, out_dir, hog_ids, workers):
    import json
    with open(os.path.join(out_dir, chunks_path)) as f:
        chunks = json.load(f)["chunks"]
    per = {}
    for c in chunks:
        if c["t1"] > 0:  # not a batch of the run before (Ray's dump looks 5 s back)
            per[c["pc"]] = per.get(c["pc"], 0) + c["items"]
    total = sum(per.values())
    hog = sum(v for pc, v in per.items() if pc in hog_ids)
    return ({pc: round(v, 1) for pc, v in sorted(per.items())},
            round(hog / total, 4) if total else None, round(len(hog_ids) / len(workers), 4))


def one(ctx, wl, system, n, size, fault, hog_pcs, cores, at_s, repeat, timeout):
    b = ctx.backend(system)
    hosts = b.hosts_for(n)
    items = int(size.get(wl.d.get("size_var", "BENCH_ITEMS"), 0))

    def start_hog():
        for h in hog_pcs:
            ctx.remote.bg(h.id, systems.STATE + "/logs/hog.log", hog_cmd(ctx, cores, timeout))
        return {"pcs": [h.id for h in hog_pcs], "cores": cores}

    def run(b_, wl_, env, logdir, n_):
        if b.kind == "ray":
            time.sleep(6)  # Ray's timeline dump reaches 5 s before the start: keep the last run out
        if b.kind == "dask":
            timeline.probe(ctx, "dask-start", "tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
        actions = [timeline.Action(at_s, "hog", start_hog)] if fault == "hog" else []
        try:
            res, events = timeline.run_with_events(ctx, b, wl, env, logdir, actions, timeout)
        finally:
            stop_hogs(ctx, hog_pcs)
        nodes = timeline.node_map(ctx, hosts) if b.kind == "pipedpeer" else {}
        dump = None
        if b.kind == "dask":
            dump = timeline.probe(ctx, "dask-dump", "tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
        actors = int(env.get("BENCH_PROCS", "0") or 0)
        res.update(timeline.collect(ctx, b, res, nodes, items, actors, logdir, dask_dump=dump))
        res["events"] = events
        ids = {h.id for h in hog_pcs}
        res["items_per_pc"], res["hog_share"], res["fair_share"] = shares(
            res["chunks_path"], ctx.out_dir, ids, work_pcs(b, hosts))
        return res

    with timeline.sized(ctx, wl, size):
        ctx.one_run(wl, system, n, repeat, reference="first", run=run,
                    extra={"fault": fault, "hog_pcs": [h.id for h in hog_pcs] if fault == "hog" else [],
                           "hog_cores": cores if fault == "hog" else 0, "hog_at_s": at_s if fault == "hog" else None})


def main(ctx):
    n = ctx.ns([4])[-1]
    k = int(os.environ.get("PL_HOG_PCS", "1"))
    at_s = float(os.environ.get("PL_HOG_AT_S", "3" if ctx.rehearsal else "5"))
    reps = int(os.environ.get("PL_HOG_REPEATS", "1"))
    timeout = float(os.environ.get("PL_HOG_TIMEOUT", "300"))
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    if wl.id in ctx.size_override:
        size = ctx.size_for(wl, "strong", n)
    else:
        scale = float(os.environ.get("PL_HOG_SCALE", "2"))
        size = {key: str(int(int(v) * scale)) for key, v in ctx.size_for(wl, "strong", n).items()}
    hog_pcs = ctx.inv.workers[:k]
    cores = max(1, ctx.nproc(hog_pcs[0]) // 2)
    timeline.put_probe(ctx)
    stop_hogs(ctx, ctx.inv.all)  # a cut-off attempt may have left some
    chosen = [s for s in SYSTEMS if ctx.enabled(s)]
    try:
        for system in ctx.rng.sample(chosen, len(chosen)):
            for r in range(1, reps + 1):
                for fault in ("none", "hog"):
                    one(ctx, wl, system, n, size, fault, hog_pcs, cores, at_s, r, timeout)
    finally:
        stop_hogs(ctx, ctx.inv.all)
