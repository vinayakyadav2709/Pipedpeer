"""E8: CPU training, T1 (its mlp-c and transformer variants). PipedPeer
exact and fast sync (`pipedpeer run --ddp N --ddp-sync exact|fast`) vs
torchrun DDP over gloo, ranks 2, 4, 8, one rank per PC on pc00..pc(N-1) for
both, the same model, batch and steps. Three seeds (repeat = seed), after one
warm-up per (system, ranks).

Recorded per run: wall_s, the validation loss (result; result_ok is the
loss within T1's rel_tol of the one-rank plain run at that seed), and
bytes_per_rank, the bytes the ranks' PCs sent during the run (/proc/net/dev)
divided by the ranks.
"""
from campaign import probes, systems

SYSTEMS = ["pipedpeer-exact", "pipedpeer-fast", "torchrun"]


def measured(ctx, inner):
    """Wrap a backend's run with the bytes the ranks' PCs sent."""
    def run(b, wl, env, logdir, n):
        hosts = ctx.inv.all[:n]
        before = probes.netdev(ctx, hosts)
        res = inner(b, wl, env, logdir, n)
        d = probes.delta(before, probes.netdev(ctx, hosts))
        res["tx_bytes"] = {h: v[1] for h, v in d.items()}
        res["bytes_per_rank"] = int(sum(v[1] for v in d.values()) / max(1, n))
        return res
    return run


def ddp_run(sync):
    def run(b, wl, env, logdir, n):
        return b.run(wl, env, logdir, n, extra_flags=["--ddp", str(n), "--ddp-sync", sync])
    return run


def main(ctx):
    ctx.add_backend("pipedpeer-exact", systems.PipedPeer(ctx, name="pipedpeer-exact"))
    ctx.add_backend("pipedpeer-fast", systems.PipedPeer(ctx, name="pipedpeer-fast"))
    ctx.add_backend("torchrun", systems.Torchrun(ctx))
    wid = ctx.wl_ids(["T1"])[0]
    if wid not in ctx.workloads:
        ctx.log("no %s in workloads.json; E8 needs lab/baselines" % wid)
        return
    wl = ctx.workloads[wid]
    runs = {"pipedpeer-exact": ddp_run("exact"), "pipedpeer-fast": ddp_run("fast"),
            "torchrun": lambda b, w, e, d, n: b.run(w, e, d, n)}
    variants = wl.variants or {"default": {}}
    for vname, venv in variants.items():
        for n in [x for x in ctx.ns([2, 4, 8]) if x >= 2] or [2]:
            for seed in range(0, ctx.repeats(3) + 1):
                size = dict(wl.smoke if ctx.rehearsal else {}, **venv)
                size["BENCH_SEED"] = str(max(seed, 1))
                ctx.size_override[wl.id] = size
                order = ctx.rng.sample(SYSTEMS, len(SYSTEMS))
                for s in order:
                    ctx.one_run(wl, s, n, seed, warmup=(seed == 0), extra={"variant": vname},
                                run=measured(ctx, runs[s]))
    ctx.size_override.pop(wl.id, None)
