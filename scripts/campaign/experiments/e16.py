"""E16: energy to solution. Package energy (RAPL) on every PC, sampled each
second by otel/energy.sh (lib/campaign/energy.py), turned into joules per
job per system.

  1. energy.sh is started on every PC where node.sh has not already
     started it. A PC that cannot read its counters (no RAPL zone, no
     root, a container) says why, and its lines get energy_available false.
  2. Idle power: PL_ENERGY_IDLE_S (10 s) with the daemons up and Ray and
     Dask stopped; one line, phase "idle-power", power_w per PC.
  3. W1 at its strong size: plain at N=1, then pipedpeer, ray and dask at
     each N of --n other than 1 (default 16), one warm-up and --repeats
     (default 1) each. Every line has energy_j (the PCs the system ran on:
     pipedpeer N, ray and dask N + 1 with the head), energy_all_pcs_j
     (every PC in the inventory: the lab's bill for the job, the fairest
     comparison) and energy_per_pc_j.
  4. Every PC's whole energy.jsonl goes to results/lab/energy/<pc>.jsonl,
     so analyse_more.py also puts joules on E1's runs (and on any earlier
     run the samples cover): the profile runs E16 right after E1 and E1b.
"""
import os
import time

from campaign import energy

SYSTEMS = ["pipedpeer", "ray", "dask"]


def idle_power(ctx, hosts, seconds):
    for b in list(ctx.backends.values()):
        if b.kind in ("ray", "dask"):
            b.deactivate()
    t0 = time.time_ns()
    time.sleep(seconds)
    t1 = time.time_ns()
    f = energy.measure(ctx, hosts, t0, t1)
    dt = (t1 - t0) / 1e9
    power = {pc: (round(j / dt, 2) if j is not None else None) for pc, j in f["energy_per_pc_j"].items()}
    rid = ctx.recorder.new_run_id()
    ctx.recorder.write(dict(f, run_id=rid, workload="E16-idle", system="idle", n=len(hosts), repeat=0,
                            phase="idle-power", hosts=[h.id for h in hosts], start_ns=t0, end_ns=t1,
                            power_w=power, result_ok=True))
    ctx.log("idle power (W): %s" % power)


def main(ctx):
    hosts = ctx.inv.all
    status = energy.ensure(ctx, hosts)
    idle_power(ctx, hosts, float(os.environ.get("PL_ENERGY_IDLE_S", "10")))
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    ns = [n for n in ctx.ns([1, 16]) if n > 1] or [len(ctx.inv.workers)]
    reps = ctx.repeats(1)

    def run(b, wl_, env, logdir, n):
        res = b.run(wl_, env, logdir, n)
        res.update(energy.measure(ctx, b.hosts_for(n), res.get("start_ns"), res.get("end_ns"), all_hosts=hosts))
        return res

    extra = {"energy_sampler": status}
    # No plain warm-up: nothing in a plain run is cold after E1, and its
    # 60 s would be the largest part of E16's time.
    for r in range(1, reps + 1):
        ctx.one_run(wl, "plain", 1, r, run=run, extra=extra)
    chosen = [s for s in SYSTEMS if ctx.enabled(s)]
    for n in ns:
        for s in ctx.rng.sample(chosen, len(chosen)):
            ctx.one_run(wl, s, n, 0, warmup=True, run=run, extra=extra)
        for r in range(1, reps + 1):
            for s in ctx.rng.sample(chosen, len(chosen)):
                ctx.one_run(wl, s, n, r, run=run, extra=extra)
    energy.archive(ctx, hosts, os.path.join(ctx.out_dir, "energy"))
