"""Throughput over time, for E12 (elastic join) and E13 (churn).

A run here starts the job detached on pc00, does timed actions while it runs
(start daemons or workers, kill one), and afterwards reads which PC finished
how many items when, from each system's own records:

  pipedpeer  the shim's receipt (PIPEDPEER_RECEIPT): its "steps" hold every
             compute step of the run, with the machine that ran it (a node
             id, mapped to a PC through each PC's own /health), start_ns and
             dur_ns. A build with commit "shim: count the items each compute
             step finished" also puts "items" on each step; with an older
             build the items are estimated from busy time (W1's items all
             cost the same), and the line says timeline_source
             "receipt-steps-estimated".
  dask       a scheduler plugin (experiments/scripts/timeline_probe.py),
             put in before the job starts: tasks finished per second per
             worker, tasks taken back from a worker that left, and every
             worker that joins or leaves. One task is one item (client.map).
  ray        ray.timeline() after the job: every PoolActor.run_batch with its
             start, end and node IP. A batch holds ceil(items / (4 x actors))
             items (ray.util.multiprocessing.Pool's own chunk size).

Each source becomes a list of chunks {pc, t0, t1, items} (seconds from the
job's start on pc00's clock), binned per second per PC with each chunk's
items spread evenly over the interval it ran (a Dask task is a point). The
runner runs on pc00 on the lab day, so the events' times (time.time_ns() in
the runner) and the job's start_ns are on one clock; in the rehearsal the
containers share the host's clock.
"""
import base64
import contextlib
import json
import math
import os
import shlex
import time

from . import systems

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.normpath(os.path.join(HERE, "..", "..", "experiments", "scripts", "timeline_probe.py"))
REMOTE_PROBE = systems.STATE + "/timeline_probe.py"
JOB_LOG = systems.STATE + "/logs/timeline-job.log"
BIN_S = 1


@contextlib.contextmanager
def sized(ctx, wl, size):
    """ctx.one_run at this size (it reads the size from --size overrides)."""
    had = wl.id in ctx.size_override
    old = ctx.size_override.get(wl.id)
    ctx.size_override[wl.id] = dict(size)
    try:
        yield
    finally:
        if had:
            ctx.size_override[wl.id] = old
        else:
            del ctx.size_override[wl.id]


def put_probe(ctx):
    with open(PROBE, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    rc, out = ctx.remote.sh(ctx.inv.head.id, "mkdir -p %s && printf %%s %s | base64 -d > %s" % (
        systems.STATE + "/logs", data, REMOTE_PROBE), timeout=60)
    if rc != 0:
        raise systems.StepError("could not stage timeline_probe.py: %s" % out[-200:], host=ctx.inv.head.id)


def probe(ctx, *args, timeout=180):
    """Run timeline_probe.py on pc00; returns its PLTL json, or None."""
    cmd = "%s/bin/python3 %s %s" % (ctx.env_dir, REMOTE_PROBE, " ".join(shlex.quote(str(a)) for a in args))
    rc, out = ctx.remote.sh(ctx.inv.head.id, cmd, timeout=timeout)
    for line in reversed(out.splitlines()):
        if line.startswith("PLTL "):
            return json.loads(line[5:])
    ctx.log("timeline probe %s gave nothing (rc %d): %s" % (args[0], rc, out.strip()[-300:]))
    return None


def node_map(ctx, hosts):
    """{node id: pc id}, each PC's daemon asked for its own id."""
    out = {}
    for h in hosts:
        rc, text = ctx.remote.sh(h.id, "%s/health" % systems.API, timeout=30)
        try:
            out[json.loads(text.strip().splitlines()[-1])["node_id"]] = h.id
        except (ValueError, KeyError, IndexError):
            ctx.log("no node id from %s's daemon (it may be stopped)" % h.id)
    return out


def ip_map(ctx):
    return {h.ip: h.id for h in ctx.inv.all}


def _addr_pc(addr, ips):
    host = str(addr or "").split("://")[-1].rsplit(":", 1)[0]
    return ips.get(host, host or "?")


# --- chunks from each system's records ----------------------------------------

def pipedpeer_chunks(receipt, nodes, start_ns, items_total, head="pc00", wall_s=0.0):
    """(chunks, source) from the receipt's compute steps. A map the shim
    kept on pc00 (no peer when it started) records no steps: its local items
    become one chunk on pc00 over the whole run."""
    steps = [s for s in (receipt or {}).get("steps") or [] if s.get("step") == "compute" and s.get("dur_ns", 0) > 0]
    if not steps:
        r = receipt or {}
        if r.get("local_items") and not r.get("dispatched_items"):
            return [{"pc": head, "t0": 0.0, "t1": round(wall_s, 3), "items": r["local_items"]}], "receipt-local-only"
        return [], "receipt-steps-missing"
    exact = all("items" in s for s in steps)
    rate = 0.0
    if not exact:
        busy = sum(s["dur_ns"] for s in steps) / 1e9
        rate = items_total / busy if busy else 0.0
    chunks = []
    for s in steps:
        m = s.get("machine") or s.get("node") or ""
        t0 = (s["start_ns"] - start_ns) / 1e9
        t1 = t0 + s["dur_ns"] / 1e9
        items = s["items"] if exact else rate * s["dur_ns"] / 1e9
        chunks.append({"pc": nodes.get(m) or nodes.get(s.get("node")) or m, "t0": round(t0, 3),
                       "t1": round(t1, 3), "items": items})
    return chunks, "receipt-steps" if exact else "receipt-steps-estimated"


def dask_chunks(dump, ips, start_ns):
    chunks = []
    for k, n in ((dump or {}).get("done") or {}).items():
        sec, _, addr = k.partition(" ")
        t = int(sec) - start_ns / 1e9
        chunks.append({"pc": _addr_pc(addr, ips), "t0": round(t, 3), "t1": round(t, 3), "items": n})
    return chunks, "dask-scheduler-plugin"


def ray_batch(items, actors):
    """ray.util.multiprocessing.Pool._calculate_chunksize."""
    size, extra = divmod(items, max(1, actors) * 4)
    return max(1, size + (1 if extra else 0))


def ray_chunks(dump, ips, start_ns, items_total, actors):
    per = ray_batch(items_total, actors)
    chunks = []
    for t0_ms, t1_ms, ip, _tid in (dump or {}).get("tasks") or []:
        chunks.append({"pc": ips.get(ip, ip), "t0": round(t0_ms / 1e3 - start_ns / 1e9, 3),
                       "t1": round(t1_ms / 1e3 - start_ns / 1e9, 3), "items": per})
    return chunks, "ray-timeline"


def bin_chunks(chunks, wall_s, bin_s=BIN_S):
    """{pc: [items per bin]} over [0, wall_s], each chunk's items spread
    evenly over [t0, t1]; a chunk outside the run is clipped into it."""
    nb = max(1, int(math.ceil(max(wall_s, max((c["t1"] for c in chunks), default=0)) / bin_s)))
    per = {}
    for c in chunks:
        row = per.setdefault(c["pc"], [0.0] * nb)
        t0, t1 = max(0.0, c["t0"]), max(0.0, c["t1"])
        if t1 - t0 < 1e-9:
            row[min(nb - 1, int(t0 / bin_s))] += c["items"]
            continue
        b = int(t0 / bin_s)
        while b < nb and b * bin_s < t1:
            lo, hi = max(t0, b * bin_s), min(t1, (b + 1) * bin_s)
            row[b] += c["items"] * (hi - lo) / (t1 - t0)
            b += 1
    return {pc: [round(x / bin_s, 2) for x in row] for pc, row in sorted(per.items())}


# --- a detached job with actions while it runs ----------------------------------

def job_command(ctx, b, wl, env):
    """The command the backend's run() would time, for a detached start."""
    ws = ctx.stage(ctx.inv.head, wl, b.kind)
    args = " ".join(shlex.quote(a) for a in wl.args)
    if b.kind == "pipedpeer":
        es = " ".join("-e %s" % shlex.quote("%s=%s" % kv) for kv in sorted(env.items()))
        return ws, "cd %s && rm -f receipt.json && pipedpeer run -e PIPEDPEER_RECEIPT=receipt.json %s %s %s" % (
            ws, es, shlex.quote(wl.script.rsplit("/", 1)[-1]), args)
    script = wl.script_for(b.kind).rsplit("/", 1)[-1]
    envs = " ".join("%s=%s" % (k, shlex.quote(v)) for k, v in sorted(env.items()))
    addr = ("RAY_ADDRESS=%s:%d" % (ctx.inv.head.ip, systems.RAY_PORT) if b.kind == "ray"
            else "DASK_SCHEDULER=tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
    return ws, "cd %s && env %s %s %s/bin/python3 %s %s" % (ws, addr, envs, ctx.env_dir, shlex.quote(script), args)


def kill_job(ctx, b, wl):
    if b.kind == "pipedpeer":
        cmd = "pkill -INT -f '^([^ ]*/)?pipedpeer run'; sleep 5; pkill -9 -f '^([^ ]*/)?pipedpeer run'; true"
    else:
        # The python process only: its command line starts with the
        # interpreter, the shells around it start with sh or env.
        name = wl.script_for(b.kind).rsplit("/", 1)[-1]
        pat = "^[^ ]*python[0-9.]*( -[^ ]*)* %s" % name.replace(".", "[.]")
        cmd = "pkill -f '%s'; sleep 3; pkill -9 -f '%s'; true" % (pat, pat)
    ctx.remote.sh(ctx.inv.head.id, cmd, timeout=60)


class Action:
    """One timed action: fn() runs at at_s after the job starts and returns a
    dict for the event; always=True runs it even after the job ended (a
    restart must put the cluster back)."""

    def __init__(self, at_s, kind, fn, always=False):
        self.at_s, self.kind, self.fn, self.always = at_s, kind, fn, always


def run_with_events(ctx, b, wl, env, logdir, actions, timeout, poll=1.0):
    """Start the job, run each action when due, wait for the end. Returns
    (result dict as Backend.run gives, events). actions may be a list or a
    callable(elapsed_s, events) -> new Actions, for plans that depend on what
    happened (the next kill picks a PC that is up)."""
    head = ctx.inv.head.id
    ws, cmd = job_command(ctx, b, wl, env)
    ctx.remote.sh(head, "mkdir -p %s/logs && rm -f %s" % (systems.STATE, JOB_LOG), timeout=30)
    plan = sorted(actions if isinstance(actions, list) else [], key=lambda a: a.at_s)
    more = actions if callable(actions) else None
    ctx.remote.start_job(head, JOB_LOG, cmd)
    t0 = time.time()
    events, finished, timed_out = [], False, False
    while True:
        now = time.time() - t0
        if more:
            plan = sorted(plan + list(more(now, events)), key=lambda a: a.at_s)
        while plan and plan[0].at_s <= now:
            a = plan.pop(0)
            events.append(_do(ctx, a, t0))
            now = time.time() - t0
        rc, tail = ctx.remote.sh(head, "tail -c 400 %s 2>/dev/null" % JOB_LOG, timeout=60)
        if "__PL_TIMES" in tail:
            finished = True
            break
        if now > timeout:
            timed_out = True
            ctx.log("job still running after %d s; stopping it" % timeout)
            kill_job(ctx, b, wl)
            break
        time.sleep(poll)
    for a in plan:
        if a.always:
            events.append(_do(ctx, a, t0, late=True))
    t = ctx.remote.wait_job(head, JOB_LOG, timeout=120)
    res = {"rc": t.rc, "out": t.out, "start_ns": t.start_ns, "end_ns": t.end_ns}
    if timed_out or not finished:
        res["rc"] = t.rc or 124
        res["out"] += "\n[runner] stopped after %d s" % timeout
    if b.kind == "pipedpeer":
        res.update(b.collect(ws, logdir))
    return res, events


def _do(ctx, a, t0, late=False):
    s = time.time_ns()
    try:
        info = a.fn() or {}
    except systems.StepError as e:
        info = {"error": str(e)[:300]}
    e = {"kind": a.kind, "t_ns": s, "done_ns": time.time_ns(), "planned_s": round(a.at_s, 2)}
    if late:
        e["after_job"] = True
    e.update(info)
    ctx.log("event %s at %.1f s: %s" % (a.kind, (s / 1e9) - t0, json.dumps(info)[:200]))
    return e


# --- what the run line gets ---------------------------------------------------

def collect(ctx, b, res, nodes, items_total, actors, logdir, dask_dump=None):
    """Chunks for this run, from the system's records; writes chunks.json in
    logdir and returns the line's timeline fields."""
    start = res.get("start_ns") or 0
    wall = ((res.get("end_ns") or start) - start) / 1e9
    if b.kind == "pipedpeer":
        chunks, src = pipedpeer_chunks(res.get("receipt"), nodes, start, items_total, ctx.inv.head.id, wall)
    elif b.kind == "dask":
        chunks, src = dask_chunks(dask_dump, ip_map(ctx), start)
    else:
        dump = probe(ctx, "ray-dump", "%s:%d" % (ctx.inv.head.ip, systems.RAY_PORT), start // 1_000_000 - 5000)
        chunks, src = ray_chunks(dump, ip_map(ctx), start, items_total, actors)
    path = os.path.join(logdir, "chunks.json")
    with open(path, "w") as f:
        json.dump({"source": src, "start_ns": start, "chunks": chunks}, f)
    counted = sum(c["items"] for c in chunks)
    return {"timeline": {"bin_s": BIN_S, "t0_ns": start, "per_pc": bin_chunks(chunks, wall)},
            "timeline_source": src, "chunks_path": os.path.relpath(path, ctx.out_dir),
            "items_total": items_total, "items_counted": round(counted, 1)}


def rerun_items(b, res, items_total, dask_dump=None, counted=None):
    """Items computed more than once, as far as the system's records show
    (rerun_source says which records)."""
    if b.kind == "pipedpeer":
        r = res.get("receipt") or {}
        steps = [s for s in r.get("steps") or [] if s.get("step") == "compute"]
        mapped = r.get("mapped_items") or items_total
        if steps and all("items" in s for s in steps):
            return max(0, sum(s["items"] for s in steps) - mapped), "receipt-steps"
        failed = sum(p.get("items", 0) for p in r.get("parts") or []
                     if not p.get("ok") and p.get("error") != "unshippable")
        dup = (r.get("local_items") or 0) + (r.get("remote_items") or 0) - mapped
        return max(0, dup) + failed, "receipt-counters"
    if b.kind == "dask":
        # Taken back while running, plus finished results that went with a
        # killed worker's memory and were computed again (counted twice).
        d = dask_dump or {}
        again = max(0, round((counted or 0) - items_total))
        return (d.get("lost") or 0) + (d.get("erred") or 0) + again, "dask-scheduler-plugin"
    return max(0, round((counted or 0) - items_total)), "ray-timeline-batches"
