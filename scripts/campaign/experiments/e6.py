"""E6: faults. W1 at N=16 on pipedpeer, ray and dask: one run with no kill
(warm-up, then the reference wall), then three runs where one worker is
killed with SIGKILL at about 30% of the reference wall:

  pipedpeer  the daemon on the last of its N PCs (`pipedpeer __daemon__`;
             a daemon installed as a service may be restarted by systemd
             after 5 s, which the line records as restart_policy)
  ray        the raylet on the last worker (its worker processes go with it)
  dask       every Dask process on the last worker (nanny and workers)

result_ok says whether the answer was still right; kill_landed (and
interrupt_landed for Ctrl-C) whether the job was still running when the
signal went in, since a kill after the end is no fault; wall_s is compared with
the no-kill wall in analyse.py. Then PipedPeer's Ctrl-C, three times: SIGINT
to `pipedpeer run` on pc00 at ~30%, and the time until the run has exited and
no node in the cluster reports an active job (ctrlc_latency_s).
"""
import json
import os
import shlex
import time

from campaign import systems

KILL = {
    "pipedpeer": systems.proc_kill(systems.DAEMON, "KILL"),
    "ray": systems.proc_kill(systems.RAYLET, "KILL"),
    "dask": systems.kill_marked("KILL") + "; echo PLPROC marked",  # the worker's nanny and all its processes
}
JOB_LOG = "$HOME/.local/share/pipedpeer-lab/logs/e6-job.log"


def job_command(ctx, b, wl, env):
    """The same command the backend's run() would time, for a detached start."""
    ws = ctx.stage(ctx.inv.head, wl, b.kind)
    envs = " ".join("%s=%s" % (k, shlex.quote(v)) for k, v in sorted(env.items()))
    if b.kind == "pipedpeer":
        es = " ".join("-e %s" % shlex.quote("%s=%s" % kv) for kv in sorted(env.items()))
        return ws, "cd %s && rm -f receipt.json && pipedpeer run -e PIPEDPEER_RECEIPT=receipt.json %s %s" % (
            ws, es, wl.script.rsplit("/", 1)[-1])
    script = wl.script_for(b.kind).rsplit("/", 1)[-1]
    addr = ("RAY_ADDRESS=%s:%d" % (ctx.inv.head.ip, systems.RAY_PORT) if b.kind == "ray"
            else "DASK_SCHEDULER=tcp://%s:%d" % (ctx.inv.head.ip, systems.DASK_PORT))
    return ws, "cd %s && env %s %s %s/bin/python3 %s" % (ws, addr, envs, ctx.env_dir, script)


def kill_run(ctx, at_s, victim):
    """A run function for ctx.one_run: start the job detached, kill the
    victim at at_s, wait for the job."""
    def run(b, wl, env, logdir, n):
        ws, cmd = job_command(ctx, b, wl, env)
        t0 = time.time()  # before the start: Ansible's own second counts toward at_s
        ctx.remote.start_job(ctx.inv.head.id, JOB_LOG, cmd)
        time.sleep(max(0.0, at_s - (time.time() - t0)))
        k0 = time.time_ns()
        # landed: the job had not finished when the kill went in. A kill
        # after the end is no fault at all, and the line must say so.
        rc, out = ctx.remote.sh(ctx.inv.head.id, "grep -c __PL_TIMES %s || true" % JOB_LOG, timeout=60)
        landed = out.strip().splitlines()[-1:] == ["0"]
        _, kout = ctx.remote.sh(victim.id, KILL[b.kind] + "; true", timeout=60)
        killed = next((ln.split()[1] for ln in kout.splitlines() if ln.startswith("PLPROC ")), "0")
        landed = landed and killed != "0"
        ctx.log("killed %s %s on %s at %.1f s%s" % (killed, b.kind, victim.id, time.time() - t0,
                                                   "" if landed else " (did not land mid-run)"))
        t = ctx.remote.wait_job(ctx.inv.head.id, JOB_LOG, timeout=ctx.run_timeout)
        res = {"rc": t.rc, "out": t.out, "start_ns": t.start_ns, "end_ns": t.end_ns,
               "killed_pc": victim.id, "kill_ns": k0, "kill_at_s": round(at_s, 2),
               "kill_landed": landed, "killed_processes": killed}
        if killed == "0":  # raised after the job ended, so the next run starts clean
            raise systems.StepError("the kill on %s found no %s process: no fault was injected"
                                    % (victim.id, b.kind), host=victim.id)
        if b.kind == "pipedpeer":
            res.update(b.collect(ws, logdir))
            rc2, unit = ctx.remote.sh(victim.id, "test -f ${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/"
                                      "pipedpeer.service && echo service || echo none", timeout=30)
            res["restart_policy"] = "systemd on-failure" if "service" in unit else "none"
            ctx.daemons[victim.id] = None  # restart it before the next run
        else:
            b.up_n = None  # rebuild the cluster before the next run
        return res
    return run


def ctrl_c(ctx, b, wl, n, repeat, at_s):
    rec = ctx.recorder
    run_id = rec.new_run_id()
    logdir = rec.log_dir(run_id)
    line = {"run_id": run_id, "workload": wl.id, "system": "pipedpeer-ctrlc", "n": n, "repeat": repeat,
            "log_path": os.path.relpath(logdir, ctx.out_dir)}
    try:
        hosts = b.activate(n)
        size = ctx.size_for(wl, "strong", n)
        env = dict(ctx.system_env(wl, "pipedpeer", n, hosts), **size)
        ws, cmd = job_command(ctx, b, wl, env)
    except systems.StepError as e:
        line.update({"error": str(e), "failing_pc": e.host, "result_ok": False})
        return rec.write(line)
    line["hosts"] = [h.id for h in hosts]
    t0 = time.time()
    ctx.remote.start_job(ctx.inv.head.id, JOB_LOG, cmd)
    time.sleep(max(0.0, at_s - (time.time() - t0)))
    # On pc00, in one shell so the timing is on one clock: SIGINT, then poll
    # until `pipedpeer run` is gone and every PC's own /health reports no
    # active job or lease (each daemon asked directly: the peer table on pc00
    # is only as fresh as its last poll).
    tok = systems.API.split("; curl")[0]
    busy = " ".join(
        "$(curl -sf --max-time 2 -H \"X-Pipedpeer-Token: $tok\" http://%s:4242/health | "
        "grep -o '\"active_[a-z]*\":[0-9]*' | head -2 | cut -d: -f2)"
        % ("127.0.0.1" if h is ctx.inv.head else h.ip) for h in hosts)
    count = systems.proc_count(systems.CLI_RUN)
    script = ("%s; %s; s=$(date +%%s%%N); %s >/dev/null; "
              "while :; do r=$(%s | cut -d' ' -f2); a=0; "
              "for x in %s; do a=$((a + ${x:-0})); done; "
              "[ \"$r\" -eq 0 ] && [ \"$a\" -eq 0 ] && break; "
              "[ $(( ($(date +%%s%%N) - s) / 1000000000 )) -gt 300 ] && { echo PLTIMEOUT; break; }; "
              "sleep 0.1; done; e=$(date +%%s%%N); echo \"PLCTRLC $s $e\"") % (
                  tok, count, systems.proc_kill(systems.CLI_RUN, "INT"), count, busy)
    rc, out = ctx.remote.sh(ctx.inv.head.id, script, timeout=400)
    t = ctx.remote.wait_job(ctx.inv.head.id, JOB_LOG, timeout=120)
    with open(os.path.join(logdir, "output.log"), "w") as f:
        f.write(t.out + "\n== ctrl-c probe\n" + out)
    m = [ln.split() for ln in out.splitlines() if ln.startswith("PLCTRLC ")]
    if not m or "PLTIMEOUT" in out:
        line.update({"error": "work still running 300 s after Ctrl-C" if m else "ctrl-c probe failed: " + out[-200:],
                     "failing_pc": ctx.inv.head.id, "result_ok": False})
    else:
        s, e = int(m[0][1]), int(m[0][2])
        running = [ln.split()[1] for ln in out.splitlines() if ln.startswith("PLPROC ")]
        line["interrupt_landed"] = bool(running) and running[0] != "0"
        line.update({"start_ns": s, "end_ns": e, "ctrlc_latency_s": round((e - s) / 1e9, 3),
                     "interrupt_at_s": round(at_s, 2), "result_ok": True, "rc": t.rc})
    ctx.log("Ctrl-C %d: %s" % (repeat, line.get("ctrlc_latency_s", line.get("error"))))
    return rec.write(line)


def main(ctx):
    n = ctx.ns([16])[-1]
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    kills = ctx.repeats(3)
    chosen = [s for s in ["pipedpeer", "ray", "dask"] if ctx.enabled(s)]
    for system in ctx.rng.sample(chosen, len(chosen)):
        b = ctx.backend(system)
        victim = b.hosts_for(n)[-1]
        ctx.one_run(wl, system, n, 0, warmup=True)
        ref = ctx.one_run(wl, system, n, 1, extra={"fault": "none"})
        if ref.get("error") or not ref.get("wall_s"):
            ctx.log("no reference wall for %s; kills skipped" % system)
            continue
        at = 0.3 * ref["wall_s"]
        for k in range(1, kills + 1):
            ctx.one_run(wl, system, n, k, extra={"fault": "kill -9", "no_kill_wall_s": ref["wall_s"]},
                        run=kill_run(ctx, at, victim))
    b = ctx.backend("pipedpeer")
    ref = [json.loads(x) for x in open(ctx.recorder.path)
           if '"fault": "none"' in x and '"system": "pipedpeer"' in x]
    at = 0.3 * (ref[-1]["wall_s"] if ref and ref[-1].get("wall_s") else 30)
    for k in range(1, ctx.repeats(3) + 1):
        ctrl_c(ctx, b, wl, n, k, at)
