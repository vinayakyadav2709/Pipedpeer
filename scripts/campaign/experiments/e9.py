"""E9: across networks. The operator puts the driver laptop on a phone
hotspot; the laptop's daemon reaches the lab through the introducer (pc00)
and submits W1. Recorded: the path type of every node in `pipedpeer status`
on the laptop, the wall, the receipt, the bytes pc00 (the introducer) moved
during the run, and scripts/net-verify.sh's verdict.

The laptop is this machine (PL_E9_HOST=local, the default). The rehearsal
has no second network: it runs the same steps from the inventory's last PC
(PL_E9_HOST=<pc id>) over the lab bridge, and skips net-verify.sh, whose
introducer kill needs a real one.
"""
import base64
import json
import os
import re
import shlex
import subprocess
import time

from campaign import probes, remote as remote_mod
from campaign.workloads import extract

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))


def local_timed(cmd, timeout):
    s = time.time_ns()
    try:
        p = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=timeout)
        rc, out = p.returncode, p.stdout + p.stderr
    except subprocess.TimeoutExpired:
        rc, out = 124, "timed out"
    return remote_mod.Timed(rc, out, s, time.time_ns())


def main(ctx):
    where = os.environ.get("PL_E9_HOST") or (ctx.inv.all[-1].id if ctx.rehearsal else "local")
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    if where == "local":
        print("\nE9: put this laptop on your phone's hotspot now (Wi-Fi off the lab network,\n"
              "cable unplugged). Wait until `pipedpeer status` lists the lab PCs, then press Enter.")
    else:
        print("\nE9 (rehearsal): the 'laptop' is %s on the lab bridge. Press Enter." % where)
    try:
        input()
    except EOFError:
        pass

    def run(cmd, timeout=600):
        if where == "local":
            return local_timed(cmd, timeout)
        return ctx.remote.timed(where, cmd, timeout=timeout)

    rec = ctx.recorder
    run_id = rec.new_run_id()
    logdir = rec.log_dir(run_id)
    line = {"run_id": run_id, "workload": wl.id, "system": "pipedpeer", "n": len(ctx.inv.all),
            "repeat": 1, "laptop": where, "log_path": os.path.relpath(logdir, ctx.out_dir)}
    st = run("pipedpeer status", 60)
    with open(os.path.join(logdir, "status.txt"), "w") as f:
        f.write(st.out)
    line["paths"] = sorted(set(re.findall(r"\b(lan|internet|gossip|punched-in|relay)\b", st.out)))
    try:
        size = line["size"] = ctx.size_for(wl, "strong", 1)
    except Exception as e:  # uncalibrated size: still record the path check
        line.update({"error": str(e), "result_ok": False})
        return rec.write(line)
    d = "$HOME/.local/share/pipedpeer-lab/work/e9"
    with open(os.path.join(REPO, wl.script), "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    script = os.path.basename(wl.script)
    es = " ".join("-e %s" % shlex.quote("%s=%s" % kv) for kv in sorted(size.items()))
    cmd = ("rm -rf {d} && mkdir -p {d} && : > {d}/.pipedpeerignore && printf %s {b} | base64 -d > {d}/{s} && "
           "cd {d} && pipedpeer run -e PIPEDPEER_RECEIPT=receipt.json {es} {s}").format(d=d, b=b64, s=script, es=es)
    before = probes.netdev(ctx, [ctx.inv.head])
    t = run(cmd, ctx.run_timeout)
    after = probes.netdev(ctx, [ctx.inv.head])
    with open(os.path.join(logdir, "output.log"), "w") as f:
        f.write(t.out)
    rc_r = run("cat %s/receipt.json" % d, 30)
    try:
        line["receipt"] = json.loads(rc_r.out)
    except ValueError:
        pass
    dd = probes.delta(before, after).get(ctx.inv.head.id)
    result = extract(wl.result_regex, t.out)
    line.update({"start_ns": t.start_ns, "end_ns": t.end_ns, "rc": t.rc, "result": result,
                 "introducer_bytes": sum(dd) if dd else None})
    if t.rc != 0:
        line.update({"error": "exit %d" % t.rc, "failing_pc": where, "result_ok": False})
    else:
        ok, src = ctx.check(wl, size, result, "pipedpeer", "plain")
        line.update({"result_ok": ok, "result_ref": src})
    if where == "local" and not ctx.rehearsal:
        _, health = ctx.remote.sh(ctx.inv.head.id, "pipedpeer status | head -1", timeout=30)
        m = re.search(r"node-id=([0-9a-f]{6,})", health)
        if m:
            nv = local_timed("bash %s/scripts/net-verify.sh %s" % (REPO, m.group(1)[:8]), 900)
            with open(os.path.join(logdir, "net-verify.txt"), "w") as f:
                f.write(nv.out)
            line["net_verify_rc"] = nv.rc
    else:
        line["net_verify"] = "skipped: rehearsal has no second network"
    rec.write(line)
