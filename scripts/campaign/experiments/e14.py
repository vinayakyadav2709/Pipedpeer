"""E14: curl to cluster. The first distributed job of the day: one pipedpeer
run of W1 at its strong size on every PC in the inventory (N = all),
right after the join (lab-day.sh's prep.firstjob step runs it before the
Nix env, calibration or any experiment). The size is workloads.json's
provisional one, since calibration comes later; --size overrides it.

The run line has phase "first-job" and first_work {pc: ns}: the start of the
first compute step each PC ran, from the shim's receipt (pc00's clock: the
origin daemon times its parts). The same go into results/lab/timeline.jsonl
as event first_work, beside the setup, install and join events of
day/timeline.sh. analyse_headline.py turns them into the staircase.
"""
import json
import os

from campaign import receipts, timeline


def main(ctx):
    n = ctx.ns([len(ctx.inv.all)])[-1]
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    b = ctx.backend("pipedpeer")
    path = os.path.join(ctx.out_dir, "timeline.jsonl")

    def run(b_, wl_, env, logdir, n_):
        res = b.run(wl, env, logdir, n)
        nodes = timeline.node_map(ctx, b.hosts_for(n))
        first = {}
        for s in receipts.full(res).get("steps") or []:
            if s.get("step") != "compute":
                continue
            pc = nodes.get(s.get("machine")) or nodes.get(s.get("node"))
            if pc and (pc not in first or s["start_ns"] < first[pc]):
                first[pc] = s["start_ns"]
        res["first_work"] = first
        res["pcs_with_work"] = len(first)
        with open(path, "a") as f:
            for pc, t in sorted(first.items()):
                f.write(json.dumps({"pc": pc, "event": "first_work", "t_ns": t, "clock": "pc00",
                                    "machine_id": ctx.inv.host(pc).vars.get("machine_id", ""),
                                    "source": "e14 receipt"}) + "\n")
            for ev, t in (("first_job_start", res.get("start_ns")), ("first_job_end", res.get("end_ns"))):
                f.write(json.dumps({"pc": ctx.inv.head.id, "event": ev, "t_ns": t, "clock": "pc00",
                                    "machine_id": "", "source": "e14 n=%d" % n}) + "\n")
        return res

    ctx.one_run(wl, "pipedpeer", n, 1, reference="first", run=run, extra={"phase": "first-job"})
