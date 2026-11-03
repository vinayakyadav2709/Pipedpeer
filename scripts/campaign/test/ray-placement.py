#!/usr/bin/env python3
"""Where Ray's actors and tasks ran, from the node collectors' process metrics:

  ray-placement.py <otel dir> [head pc id]

A Ray worker process renames itself ray::<actor or task>, and host.process
keeps every process whose name starts with "ray". This prints, per PC, the
ray:: names seen and how much CPU time they used, and exits 1 when the head
(pc00, whose Ray node has --num-cpus=0) ran any Pool actor or joblib task:
ray.util.multiprocessing.Pool makes zero-CPU actors unless the port asks for
one CPU each (docs/benchmarks/workloads/baselines/README.md).
"""
import collections
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "otel"))
import otelread  # noqa: E402

WORK = ("PoolActor", "ray::run_chunk", "ray::_run", "joblib")


def main(d, head="pc00"):
    cpu = collections.defaultdict(lambda: collections.defaultdict(dict))  # pc -> name -> pid -> max cpu s
    for p in otelread.points(d, "metrics"):
        if p.get("name") != "process.cpu.time":
            continue
        name = str(p.get("process.executable.name") or p.get("process.command") or "")
        if not name.startswith("ray::"):
            continue
        pid = p.get("process.pid")
        prev = cpu[p.get("lab.pc_id")][name].get(pid, 0)
        cpu[p.get("lab.pc_id")][name][pid] = max(prev, p.get("value") or 0)
    bad = 0
    for pc in sorted(cpu):
        for name, pids in sorted(cpu[pc].items()):
            secs = sum(pids.values())
            print("%-6s %-28s %3d process(es) %9.1f cpu s" % (pc, name, len(pids), secs))
            if pc == head and any(w in name for w in WORK) and secs > 1.0:
                bad += 1
    if not cpu:
        print("no ray:: processes in %s" % d)
        return 2
    print("head %s ran Ray work: %s" % (head, "YES" if bad else "no"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
