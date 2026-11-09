#!/usr/bin/env python3
"""E5 Ray arm: the same environment as e5_env.py, shipped by Ray's runtime_env.

One task is pinned to every alive node (NodeAffinitySchedulingStrategy). Each
task runs inside a runtime_env that pip-installs the packages from
~/pipedpeer-lab/requirements.txt (plus extra packages named with --with, the
"update"), imports them, and reports the time its body started. That start
time is the node's "ready" time in E5: environment built, task running.

Prints one line per node, then a summary:
    E5-RAY node=<ip> start_ns=<time.time_ns() on that node>
    E5-OK nodes=<n>

usage: e5_ray.py [--nodes N] [--with pkg[,pkg]]   (RAY_ADDRESS selects the cluster)
"""
import argparse
import os
import sys
import time


def requirements(path):
    out = []
    with open(os.path.expanduser(path), encoding="utf-8") as fh:
        for line in fh:
            line = line.split("#", 1)[0].strip()
            if line:
                out.append(line)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=0, help="expected node count (0 = every alive node)")
    ap.add_argument("--with", dest="extra", default="", help="extra pip packages, comma separated")
    ap.add_argument("--requirements", default="~/pipedpeer-lab/requirements.txt")
    args = ap.parse_args()

    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    pip = requirements(args.requirements) + [p for p in args.extra.split(",") if p]
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), ignore_reinit_error=True)
    nodes = [n for n in ray.nodes() if n.get("Alive")]
    if args.nodes and len(nodes) != args.nodes:
        print(f"e5_ray: {len(nodes)} alive nodes, expected {args.nodes}", file=sys.stderr)
        return 1

    extras = [p for p in args.extra.split(",") if p]

    @ray.remote(num_cpus=0)
    def probe(extra=()):
        import joblib  # noqa: F401
        import numpy  # noqa: F401
        import pandas  # noqa: F401
        import scipy  # noqa: F401
        import sklearn  # noqa: F401
        for m in extra:
            __import__(m)

        def work(i):
            acc = 0.0
            for k in range(1, 2000):
                acc += (i + k) ** 0.5
            return acc

        work(0)
        return ray.util.get_node_ip_address(), time.time_ns()

    refs = [
        probe.options(
            runtime_env={"pip": pip},
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=n["NodeID"], soft=False),
        ).remote(extras)
        for n in nodes
    ]
    failed = 0
    for n, ref in zip(nodes, refs):
        try:
            ip, t = ray.get(ref)
        except Exception as exc:  # one PC's failed env must not lose the others' times
            failed += 1
            print(f"e5_ray: no ready time for {n.get('NodeManagerAddress')}: {exc}", file=sys.stderr)
            continue
        print(f"E5-RAY node={ip} start_ns={t}", flush=True)
    print(f"E5-OK nodes={len(nodes) - failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
