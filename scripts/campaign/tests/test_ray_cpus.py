"""Every Ray port asks for one CPU per Pool actor or joblib task.

pc00 is Ray's head with --num-cpus=0, so it takes no work only when the
work asks for a CPU. ray.util.multiprocessing.Pool and the joblib backend
default to zero-CPU actors, which land anywhere, pc00 included: W1-W4's
ports were fixed for this, and E19's and E20's new ports ran on pc00 in
the 2026-11-04 rehearsal (test/ray-placement.py caught it)."""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))

WORK = re.compile(r'parallel_backend\(\s*"ray"[^)]*\)|\bPool\((?:[^()]|\([^()]*(?:\([^()]*\))*[^()]*\))*\)')


def ray_sources():
    d = os.path.join(REPO, "docs/benchmarks/workloads/baselines")
    for f in sorted(os.listdir(d)):
        if f.endswith("_ray.py"):
            yield f, open(os.path.join(d, f)).read()
    from campaign import runner
    e19 = runner.load_experiment("e19")
    for phase, offset, rich in e19.PHASES:
        yield "e19 %s" % phase, e19.source(phase, offset, rich, True)


def test_every_ray_pool_and_joblib_backend_asks_for_a_cpu():
    seen, bad = 0, []
    for name, src in ray_sources():
        for m in WORK.finditer(src):
            seen += 1
            if "num_cpus" not in m.group(0):
                bad.append("%s: %s" % (name, m.group(0)))
    assert seen >= 8, seen
    assert not bad, "zero-CPU Ray actors can run on pc00: %s" % bad
