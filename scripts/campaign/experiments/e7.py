"""E7: interception overhead, method as RESULTS.md R3: the whole job of
docs/benchmarks/workloads/trivial.py with --remote, the shim on vs
PIPEDPEER_SHIM=0, five repeats after a warm-up each. pc00 submits, the job
runs on the one other PC (N=2: pc00 and pc01). The difference of medians is
the cost of interception on a job it has nothing to intercept.
"""
from campaign import systems
from campaign.workloads import Workload

TRIVIAL = Workload({
    "id": "E7-trivial",
    "script": "docs/benchmarks/workloads/trivial.py",
    "strong": {"PL_E7": "1"},  # a size key for the result cache; the script ignores it
    "result_regex": r"TRIVIAL-OK",
})


def main(ctx):
    ctx.workloads[TRIVIAL.id] = TRIVIAL
    ctx.add_backend("pipedpeer-shim", systems.PipedPeer(ctx, name="pipedpeer-shim", run_flags=["--remote"]))
    ctx.add_backend("pipedpeer-noshim", systems.PipedPeer(
        ctx, name="pipedpeer-noshim", run_flags=["--remote"], run_env={"PIPEDPEER_SHIM": "0"}))
    ctx.scaling([TRIVIAL.id], ["pipedpeer-shim", "pipedpeer-noshim"], ctx.ns([2])[-1:],
                ctx.repeats(5), reference="first")
