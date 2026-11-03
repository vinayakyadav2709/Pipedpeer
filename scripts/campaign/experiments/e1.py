"""E1/E2: strong scaling and the baselines (plan section 3).

W1 and W2 x {plain (N=1), pipedpeer, ray, dask} x N = 1, 2, 4, 8, 16, 32;
W3 at N = 1, 4, 16, 32. Five repeats after one warm-up per (system, N).
Speedup = median wall of plain at N=1 / median at N (analyse.py).
"""
SYSTEMS = ["plain", "pipedpeer", "ray", "dask"]


def main(ctx):
    reps = ctx.repeats(5)
    if ctx.args.workloads:
        ctx.scaling(ctx.args.workloads, SYSTEMS, ctx.ns([1, 2, 4, 8, 16, 32]), reps)
        return
    ctx.scaling(["W1", "W2"], SYSTEMS, ctx.ns([1, 2, 4, 8, 16, 32]), reps)
    ctx.scaling(["W3"], SYSTEMS, ctx.ns([1, 4, 16, 32]), reps)
