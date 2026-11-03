"""E1b: weak scaling, W1: pipedpeer, ray, dask at N = 1, 4, 16, 32, five
repeats; the size is weak_per_pc x N. Plain runs at N=1 as well.

Each N has its own size, and a plain reference at N=32 would be 32 PCs' worth
of work on one, so result_ok compares against the first successful result at
that size (result_ref names the system it came from): the systems must agree
with each other, and at N=1 with plain.
"""
SYSTEMS = ["plain", "pipedpeer", "ray", "dask"]


def main(ctx):
    ctx.scaling(ctx.wl_ids(["W1"]), SYSTEMS, ctx.ns([1, 4, 16, 32]), ctx.repeats(5),
                mode="weak", reference="first")
