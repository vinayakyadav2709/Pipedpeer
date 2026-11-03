"""E4: uneven machines. N=4 with cores capped 8/4/2/1, W1, five repeats:
PipedPeer's measured split vs PipedPeer's headcount split (the even split
8c9284c replaced; a daemon started with PIPEDPEER_POOL_SPLIT=headcount) vs
Ray vs Dask.

Caps are CPU affinity (taskset, or CPUAffinity= for a daemon run as a
systemd user service): see lib/campaign/systems.py for why not a root
AllowedCPUs scope. PipedPeer's 4 PCs are pc00..pc03 (pc00 capped to 8 and
submitting); Ray's and Dask's are the workers pc01..pc04, pc00 uncapped and
idle.
"""
CAPS = [8, 4, 2, 1]
SYSTEMS = ["pipedpeer", "pipedpeer-headcount", "ray", "dask"]


def main(ctx):
    for n in ctx.ns([4]):
        caps = CAPS[:n] if not ctx.rehearsal else [2, 1, 1, 1][:n]
        ctx.scaling(ctx.wl_ids(["W1"]), SYSTEMS, [n], ctx.repeats(5), caps=caps)
