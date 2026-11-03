"""E10 (if time): 4-6 classmates new to both tools each do the E3 task for
PipedPeer and Dask, the order alternated (p1 PipedPeer first, p2 Dask
first, ...). Consent first; participants are p1..p6, no names anywhere."""
import sys

from campaign import guided
from e3 import STEPS

LOCAL_ONLY = True


def main(args, out_dir, out=sys.stdout, inp=sys.stdin):
    rec = guided.recorder(out_dir, "e10", args.rehearsal)
    n = guided.ask_int("How many participants (1-6)? ", out=out, inp=inp)
    for p in range(1, min(n, 6) + 1):
        pid = "p%d" % p
        consent = guided.ask("%s: read them the consent line (timed, no names, may stop any time). "
                             "Consent given? [y/n] " % pid, out=out, inp=inp)
        if consent.lower() not in ("y", "yes"):
            out.write("%s skipped: no consent; nothing recorded\n" % pid)
            continue
        order = ["pipedpeer", "dask"] if p % 2 else ["dask", "pipedpeer"]
        for i, s in enumerate(order, 1):
            guided.attempt(rec, s, STEPS[s], pid, i, out=out, inp=inp)
    return 0
