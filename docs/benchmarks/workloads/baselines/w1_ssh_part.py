#!/usr/bin/env python3
# Port of docs/benchmarks/workloads/pool_map.py (W1) for the plain-ssh baseline.
# One part of the item list, run on one lab PC by w1_ssh.sh (GNU parallel
# --sshlogin); the Pool itself stays on the PC it runs on.
# Distributed: the items of this part (each PC runs multiprocessing.Pool on its
#   slice); pc00's w1_ssh.sh joins the parts and prints the result line.
# Local: nothing else.
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Prints one JSON line on stdout:
#   {"part":i,"items":k,"seconds":s,"out":[p(i) for i in i::n]}
# The out list round-trips through json (shortest-repr floats), so the join can
# recompute `sum(out)` in item order and get the original's checksum bit for bit
# (a sum of part sums could round differently).
# Result line (printed by the join, byte-for-byte as the original):
#   POOLMAP-OK items=%d seconds=%.4f checksum=%.6f
import json
import os
import sys
import time
from multiprocessing import Pool


def work(i):
    acc = 0.0
    for k in range(1, 60000):
        acc += (i + k) ** 0.5
    return acc


def main(argv):
    part = parts = None
    for i, a in enumerate(argv):
        if a == "--part":
            part = int(argv[i + 1])
        elif a == "--parts":
            parts = int(argv[i + 1])
    if part is None or parts is None or not 0 <= part < parts:
        sys.stderr.write("usage: w1_ssh_part.py --part i --parts n (0 <= i < n)\n")
        raise SystemExit(2)
    n = int(os.environ.get("BENCH_ITEMS", "48"))
    items = list(range(part, n, parts))
    t0 = time.perf_counter()
    with Pool(int(os.environ.get("BENCH_PROCS", "8"))) as p:
        out = p.map(work, items)
    dt = time.perf_counter() - t0
    # Correctness: the same computation, serially (as the original).
    if os.environ.get("BENCH_VERIFY") == "1":
        assert out == [work(i) for i in items], "results differ from serial"
    print(json.dumps({"part": part, "items": len(out), "seconds": dt, "out": out}))


if __name__ == "__main__":
    main(sys.argv[1:])
