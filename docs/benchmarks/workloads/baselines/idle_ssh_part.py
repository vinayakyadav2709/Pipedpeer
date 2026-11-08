# Port of docs/benchmarks/workloads/pool_idle.py (idle) for the plain-ssh baseline.
# One part of the item list, run on one lab PC by idle_ssh.sh (GNU parallel
# --sshlogin); the Pool itself stays on the PC it runs on.
# Distributed: the items of this part (each PC runs multiprocessing.Pool on its
#   slice); pc00's idle_ssh.sh joins the parts and prints the result line.
# Local: nothing else.
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Prints one JSON line on stdout:
#   {"part":i,"items":k,"seconds":s,"out":[p(i) for i in i::n]}
# The out list round-trips through json, so the join recomputes sum(out) in
# item order and gets the original's checksum bit for bit. This part keeps the
# original's always-on serial check on its own slice.
# Result line (printed by the join, byte-for-byte as the original):
#   POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f
import json
import os
import sys
import time
from multiprocessing import Pool


def work(i):
    acc = 0.0
    for k in range(1, 1500):
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
        sys.stderr.write("usage: idle_ssh_part.py --part i --parts n (0 <= i < n)\n")
        raise SystemExit(2)
    n = int(os.environ.get("BENCH_ITEMS", "16"))
    items = list(range(part, n, parts))
    t0 = time.perf_counter()
    with Pool(int(os.environ.get("BENCH_PROCS", "8"))) as p:
        out = p.map(work, items)
    dt = time.perf_counter() - t0
    assert out == [work(i) for i in items], "results differ from serial"
    print(json.dumps({"part": part, "items": len(out), "seconds": dt, "out": out}))


if __name__ == "__main__":
    main(sys.argv[1:])
