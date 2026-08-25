"""CPU-bound Pool.map. The unit of work is deliberately large enough that
per-item dispatch cost is not what is being measured."""
import os, sys, time
from multiprocessing import Pool


def work(i):
    # ~40 ms of arithmetic per item, no allocation of note.
    acc = 0.0
    for k in range(1, 60000):
        acc += (i + k) ** 0.5
    return acc


if __name__ == "__main__":
    n = int(os.environ.get("BENCH_ITEMS", "48"))
    t0 = time.perf_counter()
    with Pool(int(os.environ.get("BENCH_PROCS", "8"))) as p:
        out = p.map(work, range(n))
    dt = time.perf_counter() - t0
    assert len(out) == n
    # Correctness: the same computation, serially.
    if os.environ.get("BENCH_VERIFY") == "1":
        assert out == [work(i) for i in range(n)], "results differ from serial"
    print("POOLMAP-OK items=%d seconds=%.4f checksum=%.6f" % (n, dt, sum(out)))
