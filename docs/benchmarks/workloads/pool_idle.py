"""A Pool.map the shim's cost model must decline (N7: idle interception).

The gate (nixgen/shim.go, _ClusterPool._run) runs a head of
max(4, min(procs, n // 2)) items locally, prices each item at the head's wall
time, and stays local when tail_items * cost < PIPEDPEER_POOL_MIN_WORK (0.5 s).
Sixteen items leave a tail of eight, so the map stays local whenever the head
takes under 62 ms. Each item is ~0.1 ms of arithmetic, so it declines with a
wide margin while still going through the measurement path. bench.py checks the
receipt: any dispatched item fails the run.
"""
import os, time
from multiprocessing import Pool


def work(i):
    acc = 0.0
    for k in range(1, 1500):
        acc += (i + k) ** 0.5
    return acc


if __name__ == "__main__":
    n = int(os.environ.get("BENCH_ITEMS", "16"))
    t0 = time.perf_counter()
    with Pool(int(os.environ.get("BENCH_PROCS", "8"))) as p:
        out = p.map(work, range(n))
    dt = time.perf_counter() - t0
    assert out == [work(i) for i in range(n)], "results differ from serial"
    print("POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f" % (n, dt, sum(out)))
