# Port of docs/benchmarks/workloads/pool_idle.py (idle) to Dask Distributed.
# Distributed: the Pool items, as dask tasks; each item is ~0.1 ms of arithmetic.
# Local: the checksum reduction and the result line.
# Follows: https://docs.dask.org/en/stable/scheduling.html (client.map for
# Pool-shaped work). No fixed pool size for map: BENCH_PROCS is not read.
# Original result line, byte-for-byte: POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f
import os
import time

from dask.distributed import Client


def work(i):
    acc = 0.0
    for k in range(1, 1500):
        acc += (i + k) ** 0.5
    return acc


if __name__ == "__main__":
    client = Client(os.environ["DASK_SCHEDULER"])
    n = int(os.environ.get("BENCH_ITEMS", "16"))
    t0 = time.perf_counter()
    out = client.gather(client.map(work, range(n)))
    dt = time.perf_counter() - t0
    assert out == [work(i) for i in range(n)], "results differ from serial"
    print("POOLIDLE-OK items=%d seconds=%.4f checksum=%.6f" % (n, dt, sum(out)))
