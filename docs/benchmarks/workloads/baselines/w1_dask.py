# Port of docs/benchmarks/workloads/pool_map.py (W1) to Dask Distributed.
# Distributed: the Pool items, as dask tasks (client.map) on the cluster.
# Local: the checksum reduction and the result line.
# Follows: https://docs.dask.org/en/stable/scheduling.html (dask.distributed for
# Pool-shaped work; distributed's own multiprocessing docs recommend client.map).
# The distributed scheduler has no fixed pool size for map: every worker runs as
# many tasks as it has threads, so BENCH_PROCS is not read (a changed line).
# Original result line, byte-for-byte: POOLMAP-OK items=%d seconds=%.4f checksum=%.6f
import os
import time

from dask.distributed import Client


def work(i):
    acc = 0.0
    for k in range(1, 60000):
        acc += (i + k) ** 0.5
    return acc


if __name__ == "__main__":
    client = Client(os.environ["DASK_SCHEDULER"])
    n = int(os.environ.get("BENCH_ITEMS", "48"))
    t0 = time.perf_counter()
    out = client.gather(client.map(work, range(n)))
    dt = time.perf_counter() - t0
    assert len(out) == n
    # Correctness: the same computation, serially (as the original).
    if os.environ.get("BENCH_VERIFY") == "1":
        assert out == [work(i) for i in range(n)], "results differ from serial"
    print("POOLMAP-OK items=%d seconds=%.4f checksum=%.6f" % (n, dt, sum(out)))
