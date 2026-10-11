# Estimate pi by throwing random darts at a square.
# Uses multiprocessing so it runs on all my cores.
import multiprocessing
import random
import time

BATCHES = 100
DARTS_PER_BATCH = 10_000_000


def throw_darts(seed):
    rng = random.Random(seed)
    inside = 0
    for _ in range(DARTS_PER_BATCH):
        x = rng.random()
        y = rng.random()
        if x * x + y * y <= 1.0:
            inside += 1
    return inside


if __name__ == "__main__":
    start = time.time()
    print("throwing", BATCHES * DARTS_PER_BATCH, "darts")

    pool = multiprocessing.Pool()
    hits = pool.map(throw_darts, range(BATCHES))
    pool.close()
    pool.join()

    pi = 4 * sum(hits) / (BATCHES * DARTS_PER_BATCH)
    print("pi is about", pi)
    print("TOTAL %.1fs" % (time.time() - start))
