# Count the prime numbers below LIMIT, the slow way (trial division),
# split into ranges and run in parallel with concurrent.futures.
import time
from concurrent.futures import ProcessPoolExecutor

LIMIT = 16_000_000
STEP = 160_000


def is_prime(n):
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    d = 3
    while d * d <= n:
        if n % d == 0:
            return False
        d += 2
    return True


def count_primes(start):
    count = 0
    for n in range(start, min(start + STEP, LIMIT)):
        if is_prime(n):
            count += 1
    return count


if __name__ == "__main__":
    t = time.time()
    starts = list(range(0, LIMIT, STEP))
    print("checking", LIMIT, "numbers in", len(starts), "ranges")

    with ProcessPoolExecutor() as ex:
        counts = list(ex.map(count_primes, starts))

    print("primes below", LIMIT, ":", sum(counts))
    print("TOTAL %.1fs" % (time.time() - t))
