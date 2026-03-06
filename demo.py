#!/usr/bin/env python3
"""
Usage demo — np_d as a drop-in numpy replacement.
Shows execution time for matrix operations at various sizes.
"""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import np_d as np


def main():
    sizes = [64, 256, 512, 1024, 2048]

    print("=" * 50)
    print("  np_d — distributed numpy")
    print("=" * 50)

    try:
        np.connect()
        print("  connected to network\n")
    except Exception:
        print("  no coordinator, using local fallback\n")

    # matmul
    print("-" * 50)
    print(f"  {'size':>10s}   {'matmul':>12s}")
    print("-" * 50)

    for n in sizes:
        A = np.random.rand(n, n).astype(np.float32)
        B = np.random.rand(n, n).astype(np.float32)

        start = time.perf_counter()
        C = np.matmul(A, B)
        elapsed = (time.perf_counter() - start) * 1000

        print(f"  {n:>5d}x{n:<5d}   {elapsed:>10.2f} ms")

    print("-" * 50)

    # dot
    print(f"\n  dot:")
    n = 1024
    A = np.random.rand(n, n)
    B = np.random.rand(n, n)

    start = time.perf_counter()
    C = np.dot(A, B)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"    {n}x{n}: {elapsed:.2f} ms")

    # other numpy ops — just work
    print(f"\n  other ops (inherited from numpy):")

    start = time.perf_counter()
    np.linalg.inv(np.random.rand(512, 512))
    print(f"    linalg.inv(512x512): {(time.perf_counter() - start) * 1000:.2f} ms")

    start = time.perf_counter()
    np.fft.fft2(np.random.rand(512, 512))
    print(f"    fft2(512x512):       {(time.perf_counter() - start) * 1000:.2f} ms")

    start = time.perf_counter()
    np.sort(np.random.rand(1_000_000))
    print(f"    sort(1M):            {(time.perf_counter() - start) * 1000:.2f} ms")

    print()
    print("=" * 50)
    print("  done.")
    print("=" * 50)

    np.shutdown()


if __name__ == "__main__":
    main()
