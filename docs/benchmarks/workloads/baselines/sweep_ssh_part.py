#!/usr/bin/env python3
# One part of docs/benchmarks/workloads/sweep_map.py for the plain-ssh
# baseline. Run on one lab PC by sweep_ssh.sh through GNU parallel
# --sshlogin; the Pool stays on this PC.
# Distributed: the items of this part (each PC runs multiprocessing.Pool on
#   its strided slice); pc00's sweep_ssh.sh joins the parts and prints the
#   result line.
# Local: nothing else.
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Payload arms: sweep_ssh.sh writes one fixed-record file per part on pc00 and
# ships it with --transferfile; this part reads those records. It never
# regenerates a payload, because that would hide the transfer being measured.
# Prints one JSON line on stdout:
#   {"part":i,"items":k,"seconds":s,"out":[...]}
# The out list round-trips through json (shortest-repr floats), so the join can
# recompute sum(out) in item order and get sweep_map.py's checksum bit for bit.
import json
import os
import sys
import time
import zlib
from functools import partial
from multiprocessing import Pool


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"sweep_ssh_part.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"sweep_ssh_part.py: {name} is not an integer: {raw!r}")


_TASK_ITERS = 0


def work(item, iters=None):
    i, payload = item
    acc = 0.0
    for k in range(1, 1 + (_TASK_ITERS if iters is None else iters)):
        acc += (i + k) ** 0.5
    if payload:
        acc += zlib.crc32(payload)
    return acc


def part_indices(n, part, parts):
    return list(range(part, n, parts))


def read_payloads(path, count, record_bytes):
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as e:
        sys.exit(f"sweep_ssh_part.py: cannot read the shipped payload file {path}: {e}")
    want = count * record_bytes
    if len(data) != want:
        sys.exit(
            f"sweep_ssh_part.py: {path} has {len(data)} bytes, expected {want} "
            f"({count} records of {record_bytes})"
        )
    return [
        data[i * record_bytes : (i + 1) * record_bytes] for i in range(count)
    ]


def main(argv):
    global _TASK_ITERS
    part = parts = payload_file = None
    for i, a in enumerate(argv):
        if a == "--part":
            part = int(argv[i + 1])
        elif a == "--parts":
            parts = int(argv[i + 1])
        elif a == "--payload-file":
            payload_file = argv[i + 1]
    if part is None or parts is None or not 0 <= part < parts:
        sys.stderr.write("usage: sweep_ssh_part.py --part i --parts n [--payload-file f]\n")
        raise SystemExit(2)

    n = _env_int("BENCH_ITEMS")
    task_ms = _env_int("BENCH_TASK_MS")
    iters_per_ms = _env_int("BENCH_ITERS_PER_MS")
    procs = _env_int("BENCH_PROCS", required=False, default=0)
    payload_bytes = _env_int("BENCH_PAYLOAD_BYTES", required=False, default=0)
    _TASK_ITERS = task_ms * iters_per_ms

    indices = part_indices(n, part, parts)
    if payload_bytes > 0:
        if payload_file is None:
            sys.exit(f"sweep_ssh_part.py: part {part} needs --payload-file")
        payloads = read_payloads(payload_file, len(indices), payload_bytes)
    else:
        payloads = [b"" for _ in indices]
    items = list(zip(indices, payloads))

    t0 = time.perf_counter()
    with Pool(procs or None) as p:
        out = p.map(partial(work, iters=_TASK_ITERS), items)
    dt = time.perf_counter() - t0
    print(json.dumps({"part": part, "items": len(out), "seconds": dt, "out": out}))


if __name__ == "__main__":
    main(sys.argv[1:])
