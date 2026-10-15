#!/usr/bin/env python3
# Pool request body build: c7e2c7f's body += loop vs _frames_body's join, 100 MB.
import json, os, struct, sys, time
def old(header, g, items):  # c7e2c7f shim_pool.go:1153-1157
    body = json.dumps(header).encode() + b"\n"
    if g is not None:
        body += struct.pack(">I", len(g)) + g
    for it in items:
        body += struct.pack(">I", len(it)) + it
    return body
def new(header, g, items):  # transfer.py:204-213
    parts = [json.dumps(header).encode(), b"\n"]
    if g is not None:
        parts += [struct.pack(">I", len(g)), g]
    for it in items:
        parts += [struct.pack(">I", len(it)), it]
    return b"".join(parts)
TOTAL = 100 << 20
for n in (12, 100, 1000):
    items = [os.urandom(TOTAL // n) for _ in range(n)]
    for f in (old, new):
        ts = []
        for _ in range(3):
            t = time.perf_counter(); b = f({"n": n}, None, items); ts.append(time.perf_counter() - t)
        assert len(b) > TOTAL
        print("%5d items x %7.1f KB  %s  median %.3f s" % (n, TOTAL / n / 1024, f.__name__, sorted(ts)[1]))
    assert old({"n": n}, b"g", items) == new({"n": n}, b"g", items)
