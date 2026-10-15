"""Run-record steps (C24): every piece of shim work gets its own timed step,
never folded into the user script's time (that is how the O(n^2) _pool_send
bug hid). step() only appends a tuple: no I/O until the receipt is written,
when into() puts the steps under the receipt's additive key "steps".

Usage from a producer:
    t0 = time.perf_counter()
    blob = pickle.dumps(chunk)
    record.step("pickle", time.perf_counter() - t0, len(blob), chunk=i)
"""
import os
import time

NAMES = frozenset((
    "pickle", "compress", "send", "queue", "compute", "receive", "unpickle",
    "env_build", "env_send", "ddp_sync", "script"))

# list.append is atomic under the GIL, so producers on several threads need
# no lock here. Each entry: (name, seconds, nbytes, chunk, node, end_ns).
_STEPS = []
_now = time.time_ns


def step(name, seconds, nbytes=0, chunk=-1, node=""):
    """Record one finished step of the given length, ending now."""
    _STEPS.append((name, seconds, nbytes, chunk, node, _now()))


def steps(machine=None):
    """The recorded steps as C24 Step dicts. machine defaults to this job's
    node id (PIPEDPEER_NODE_ID, the key fingerprint the daemon sets)."""
    if machine is None:
        machine = os.environ.get("PIPEDPEER_NODE_ID", "")
    out = []
    for name, seconds, nbytes, chunk, node, end_ns in list(_STEPS):
        dur = int(seconds * 1e9)
        s = {"machine": machine, "node": node, "chunk": chunk, "step": name,
             "start_ns": end_ns - dur, "dur_ns": dur}
        if nbytes:
            s["bytes"] = int(nbytes)
        out.append(s)
    return out


def into(receipt):
    """Append the recorded steps to receipt["steps"] (the dict the shim
    writes as its receipt) and return it."""
    receipt.setdefault("steps", []).extend(steps())
    return receipt
