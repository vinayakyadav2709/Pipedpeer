"""The run receipt: counters and decisions, written at exit."""
import atexit
import os
import threading

from pipedpeer_shim.core import (
    _log,
)


# What interception actually did, as opposed to what it was supposed to do.
# Every primitive here can fall back to local work on failure, which makes the
# results identical whether the cluster did all of the work or none of it —
# that is precisely how Pool.map shipped for months distributing nothing.
# Success was silent and failure was a log line nobody read, so there was no
# signal to assert on. These counters are that signal: PIPEDPEER_RECEIPT names
# a file to write them to at exit, and the tests fail on a receipt that shows
# no remote work.
#
# remote_items counts items a daemon reported running on ANOTHER machine.
# dispatched_items counts everything handed to the cluster layer, which is a
# weaker claim: a daemon with no eligible peers runs the chunk itself, and
# calling that distributed work would be the same comfortable half-truth this
# receipt exists to prevent. When the two differ, the gap is work that went
# out to a socket and came straight back.
# shipped_pickled counts items whose kernel could only travel by value -
# a lambda, a closure, a method. Those used to be the whole of
# "unshippable", so without a counter of its own the fix would show up only
# as that number falling, which is indistinguishable from the workload
# changing shape. It is a subset of dispatched_items, not a peer of it.
_STATS = {"remote_items": 0, "local_items": 0, "dispatched_items": 0,
          "remote_failures": 0, "unshippable": 0, "shipped_pickled": 0,
          "parts": [], "by_where": {}, "decisions": [], "decisions_dropped": 0,
          "mapped_items": 0, "remote_used": 0}
# mapped_items counts items handed to an intercepted map, once each.
# remote_used counts those whose kept result came from another machine. The
# two counters above it count work done, and with local cores racing slow
# remote chunks an item can be computed twice - so they can sum to more than
# was mapped, and only these two say what the run's results are made of.
_STATS_LOCK = threading.Lock()
# The pid that actually intercepted something, claimed on the first record.
# Pool workers and multiprocessing's own helper processes (the forkserver, on
# 3.14+) import this shim as well and reach exit with pristine counters; if
# any of them wrote, it would erase the real receipt. Only the process that
# did the work owns the file.
_RECEIPT_OWNER = [None]


def _claim_receipt():
    if _RECEIPT_OWNER[0] is None:
        _RECEIPT_OWNER[0] = os.getpid()


def _record(kind, items, ok, error="", ms=0.0, receipt=None):
    """Record the outcome of one offload attempt.

    receipt is the daemon's own account of where the parts ran; without one
    (an older daemon) the items are counted as dispatched but not as remote,
    because we have no evidence either way and guessing in our own favour is
    how this went unnoticed the first time."""
    elsewhere = 0
    here = 0
    where_n = {}
    by_peer = {}
    for p in (receipt or {}).get("parts", []):
        w = str(p.get("where", "")) or "local"
        n = int(p.get("items", 0))
        where_n[w] = where_n.get(w, 0) + n
        if w.startswith("peer:"):
            elsewhere += n
            by_peer[w[5:]] = by_peer.get(w[5:], 0) + n
        else:
            here += n
    with _STATS_LOCK:
        _claim_receipt()
        if ok:
            _STATS["dispatched_items"] += items
            _STATS["remote_items"] += elsewhere
            _STATS["local_items"] += here
            for w, n in where_n.items():
                _STATS["by_where"][w] = _STATS["by_where"].get(w, 0) + n
        elif error == "unshippable":
            _STATS["unshippable"] += items
        else:
            _STATS["remote_failures"] += 1
        entry = {"kind": kind, "items": items, "ok": ok,
                 "error": error, "ms": round(ms, 1)}
        if ok:
            entry["ran_elsewhere"] = elsewhere
            entry["ran_on_origin"] = here
            if receipt:
                entry["via"] = receipt.get("node", "")
            if by_peer:
                entry["peers"] = by_peer
        _STATS["parts"].append(entry)


# Every time a cost model weighs shipping work against keeping it here, and
# why it decided as it did. The counters above say what happened; this says
# what the shim was thinking, which is the question when a run that should
# have spread out stayed home. Capped: a loop calling np.dot a million times
# must not grow the receipt without bound.
_MAX_DECISIONS = 100
_WHY = {"v": ""}


def _decide(op, shipped, why, nbytes=0):
    with _STATS_LOCK:
        _claim_receipt()
        if len(_STATS["decisions"]) >= _MAX_DECISIONS:
            _STATS["decisions_dropped"] += 1
            return
        _STATS["decisions"].append({
            "op": op, "shipped": bool(shipped), "why": why,
            "mb": round(nbytes / 1e6, 1)})


def _add_stat(key, n):
    with _STATS_LOCK:
        _claim_receipt()
        _STATS[key] += n


def _record_local(items):
    with _STATS_LOCK:
        _claim_receipt()
        _STATS["local_items"] += items
        _STATS["by_where"]["local"] = _STATS["by_where"].get("local", 0) + items


@atexit.register
def _write_receipt():
    # The daemon reads this file and returns it on the job's done frame, so
    # the receipt reaches history, "job --receipt" and /ui without a stderr line.
    path = os.environ.get("PIPEDPEER_RECEIPT")
    if not path or _RECEIPT_OWNER[0] != os.getpid():
        return
    import json
    with _STATS_LOCK:
        body = json.dumps(_STATS)
    try:
        with open(path, "w") as f:
            f.write(body)
    except Exception as e:
        _log("could not write receipt file %s: %r" % (path, e))
