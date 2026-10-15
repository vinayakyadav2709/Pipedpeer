"""Pacing, cancellation and in-flight chunk plumbing for remote pool work."""
import sys
import threading
import time

from pipedpeer_shim import core
from pipedpeer_shim.core import (
    _SUBMITTER,
    _log,
)
from pipedpeer_shim.stats import (
    _RELAYED_STEPS, _add_stat,
)
from pipedpeer_shim.transfer import (
    _daemon_open,
)


# ---- pacing and cancellation of remote chunks (research R5/R8) ----

_PACING = {"t": -1e9, "v": None}
_PACING_TTL = 5.0
# Remote chunks in a row that won nothing, after which this process stops
# sending chunks out for _LOST_PAUSE seconds (F20: on a fast origin local won
# all 300 races and every remote copy was wasted peer time). A win resets it.
_LOST_LIMIT = 3
_LOST_PAUSE = 60.0
_LOST = {"n": 0, "until": 0.0}
_LOST_LOCK = threading.Lock()


def _remote_paused():
    return time.monotonic() < _LOST["until"]


def _race_won():
    with _LOST_LOCK:
        _LOST["n"] = 0


def _race_lost(secs):
    """A remote chunk won nothing: count the peer time it wasted, and pause
    remote dispatch after _LOST_LIMIT such chunks in a row."""
    _add_stat("races_lost", 1)
    _add_stat("wasted_peer_s", round(secs, 3))
    with _LOST_LOCK:
        _LOST["n"] += 1
        pause = _LOST["n"] >= _LOST_LIMIT
        if pause:
            _LOST["n"] = 0
            _LOST["until"] = time.monotonic() + _LOST_PAUSE
    if pause:
        _log("this machine won the last %d races against the cluster; keeping "
             "pool work here for %.0f s" % (_LOST_LIMIT, _LOST_PAUSE))


def _cluster_pacing():
    """How the daemon wants a map paced, from /v1/pool/stats?store=:
    inflight (chunks to keep in flight: the sum over peers of each node's
    depth), part_s (target seconds of one part), cores (the peers' cores) and
    peers. Cached a few seconds; without an answer one peer's worth (2 in
    flight, half a second a part) is assumed, said in a log line."""
    now = time.monotonic()
    if _PACING["v"] is not None and now - _PACING["t"] < _PACING_TTL:
        return _PACING["v"]
    v = {"inflight": 2, "part_s": 0.5, "cores": 0, "peers": 0}
    if core._URL:
        import json
        import urllib.parse
        import urllib.request
        try:
            req = urllib.request.Request(
                core._URL + "/v1/pool/stats?store=" + urllib.parse.quote(core._STORE, safe=""),
                headers={"X-Pipedpeer-Submitter": _SUBMITTER})
            with _daemon_open(req, 5) as resp:
                got = json.loads(resp.read())
            if "inflight" in got:
                v = {"inflight": max(1, int(got["inflight"])),
                     "part_s": max(0.25, int(got.get("part_ms", 500)) / 1000.0),
                     "cores": int(got.get("cores", 0)),
                     "peers": int(got.get("peers", 0))}
            else:
                _log("the daemon sent no pacing (an older build); keeping %d chunks in flight"
                     % v["inflight"])
        except Exception as e:
            _log("could not ask the daemon how to pace this map (%s); keeping %d chunks in flight"
                 % (e, v["inflight"]))
    _PACING["t"], _PACING["v"] = now, v
    return v


def _fac2_size(remaining, pullers, floor):
    """Items in the next chunk (FAC2 factoring with an overhead floor): each
    round hands out half of what remains, split over every puller, so the
    first chunks amortise the start-up and the last ones are small and the
    tail short. Never under floor (a part too short to pay for its round
    trip), never over a puller's fair share of what is left."""
    if remaining <= 0:
        return 0
    pullers = max(1, pullers)
    size = max(floor, -(-remaining // (2 * pullers)))
    return max(1, min(size, -(-remaining // pullers), remaining))


def _chunk_floor(per_item_cost, width, pacing):
    """The smallest worthwhile chunk: enough items to keep the receiving
    cores busy for one target part (pacing part_s), and at least the local
    pool's width."""
    lanes = max(width, pacing.get("cores") or 0)
    if per_item_cost <= 0:
        return max(width, 64)
    return max(width, 1, round(pacing["part_s"] * lanes / per_item_cost))


def _fn_fp(payload):
    """The function's fingerprint (C22): sha256 over the store path, the
    function's source or pickle and its name, 16 hex. With the node id it
    keys the daemon's speed stats kept across runs."""
    fp = payload.get("fp")
    if fp is None:
        import hashlib
        body = payload.get("src") or ""
        if payload.get("pickled"):
            body = hashlib.sha256(payload["pickled"]).hexdigest()
        fp = hashlib.sha256(("%s\0%s\0%s" % (core._STORE, body, payload.get("name", ""))
                             ).encode()).hexdigest()[:16]
        payload["fp"] = fp
    return fp


def _relay_steps(receipt):
    """Put the daemons' own steps from a chunk receipt (queue at a node,
    compute there) into this job's run record as they are: each names the
    machine that ran it, a peer's node id for a remote part. A step without
    one is a peer's part as far as the receipt says (where "peer:<addr>")."""
    for p in (receipt or {}).get("parts", []):
        where = str(p.get("where", ""))
        for st in p.get("steps") or []:
            try:
                machine = (st.get("machine") or st.get("node")
                           or (where[5:] if where.startswith("peer:") else ""))
                s = {"machine": str(machine),
                     "node": str(st.get("node", "")), "chunk": int(st.get("chunk", -1)),
                     "step": str(st["step"]), "start_ns": int(st["start_ns"]),
                     "dur_ns": int(st["dur_ns"])}
                if st.get("bytes"):
                    s["bytes"] = int(st["bytes"])
            except (KeyError, TypeError, ValueError) as e:
                _log("skipped an unreadable step in a chunk receipt (%s): %r" % (e, st))
                continue
            _RELAYED_STEPS.append(s)


# The _Flight the current deliver thread's request belongs to.
_TLS = threading.local()


class _Flight:
    """One remote chunk's request, so the race can cut it off once it lost.

    Closing the socket is the cancel: the daemon reads the requester going
    away as its request context ending, drops its peer forwards, and each
    peer kills the worker running the part (see the daemon's poolcancel
    tests)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.conns = []
        self.cancelled = False

    def add(self, conn):
        with self.lock:
            self.conns.append(conn)
            cut = self.cancelled
        if cut:
            _shut(conn)

    def cancel(self):
        with self.lock:
            if self.cancelled:
                return
            self.cancelled = True
            conns = list(self.conns)
        for c in conns:
            _shut(c)


def _shut(conn):
    import socket
    try:
        if conn.sock is not None:
            conn.sock.shutdown(socket.SHUT_RDWR)
    except OSError as e:
        _log("could not close a losing chunk's request: %s" % e)


_STOP_GRACE = 2.0
# The globals-frame key a by-value callable travels under; the daemon's
# worker scripts (daemonapi/poolwire.go) pop the same name.
_FUNC_KEY = "__pipedpeer_func__"


def _stop_pool(pool, grace=_STOP_GRACE):
    """multiprocessing.Pool.terminate() with a deadline.

    terminate() sends each worker one SIGTERM and then joins it with no
    timeout, so a busy worker that survives the signal (user code or a
    library that installs its own SIGTERM handler, or one that ignores it)
    hung the job's exit forever. terminate runs on a helper thread; workers
    still alive after grace seconds get SIGKILL, which unblocks its join."""
    try:
        t = threading.Thread(target=pool.terminate, name="pipedpeer-pool-stop", daemon=True)
        t.start()
    except RuntimeError as e:     # interpreter finalising: no new threads
        _log("pool exit: no thread to bound terminate() (%s); terminating inline" % e)
        pool.terminate()
        return
    t.join(grace)
    if not t.is_alive():
        return
    stuck = [p for p in list(getattr(pool, "_pool", None) or [])
             if hasattr(p, "kill") and p.is_alive()]
    _log("pool exit: %d worker(s) still running %.0fs after SIGTERM; killing them"
         % (len(stuck), grace))
    for p in stuck:
        try:
            p.kill()
        except OSError as e:
            _log("pool exit: could not kill worker %s: %s" % (p.pid, e))
    t.join(grace)
    if t.is_alive():
        _log("pool exit: terminate() still blocked after SIGKILL; leaving it behind")


def _flight_opener(flight):
    """A proxy-less opener whose connections join flight (so a cancel can
    close them) and that tells the daemon this interpreter's version
    (X-Pipedpeer-Python: a peer with another Python cannot load by-value
    bytecode)."""
    import http.client
    import urllib.request

    class _Conn(http.client.HTTPConnection):
        def connect(self):
            super().connect()
            if flight is not None:
                flight.add(self)

    class _Handler(urllib.request.HTTPHandler):
        def http_open(self, req):
            return self.do_open(_Conn, req)

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _Handler())
    opener.addheaders = [("X-Pipedpeer-Python", "%d.%d" % sys.version_info[:2])]
    return opener
