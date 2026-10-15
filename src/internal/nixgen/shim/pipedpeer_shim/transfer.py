"""Spill cost model, bandwidth probe, compression and _pool_send."""
import os
import threading
import time

from pipedpeer_shim import core
from pipedpeer_shim.core import (
    _OFFLOAD, _SUBMITTER, _TOKEN, _log, _offload_warn, _spill_min,
)
from pipedpeer_shim.stats import (
    _WHY, _decide,
)


# ---- distributed pandas: cost model, hash-shuffle groupby/merge, OOC ----
# Always on, and safe to be: every intercept gates on the latency cost
# model (_should_spill): a shuffle only fires when transfer + remote estimate
# beats the single-node estimate, so interception is never slower than plain
# pandas (D2). Shuffles bucket rows by hash(key) % K (K = peers+1, bucket 0
# stays local) and ship one bucket per no_split item, so every key's rows land
# complete on one node and the per-node agg/merge is exact — no combiners, any
# agg spec works.
_IN_MERGE = False
_BW_CACHE = {"t": 0.0, "bw": None}
# Floor of an svd/eig offload's wall-clock limit (tests lower it).
_LINALG_MIN_DEADLINE = 30.0
_BW_TTL = 300.0

# The daemon always listens on loopback (http://127.0.0.1:<selfPort>), so its
# requests must never be routed through an http(s)_proxy / no_proxy env proxy
# (GH Actions runners and many sandboxes set these, which would otherwise
# hijack or refuse loopback traffic and make the shim silently fall back to
# local). Build a proxy-less opener and reuse it everywhere.
_DaemonOpener = None
def _daemon_open(req, timeout, opener=None):
    """Send req to the loopback daemon, bypassing any environment HTTP proxy.

    The shared secret is attached here rather than at each call site: there
    are half a dozen of those and a new one that forgot would 401, fall back
    to local work, and look like a slow job rather than a misconfiguration."""
    if _TOKEN and not req.has_header("X-pipedpeer-token"):
        req.add_header("X-Pipedpeer-Token", _TOKEN)
    global _DaemonOpener
    import urllib.error
    import urllib.request
    if _DaemonOpener is None:
        _DaemonOpener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        return (opener or _DaemonOpener).open(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        raise RuntimeError("daemon HTTP %d: %s" % (e.code, e.read())) from e


def _should_spill(nbytes, flops_per_byte, op="pandas"):
    """_should_spill_why, with its decision recorded in the receipt when the
    work was big enough to be considered at all."""
    _WHY["v"] = ""
    r = _should_spill_why(nbytes, flops_per_byte)
    if _WHY["v"]:
        _decide(op, r, _WHY["v"], nbytes)
    return r


def _should_spill_why(nbytes, flops_per_byte):
    """Latency cost model: spill nbytes of work (flops_per_byte per byte) when
    the estimated transfer + remote compute beats local single-node time.
    est_local = nbytes*flops_per_byte/1e9 (~1e9 flops/s effective), est_remote
    = est_local/K*1.3 (parallel speedup, 30% overhead)."""
    if not (core._URL and core._ENABLED):
        return False
    K = int(core._NUM_SHARDS)
    if K < 2:
        _offload_warn("distributed work", "no other machine is in the cluster")
    if K < 2 or nbytes < _spill_min():
        return False
    if core._FORCE:
        _WHY["v"] = "forced (PIPEDPEER_DISTRIBUTE=force)"
        return True
    if flops_per_byte < 8 and nbytes <= 512 * 1024 * 1024:
        _WHY["v"] = "too little compute per byte to pay for moving %.0f MB" % (nbytes / 1e6)
        return False
    bw = _measure_bandwidth()
    if not bw:
        _WHY["v"] = "could not measure the link to the cluster"
        return False
    est_transfer = nbytes / bw
    est_local = nbytes * flops_per_byte / 1e9
    est_remote = est_local / K * 1.3
    _WHY["v"] = ("here ~%.2fs, there ~%.2fs transfer + %.2fs compute on %d nodes"
                 % (est_local, est_transfer, est_remote, K))
    return est_local > est_transfer + est_remote


def _measure_bandwidth():
    """Effective bytes/sec to the cluster, cached 300s. Probes through the
    same frames path real work uses (the base64-in-JSON path measured ~30MB/s
    and the cost model then refused every spill; frames ships raw pickle).
    64MB of blobs so the measurement is throughput-bound, not latency-bound.
    None on failure means stay local."""
    now = time.monotonic()
    if now - _BW_CACHE["t"] < _BW_TTL:
        return _BW_CACHE["bw"]
    import pickle
    try:
        blobs = [os.urandom(8 * 1024 * 1024) for _ in range(8)]
        items = [pickle.dumps(b) for b in blobs]
        hdr = {
            "func_src": "def run(x):\n    return len(x)\n",
            "items_frames": len(items),
        }
        t0 = time.monotonic()
        _pool_send(hdr, None, items, 60)
        bw = sum(len(b) for b in blobs) / max(time.monotonic() - t0, 1e-6)
    except Exception:
        bw = None
    _BW_CACHE["t"] = now
    _BW_CACHE["bw"] = bw
    return bw


def _maybe_compress(items, header):
    """Compress item frames when it pays for itself.

    Whether it does depends entirely on the payload. A pandas frame of
    strings or categoricals shrinks by most of its size; a dense float array
    is already incompressible and every byte of CPU spent on it is waste. So
    try, keep the result only if it is meaningfully smaller, and record the
    choice in the header rather than guessing again on the other side.

    The daemon relays these bytes without looking inside them, and carries
    the items_zlib flag across every header it rebuilds when it splits
    or forwards the work, so the choice reaches whichever worker runs it.
    """
    if not items:
        return items
    raw_total = sum(len(i) for i in items)
    if raw_total < 1 << 20:
        return items                  # too small for the CPU to be worth it
    import zlib
    try:
        packed = [zlib.compress(i, 1) for i in items]
    except Exception:
        return items
    if sum(len(p) for p in packed) > raw_total * 0.9:
        return items                  # not worth the decompression on the far side
    header["items_zlib"] = True
    return packed


def _pool_send(header, globals_pickle, items, timeout, out_header=None, opener=None):
    """POST a frames pool/map request: small JSON header line + optional
    globals frame + length-prefixed raw pickle item frames. Returns the raw
    pickle blobs of the results (frames response). Bulk data never touches
    base64 or a giant JSON parse, which is what made the old encoding ~30MB/s."""
    import json
    import struct
    import urllib.request
    items = _maybe_compress(items, header)
    body = json.dumps(header).encode() + b"\n"
    if globals_pickle is not None:
        body += struct.pack(">I", len(globals_pickle)) + globals_pickle
    for it in items:
        body += struct.pack(">I", len(it)) + it
    req = urllib.request.Request(core._URL + "/v1/pool/map", data=body,
                                 headers={"Content-Type": "application/vnd.pipedpeer.frames",
                                          "X-Pipedpeer-Store": core._STORE,
                                          "X-Pipedpeer-Submitter": _SUBMITTER,
                                          "X-Pipedpeer-Offload": _OFFLOAD})
    try:
        with _daemon_open(req, timeout, opener) as resp:
            data = resp.read()
    except Exception as e:
        # Every caller falls back to running the work here.
        _offload_warn("work the cluster refused", "the cluster could not take it (%s)" % str(e)[:200])
        raise
    nl = data.find(b"\n")
    hdr = json.loads(data[:nl])
    here = sum(int(p.get("items", 0)) for p in (hdr.get("receipt") or {}).get("parts", [])
               if not str(p.get("where", "")).startswith("peer:"))
    if here:
        _offload_warn("%d item(s)" % here, "no other machine could take them "
                      "(none healthy with this environment, or each one failed)")
    if out_header is not None:
        out_header.update(hdr)
    rest = data[nl + 1:]
    out = []
    for _ in range(hdr.get("results_frames", 0)):
        n = struct.unpack(">I", rest[:4])[0]
        out.append(rest[4:4 + n])
        rest = rest[4 + n:]
    return out


def _pool_send_within(header, globals_pickle, items, deadline):
    """_pool_send with a wall-clock limit, raising TimeoutError past it.

    urllib's timeout is per socket operation, so a peer that is slow but not
    silent, or a daemon that tries peer after peer, keeps it waiting for ever:
    02_numpy_heavy.py's svd sat 15 minutes while the daemon gave four
    container peers 300 s each. On expiry the connection is shut, which the
    daemon reads as the requester going away and drops its peer forwards."""
    import socket
    import http.client
    import urllib.request
    conns = []

    class _Tracked(urllib.request.HTTPHandler):
        def http_open(self, req):
            def make(host, **kw):
                c = http.client.HTTPConnection(host, **kw)
                conns.append(c)
                return c
            return self.do_open(make, req)

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _Tracked())
    box = {}

    def send():
        try:
            box["r"] = _pool_send(header, globals_pickle, items, deadline, opener=opener)
        except BaseException as e:
            box["e"] = e

    t = threading.Thread(target=send, daemon=True)
    t.start()
    t.join(deadline)
    if t.is_alive():
        for c in conns:
            try:
                if c.sock is not None:
                    c.sock.shutdown(socket.SHUT_RDWR)
            except OSError as e:
                _log("could not close the timed-out cluster request: %s" % e)
        raise TimeoutError("no answer from the cluster in %.0f s" % deadline)
    if "e" in box:
        raise box["e"]
    return box["r"]




def _pool_required(globals_pickle, items, func_payload=""):
    """Honest per-node working-set estimate for a noSplit/fanned-out chunk:
    one node holds the globals plus the single largest item (parts are spread
    across peers, so the request total overstates any one node). 2x covers
    parse/output expansion. Keeps the daemon's admission control meaningful
    instead of 503-ing large-but-spread reads.

    func_payload is the serialised callable, when it is one that carries data
    rather than pointing at it. A named function ships as source and weighs
    nothing; a lambda closing over an array ships that array inside itself,
    and every node receiving a part holds a copy of it."""
    if items:
        return 2 * (len(globals_pickle) + len(func_payload) + max(len(i) for i in items))
    return 0
