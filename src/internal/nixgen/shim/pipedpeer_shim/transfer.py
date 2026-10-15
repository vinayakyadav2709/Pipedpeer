"""Spill cost model, link speed, pickling helpers and _pool_send.

Compression, send-once-by-hash and link measurement live in the daemon's peer
link (poolwire.go): the loopback hop to the local daemon is never worth
compressing, and the daemon sees every real transfer."""
import os
import struct
import threading
import time

from pipedpeer_shim import core
from pipedpeer_shim.core import (
    _OFFLOAD, _SUBMITTER, _SUBMITTER_NODE, _TOKEN, _log, _offload_warn, _spill_min,
)
from pipedpeer_shim.stats import (
    _WHY, _decide,
)

try:
    from pipedpeer_shim import record as _rec
except ImportError:  # record.py sits beside sitecustomize.py until U03 moves it
    import record as _rec


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
# How long the daemon's measured link speed is trusted before asking again.
_LINK_TTL = 30.0
# Fallback probe when the daemon has no transfer samples yet (R9 a4: at most
# 8 MB; the old 64 MB probe was half a minute of a 2 MB/s link).
_PROBE_BYTES = 8 * 1024 * 1024

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
    """Effective bytes/sec to the cluster (the aggregate over peers). First
    the daemon's passive measurement of real transfers, /v1/pool/stats
    link_bytes_per_sec, trusted for _LINK_TTL; when it has no samples, one
    8 MB probe through the frames path, cached _BW_TTL. None on failure means
    stay local."""
    now = time.monotonic()
    if now - _BW_CACHE["t"] < _BW_TTL:
        return _BW_CACHE["bw"]
    bw = _daemon_link_speed()
    if bw:
        _BW_CACHE["t"] = now - (_BW_TTL - _LINK_TTL)
        _BW_CACHE["bw"] = bw
        return bw
    import pickle
    try:
        blobs = [os.urandom(_PROBE_BYTES // 2) for _ in range(2)]
        items = [pickle.dumps(b, protocol=5) for b in blobs]
        hdr = {
            "func_src": "def run(x):\n    return len(x)\n",
            "items_frames": len(items),
        }
        t0 = time.monotonic()
        _pool_send(hdr, None, items, 60)
        bw = sum(len(b) for b in blobs) / max(time.monotonic() - t0, 1e-6)
    except Exception as e:
        _log("bandwidth probe failed, keeping work here: %s" % str(e)[:200])
        bw = None
    _BW_CACHE["t"] = now
    _BW_CACHE["bw"] = bw
    return bw


def _daemon_link_speed():
    """link_bytes_per_sec from the local daemon, or None (an older daemon, or
    no transfer measured yet)."""
    import json
    import urllib.request
    try:
        req = urllib.request.Request(core._URL + "/v1/pool/stats")
        with _daemon_open(req, 5) as resp:
            v = json.loads(resp.read()).get("link_bytes_per_sec")
    except Exception as e:
        _log("no link speed from the daemon (%s); probing" % str(e)[:200])
        return None
    return float(v) if v else None


# An out-of-band pickle frame (R9 a3), read by the worker scripts in
# poolwire.go (frameCodec): magic, buffer count, pickle length, each buffer
# length (all >I), the pickle, then the buffers. Self-describing, so it never
# depends on a header flag surviving the daemon's header rebuilds.
_OOB_MAGIC = b"\x00PP5"


def _dumps(obj, chunk=-1):
    """Pickle one item for a frames request: protocol 5, numpy-style buffers
    out of band (one copy fewer than in-band, R9 a3). Plain protocol-5 bytes
    when nothing went out of band."""
    import pickle
    t0 = time.perf_counter()
    bufs = []
    p = pickle.dumps(obj, protocol=5, buffer_callback=bufs.append)
    if bufs:
        raws = [b.raw() for b in bufs]
        p = b"".join([_OOB_MAGIC + struct.pack(">II%dI" % len(raws), len(raws), len(p),
                                               *[r.nbytes for r in raws]), p] + raws)
    _rec.step("pickle", time.perf_counter() - t0, len(p), chunk=chunk)
    return p


def _loads(blob, chunk=-1):
    """Unpickle one result frame, plain or out of band. Buffers stay views
    over the reply, which _pool_send reads into a bytearray, so arrays come
    back writable."""
    import pickle
    t0 = time.perf_counter()
    if bytes(blob[:4]) == _OOB_MAGIC:
        mv = memoryview(blob)
        nbuf, plen = struct.unpack_from(">II", mv, 4)
        lens = struct.unpack_from(">%dI" % nbuf, mv, 12)
        off = 12 + 4 * nbuf
        p = mv[off:off + plen]
        off += plen
        bufs = []
        for n in lens:
            bufs.append(mv[off:off + n])
            off += n
        v = pickle.loads(p, buffers=bufs)
    else:
        v = pickle.loads(blob)
    _rec.step("unpickle", time.perf_counter() - t0, len(blob), chunk=chunk)
    return v


def _frames_body(header, globals_pickle, items):
    """The frames request body, built as a list and joined once: the old
    body += loop copied everything built so far per item (O(n^2) bytes)."""
    import json
    parts = [json.dumps(header).encode(), b"\n"]
    if globals_pickle is not None:
        parts += [struct.pack(">I", len(globals_pickle)), globals_pickle]
    for it in items:
        parts += [struct.pack(">I", len(it)), it]
    return b"".join(parts)


def _read_reply(resp):
    """The whole reply in a bytearray (writable, for out-of-band results),
    read straight into place when the length is known."""
    n = resp.length
    if n is None:
        return bytearray(resp.read())
    buf = bytearray(n)
    mv = memoryview(buf)
    got = 0
    while got < n:
        k = resp.readinto(mv[got:])
        if not k:
            raise RuntimeError("daemon reply cut short: %d of %d bytes" % (got, n))
        got += k
    return buf


def _pool_send(header, globals_pickle, items, timeout, out_header=None, opener=None):
    """POST a frames pool/map request: small JSON header line + optional
    globals frame + length-prefixed raw pickle item frames. Returns the raw
    pickle blobs of the results (frames response). Bulk data never touches
    base64 or a giant JSON parse, which is what made the old encoding ~30MB/s."""
    import json
    import urllib.request
    body = _frames_body(header, globals_pickle, items)
    req = urllib.request.Request(core._URL + "/v1/pool/map", data=body,
                                 headers={"Content-Type": "application/vnd.pipedpeer.frames",
                                          "X-Pipedpeer-Store": core._STORE,
                                          "X-Pipedpeer-Submitter": _SUBMITTER,
                                          "X-Pipedpeer-Submitter-Node": _SUBMITTER_NODE,
                                          "X-Pipedpeer-Offload": _OFFLOAD})
    t0 = time.perf_counter()
    try:
        with _daemon_open(req, timeout, opener) as resp:
            # send = upload plus the cluster's work, up to the reply headers.
            _rec.step("send", time.perf_counter() - t0, len(body))
            t0 = time.perf_counter()
            data = _read_reply(resp)
            _rec.step("receive", time.perf_counter() - t0, len(data))
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
    # Views at offsets: slicing off the rest per frame was O(n^2) as well.
    mv = memoryview(data)
    off = nl + 1
    out = []
    for _ in range(hdr.get("results_frames", 0)):
        n = struct.unpack_from(">I", mv, off)[0]
        out.append(mv[off + 4:off + 4 + n])
        off += 4 + n
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
