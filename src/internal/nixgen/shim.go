package nixgen

// ShimSitecustomize is a sitecustomize.py auto-imported by Python before the
// user's first line (sitecustomize is always imported at interpreter startup).
// It patches the parallel primitives to route through the local pipedpeer
// daemon — no code changes required on the user's script.
//
// The design (see handoff.md §4/D1): interception only, no SDK. It patches
// multiprocessing.Pool, concurrent.futures.ProcessPoolExecutor and registers a
// joblib backend, leaving thread pools alone (shared memory).
const ShimSitecustomize = `# Auto-imported by Python at startup when on PYTHONPATH.
# Patches parallel primitives to route through the local pipedpeer daemon.
import atexit
import functools
import os
import site
import sys
import threading
import time
import weakref

# This file shadows the Nix-supplied sitecustomize.py (both are named
# sitecustomize and only the first on PYTHONPATH is imported). The Nix one is
# what adds NIX_PYTHONPATH — the python-with-packages env's site-packages, where
# numpy/torch/etc. live — so without this the shim would silently strip the
# env's packages and every import would fail. Do exactly what Nix's does.
if "NIX_PYTHONPATH" in os.environ:
    _nix_paths = os.environ.pop("NIX_PYTHONPATH", None)
    if _nix_paths:
        functools.reduce(lambda k, p: site.addsitedir(p, k), _nix_paths.split(":"), site._init_pathinfo())

_ENABLED = os.environ.get("PIPEDPEER_SHIM") == "1"
_URL = os.environ.get("PIPEDPEER_DAEMON_URL", "")
_STORE = os.environ.get("PIPEDPEER_STORE_PATH", "")
_NODE_ID = os.environ.get("PIPEDPEER_NODE_ID", "")
_NUM_SHARDS = os.environ.get("PIPEDPEER_NUM_SHARDS", "0")
# PIPEDPEER_DISTRIBUTE=force skips every cost model: any interceptable
# operation over the size floor ships to the cluster even when local would
# win. This deliberately breaks the never-slower invariant — it exists to
# demonstrate distribution, not to be fast. "auto" (default) keeps the
# cost models in charge.
_FORCE = os.environ.get("PIPEDPEER_DISTRIBUTE", "auto") == "force"


def _spill_min(default=32 * 1024 * 1024):
    """Size floor (bytes) below which work never ships. PIPEDPEER_SPILL_MIN
    overrides the 32 MB default; force mode drops it to 0 unless the env sets
    an explicit floor."""
    override = os.environ.get("PIPEDPEER_SPILL_MIN")
    if override:
        try:
            return float(override)
        except ValueError:
            pass
    return 0 if _FORCE else default
# Where the job was submitted from (host:port of the submitter's daemon).
# The executing node sinks this peer to the end of its spill order so an
# idle orchestrator never outranks real workers; empty when unset.
_SUBMITTER = os.environ.get("PIPEDPEER_SUBMITTER", "")


def _log(msg):
    if _ENABLED:
        sys.stderr.write("[pipedpeer] " + msg + "\n")


class _ClusterPool:
    """drop-in replacement for multiprocessing.Pool.

    Starts as a real local pool; the first chunks run on local cores, which is
    the measurement. Spills to the cluster only when measured per-item cost x
    remaining clearly exceeds the dispatch cost. Local cores never stop pulling,
    so a remote node is an additional consumer, never a subtracter.
    """

    def __init__(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        import multiprocessing.pool as _mp
        self._remote = _URL and _ENABLED and _NUM_SHARDS != "0"
        self._ctx = _mp.Pool(
            processes=processes or os.cpu_count(),
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
        )
        self._pending = 0
        self._measure_items = 4
        self._spilled = False
        atexit.register(self.close)

    # ---- the four Pool workhorses ----
    def apply(self, func, args=(), kwds=None):
        return self._ctx.apply(func, args=args, kwds=kwds)

    def apply_async(self, func, args=(), kwds=None, callback=None, error_callback=None):
        return self._ctx.apply_async(func, args=args, kwds=kwds, callback=callback, error_callback=error_callback)

    def map(self, func, iterable, chunksize=None):
        return self._run(func, list(iterable))

    def starmap(self, func, iterable, chunksize=None):
        return self._run(func, list(iterable), starmap=True)

    def imap(self, func, iterable, chunksize=None):
        return iter(self.map(func, iterable))

    def imap_unordered(self, func, iterable, chunksize=None):
        return iter(self.map(func, iterable))

    def _run(self, func, items, starmap=False):
        if not self._remote or len(items) <= self._measure_items:
            return self._local(func, items, starmap)
        # Measure the first few locally, then decide whether to spill the rest.
        head, tail = items[:self._measure_items], items[self._measure_items:]
        head_results = self._local(func, head, starmap)
        if not tail:
            return head_results
        cost = _measure_cost(func, head, starmap)
        if cost <= 0:
            return head_results + self._local(func, tail, starmap)
        return head_results + self._race(func, tail, starmap, cost)

    def _local(self, func, items, starmap):
        _log("local %d items" % len(items))
        if starmap:
            return self._ctx.starmap(func, items)
        return self._ctx.map(func, items)

    def _race(self, func, items, starmap, per_item_cost):
        # D2: local and remote each pull ~half the tail concurrently; when local
        # finishes its share and a remote chunk is still in flight, an idle
        # local core speculatively re-runs it — first result wins per item.
        # Worst case (remote dead/absent) local eventually does everything, so
        # this is never slower than a plain local Pool; a working remote halves
        # the tail's wall time.
        self._spilled = True
        n = len(items)
        slots = [None] * n
        lock = threading.Lock()
        chunk_size = _adaptive_chunk(per_item_cost)
        chunks = _chunk(list(enumerate(items)), chunk_size)  # [(orig_idx, item), ...]

        # Interleave chunk ownership so neither side is strictly first.
        local_chunks = chunks[::2]
        remote_chunks = chunks[1::2]

        def fill(pairs):
            """Run user func over (idx, item) pairs and first-wins into slots."""
            idxs = [p[0] for p in pairs]
            vals = [p[1] for p in pairs]
            if starmap:
                res = self._ctx.starmap(_apply, [(func, v) for v in vals])
            else:
                res = self._ctx.map(func, vals)
            with lock:
                for i, v in zip(idxs, res):
                    if slots[i] is None:
                        slots[i] = v

        # Remote thread: dispatch remote chunks, first-wins into slots.
        def remote_run():
            for chunk in remote_chunks:
                res = self._remote_chunk(func, chunk, starmap)
                if res is None:
                    continue
                with lock:
                    for orig_i, v in res:
                        if slots[orig_i] is None:
                            slots[orig_i] = v

        # Local runs its share; when done, re-run any remote chunk still not
        # filled (straggler tail). done is set once local has given every item a
        # chance, so the caller can always return a complete result.
        def local_run():
            fill([p for c in local_chunks for p in c])
            while True:
                with lock:
                    pending = [p for c in remote_chunks for p in c if slots[p[0]] is None]
                if not pending:
                    break
                fill(pending)  # idle local cores re-run the stragglers
            done.set()

        done = threading.Event()
        t_local = threading.Thread(target=local_run)
        t_remote = threading.Thread(target=remote_run)
        t_local.start()
        t_remote.start()
        done.wait()  # local guarantees a complete result
        return slots

    def _remote_chunk(self, func, chunk, starmap):
        """Dispatch one chunk to the cluster; returns [(orig_idx, result), ...]
        or None on failure (local already has the work covered). chunk is a list
        of (orig_idx, item) pairs."""
        import json
        import urllib.request
        import base64
        import pickle
        idxs = [p[0] for p in chunk]
        vals = [p[1] for p in chunk]
        if starmap:
            payload = [c if isinstance(c, (list, tuple)) else (c,) for c in vals]
        else:
            payload = vals
        body = json.dumps({"func": _pickle_func(func), "items": payload,
                           "starmap": starmap}).encode()
        req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                     headers={"Content-Type": "application/json",
                                              "X-Pipedpeer-Store": _STORE})
        try:
            with _daemon_open(req, 600) as resp:
                rs = json.loads(resp.read())["results"]
                return [(idxs[i], pickle.loads(base64.b64decode(r["pickle"])))
                        for i, r in enumerate(rs)]
        except Exception as e:
            _log("remote failed (%s); local covers it" % e)
            return None

    def close(self):
        try:
            self._ctx.close()
            self._ctx.terminate()
        except Exception:
            pass

    def terminate(self):
        self.close()

    def join(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def _pickle_func(func):
    import base64
    import pickle
    return base64.b64encode(pickle.dumps(func)).decode()


def _apply(func, item):
    """Run func over one starmap item (a tuple of args)."""
    return func(*item) if isinstance(item, tuple) else func(item)


def _measure_cost(func, items, starmap):
    """Seconds per item, from running the first chunks locally."""
    try:
        t0 = time.monotonic()
        if starmap:
            func(*items[0])
        else:
            func(items[0])
        dt = time.monotonic() - t0
        return dt / max(len(items), 1)
    except Exception:
        return -1.0


def _adaptive_chunk(per_item_cost):
    """Chunk size from measured per-item cost. Cheap items get large chunks
    (amortise round-trips), costly items get small chunks (more parallelism).
    Bounded to keep remote dispatch from dominating on either extreme."""
    if per_item_cost <= 0:
        return 64
    # Target ~0.5s of work per remote chunk.
    target = 0.5 / per_item_cost
    return max(1, min(256, int(target)))


def _chunk(seq, size):
    if size <= 0:
        size = 64
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def _np_dispatch(blocks, other):
    """Run [np.matmul(block, other) for block in blocks] over the cluster via
    the warm-worker /v1/pool/map path. Blocks and the fixed operand ship as
    raw pickle frames; the daemon splits the blocks across peers. Falls back
    to local on any failure so an absent cluster never breaks math."""
    import pickle
    try:
        # The worker runs the closure's python with no shim on its path, so the
        # function ships as source (pickling by reference would resolve
        # sitecustomize._matmul_with to the Nix sitecustomize and fail). The
        # fixed right-hand operand rides along as a globals frame the worker
        # unpickles into the namespace before running.
        items = [pickle.dumps(b) for b in blocks]
        globals_pickle = pickle.dumps({"_other": other})
        header = {
            "func_src": ("import os\nimport numpy as _np\ndef run(block):\n"
                         "    os.environ['PIPEDPEER_NUMPY_NESTED'] = '1'\n"
                         "    return _np.matmul(block, _other)\n"),
            "func_name": "run",
            "items_frames": len(items),
            "globals": True,
        }
        # Admission control hint: the daemon refuses (503) when it cannot spare
        # roughly the payload's size in RAM, and the shim falls back locally.
        header["required_mem"] = _pool_required(globals_pickle, items)
        if _FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, items, 1200)
        return [pickle.loads(p) for p in blobs]
    except Exception as e:
        _log("numpy remote failed (%s); local fallback" % e)
        import numpy as _np
        return [_np.matmul(b, other) for b in blocks]


def _matmul_with(block, other):
    import numpy as _np
    return _np.matmul(block, other)


def _torch_dispatch(blocks, other):
    """Run [torch.matmul(block, other) for block in blocks] over the cluster.
    Uses GPU on the worker when available (moves both tensors to cuda before
    matmul) so ML tensor work utilises remote GPUs fully. Blocks and the fixed
    operand ship as raw pickle frames. Falls back to local on any failure so an
    absent cluster never breaks the model."""
    import pickle
    try:
        # Ships by source, not by reference: the worker's closure python has no
        # shim module on its path (see _np_dispatch). The fixed right-hand
        # operand rides along as a globals frame the worker unpickles first.
        items = [pickle.dumps(b) for b in blocks]
        globals_pickle = pickle.dumps({"_other": other})
        header = {
            "func_src": ("import os\nimport torch as _th\n"
                         "def run(block):\n"
                         "    os.environ['PIPEDPEER_NUMPY_NESTED'] = '1'\n"
                         "    if _th.cuda.is_available():\n"
                         "        block, other = block.cuda(), _other.cuda()\n"
                         "        return _th.matmul(block, other).cpu()\n"
                         "    return _th.matmul(block, _other)\n"),
            "func_name": "run",
            "items_frames": len(items),
            "globals": True,
        }
        header["required_mem"] = _pool_required(globals_pickle, items)
        if _FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, items, 1200)
        return [pickle.loads(p) for p in blobs]
    except Exception as e:
        _log("torch remote failed (%s); local fallback" % e)
        import torch as _th
        return [_torch_matmul_with(b, other) for b in blocks]


def _torch_matmul_with(block, other):
    import torch as _th
    if _TORCH_ORIG_MATMUL is not None:
        matmul = _TORCH_ORIG_MATMUL
    else:
        matmul = _th.matmul
    if _th.cuda.is_available():
        block, other = block.cuda(), other.cuda()
        return matmul(block, other).cpu()
    return matmul(block, other)


_TORCH_ORIG_MATMUL = None
_TORCH_ORIG_MM = None


def _install_torch():
    # torch block-row matmul/mm: intercept large 2D tensor ops and split A's
    # rows across the cluster, each block computed on the worker (on GPU when
    # the worker has one), results cat'd back. DDP ranks skip this — gradient
    # sync must not race with offloaded matmul.
    if os.environ.get("PIPEDPEER_DDP") == "1":
        return
    try:
        import torch as _th
    except ImportError:
        return

    _MIN_BYTES = _spill_min()

    _orig_matmul = _th.matmul
    _orig_mm = _th.mm
    global _TORCH_ORIG_MATMUL, _TORCH_ORIG_MM
    _TORCH_ORIG_MATMUL = _orig_matmul
    _TORCH_ORIG_MM = _orig_mm

    def _matmul(a, b, *args, **kw):
        import torch as _th
        if (a.dim() == 2 and b.dim() == 2 and a.shape[1] == b.shape[0]
                and a.element_size() * a.nelement() >= _MIN_BYTES
                and a.shape[0] >= 8 and _URL and _ENABLED
                and not os.environ.get("PIPEDPEER_NUMPY_NESTED")):
            try:
                n_blocks = max(2, min(64, a.shape[0] // 8))
                rows = max(1, a.shape[0] // n_blocks)
                blocks = [a[i:i + rows] for i in range(0, a.shape[0], rows)]
                return _th.cat(_torch_dispatch(blocks, b), dim=0)
            except Exception as e:
                _log("torch matmul fallback (%s)" % e)
        return _orig_matmul(a, b, *args, **kw)

    def _mm(a, b, *args, **kw):
        return _matmul(a, b, *args, **kw)

    _th.matmul = _matmul
    _th.mm = _mm
    try:
        _th.Tensor.matmul = _matmul
        _th.Tensor.mm = _mm
    except Exception:
        pass
    _log("torch matmul/mm interception installed")


def _install_numpy():
    # numpy block-row matmul: intercept A @ B when the latency cost model says
    # remote beats local, splitting A's rows across the cluster so a worker
    # never allocates the whole global matrix. Each block ships to the warm
    # worker which already unpickles it (items_b64). Default-on: the cost model
    # guarantees offload is never slower than running locally.
    try:
        import numpy as _np
    except ImportError:
        return

    _orig_matmul = _np.matmul
    _orig_dot = _np.dot
    _orig_tensordot = _np.tensordot
    # Capture the pre-patch callables directly: getattr(_np.linalg, name)
    # after patching would resolve the wrapper itself and recurse forever.
    _orig_linalg = {n: getattr(_np.linalg, n) for n in ("svd", "eig")}

    def _mm_gate(a, b):
        # Heavy compute only (matmul is ~N/8 flops per byte at the shapes that
        # matter): the BLAS-realistic cost model keeps bandwidth-bound element
        # math local by leaving it unpatched, and refuses to ship when the
        # probe or peer count says no. Round-trip factor 3: A in, the replicated
        # operand B in, and the product C back.
        return (a.ndim == 2 and b.ndim == 2 and a.shape[1] == b.shape[0]
                and a.shape[0] >= 8 and _URL and _ENABLED
                and not os.environ.get("PIPEDPEER_NUMPY_NESTED")
                and _numpy_should_offload(a.nbytes, max(8, a.shape[0] // 8), 5, True, 200e9))

    def _mm_dispatch(a, b):
        import numpy as _np
        n_blocks = max(2, min(64, a.shape[0] // 8))
        rows = max(1, a.shape[0] // n_blocks)
        blocks = [a[i:i + rows] for i in range(0, a.shape[0], rows)]
        _log('matmul: sending %d row blocks (%.0f MB) to cluster' % (len(blocks), a.nbytes / 1e6))
        return _np.vstack(_np_dispatch(blocks, b))

    def _matmul(a, b, *args, **kw):
        import numpy as _np
        if _mm_gate(a, b):
            try:
                return _mm_dispatch(a, b)
            except Exception as e:
                _log("numpy matmul fallback (%s)" % e)
        return _orig_matmul(a, b, *args, **kw)

    def _dot(a, b, *args, **kw):
        import numpy as _np
        if _mm_gate(a, b):
            try:
                return _mm_dispatch(a, b)
            except Exception as e:
                _log("numpy dot fallback (%s)" % e)
        return _orig_dot(a, b, *args, **kw)

    def _tensordot(a, b, axes=2, *args, **kw):
        import numpy as _np
        # 2D contraction forms are matmul semantics: (last, first) axes.
        if _mm_gate(a, b) and axes in (((1,), (0,)), (1, 0), 1):
            try:
                return _mm_dispatch(a, b)
            except Exception as e:
                _log("numpy tensordot fallback (%s)" % e)
        return _orig_tensordot(a, b, axes, *args, **kw)

    def _linalg_offload(name, a, args, kw):
        # Ship np.linalg.<name>(a) as a single noSplit item so it lands on the
        # best peer (part 0 prefers peers[0], falling through to local on
        # failure); a constrained orchestrator never materialises the matrix.
        import pickle
        extra = {"_fn": name, "_args": args, "_kw": kw}
        globals_pickle = pickle.dumps(extra)
        items = [pickle.dumps(a)]
        _log('%s: offloading %.0f MB matrix to one worker' % (name, a.nbytes / 1e6))
        header = {
            # PIPEDPEER_NUMPY_NESTED: the worker's numpy is also patched, so
            # the offloaded call must bypass the shim or it re-dispatches in a
            # loop. The marker makes _linalg fall through to the original.
            "func_src": ("import os\nimport numpy as _np\ndef run(x):\n"
                         "    os.environ['PIPEDPEER_NUMPY_NESTED'] = '1'\n"
                         "    return getattr(_np.linalg, _fn)(x, *_args, **_kw)\n"),
            "func_name": "run",
            "items_frames": len(items),
            "globals": True,
            "no_split": True,
        }
        header["required_mem"] = _pool_required(globals_pickle, items)
        if _FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, items, 1200)
        return pickle.loads(blobs[0])

    def _linalg(name):
        orig = _orig_linalg[name]
        def wrapped(a, *args, **kw):
            import numpy as _np
            if (a.ndim == 2 and _URL and _ENABLED
                    and not os.environ.get("PIPEDPEER_NUMPY_NESTED")
                    and _numpy_should_offload(a.nbytes, max(8, a.shape[0] // 4), 3, False, 1.5e9)):
                try:
                    return _linalg_offload(name, a, args, kw)
                except Exception as e:
                    _log("numpy %s fallback (%s)" % (name, e))
            return orig(a, *args, **kw)
        return wrapped

    _np.matmul = _matmul
    _np.dot = _dot
    _np.tensordot = _tensordot
    _np.linalg.svd = _linalg("svd")
    _np.linalg.eig = _linalg("eig")
    _log("numpy matmul/dot/tensordot/svd/eig interception installed")


# ---- distributed pandas: cost model, hash-shuffle groupby/merge, OOC ----
# Opt-in via PIPEDPEER_PANDAS=1. Every intercept gates on the latency cost
# model (_should_spill): a shuffle only fires when transfer + remote estimate
# beats the single-node estimate, so interception is never slower than plain
# pandas (D2). Shuffles bucket rows by hash(key) % K (K = peers+1, bucket 0
# stays local) and ship one bucket per no_split item, so every key's rows land
# complete on one node and the per-node agg/merge is exact — no combiners, any
# agg spec works.
_IN_MERGE = False
_BW_CACHE = {"t": 0.0, "bw": None}
_BW_TTL = 300.0

# The daemon always listens on loopback (http://127.0.0.1:<selfPort>), so its
# requests must never be routed through an http(s)_proxy / no_proxy env proxy
# (GH Actions runners and many sandboxes set these, which would otherwise
# hijack or refuse loopback traffic and make the shim silently fall back to
# local). Build a proxy-less opener and reuse it everywhere.
_DaemonOpener = None
def _daemon_open(req, timeout):
    """Send req to the loopback daemon, bypassing any environment HTTP proxy."""
    global _DaemonOpener
    import urllib.error
    import urllib.request
    if _DaemonOpener is None:
        _DaemonOpener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        return _DaemonOpener.open(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        raise RuntimeError("daemon HTTP %d: %s" % (e.code, e.read())) from e


def _should_spill(nbytes, flops_per_byte):
    """Latency cost model: spill nbytes of work (flops_per_byte per byte) when
    the estimated transfer + remote compute beats local single-node time.
    est_local = nbytes*flops_per_byte/1e9 (~1e9 flops/s effective), est_remote
    = est_local/K*1.3 (parallel speedup, 30% overhead)."""
    if not (_URL and _ENABLED):
        return False
    K = int(_NUM_SHARDS)
    if K < 2 or nbytes < _spill_min():
        return False
    if _FORCE:
        return True
    if flops_per_byte < 8 and nbytes <= 512 * 1024 * 1024:
        return False
    bw = _measure_bandwidth()
    if not bw:
        return False
    est_transfer = nbytes / bw
    est_local = nbytes * flops_per_byte / 1e9
    est_remote = est_local / K * 1.3
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


def _pool_send(header, globals_pickle, items, timeout):
    """POST a frames pool/map request: small JSON header line + optional
    globals frame + length-prefixed raw pickle item frames. Returns the raw
    pickle blobs of the results (frames response). Bulk data never touches
    base64 or a giant JSON parse, which is what made the old encoding ~30MB/s."""
    import json
    import pickle
    import struct
    import urllib.request
    body = json.dumps(header).encode() + b"\n"
    if globals_pickle is not None:
        body += struct.pack(">I", len(globals_pickle)) + globals_pickle
    for it in items:
        body += struct.pack(">I", len(it)) + it
    req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                 headers={"Content-Type": "application/vnd.pipedpeer.frames",
                                          "X-Pipedpeer-Store": _STORE,
                                          "X-Pipedpeer-Submitter": _SUBMITTER})
    with _daemon_open(req, timeout) as resp:
        data = resp.read()
    nl = data.find(b"\n")
    hdr = json.loads(data[:nl])
    rest = data[nl + 1:]
    out = []
    for _ in range(hdr.get("results_frames", 0)):
        n = struct.unpack(">I", rest[:4])[0]
        out.append(rest[4:4 + n])
        rest = rest[4 + n:]
    return out


def _numpy_should_offload(nbytes, flops_per_byte, round_trip, split, flops_per_sec):
    """BLAS-realistic cost model for numpy offloads, calibrated to real BLAS
    throughput (matmul ~200 GFLOP/s, svd ~1.5 GFLOP/s on this cluster's numpy).
    est_transfer counts the full round trip (in + out, plus the replicated
    operand for matmul). Split work (matmul) spills only when remote beats
    local, which at practical sizes it never does over the pool transport —
    local BLAS is faster, so interception stays honest (D2: never slower).
    Single-worker offloads (svd/eig) relocate compute for an asymmetric
    orchestrator: worth it when shipping the matrix costs far less than the
    local compute, since the origin node is freed entirely."""
    if not (_URL and _ENABLED):
        return False
    K = int(_NUM_SHARDS)
    if K < 2 or nbytes < _spill_min():
        return False
    if _FORCE:
        return True
    bw = _measure_bandwidth()
    if not bw:
        return False
    est_local = nbytes * flops_per_byte / flops_per_sec
    est_transfer = nbytes * round_trip / bw
    if not split:
        return est_transfer < est_local * 0.5
    est_remote = est_local / K * 1.3
    return est_local > est_transfer + est_remote


def _groupby_worker_src():
    return ("import pandas as _pd\n"
            "def run(sub):\n"
            "    return sub.groupby(_by, as_index=_as_index, sort=False, dropna=_dropna).agg(_spec, *_args, **_kwargs)\n")


def _groupby_shuffle(gb, spec, args, kw):
    _log('groupby: hash-shuffling %d rows across %d nodes' % (len(gb.obj), int(_NUM_SHARDS)))
    """Hash-shuffle a DataFrameGroupBy.agg: rows bucketed by hash(key)%K,
    bucket 0 aggregated locally, buckets 1..K-1 shipped as no_split items
    (item i -> peer i), so every key's rows land complete on one node and the
    per-node agg is exact for any spec. NaN keys dropped here when
    dropna=True. Result sorted by key (pandas default sort=True)."""
    import base64
    import json
    import pickle
    import urllib.request
    import pandas as _pd
    df = gb.obj
    names = gb._grouper.names
    keys = [n for n in names if isinstance(n, str) and n in df.columns]
    if not keys or len(keys) != len(names):
        raise ValueError("unsupported groupby keys")
    K = int(_NUM_SHARDS)
    as_index = gb.as_index
    dropna = getattr(gb, "dropna", True)
    key_vals = df[keys]
    buckets = _pd.util.hash_pandas_object(key_vals, index=False) % K
    if dropna:
        keep = key_vals.notna().all(axis=1)
        buckets = buckets[keep]
        df = df[keep]
    parts = []
    local = df[buckets == 0]
    if len(local):
        parts.append(local.groupby(keys, as_index=as_index, sort=False,
                                   dropna=dropna).agg(spec, *args, **kw))
    remote = [df[buckets == b] for b in range(1, K) if (buckets == b).any()]
    if remote:
        globals_pickle = pickle.dumps({
            "_by": keys, "_as_index": as_index, "_dropna": dropna,
            "_spec": spec, "_args": args, "_kwargs": kw,
        })
        header = {
            "func_src": _groupby_worker_src(),
            "func_name": "run",
            "items_frames": len(remote),
            "globals": True,
            "no_split": True,
        }
        header["required_mem"] = _pool_required(globals_pickle, [pickle.dumps(d) for d in remote])
        if _FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, [pickle.dumps(d) for d in remote], 1800)
        parts.extend(pickle.loads(p) for p in blobs)
    res = _pd.concat(parts)
    if as_index:
        return res.sort_index()
    return res.reset_index(drop=True).sort_values(by=keys, kind="stable").reset_index(drop=True)


def _groupby_gate(gb):
    """True when a DataFrameGroupBy.agg should shuffle: big enough to pay for
    the transfer, keys are plain columns (no index/categorical/array keys)."""
    from pandas.core.groupby import generic as _gbg
    if not isinstance(gb, _gbg.DataFrameGroupBy):
        return False
    if gb.level is not None or getattr(gb, "axis", 0) != 0:
        return False
    df = gb.obj
    try:
        nbytes = df.memory_usage(deep=False).sum()
    except Exception:
        return False
    if not _should_spill(nbytes, 4):
        return False
    for n in gb._grouper.names:
        if not (isinstance(n, str) and n in df.columns):
            return False
        if getattr(df[n].dtype, "categories", None) is not None:
            return False
    return True


def _merge_keys(frame, cols, index):
    """Join-key values of one merge side as a DataFrame (one column per key).
    The key frame keeps the frame's index (duplicates included) so the hash
    buckets align with the rows."""
    if index:
        kf = frame.index.to_frame(index=False)
        kf.index = frame.index
        return kf
    if isinstance(cols, str):
        cols = [cols]
    if not isinstance(cols, list) or not cols:
        raise ValueError("unsupported merge keys")
    if not all(isinstance(c, str) and c in frame.columns for c in cols):
        raise ValueError("unsupported merge keys")
    return frame[cols]


def _merge_shuffle(left, right, kw):
    """Hash-shuffle merge: both frames bucketed by hash of their own join keys
    % K (equal key values hash equal, so matching rows share a bucket), each
    (left_bucket, right_bucket) pair merged on one node, origin concatenates.
    Buckets with no rows on a side ship an empty frame (columns intact) so
    left/right joins keep unmatched rows. sort=True sorts by the join keys;
    sort=False leaves bucket order (documented deviation)."""
    global _IN_MERGE
    import base64
    import json
    import pickle
    import urllib.request
    import pandas as _pd
    K = int(_NUM_SHARDS)
    how = kw.get("how", "inner")
    sort = kw.get("sort", False)
    on = kw.get("on")
    left_on = kw.get("left_on")
    right_on = kw.get("right_on")
    left_index = kw.get("left_index", False)
    right_index = kw.get("right_index", False)
    if how == "cross":
        raise ValueError("cross join unsupported")
    if not (on or left_on is not None or right_on is not None or (left_index and right_index)):
        raise ValueError("unsupported merge keys")
    l_keys = _merge_keys(left, left_on if left_on is not None else on, left_index)
    r_keys = _merge_keys(right, right_on if right_on is not None else on, right_index)
    lb = _pd.util.hash_pandas_object(l_keys, index=False) % K
    rb = _pd.util.hash_pandas_object(r_keys, index=False) % K
    worker_kw = dict(kw)
    worker_kw["sort"] = False
    local_part = None
    items = []
    for b in range(K):
        L = left[lb == b]
        R = right[rb == b]
        if not len(L) and not len(R):
            continue
        if not len(R):
            R = right.iloc[0:0]
        if not len(L):
            L = left.iloc[0:0]
        if b == 0:
            local_part = (L, R)
        else:
            items.append((L, R))
    parts = []
    _IN_MERGE = True
    try:
        if local_part is not None:
            L, R = local_part
            parts.append(_pd.merge(L, R, **worker_kw))
    finally:
        _IN_MERGE = False
    if items:
        globals_pickle = pickle.dumps({"_mw": worker_kw})
        header = {
            "func_src": ("import pandas as _pd\n"
                         "def run(item):\n"
                         "    L, R = item\n"
                         "    return _pd.merge(L, R, **_mw)\n"),
            "func_name": "run",
            "items_frames": len(items),
            "globals": True,
            "no_split": True,
        }
        header["required_mem"] = _pool_required(globals_pickle, [pickle.dumps(i) for i in items])
        if _FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, [pickle.dumps(i) for i in items], 1800)
        parts.extend(pickle.loads(p) for p in blobs)
    res = _pd.concat(parts)
    if left_index or right_index:
        if sort:
            return res.sort_index()
        return res
    res = res.reset_index(drop=True)
    if sort:
        sc = left_on if left_on is not None else (right_on if right_on is not None else on)
        if isinstance(sc, str):
            sc = [sc]
        return res.sort_values(by=sc, kind="stable").reset_index(drop=True)
    return res


def _merge_gate(left, right):
    try:
        nbytes = left.memory_usage(deep=False).sum() + right.memory_usage(deep=False).sum()
    except Exception:
        return False
    return _should_spill(nbytes, 2)


def _install_pandas():
    try:
        import pandas as _pd
    except ImportError:
        return
    from pandas.core.groupby import generic as _gbg

    _orig_agg = _gbg.DataFrameGroupBy.agg
    _orig_named = {}
    for _name in ("sum", "mean", "count", "size", "min", "max", "first",
                  "last", "std", "var", "median", "nunique", "prod", "any",
                  "all"):
        _orig_named[_name] = getattr(_gbg.DataFrameGroupBy, _name, None)

    def _named_agg(name, orig):
        def _wrapped(self, *args, **kw):
            try:
                if _groupby_gate(self):
                    return _groupby_shuffle(self, name, args, kw)
            except Exception as e:
                _log("groupby fallback (%s)" % e)
            return orig(self, *args, **kw)
        return _wrapped

    def _agg(self, spec, *args, **kw):
        try:
            if _groupby_gate(self):
                return _groupby_shuffle(self, spec, args, kw)
        except Exception as e:
            _log("groupby fallback (%s)" % e)
        return _orig_agg(self, spec, *args, **kw)

    for _name, _orig in _orig_named.items():
        if _orig is not None:
            setattr(_gbg.DataFrameGroupBy, _name, _named_agg(_name, _orig))
    _gbg.DataFrameGroupBy.agg = _agg

    _orig_merge = _pd.DataFrame.merge
    _orig_join = _pd.DataFrame.join

    def _merge(self, right, how="inner", on=None, left_on=None, right_on=None,
               left_index=False, right_index=False, sort=False,
               suffixes=("_x", "_y"), copy=None, indicator=False,
               validate=None):
        if (isinstance(right, _pd.DataFrame) and not _IN_MERGE
                and _merge_gate(self, right)):
            kw = {"how": how, "on": on, "left_on": left_on,
                  "right_on": right_on, "left_index": left_index,
                  "right_index": right_index, "sort": sort,
                  "suffixes": suffixes, "validate": validate}
            if indicator:
                kw["indicator"] = indicator
            if copy is not None:
                kw["copy"] = copy
            try:
                return _merge_shuffle(self, right, kw)
            except Exception as e:
                _log("merge fallback (%s)" % e)
        return _orig_merge(self, right, how=how, on=on, left_on=left_on,
                           right_on=right_on, left_index=left_index,
                           right_index=right_index, sort=sort,
                           suffixes=suffixes, copy=copy, indicator=indicator,
                           validate=validate)

    def _join(self, other, on=None, how="left", lsuffix="", rsuffix="",
              sort=False, validate=None):
        if (isinstance(other, _pd.DataFrame) and not _IN_MERGE
                and _merge_gate(self, other)):
            kw = {"how": how, "on": None, "left_on": on, "right_on": None,
                  "left_index": on is None, "right_index": True, "sort": sort,
                  "suffixes": (lsuffix, rsuffix), "validate": validate}
            try:
                return _merge_shuffle(self, other, kw)
            except Exception as e:
                _log("join fallback (%s)" % e)
        return _orig_join(self, other, on=on, how=how, lsuffix=lsuffix,
                          rsuffix=rsuffix, sort=sort, validate=validate)

    _pd.DataFrame.merge = _merge
    _pd.DataFrame.join = _join
    _log("pandas groupby/merge/join interception installed")


# ---- out-of-core reads: chunked parse + partitioned frame ----
_OOC_CHUNK = int(float(os.environ.get("PIPEDPEER_OOC_CHUNK") or (64 * 1024 * 1024)))
_COMBINE_SAFE = ("sum", "count", "size", "mean", "min", "max", "first",
                 "last", "any", "all")
_OOC_BATCH_MAX = int(os.environ.get("PIPEDPEER_OOC_BATCH_MAX") or 8)
_OOC_BATCH_CACHE = {"t": 0.0, "n": 2}


def _ooc_batch():
    """Auto-size how many chunks ship per request: parse working set is ~5x a
    chunk, and a node must never hold more than ~40% of its free RAM at once.
    Probes the local daemon's /health available_mem, clamped to
    [1, _OOC_BATCH_MAX]; falls back to 2 when the daemon is unreachable."""
    now = time.monotonic()
    if now - _OOC_BATCH_CACHE["t"] < 5.0:
        return _OOC_BATCH_CACHE["n"]
    n = 2
    if _URL:
        try:
            import json
            import urllib.request
            with urllib.request.urlopen(_URL + "/health", timeout=2) as r:
                h = json.loads(r.read().decode())
                avail = h.get("available_mem") or 0
                if avail > 0:
                    n = max(1, int(0.4 * avail / (5 * _OOC_CHUNK)))
        except Exception:
            n = 2
    n = min(n, _OOC_BATCH_MAX)
    _OOC_BATCH_CACHE["t"] = now
    _OOC_BATCH_CACHE["n"] = n
    return n


def _ooc_eligible(path):
    """Out-of-core read when the file is too big to parse in one node's RAM:
    size > 0.5 * MemAvailable (the parse working set is a few x the file).
    PIPEDPEER_OOC_MIN overrides the threshold (bytes) for tests."""
    try:
        size = os.path.getsize(path)
        if not size:
            return False
        override = os.environ.get("PIPEDPEER_OOC_MIN")
        if override:
            return size > float(override)
        if _FORCE:
            return size > _spill_min()
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
                    return size > 0.5 * avail
    except Exception:
        return False


def _pool_required(globals_pickle, items):
    """Honest per-node working-set estimate for a noSplit/fanned-out chunk:
    one node holds the globals plus the single largest item (parts are spread
    across peers, so the request total overstates any one node). 2x covers
    parse/output expansion. Keeps the daemon's admission control meaningful
    instead of 503-ing large-but-spread reads."""
    if items:
        return 2 * (len(globals_pickle) + max(len(i) for i in items))
    return 0


def _partitioned_read(func_src, extra, keys, items):
    """Send one batch of chunk frames. Batch sizes stay bounded (_ooc_batch),
    so an out-of-core read never builds the whole file in memory; item_keys
    pin each chunk to the peer the daemon's key-hash routing chooses, so a
    later cache_keys fetch for the same key resolves on the node that parsed
    it no matter which batch carried it."""
    import pickle
    globals_pickle = pickle.dumps(extra)
    header = {
        "func_src": func_src,
        "func_name": "run",
        "items_frames": len(items),
        "globals": True,
        "no_split": True,
        "item_keys": keys,
    }
    header["required_mem"] = _pool_required(globals_pickle, items)
    if _FORCE:
        header["force"] = True
    blobs = _pool_send(header, globals_pickle, items, 3600)
    return [pickle.loads(p) for p in blobs]


def _partitioned_read_csv(path, kw):
    """Chunk the file at line boundaries, parse each chunk on its own node (the
    parse working set is then one chunk), cache chunks in the workers' _CACHE
    and return a _PartitionedFrame proxy over them. Reads and ships a bounded
    batch of chunks at a time, so origin RAM never holds the whole file."""
    import base64
    import hashlib
    import os
    import pickle
    import pandas as _pd
    _log('read_csv: streaming %.0f MB out-of-core' % (os.path.getsize(path) / 1e6))
    header = kw.get("header", "infer")
    if header == "infer":
        header = 0
    if header not in (0, None):
        raise ValueError("unsupported header")
    names = kw.get("names")
    if names is None and header == 0:
        sniff_kw = dict(kw)
        sniff_kw["nrows"] = 0
        names = list(_pd.read_csv(str(path), **sniff_kw).columns)
    parse_kw = dict(kw)
    parse_kw.pop("header", None)
    parse_kw.pop("names", None)
    src = ("import hashlib\n"
           "import io\n"
           "import os\n"
           "import pickle\n"
           "import pandas as _pd\n"
           "def run(raw):\n"
           "    k = 'df:' + hashlib.sha256(raw).hexdigest()\n"
           "    if k not in _CACHE:\n"
           "        _CACHE[k] = _pd.read_csv(io.BytesIO(raw), names=_names, header=None, **_csv_kw)\n"
           "        if globals().get('_CHUNK_DIR', ''):\n"
           "            with open(os.path.join(globals()['_CHUNK_DIR'], hashlib.sha256(k.encode()).hexdigest()), 'wb') as f:\n"
           "                pickle.dump(_CACHE[k], f)\n"
           "    return {'rows': len(_CACHE[k]), 'dtypes': {c: str(t) for c, t in _CACHE[k].dtypes.items()}}\n")
    extra = {"_names": names, "_csv_kw": parse_kw}
    all_keys = []
    keys, items, metas = [], [], []
    batch = _ooc_batch()
    for start, end in _csv_ranges(str(path), header):
        with open(str(path), "rb") as f:
            f.seek(start)
            data = f.read(end - start)
        key = "df:" + hashlib.sha256(data).hexdigest()
        all_keys.append(key)
        keys.append(key)
        items.append(pickle.dumps(data))
        if len(items) == batch:
            metas.extend(_partitioned_read(src, extra, keys, items))
            keys, items = [], []
    if items:
        metas.extend(_partitioned_read(src, extra, keys, items))
    cols = names if names is not None else list(metas[0]["dtypes"])
    return _PartitionedFrame(all_keys, cols, metas, str(path))


def _csv_ranges(path, header):
    """Yield (start, end) byte ranges of the data rows, split at line
    boundaries. The header line (header=0) is skipped by starting after the
    first newline; every chunk then parses identically (names, header=None)."""
    import os
    with open(path, "rb") as f:
        head = f.read(4096)
    start = 0
    if header == 0:
        nl = head.find(b"\n")
        if nl < 0:
            raise ValueError("no header line")
        start = nl + 1
    size = os.path.getsize(path)
    pos = start
    with open(path, "rb") as f:
        while pos < size:
            f.seek(pos)
            data = f.read(_OOC_CHUNK)
            if not data:
                break
            nl = data.rfind(b"\n")
            if nl < 0:
                nl = len(data)  # ponytail: pathological single-line file
            end = pos + nl + 1
            yield start, end
            start = end
            pos = end


def _partitioned_read_parquet(path, kw):
    """Chunk at row-group boundaries (origin RAM = one row group), parse each
    on its own node, cache and proxy as _PartitionedFrame."""
    import base64
    import hashlib
    import io
    import os
    import pickle
    import pandas as _pd
    import pyarrow.parquet as _pq
    _log('read_parquet: streaming %.0f MB out-of-core' % (os.path.getsize(path) / 1e6))
    pf = _pq.ParquetFile(str(path))
    src = ("import hashlib\n"
           "import io\n"
           "import os\n"
           "import pickle\n"
           "import pandas as _pd\n"
           "def run(raw):\n"
           "    k = 'df:' + hashlib.sha256(raw).hexdigest()\n"
           "    if k not in _CACHE:\n"
           "        _CACHE[k] = _pd.read_parquet(io.BytesIO(raw), **_pq_kw)\n"
           "        if globals().get('_CHUNK_DIR', ''):\n"
           "            with open(os.path.join(globals()['_CHUNK_DIR'], hashlib.sha256(k.encode()).hexdigest()), 'wb') as f:\n"
           "                pickle.dump(_CACHE[k], f)\n"
           "    return {'rows': len(_CACHE[k]), 'dtypes': {c: str(t) for c, t in _CACHE[k].dtypes.items()}}\n")
    extra = {"_pq_kw": kw}
    all_keys = []
    keys, items, metas = [], [], []
    batch = _ooc_batch()
    for i in range(pf.metadata.num_row_groups):
        buf = io.BytesIO()
        _pq.write_table(pf.read_row_group(i), buf)
        data = buf.getvalue()
        key = "df:" + hashlib.sha256(data).hexdigest()
        all_keys.append(key)
        keys.append(key)
        items.append(pickle.dumps(data))
        if len(items) == batch:
            metas.extend(_partitioned_read(src, extra, keys, items))
            keys, items = [], []
    if items:
        metas.extend(_partitioned_read(src, extra, keys, items))
    return _PartitionedFrame(all_keys, list(metas[0]["dtypes"]), metas, str(path))


class _PartitionedFrame:
    """Proxy over an out-of-core file: chunks parsed on the cluster (cached
    content-addressed in the warm workers' _CACHE), metadata local. groupby
    and merge run distributed; anything else materializes the chunks back into
    one local DataFrame (the documented fallback)."""

    def __init__(self, keys, cols, meta, path):
        self._keys = keys
        self._cols = list(cols)
        self._meta = meta
        self._path = path

    @property
    def shape(self):
        return (sum(m["rows"] for m in self._meta), len(self._cols))

    @property
    def columns(self):
        return self._cols

    @property
    def dtypes(self):
        return dict(self._meta[0]["dtypes"])  # ponytail: chunks are uniform

    @property
    def index(self):
        return self._materialize().index

    def __len__(self):
        return sum(m["rows"] for m in self._meta)

    def __iter__(self):
        return iter(self._cols)

    def __getitem__(self, key):
        return self._materialize()[key]

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        _log("partitioned frame: materializing (%s)" % name)
        return getattr(self._materialize(), name)

    def head(self, n=5):
        return self._chunk(0).head(n).reset_index(drop=True)

    def tail(self, n=5):
        if self._meta[-1]["rows"] >= n:
            return self._chunk(len(self._keys) - 1).tail(n).reset_index(drop=True)
        return self._materialize().tail(n)

    def groupby(self, by, **kw):
        return _PartitionedGroupBy(self, by, **kw)

    def merge(self, right, **kw):
        return _partitioned_merge(self, right, **kw)

    def _chunk(self, i):
        """Fetch one chunk (from its node's cache) as a local DataFrame."""
        import base64
        import json
        import pickle
        import urllib.request
        body = json.dumps({
            "func_src": "def run(df):\n    return df\n",
            "items": [0], "starmap": False, "cache_keys": [self._keys[i]],
            "no_split": True,
        }).encode()
        req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                     headers={"Content-Type": "application/json",
                                              "X-Pipedpeer-Store": _STORE})
        with _daemon_open(req, 1800) as resp:
            rs = json.loads(resp.read())["results"]
        return pickle.loads(base64.b64decode(rs[0]["pickle"]))

    def _materialize(self):
        """Fetch all chunks and concat into one local DataFrame."""
        import base64
        import json
        import pickle
        import urllib.request
        import pandas as _pd
        body = json.dumps({
            "func_src": "def run(df):\n    return df\n",
            "items": list(range(len(self._keys))), "starmap": False,
            "cache_keys": self._keys, "no_split": True,
        }).encode()
        req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                     headers={"Content-Type": "application/json",
                                              "X-Pipedpeer-Store": _STORE})
        with _daemon_open(req, 3600) as resp:
            rs = json.loads(resp.read())["results"]
        dfs = [pickle.loads(base64.b64decode(r["pickle"])) for r in rs]
        return _pd.concat(dfs, ignore_index=True)

    def _combine_agg(self, by, spec, args, kw, gb_kw):
        """Combine-safe agg over the chunks: each chunk node aggregates its
        rows (partials are tiny), the origin re-aggregates the partials —
        exact because sum/count/size/min/max/first/last/any/all compose, and
        mean recombines as sum(sum)/sum(count)."""
        import base64
        import json
        import pickle
        import urllib.request
        import pandas as _pd
        as_index = gb_kw.get("as_index", True)
        dropna = gb_kw.get("dropna", True)
        named = (isinstance(spec, dict) and spec and all(
            isinstance(v, tuple) and len(v) == 2
            and isinstance(v[0], str) and isinstance(v[1], str)
            for v in spec.values()))
        has_mean, spec_sum, spec_count = _mean_split(spec)
        n = len(self._keys)
        _log('groupby: combining %d chunk partials from the cluster' % n)
        src = _combine_agg_worker_src(has_mean)
        extra = {"_by": by, "_as_index": as_index, "_dropna": dropna,
                 "_spec_sum": spec_sum, "_spec_count": spec_count,
                 "_args": args, "_kwargs": kw}
        if named:
            # named aggregations agg(vm=("v", "mean")) arrive as a dict of
            # pairs; pandas only accepts them as **kwargs on agg, so route
            # them through _named instead of a positional spec.
            extra["_spec_sum"] = None
            extra["_spec_count"] = None
            extra["_named"] = dict(spec_sum)
            extra["_named_count"] = dict(spec_count)
        req = {
            "func_src": src,
            "func_name": "run",
            "extra_b64": base64.b64encode(pickle.dumps(extra)).decode(),
            "items": list(range(n)), "starmap": False,
            "cache_keys": self._keys, "no_split": True,
        }
        body = json.dumps(req).encode()
        req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                     headers={"Content-Type": "application/json",
                                              "X-Pipedpeer-Store": _STORE})
        with _daemon_open(req, 3600) as resp:
            rs = json.loads(resp.read())["results"]
        parts = [pickle.loads(base64.b64decode(r["pickle"])) for r in rs]
        if has_mean:
            sums = [p[0] for p in parts]
            counts = [p[1] for p in parts]
        else:
            sums = parts
            counts = []
        level = 0 if len(by) == 1 else list(range(len(by)))
        # The chunk partials carry MultiIndex columns (col, func); pandas 3.0
        # cannot dict-agg against MultiIndex labels, so flatten to "col_func"
        # unique names, re-aggregate each column with its own combine op
        # (sums add, mins min, firsts keep first, ...), then restore the
        # MultiIndex layout the caller expects.
        _COMBINE_OPS = {"sum": "sum", "count": "sum", "size": "sum",
                        "prod": "sum", "min": "min", "max": "max",
                        "first": "first", "last": "last", "any": "any",
                        "all": "all"}

        def _combine(parts):
            flat = _pd.concat(parts)
            cols = flat.columns
            if isinstance(cols, _pd.MultiIndex):
                tuples = list(cols)
                flat.columns = ["%s_%s" % (c, f) for c, f in tuples]
                ops = {"%s_%s" % (c, f): _COMBINE_OPS.get(f, "sum") for (c, f) in tuples}
            else:
                tuples = None
                by_cols = set(by)
                ops = {c: (spec if isinstance(spec, str) else "sum")
                       for c in cols if c not in by_cols}
            g = (flat.groupby(level=level, sort=False, dropna=dropna) if as_index
                 else flat.groupby(by, as_index=False, sort=False, dropna=dropna))
            res = g.agg(ops)
            if tuples is not None:
                res.columns = _pd.MultiIndex.from_tuples(tuples)
            return res

        res = _combine(sums)
        if has_mean:
            res_c = _combine(counts)
            res = _combine_means(res, res_c, spec)
        if as_index:
            return res.sort_index()
        return res.reset_index(drop=True).sort_values(by=by, kind="stable").reset_index(drop=True)


def _combine_agg_worker_src(has_mean):
    if has_mean:
        return ("import pandas as _pd\n"
                "def run(df):\n"
                "    g = df.groupby(_by, as_index=_as_index, sort=False, dropna=_dropna)\n"
                "    if _spec_sum is None:\n"
                "        s = g.agg(*_args, **_kwargs, **_named)\n"
                "        return (s, g.agg(**_named_count))\n"
                "    s = g.agg(_spec_sum, *_args, **_kwargs)\n"
                "    return (s, g.agg(_spec_count))\n")
    return ("import pandas as _pd\n"
            "def run(df):\n"
            "    g = df.groupby(_by, as_index=_as_index, sort=False, dropna=_dropna)\n"
            "    if _spec_sum is None:\n"
            "        return g.agg(*_args, **_kwargs, **_named)\n"
            "    return g.agg(_spec_sum, *_args, **_kwargs)\n")


def _transform_spec(spec, fn):
    """Map fn over the agg funcs of a spec (str | list | dict), preserving
    shape. Two-element tuples inside a list are named-agg pairs (col, func):
    only the func slot is transformed."""
    if isinstance(spec, str):
        return fn(spec)
    if isinstance(spec, dict):
        return {k: _transform_spec(v, fn) for k, v in spec.items()}
    if isinstance(spec, (list, tuple)):
        if (len(spec) == 2 and isinstance(spec[0], str)
                and isinstance(spec[1], (list, tuple)) and len(spec[1]) == 2
                and isinstance(spec[1][0], str) and isinstance(spec[1][1], str)):
            return (spec[0], _transform_spec(spec[1], fn))  # (name, (col, func))
        if (len(spec) == 2 and isinstance(spec[0], str) and isinstance(spec[1], str)
                and not isinstance(spec, list)):
            return (spec[0], _transform_spec(spec[1], fn))  # (col, func) pair
        out = []
        for s in spec:
            if (isinstance(s, (list, tuple)) and len(s) == 2
                    and isinstance(s[1], str) and not isinstance(s[0], (list, tuple, dict))):
                out.append((s[0], _transform_spec(s[1], fn)))
            else:
                out.append(_transform_spec(s, fn))
        return out
    return spec


def _mean_split(spec):
    """Split a spec into sum- and count- variants (mean recombines as
    sum(sum)/sum(count) across chunks). Returns (has_mean, sum_spec,
    count_spec)."""
    has_mean = [False]

    def _to_sum(s):
        if s == "mean":
            has_mean[0] = True
            return "sum"
        return s

    def _to_count(s):
        return "count" if s == "mean" else s

    spec_sum = _transform_spec(spec, _to_sum)
    spec_count = _transform_spec(spec, _to_count)
    # pandas 3.0 rejects duplicate funcs in list/named specs (which the mean
    # transforms produce, e.g. mean+mean); duplicates carry no information.
    def _dedupe(s):
        if isinstance(s, list):
            out = []
            for x in s:
                if x not in out:
                    out.append(x)
            return out
        if isinstance(s, dict):
            return {c: _dedupe(v) for c, v in s.items()}
        return s
    return has_mean[0], _dedupe(spec_sum), _dedupe(spec_count)


def _spec_has_mean(spec):
    if isinstance(spec, str):
        return spec == "mean"
    if isinstance(spec, dict):
        return any(_spec_has_mean(v) for v in spec.values())
    if isinstance(spec, (list, tuple)):
        return any(_spec_has_mean(s) for s in spec)
    return False


def _combine_means(res_sum, res_count, spec):
    """Divide the sum-partials by the count-partials at the positions where the
    original spec asked for mean, keeping the original column layout. Columns
    explicitly requested as sum/count stay; the working columns behind a mean
    are dropped."""
    import pandas as _pd

    def _first(frame, key):
        c = frame[key]
        return c.iloc[:, 0] if isinstance(c, _pd.DataFrame) else c

    if isinstance(spec, str):
        return res_sum / res_count
    out = res_sum.copy()
    if isinstance(spec, dict):
        multi = isinstance(out.columns, _pd.MultiIndex)
        for col, s in spec.items():
            if _spec_has_mean(s):
                funcs = s if isinstance(s, list) else [s]
                if multi:
                    if "sum" in funcs:
                        keep = [True] * len(out.columns)
                        seen = False
                        for i, c in enumerate(out.columns):
                            if c == (col, "sum"):
                                if seen:
                                    keep[i] = False
                                seen = True
                        out = out.loc[:, keep]
                    out[(col, "mean")] = _first(res_sum, (col, "sum")) / _first(res_count, (col, "count"))
                    drops = []
                    if "sum" not in funcs and (col, "sum") in out.columns:
                        drops.append((col, "sum"))
                    if "count" not in funcs and (col, "count") in out.columns:
                        drops.append((col, "count"))
                    if drops:
                        out = out.drop(columns=drops)
                else:
                    if "sum" not in funcs:
                        out = out.drop(columns=[col])
                    out[col] = _first(res_sum, col) / _first(res_count, col)
        if multi:
            layout = [(c, f) for c, s in spec.items()
                      for f in (s if isinstance(s, list) else [s])
                      if (c, f) in out.columns]
        else:
            layout = [c for c in spec if c in out.columns]
        return out.reindex(columns=layout)
    if isinstance(res_sum.columns, _pd.MultiIndex):
        for col in res_sum.columns.get_level_values(0).unique():
            if "mean" in spec and (col, "sum") in out.columns and (col, "count") in res_count.columns:
                out[(col, "mean")] = _first(res_sum, (col, "sum")) / _first(res_count, (col, "count"))
                if "sum" not in spec and (col, "sum") in out.columns:
                    out = out.drop(columns=[(col, "sum")])
        cols0 = list(res_sum.columns.get_level_values(0).unique())
        layout = [(c, f) for c in cols0 for f in spec if (c, f) in out.columns]
        return out.reindex(columns=layout)
    for name, s in spec:
        if isinstance(s, (list, tuple)) and len(s) == 2 and s[1] == "mean":
            if "sum" not in s:
                out = out.drop(columns=[name])
            out[name] = res_sum[name] / res_count[name]
    return out


def _combine_safe(spec):
    if isinstance(spec, str):
        return spec in _COMBINE_SAFE
    if isinstance(spec, dict):
        return all(_combine_safe(v) for v in spec.values())
    if isinstance(spec, tuple):
        if len(spec) == 2 and isinstance(spec[0], str) and isinstance(spec[1], str):
            return spec[1] in _COMBINE_SAFE  # named pair (name, func)
        return all(_combine_safe(s) for s in spec)
    if isinstance(spec, list):
        for s in spec:
            if (isinstance(s, (list, tuple)) and len(s) == 2
                    and isinstance(s[1], str)
                    and not isinstance(s[0], (list, tuple, dict))):
                if s[1] not in _COMBINE_SAFE:
                    return False
            elif (isinstance(s, tuple) and len(s) == 2 and isinstance(s[0], str)
                    and isinstance(s[1], tuple) and len(s[1]) == 2
                    and isinstance(s[1][0], str) and isinstance(s[1][1], str)):
                if s[1][1] not in _COMBINE_SAFE:
                    return False
            elif not _combine_safe(s):
                return False
        return True
    return False


class _PartitionedGroupBy:
    """GroupBy over a _PartitionedFrame. agg() with combine-safe specs
    aggregates per chunk and combines at the origin (exact); anything else
    (median, std, nunique, custom funcs...) materializes the chunks first and
    runs the normal hash-shuffle groupby."""

    _NAMED = ("sum", "mean", "count", "size", "min", "max", "first", "last",
              "std", "var", "median", "nunique", "prod", "any", "all",
              "idxmin", "idxmax")

    def __init__(self, pf, by, **kw):
        self._pf = pf
        self._by = by
        self._kw = kw

    def agg(self, spec=None, *args, **kw):
        if spec is None:
            spec = dict(kw)
            kw = {}
        by = self._by
        if isinstance(by, str):
            by = [by]
        if (isinstance(by, list) and by
                and all(isinstance(b, str) and b in self._pf._cols for b in by)):
            if _combine_safe(spec):
                return self._pf._combine_agg(by, spec, args, kw, self._kw)
            _log("partitioned groupby: materializing (%s)" % spec)
            return self._pf._materialize().groupby(self._by, **self._kw).agg(spec, *args, **kw)
        _log("partitioned groupby: materializing (unsupported keys)")
        return self._pf._materialize().groupby(self._by, **self._kw).agg(spec, *args, **kw)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._NAMED:
            def _agg(*args, **kw):
                return self.agg(name, *args, **kw)
            return _agg
        raise AttributeError(name)

    def __getitem__(self, key):
        return _PartitionedGroupByCols(self, key if isinstance(key, list) else [key])


class _PartitionedGroupByCols:
    """gb["v"].mean() / gb[["a", "b"]].sum() over a _PartitionedGroupBy.
    Single-column selection squeezes to a Series like pandas does."""

    def __init__(self, gb, cols):
        self._gb = gb
        self._cols = cols

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in _PartitionedGroupBy._NAMED:
            def _agg(*args, **kw):
                res = self._gb.agg({c: name for c in self._cols}, *args, **kw)
                if len(self._cols) == 1:
                    return res.iloc[:, 0].rename(self._cols[0])
                return res
            return _agg
        raise AttributeError(name)


def _partitioned_merge(pf, right, how="inner", on=None, left_on=None,
                       right_on=None, left_index=False, right_index=False,
                       sort=False, suffixes=("_x", "_y"), **kw):
    """Merge a partitioned frame with a small in-RAM right frame: every chunk
    merges against the FULL right frame on its own node, so matching rows are
    found wherever they live. inner/left are exact (each left row exists in
    exactly one chunk); right/outer would duplicate right-only rows per chunk,
    so those materialize the left frame first (regular shuffle merge)."""
    import json
    import pickle
    import urllib.request
    import pandas as _pd
    if (how not in ("inner", "left")
            or not isinstance(right, _pd.DataFrame)
            or right.memory_usage(deep=False).sum() > 32 * 1024 * 1024):
        _log("partitioned merge: materializing left")
        return pf._materialize().merge(right, how=how, on=on, left_on=left_on,
                                       right_on=right_on, left_index=left_index,
                                       right_index=right_index, sort=sort,
                                       suffixes=suffixes, **kw)
    merge_kw = {"how": how, "on": on, "left_on": left_on, "right_on": right_on,
                "left_index": left_index, "right_index": right_index,
                "sort": False, "suffixes": suffixes}
    merge_kw.update(kw)
    import base64
    req = {
        "func_src": ("import pandas as _pd\n"
                     "def run(df):\n"
                     "    return _pd.merge(df, _right, **_merge_kw)\n"),
        "func_name": "run",
        "extra_b64": base64.b64encode(pickle.dumps({"_right": right,
                                                   "_merge_kw": merge_kw})).decode(),
        "items": list(range(len(pf._keys))), "starmap": False,
        "cache_keys": pf._keys, "no_split": True,
    }
    body = json.dumps(req).encode()
    req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                 headers={"Content-Type": "application/json",
                                          "X-Pipedpeer-Store": _STORE})
    with _daemon_open(req, 3600) as resp:
        rs = json.loads(resp.read())["results"]
    res = _pd.concat([pickle.loads(base64.b64decode(r["pickle"])) for r in rs],
                     ignore_index=True)
    if sort:
        if left_index or right_index:
            return res.sort_index()
        sc = left_on if left_on is not None else (right_on if right_on is not None else on)
        if isinstance(sc, str):
            sc = [sc]
        if sc:
            return res.sort_values(by=sc, kind="stable").reset_index(drop=True)
    return res


def _install_io():
    try:
        import pandas as _pd
    except ImportError:
        return

    _orig_read_csv = _pd.read_csv
    _orig_read_parquet = _pd.read_parquet
    _FORBIDDEN_CSV = {"skiprows", "skipfooter", "nrows", "usecols"}

    def _read_csv(path, *args, **kw):
        try:
            if (not args and not (_FORBIDDEN_CSV & set(kw))
                    and kw.get("header", "infer") in (0, None, "infer")
                    and _ooc_eligible(str(path))
                    and int(_NUM_SHARDS) >= 2):
                return _partitioned_read_csv(path, kw)
        except Exception as e:
            _log("ooc csv fallback (%s)" % e)
        return _orig_read_csv(path, *args, **kw)

    def _read_parquet(path, *args, **kw):
        try:
            if (not args and _ooc_eligible(str(path))
                    and int(_NUM_SHARDS) >= 2):
                return _partitioned_read_parquet(path, kw)
        except Exception as e:
            _log("ooc parquet fallback (%s)" % e)
        return _orig_read_parquet(path, *args, **kw)

    _pd.read_csv = _read_csv
    _pd.read_parquet = _read_parquet
    _log("pandas read_csv/read_parquet interception installed")


# ---- transparent PyTorch DDP (PIPEDPEER_DDP=1) ----
# pipedpeer run --ddp K starts K ranks, each with PIPEDPEER_RANK /
# PIPEDPEER_WORLD_SIZE / MASTER_ADDR / MASTER_PORT / PIPEDPEER_DDP=1 set, so
# plain single-process training code runs data-parallel with zero changes:
# the shim initialises the process group on first optimizer.step, averages the
# weights once, averages gradients every step, broadcasts buffers on first
# forward, and drives DistributedSampler epochs. Native torch DDP (the user
# wraps the model themselves) is detected and left to its own sync.
def _install_ddp():
    if os.environ.get("PIPEDPEER_DDP") != "1":
        return
    try:
        import torch as _th
    except ImportError:
        return
    import torch.distributed as _dist

    _NATIVE_DDP = []
    _WORLD = int(os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
    _RANK = int(os.environ.get("PIPEDPEER_RANK", "0"))
    # daemon (default): every sync is one POST to the lead rank's daemon on
    # the same port every other byte of pipedpeer traffic uses — no sockets
    # of our own, no MASTER_PORT, nothing new to firewall. gloo/nccl remain
    # as an escape hatch for clusters where torch's own mesh is preferable.
    _BACKEND = os.environ.get("PIPEDPEER_DDP_BACKEND", "daemon")
    _SYNC_URL = os.environ.get("PIPEDPEER_DDP_SYNC", "")
    _GROUP = os.environ.get("PIPEDPEER_DDP_GROUP", "ddp")
    # 1 = average gradients every step (exact DDP semantics). N>1 = local
    # SGD: train locally, average weights every Nth step — the knob for
    # high-latency links where a per-step round trip dominates.
    _SYNC_EVERY = max(1, int(os.environ.get("PIPEDPEER_DDP_SYNC_EVERY", "1")))
    _SEQ = [0]
    _STEPN = {}
    if _BACKEND == "daemon" and not _SYNC_URL:
        _BACKEND = "gloo"  # no sync endpoint handed down; old transport

    def _daemon_exchange(payload):
        import base64
        import json
        import urllib.request
        _SEQ[0] += 1
        body = json.dumps({"group": _GROUP, "seq": _SEQ[0], "rank": _RANK,
                           "world": _WORLD,
                           "data": base64.b64encode(payload).decode()}).encode()
        req = urllib.request.Request(_SYNC_URL, data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=240) as resp:
            out = json.loads(resp.read())
        if "blobs" not in out:
            raise RuntimeError("ddp sync failed: %s" % out)
        return [base64.b64decode(b) for b in out["blobs"]]

    def _daemon_allreduce(tensors):
        """Average tensors across ranks in place, through the daemon channel."""
        import pickle
        arrs = [t.detach().cpu().numpy() for t in tensors]
        blobs = _daemon_exchange(pickle.dumps(arrs, protocol=pickle.HIGHEST_PROTOCOL))
        peers = [pickle.loads(b) for b in blobs]
        for i, t in enumerate(tensors):
            acc = peers[0][i].astype("float64", copy=True)
            for pr in peers[1:]:
                acc += pr[i]
            acc /= len(peers)
            t.data.copy_(_th.from_numpy(acc.astype(arrs[i].dtype)).to(t.device))

    def _daemon_broadcast(tensors):
        """Overwrite tensors with rank 0's values, through the daemon channel."""
        import pickle
        arrs = [t.detach().cpu().numpy() for t in tensors]
        blobs = _daemon_exchange(pickle.dumps(arrs, protocol=pickle.HIGHEST_PROTOCOL))
        lead = pickle.loads(blobs[0])
        for i, t in enumerate(tensors):
            t.data.copy_(_th.from_numpy(lead[i]).to(t.device))
    _STEPPED = weakref.WeakSet()
    _ORIG_FWD = _th.nn.Module.forward
    _FWD = weakref.WeakSet()
    _ORIG_ITER = _th.utils.data.DataLoader.__iter__
    _EPOCHS = weakref.WeakKeyDictionary()

    def _init_group():
        backend = os.environ.get("PIPEDPEER_DDP_BACKEND", "gloo")
        if backend == "nccl" and not _th.cuda.is_available():
            backend = "gloo"
        # When the master is a Tailscale address (CGNAT 100.64.0.0/10), the
        # ranks can only reach each other through the tunnel — but gloo
        # advertises the default interface's LAN address, which the other
        # city cannot dial, so the mesh never forms. Pin gloo (and tensor
        # pipes) to the tunnel interface.
        master = os.environ.get("MASTER_ADDR", "")
        try:
            first, second = (int(x) for x in master.split(".")[:2])
            is_ts = first == 100 and 64 <= second <= 127
        except Exception:
            is_ts = False
        if is_ts:
            # if_nameindex works without /sys, which the sandbox doesn't mount.
            try:
                import socket as _sk
                names = [n for _, n in _sk.if_nameindex()]
            except Exception:
                names = []
            if "tailscale0" in names:
                os.environ.setdefault("GLOO_SOCKET_IFNAME", "tailscale0")
                os.environ.setdefault("TP_SOCKET_IFNAME", "tailscale0")
        # Bound the rendezvous: torch's default is 30 minutes, which turns a
        # firewalled master port into a silent infinite hang. Fail in minutes
        # with a traceback instead; PIPEDPEER_DDP_TIMEOUT overrides (seconds).
        from datetime import timedelta
        _timeout = timedelta(seconds=int(os.environ.get("PIPEDPEER_DDP_TIMEOUT", "120")))
        _dist.init_process_group(backend=backend, init_method="env://",
                                 rank=_RANK, world_size=_WORLD, timeout=_timeout)
        _log("ddp process group ready (rank %d/%d, %s)" % (_RANK, _WORLD, backend))

    # Patch at Optimizer.__init__ instead of Optimizer.step: the concrete
    # optimizers (SGD, Adam, ...) override step on their own classes, so a
    # class-level step patch would never fire. Wrapping the instance's own
    # step catches every optimizer.
    _ORIG_OPT_INIT = _th.optim.Optimizer.__init__

    def _opt_init(self, *args, **kw):
        _ORIG_OPT_INIT(self, *args, **kw)
        if _WORLD <= 1 or _NATIVE_DDP:
            return
        _orig_step = self.step

        def _step(*sa, **skw):
            if _BACKEND == "daemon":
                n = _STEPN.get(id(self), 0)
                _STEPN[id(self)] = n + 1
                params = [p for g in self.param_groups for p in g["params"]]
                if self not in _STEPPED:
                    _STEPPED.add(self)
                    _daemon_allreduce([p.data for p in params])
                    _log("ddp daemon-channel sync ready (rank %d/%d, every %d step%s)"
                         % (_RANK, _WORLD, _SYNC_EVERY, "" if _SYNC_EVERY == 1 else "s"))
                if _SYNC_EVERY == 1:
                    grads = [p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad
                             for p in params if p.grad is not None]
                    if grads:
                        _daemon_allreduce(grads)
                    return _orig_step(*sa, **skw)
                out = _orig_step(*sa, **skw)
                if (n + 1) % _SYNC_EVERY == 0:
                    _daemon_allreduce([p.data for p in params])
                return out
            if not _dist.is_initialized():
                _init_group()
            if self not in _STEPPED:
                _STEPPED.add(self)
                for g in self.param_groups:
                    for p in g["params"]:
                        _dist.all_reduce(p.data, op=_dist.ReduceOp.SUM)
                        p.data.div_(_WORLD)
            for g in self.param_groups:
                for p in g["params"]:
                    if p.grad is None:
                        continue
                    if p.grad.is_sparse:
                        p.grad = p.grad.coalesce().to_dense()  # ponytail: dense reduce; sparse values differ per rank
                    _dist.all_reduce(p.grad, op=_dist.ReduceOp.SUM)
                    p.grad.div_(_WORLD)
            return _orig_step(*sa, **skw)

        self.step = _step

    def _forward(self, *args, **kw):
        if _WORLD > 1 and not _NATIVE_DDP and self not in _FWD:
            if _BACKEND == "daemon":
                # Only sync once training is underway (mirrors the gloo
                # branch, which waits for the process group): the optimizer's
                # first step is the rendezvous.
                if _STEPPED:
                    _FWD.add(self)
                    bufs = [b for b in self._buffers.values()
                            if b is not None and b.is_floating_point() and b.numel()]
                    if bufs:
                        _daemon_broadcast(bufs)
            elif _dist.is_initialized():
                _FWD.add(self)
                for buf in self._buffers.values():
                    if buf is not None and buf.is_floating_point() and buf.numel():
                        _dist.broadcast(buf, src=0)
        return _ORIG_FWD(self, *args, **kw)

    def _iter(self):
        sampler = getattr(self, "sampler", None)
        if (_WORLD > 1 and not _NATIVE_DDP
                and isinstance(sampler, _th.utils.data.distributed.DistributedSampler)):
            sampler.set_epoch(_EPOCHS.get(self, 0))
            _EPOCHS[self] = _EPOCHS.get(self, 0) + 1
        return _ORIG_ITER(self)

    def _ddp_init(self, *a, **kw):
        _NATIVE_DDP.append(True)
        _log("native DistributedDataParallel detected; shim sync disabled")
        return _ORIG_DDP(self, *a, **kw)

    _ORIG_DDP = _th.nn.parallel.DistributedDataParallel.__init__
    _th.nn.parallel.DistributedDataParallel.__init__ = _ddp_init
    _th.optim.Optimizer.__init__ = _opt_init
    _th.nn.Module.forward = _forward
    _th.utils.data.DataLoader.__iter__ = _iter
    _log("ddp interception installed")


_PENDING_PATCHES = {}


class _PatchOnImport:
    """Patch a library the first time the job imports it.

    Importing torch costs ~1.7s. Paying that while the shim installs charged
    it to every job, including jobs that never mention torch, which is the
    opposite of the never-slower guarantee the shim exists to keep (and what
    scripts/bench-shim-d2.sh gates). Watching for the import instead means a
    library costs nothing until the script actually asks for it.
    """

    def find_spec(self, fullname, path=None, target=None):
        fns = _PENDING_PATCHES.get(fullname)
        if not fns:
            return None
        import importlib.util
        # Step aside while the real finders resolve the module, or find_spec
        # re-enters this method forever.
        sys.meta_path.remove(self)
        try:
            spec = importlib.util.find_spec(fullname)
        except Exception:
            spec = None
        finally:
            sys.meta_path.insert(0, self)
        if spec is None or getattr(spec, "loader", None) is None:
            return None
        if not hasattr(spec.loader, "exec_module"):
            return None
        _PENDING_PATCHES.pop(fullname, None)
        _inner = spec.loader.exec_module

        def exec_module(module):
            _inner(module)
            _run_patches(fullname, fns)

        spec.loader.exec_module = exec_module
        return spec


def _run_patches(name, fns):
    for fn in fns:
        try:
            fn()
        except Exception as exc:
            _log("%s interception unavailable: %s" % (name, exc))


def _defer(name, *fns):
    """Run fns now if name is already imported, else on its first import."""
    if name in sys.modules:
        _run_patches(name, fns)
        return
    _PENDING_PATCHES[name] = fns


def _install():
    if not _ENABLED:
        return
    sys.meta_path.insert(0, _PatchOnImport())
    _defer("numpy", _install_numpy)
    _defer("torch", _install_ddp, _install_torch)
    _defer("pandas", _install_pandas, _install_io)
    _defer("joblib", _install_joblib)
    import multiprocessing
    multiprocessing.Pool = _ClusterPool

    import concurrent.futures as _cf
    _cf.ProcessPoolExecutor = _ClusterPool  # has map/apply; close() present


def _install_joblib():
    try:
        import joblib.parallel as _jp
    except ImportError:
        return
    if not (_URL and _ENABLED) or int(_NUM_SHARDS) < 2:
        _log("joblib backend skipped (no cluster peers)")
        return

    import pickle
    class _SafePickler(pickle.Pickler):
        """Pickler subclass that replaces unpicklable _thread.lock objects
        with fresh threading.Lock() — the remote worker gets a usable lock.
        Safety net: shared-memory accumulator batches already run locally, but
        any other lock-bearing object that reaches dispatch still ships."""
        def reducer_override(self, obj):
            import _thread, threading
            if type(obj).__name__ == 'lock' and type(obj).__module__ == '_thread':
                return (threading.Lock, ())
            return NotImplemented

    def _jb_has_shared_accumulator(batch):
        # sklearn's _accumulate_prediction-style tasks ship as
        # (bound_method, X, all_proba_list, threading.Lock): they mutate the
        # shared list in place (the lock is the tell) and return None. Shipped
        # to a remote worker the mutation lands on a copy and is lost, so any
        # batch carrying a lock in its args must run in-process.
        for t in _jb_tasks(batch):
            if any(type(x).__module__ == "_thread" and type(x).__name__ == "lock"
                   for x in t[1]):
                return True
        return False

    def _jb_tasks(batch):
        # joblib submits BatchedCalls whose .items are (func, args, kwargs)
        # tuples; a bare callable is a single task.
        items = getattr(batch, "items", None)
        if items is None:
            return [(batch, (), {})]
        return items

    def _safe_pickle(obj):
        """Pickle an object, replacing unpicklable _thread.lock with fresh locks."""
        import io as _io
        buf = _io.BytesIO()
        _SafePickler(buf, protocol=pickle.HIGHEST_PROTOCOL).dump(obj)
        return buf.getvalue()

    def _jb_dispatch(batch):
        # One joblib batch per /v1/pool/map round trip. The runner marks
        # PIPEDPEER_JB_NESTED so worker-side joblib calls (RF fit inside a
        # GridSearchCV task) run inline instead of recursing into the mesh.
        # ponytail: sklearn passes X/y to every task, so the payload ships
        # once per task; a memmap/temp-folder pass (like loky) is the upgrade.
        import base64
        import json
        import pickle
        import urllib.request
        items = _jb_tasks(batch)
        payload = [base64.b64encode(_safe_pickle(t)).decode() for t in items]
        _log('joblib: dispatching %d tasks (%.0f KB) to cluster' % (len(items), sum(len(q) for q in payload) / 1024))
        req = {
            "func_src": ("import os\ndef run(item):\n"
                         "    os.environ['PIPEDPEER_JB_NESTED'] = '1'\n"
                         "    f, a, k = item\n"
                         "    return f(*a, **k)\n"),
            "func_name": "run",
            "items": payload,
            "items_b64": True,
            "starmap": False,
        }
        req["required_mem"] = 2 * sum(len(p) for p in payload)
        body = json.dumps(req).encode()
        req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                     headers={"Content-Type": "application/json",
                                              "X-Pipedpeer-Store": _STORE})
        with _daemon_open(req, 600) as resp:
            rs = json.loads(resp.read())["results"]
        return [pickle.loads(base64.b64decode(r["pickle"])) for r in rs]

    class _JBJob:
        """Future-like object satisfying joblib's retrieve_result contract."""
        __slots__ = ("results", "error")

        def __init__(self, results=None, error=None):
            self.results = results
            self.error = error

    class _PipedpeerBackend(_jp.ParallelBackendBase):
        supports_retrieve_callback = False
        supports_timeout = False
        supports_sharedmem = True
        supports_return_generator = True

        def effective_n_jobs(self, n_jobs):
            # The cluster is the "cpu count": batches are sized for the mesh.
            if n_jobs < 0:
                return int(_NUM_SHARDS)
            if n_jobs == 0:
                return os.cpu_count() or 1
            return n_jobs

        def submit(self, func, callback=None):
            if os.environ.get("PIPEDPEER_JB_NESTED") or not (_URL and _ENABLED):
                try:
                    job = _JBJob(results=[f(*a, **k) for f, a, k in _jb_tasks(func)])
                except BaseException as e:
                    job = _JBJob(error=e)
            else:
                if _jb_has_shared_accumulator(func):
                    # _accumulate_prediction-style tasks mutate a shared list
                    # guarded by the lock in their args; only in-process shared
                    # memory makes the mutation visible, so run them locally.
                    job = _JBJob(results=[f(*a, **k) for f, a, k in _jb_tasks(func)])
                else:
                    try:
                        job = _JBJob(results=_jb_dispatch(func))
                    except BaseException as e:
                        # D2/D3: a remote node adds capacity, never subtracts.
                        _log("joblib remote failed (%s); local covers it" % e)
                        try:
                            job = _JBJob(results=[f(*a, **k) for f, a, k in _jb_tasks(func)])
                        except BaseException as e2:
                            job = _JBJob(error=e2)
            if callback is not None:
                callback(job)
            return job

        def retrieve_result(self, job, timeout=None):
            if job.error is not None:
                raise job.error
            return job.results

    _ORIG_PARALLEL_INIT = _jp.Parallel.__init__

    def _parallel_init(self, *args, **kw):
        _ORIG_PARALLEL_INIT(self, *args, **kw)
        # sklearn forces prefer="threads" in most fit paths, which would
        # bypass a default backend entirely; force ours on any resolved
        # backend once there is real parallel work and we are not already
        # nested on a worker.
        if (os.environ.get("PIPEDPEER_JB_NESTED")
                or not (_URL and _ENABLED) or self.n_jobs in (0, 1)):
            return
        self._backend = _PipedpeerBackend(nesting_level=0)
        # Auto-batching sizes batches from measured duration, and against a
        # synchronous remote submit it never grows past one task — a 1-item
        # chunk is unsplittable, so nothing fans out. Under force, pin a batch
        # size worth splitting.
        if _FORCE and self.batch_size == 'auto':
            self.batch_size = max(2, 4 * int(_NUM_SHARDS))

    try:
        _jp.register_parallel_backend(
            "pipedpeer", lambda *a, **k: _PipedpeerBackend(nesting_level=0), make_default=True)
        _jp.Parallel.__init__ = _parallel_init
        _log("joblib backend 'pipedpeer' installed as default")
    except Exception as e:
        _log("joblib backend installation failed: %s" % e)
_install()
`

// WriteShim returns the sitecustomize.py content and whether interception is
// enabled for a run.
func WriteShim(enabled bool) string {
	if !enabled {
		return ""
	}
	return ShimSitecustomize
}
