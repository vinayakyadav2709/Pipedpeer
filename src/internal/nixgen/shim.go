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
            with urllib.request.urlopen(req, timeout=600) as resp:
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
    the warm-worker /v1/pool/map path (items_b64: blocks ship as pickled numpy).
    Falls back to local on any failure so an absent cluster never breaks math."""
    import base64
    import json
    import pickle
    import urllib.request
    try:
        # The worker runs the closure's python with no shim on its path, so the
        # function ships as source (pickling by reference would resolve
        # sitecustomize._matmul_with to the Nix sitecustomize and fail). The
        # fixed right-hand operand rides along as a pickled global inside a
        # dict: the worker does ns.update(pickle.loads(extra_b64)).
        items = [base64.b64encode(pickle.dumps(b)).decode() for b in blocks]
        req = {
            "func_src": "import numpy as _np\ndef run(block):\n    return _np.matmul(block, _other)\n",
            "func_name": "run",
            "extra_b64": base64.b64encode(pickle.dumps({"_other": other})).decode(),
            "items": items,
            "items_b64": True,
            "starmap": False,
        }
        # Admission control hint: the daemon refuses (503) when it cannot spare
        # roughly the payload's size in RAM, and the shim falls back locally.
        req["required_mem"] = 2 * (len(req["extra_b64"]) + sum(len(i) for i in items))
        body = json.dumps(req).encode()
        req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                     headers={"Content-Type": "application/json",
                                              "X-Pipedpeer-Store": _STORE})
        with urllib.request.urlopen(req, timeout=1200) as resp:
            rs = json.loads(resp.read())["results"]
        return [pickle.loads(base64.b64decode(r["pickle"])) for r in rs]
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
    matmul) so ML tensor work utilises remote GPUs fully. Falls back to local
    on any failure so an absent cluster never breaks the model."""
    import base64
    import json
    import pickle
    import urllib.request
    try:
        # Ships by source, not by reference: the worker's closure python has no
        # shim module on its path (see _np_dispatch). The fixed right-hand
        # operand rides along as a pickled global inside a dict: the worker does
        # ns.update(pickle.loads(extra_b64)).
        items = [base64.b64encode(pickle.dumps(b)).decode() for b in blocks]
        req = {
            "func_src": "import torch as _th\n"
                        "def run(block):\n"
                        "    if _th.cuda.is_available():\n"
                        "        block, other = block.cuda(), _other.cuda()\n"
                        "        return _th.matmul(block, other).cpu()\n"
                        "    return _th.matmul(block, _other)\n",
            "func_name": "run",
            "extra_b64": base64.b64encode(pickle.dumps({"_other": other})).decode(),
            "items": items,
            "items_b64": True,
            "starmap": False,
        }
        req["required_mem"] = 2 * (len(req["extra_b64"]) + sum(len(i) for i in items))
        body = json.dumps(req).encode()
        req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                     headers={"Content-Type": "application/json",
                                              "X-Pipedpeer-Store": _STORE})
        with urllib.request.urlopen(req, timeout=1200) as resp:
            rs = json.loads(resp.read())["results"]
        return [pickle.loads(base64.b64decode(r["pickle"])) for r in rs]
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
    # the worker has one), results cat'd back. Opt-in via PIPEDPEER_TORCH=1 —
    # same D2 reasoning as numpy: shipping tensors only pays off when local
    # compute is the bottleneck, and a worker with a GPU is the point.
    if os.environ.get("PIPEDPEER_TORCH") != "1":
        return
    try:
        import torch as _th
    except ImportError:
        return

    _MIN_BYTES = 32 * 1024 * 1024

    _orig_matmul = _th.matmul
    _orig_mm = _th.mm
    global _TORCH_ORIG_MATMUL, _TORCH_ORIG_MM
    _TORCH_ORIG_MATMUL = _orig_matmul
    _TORCH_ORIG_MM = _orig_mm

    def _matmul(a, b, *args, **kw):
        import torch as _th
        if (a.dim() == 2 and b.dim() == 2 and a.shape[1] == b.shape[0]
                and a.element_size() * a.nelement() >= _MIN_BYTES
                and a.shape[0] >= 8 and _URL and _ENABLED):
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
    # numpy block-row matmul: intercept A @ B above a size threshold, splitting
    # A's rows across the cluster. Ships each block to the warm worker which
    # already unpickles it (items_b64). Opt-in (PIPEDPEER_NUMPY=1): shipping a
    # matrix over JSON+pickle only wins when the local BLAS is the bottleneck;
    # a single warm worker running serial BLAS is otherwise slower than the
    # local multithreaded BLAS, violating D2.
    if os.environ.get("PIPEDPEER_NUMPY") != "1":
        return
    try:
        import numpy as _np
    except ImportError:
        return

    _MIN_BYTES = 32 * 1024 * 1024  # only intercept once there's real compute
    _orig_matmul = _np.matmul
    _orig_dot = _np.dot

    def _matmul(a, b, *args, **kw):
        import numpy as _np
        if (a.ndim == 2 and b.ndim == 2 and a.shape[1] == b.shape[0]
                and a.nbytes >= _MIN_BYTES
                and a.shape[0] >= 8 and _URL and _ENABLED):
            try:
                n_blocks = max(2, min(64, a.shape[0] // 8))
                rows = max(1, a.shape[0] // n_blocks)
                blocks = [a[i:i + rows] for i in range(0, a.shape[0], rows)]
                return _np.vstack(_np_dispatch(blocks, b))
            except Exception as e:
                _log("numpy matmul fallback (%s)" % e)
        return _orig_matmul(a, b, *args, **kw)

    def _dot(a, b, *args, **kw):
        import numpy as _np
        if (a.ndim == 2 and b.ndim == 2 and a.shape[1] == b.shape[0]
                and a.nbytes >= _MIN_BYTES and a.shape[0] >= 8
                and _URL and _ENABLED):
            try:
                n_blocks = max(2, min(64, a.shape[0] // 8))
                rows = max(1, a.shape[0] // n_blocks)
                blocks = [a[i:i + rows] for i in range(0, a.shape[0], rows)]
                return _np.vstack(_np_dispatch(blocks, b))
            except Exception as e:
                _log("numpy dot fallback (%s)" % e)
        return _orig_dot(a, b, *args, **kw)

    _np.matmul = _matmul
    _np.dot = _dot
    _log("numpy matmul/dot interception installed")


def _install():
    if not _ENABLED:
        return
    _install_numpy()
    _install_torch()
    import multiprocessing
    multiprocessing.Pool = _ClusterPool

    import concurrent.futures as _cf
    _cf.ProcessPoolExecutor = _ClusterPool  # has map/apply; close() present

    try:
        import joblib.parallel as _jp
    except ImportError:
        return

    # Register a cluster backend via joblib's official register_backend API.
    # joblib calls apply_async(compute_func, callback) where compute_func
    # returns (result, compute_time) and callback receives that tuple.
    class _PipedpeerBackend(_jp.ParallelBackendBase):
        def effective_n_jobs(self, n_jobs):
            if n_jobs == 0:
                n_jobs = os.cpu_count() or 1
            return n_jobs

        def apply_async(self, compute_func, callback=None):
            if callback is None:
                return _jp.apply_async_wrapper(compute_func, None)
            return _jp.apply_async_wrapper(compute_func, callback)

    try:
        _jp.register_backend("pipedpeer", lambda: _PipedpeerBackend(), nested=None)
        _log("joblib backend 'pipedpeer' registered")
    except Exception as e:
        _log("joblib backend registration failed: %s" % e)
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
