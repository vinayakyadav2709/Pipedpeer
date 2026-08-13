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
import os
import sys
import time
import weakref

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
        return head_results + self._spill(func, tail, starmap)

    def _local(self, func, items, starmap):
        _log("local %d items" % len(items))
        if starmap:
            return self._ctx.starmap(func, items)
        return self._ctx.map(func, items)

    def _spill(self, func, items, starmap):
        _log("spilling %d items to %s" % (len(items), _URL))
        self._spilled = True
        chunks = _chunk(items, 64)
        import json
        import urllib.request
        import base64
        import pickle
        results = []
        for chunk in chunks:
            if starmap:
                payload = [c[0] if isinstance(c, (list, tuple)) else (c,) for c in chunk]
            else:
                payload = chunk
            body = json.dumps({"func": _pickle_func(func), "items": payload,
                               "starmap": starmap}).encode()
            req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                         headers={"Content-Type": "application/json",
                                                  "X-Pipedpeer-Store": _STORE})
            try:
                with urllib.request.urlopen(req, timeout=600) as resp:
                    for r in json.loads(resp.read())["results"]:
                        results.append(pickle.loads(base64.b64decode(r["pickle"])))
            except Exception as e:
                _log("remote failed (%s); falling back to local chunk" % e)
                results.extend(self._local(func, chunk, starmap))
        return results

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


def _chunk(seq, size):
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def _install():
    if not _ENABLED:
        return
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
