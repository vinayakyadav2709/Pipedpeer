package nixgen

// ShimSitecustomize is a sitecustomize.py auto-imported by Python before the
// user's first line (sitecustomize is always imported at interpreter startup).
// It patches the parallel primitives to route through the local pipedpeer
// daemon — no code changes required on the user's script.
//
// The design (see handoff.md §4/D1): interception only, no SDK. It patches
// multiprocessing.Pool, concurrent.futures.ProcessPoolExecutor and
// joblib.Parallel, leaving thread pools alone (shared memory).
//
// The Python is split across shim.go, shim_pool.go and shim_ddp.go so separate
// people can edit the pool, DDP and core parts; the pieces concatenate, in
// this order, into one sitecustomize.py. Each piece starts at a top-level
// Python statement. None of them may contain a backtick.
const ShimSitecustomize = shimHead + shimPoolA + shimOffload + shimPoolB + shimDDPA + shimData + shimDDPB + shimInstall

// shimHead: imports, env, logging, receipts, submit batcher.
const shimHead = `# Auto-imported by Python at startup when on PYTHONPATH.
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
# run --offload: distributed work stays off the machine whose key
# PIPEDPEER_OFFLOAD names (the submitter's). The key rides on every pool
# request; the daemon drops that machine's peers, and takes no share itself
# when it runs there. When this job runs on that machine itself (--offload
# without --remote) the shim keeps no share either: every cost model ships,
# and anything that still has to run here says why. The key is the kernel's
# boot id, the daemon's machine key; a sandbox shares the host's kernel.
_OFFLOAD = os.environ.get("PIPEDPEER_OFFLOAD", "")


def _offload_here():
    if not _OFFLOAD:
        return False
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip() == _OFFLOAD
    except OSError as e:
        # Unknown: keep the promise rather than guess this is elsewhere.
        sys.stderr.write("[pipedpeer] --offload: cannot read this machine's key (%s); "
                         "treating it as the machine to keep work off\n" % e)
        return True


_OFFLOAD_HERE = _offload_here()
_FORCE = _FORCE or _OFFLOAD_HERE


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
# Dispatch floor for Pool work: a tail carrying less compute than this is not
# worth a round trip and stays local. This used to be decided by accident —
# an over-large chunk left chunks[1::2] empty, so nothing shipped and nothing
# said so. It is now a stated rule with a log line behind it.
_POOL_MIN_WORK = float(os.environ.get("PIPEDPEER_POOL_MIN_WORK", "0.5"))
# How many chunks may be in flight to the cluster at once.
_REMOTE_INFLIGHT = max(1, int(os.environ.get("PIPEDPEER_REMOTE_INFLIGHT", "4")))
# Where the job was submitted from (host:port of the submitter's daemon).
# The executing node sinks this peer to the end of its spill order so an
# idle orchestrator never outranks real workers; empty when unset.
_SUBMITTER = os.environ.get("PIPEDPEER_SUBMITTER", "")
# Shared secret for the daemon API. Empty when the daemon is open; when it is
# not, an intercepted call without it 401s and the work quietly runs here.
_TOKEN = os.environ.get("PIPEDPEER_TOKEN", "")


def _log(msg):
    if _ENABLED:
        sys.stderr.write("[pipedpeer] " + msg + "\n")


_OFFLOAD_SAID = set()


def _offload_warn(what, why):
    """--offload promised this machine computes no distributed work; when
    some has to run here anyway, say so once per reason, never fail the run."""
    if not _OFFLOAD_HERE or why in _OFFLOAD_SAID:
        return
    _OFFLOAD_SAID.add(why)
    sys.stderr.write("[pipedpeer] warning: --offload: %s ran on this machine: %s\n"
                     % (what, why))


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


def _cluster_ready():
    """True when there is a cluster to spill to. NUM_SHARDS counts this node
    too, so "1" means there is nobody: dispatching then posts work to our own
    daemon, which runs it through a single serialised warm worker while the
    local pool sits idle - strictly slower than doing it here."""
    try:
        shards = int(_NUM_SHARDS)
    except ValueError:
        shards = 0
    ready = bool(_URL) and _ENABLED and shards >= 2
    if not ready:
        _offload_warn("distributed work", "no other machine is in the cluster")
    return ready


# How long the submit queue must sit idle before it is sent as one batch. A
# submit loop queues its items microseconds apart, so the whole loop lands in
# one batch; a lone submit waits this long before it runs.
_SUBMIT_IDLE = 0.02
_BATCH_FUTURE = []


def _batch_future(batcher):
    """A real concurrent.futures.Future whose result() sends the queued batch
    at once instead of waiting out the idle window. wait() and as_completed()
    read the future's state directly, so the idle window is what sends theirs.
    The class is built on first use: importing concurrent.futures at startup
    would charge every job for it."""
    if not _BATCH_FUTURE:
        import concurrent.futures as _cf

        class Future(_cf.Future):
            def result(self, timeout=None):
                self._pp_batcher.flush()
                return super().result(timeout)

            def exception(self, timeout=None):
                self._pp_batcher.flush()
                return super().exception(timeout)

        _BATCH_FUTURE.append(Future)
    fut = _BATCH_FUTURE[0]()
    fut._pp_batcher = batcher
    return fut


class _BatchResult:
    """multiprocessing's AsyncResult over a batched future."""

    def __init__(self, fut):
        self._f = fut

    def ready(self):
        return self._f.done()

    def successful(self):
        if not self._f.done():
            raise ValueError("%r not ready" % self)
        return self._f.exception() is None

    def wait(self, timeout=None):
        import concurrent.futures as _cf
        self._f._pp_batcher.flush()
        _cf.wait([self._f], timeout)

    def get(self, timeout=None):
        try:
            return self._f.result(timeout)
        except TimeoutError:
            import multiprocessing
            raise multiprocessing.TimeoutError


def _settle(entry, result, err):
    """Fire the call's callback, then complete its future - the order
    multiprocessing uses, so get() never returns before the callback ran."""
    fn, _args, fut, callback, error_callback = entry
    try:
        if err is None:
            if callback is not None:
                callback(result)
        elif error_callback is not None:
            error_callback(err)
    except Exception as e:
        _log("callback for %r raised %r; the result is still delivered"
             % (getattr(fn, "__name__", "?"), e))
    if fut.done():
        return
    if err is None:
        fut.set_result(result)
    else:
        fut.set_exception(err)


class _SubmitBatcher:
    """Turns single calls (executor.submit, Pool.apply_async) into maps.

    One call has no tail to spill, so these used to run local only, and a
    loop of them - a map in disguise - never left the machine. Calls are
    queued; once the queue has been idle for _SUBMIT_IDLE, or someone asks
    for a result, they are grouped by function and each group goes through
    _ClusterPool._run as a starmap, where the usual measurement and cost
    gate decide what ships. A call that raises comes back as a _Raised
    value, so its exception lands on its own future and no other call runs
    twice; only a group that fails outside its calls is re-run call by call.
    """

    def __init__(self, pool):
        self._pool = pool
        self._cv = threading.Condition()
        self._q = []          # (fn, args, future, callback, error_callback)
        self._now = False
        self._busy = 0
        self._last = 0.0
        self._thread = None

    def submit(self, fn, args, callback=None, error_callback=None):
        fut = _batch_future(self)
        with self._cv:
            self._q.append((fn, tuple(args), fut, callback, error_callback))
            self._last = time.monotonic()
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._loop, name="pipedpeer-submit", daemon=True)
                self._thread.start()
            self._cv.notify_all()
        return fut

    def flush(self):
        with self._cv:
            if self._q:
                self._now = True
                self._cv.notify_all()

    def drain(self):
        """Send what is queued and wait until every batch has finished."""
        if threading.current_thread() is self._thread:
            return            # a callback closing its own pool
        with self._cv:
            if self._q:
                self._now = True
                self._cv.notify_all()
            while self._q or self._busy:
                self._cv.wait()

    def _loop(self):
        while True:
            with self._cv:
                while not self._q:
                    self._cv.wait()
                while not self._now:
                    left = self._last + _SUBMIT_IDLE - time.monotonic()
                    if left <= 0:
                        break
                    self._cv.wait(left)
                batch, self._q, self._now = self._q, [], False
                self._busy += 1
            try:
                self._dispatch(batch)
            except BaseException as e:
                # Never leave a caller blocked on a future nobody will set.
                _log("submitted batch of %d calls failed outside the calls "
                     "themselves (%r)" % (len(batch), e))
                for entry in batch:
                    if not entry[2].done():
                        _settle(entry, None, e)
            finally:
                with self._cv:
                    self._busy -= 1
                    self._cv.notify_all()

    def _dispatch(self, batch):
        groups = {}
        for entry in batch:
            if entry[2].set_running_or_notify_cancel():
                groups.setdefault(id(entry[0]), []).append(entry)
        for group in groups.values():
            fn = group[0][0]
            try:
                results = self._pool._run(fn, [e[1] for e in group], starmap=True,
                                          catch=True)
            except Exception as e:
                _log("batch of %d %r calls raised (%r); re-running each "
                     "locally so the error lands on its own call"
                     % (len(group), getattr(fn, "__name__", "?"), e))
                self._one_by_one(fn, group)
                continue
            for entry, r in zip(group, results):
                if isinstance(r, _Raised):
                    _settle(entry, None, r.exc)
                else:
                    _settle(entry, r, None)

    def _one_by_one(self, fn, group):
        ctx = self._pool._local_ctx(fn)
        _record_local(len(group))
        pending = [(entry, ctx.apply_async(fn, entry[1])) for entry in group]
        for entry, ar in pending:
            try:
                r = ar.get()
            except Exception as e:
                _settle(entry, None, e)
            else:
                _settle(entry, r, None)


`

// shimOffload: numpy/torch matmul offload installs.
const shimOffload = `def _np_dispatch(blocks, other):
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
        if not (a.ndim == 2 and b.ndim == 2 and a.shape[1] == b.shape[0]
                and a.shape[0] >= 8 and _URL and _ENABLED
                and not os.environ.get("PIPEDPEER_NUMPY_NESTED")):
            return False
        # What the product needs here: both operands, the result, and the
        # copies made while dispatching. Measured at roughly eight times the
        # arrays for the forced path, which is what took a 14 GB machine to
        # 194 MB free.
        working = int((a.nbytes + b.nbytes + a.shape[0] * b.shape[1] * a.itemsize) * 8)
        return _numpy_should_offload(a.nbytes, max(8, a.shape[0] // 8), 5, True,
                                     200e9, working, op="numpy.matmul")

    def _mm_no_fallback(a, b, err):
        """Refuse rather than fall back, when falling back means the OOM killer.

        Every other failure here ends in a local run, which is right: a
        cluster that cannot take the work is not a reason to fail. But when
        the operands do not fit in this machine's memory, the local run is not
        a slower answer - it is the thing the offload existed to avoid, and it
        takes the machine down with it. Better to say so, with the numbers and
        the flag that changes them.
        """
        working = int((a.nbytes + b.nbytes
                       + a.shape[0] * b.shape[1] * a.itemsize) * 8)
        if _fits_locally(working):
            return
        raise MemoryError(
            "this %s x %s product needs about %.1f GB and this machine has "
            "%.1f GB available, so it cannot run here - and the cluster could "
            "not take it either (%s). Give a peer more memory, or run it on "
            "fewer rows at a time."
            % (a.shape, b.shape, working / 1e9,
               _local_free_bytes() / 1e9, err))

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
                _mm_no_fallback(a, b, e)
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

    def _linalg_offload(name, a, args, kw, deadline=1200):
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
        blobs = _pool_send_within(header, globals_pickle, items, deadline)
        return pickle.loads(blobs[0])

    def _linalg_deadline(a):
        # Past the time a peer like this machine needs (ship it, compute it,
        # ship U/S/Vh back), the offload has already lost; half again as
        # slack, and never under 30 s.
        est_local = a.nbytes * max(8, a.shape[0] // 4) / 1.5e9
        bw = _per_peer_bw(_BW_CACHE["bw"] or _OPTIMISTIC_BW)
        return max(_LINALG_MIN_DEADLINE, 1.5 * (est_local + a.nbytes * 3 / bw))

    def _linalg(name):
        orig = _orig_linalg[name]
        def wrapped(a, *args, **kw):
            import numpy as _np
            if (a.ndim == 2 and _URL and _ENABLED
                    and not os.environ.get("PIPEDPEER_NUMPY_NESTED")
                    and _numpy_should_offload(a.nbytes, max(8, a.shape[0] // 4), 3, False, 1.5e9,
                                              op="numpy.linalg." + name)):
                try:
                    return _linalg_offload(name, a, args, kw, _linalg_deadline(a))
                except Exception as e:
                    _log("numpy %s fallback (%s); running it here" % (name, e))
            return orig(a, *args, **kw)
        return wrapped

    _np.matmul = _matmul
    _np.dot = _dot
    _np.tensordot = _tensordot
    _np.linalg.svd = _linalg("svd")
    _np.linalg.eig = _linalg("eig")
    _log("numpy matmul/dot/tensordot/svd/eig interception installed")


`

// shimData: memory helpers, numpy offload cost model, pandas shuffle, out-of-core reads.
const shimData = `def _local_free_bytes():
    """What this machine could give a new allocation.

    MemAvailable, not free pages: the kernel reclaims page cache on demand,
    so a box with a warm cache reads several times too low and every limit
    built on it collapses. PIPEDPEER_TEST_FREE_MEM overrides it, which is how
    the out-of-memory branch is tested without actually running a machine out
    of memory.
    """
    override = os.environ.get("PIPEDPEER_TEST_FREE_MEM")
    if override:
        try:
            return int(float(override))
        except ValueError:
            pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def _fits_locally(working_set):
    """Whether an operation's working set has room on this machine.

    Nine tenths of what is available, because an allocation that exactly fills
    memory leaves the machine thrashing rather than working, and because the
    estimate is an estimate.
    """
    if working_set <= 0:
        return True
    free = _local_free_bytes()
    if free <= 0:
        return True          # cannot tell; do not invent a refusal
    return working_set <= free * 0.9


_GPU_PEERS = {"t": 0.0, "any": None}


def _cluster_has_gpu():
    """Whether any peer offers an accelerator, cached like the bandwidth probe.

    The cost model prices remote compute at this cluster's CPU BLAS. A peer
    with a GPU is not that: measured on this hardware, the same model and
    batch took 1.5s on the GPU against 56.2s on the CPU. Charging CPU rates
    for a GPU peer refuses offloads that would have paid for themselves many
    times over.
    """
    now = time.monotonic()
    if now - _GPU_PEERS["t"] < _BW_TTL:
        return _GPU_PEERS["any"]
    _GPU_PEERS["t"] = now
    _GPU_PEERS["any"] = False
    if not _URL:
        return False
    try:
        import json as _json
        import urllib.request
        req = urllib.request.Request(_URL.rstrip("/") + "/v1/nodes")
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        with urllib.request.urlopen(req, timeout=3) as resp:
            nodes = _json.loads(resp.read())
        for n in nodes:
            if n.get("state") != "healthy" or n.get("source") == "self":
                continue
            load = n.get("load") or {}
            caps = n.get("capabilities") or {}
            if load.get("gpus") or load.get("gpu_model") or caps.get("gpu"):
                _GPU_PEERS["any"] = True
                break
    except Exception:
        pass
    return _GPU_PEERS["any"]


# How much faster a remote GPU is than this cluster's CPU BLAS for the shapes
# that reach the cost model. Measured on the RTX 3060 here: 37x at a
# saturating batch, 6x at a toy one. The smaller figure is used, because
# claiming the larger one for a shape that will not saturate the device is how
# a cost model talks itself into a transfer that does not pay.
_GPU_SPEEDUP = 6.0


def _numpy_should_offload(nbytes, flops_per_byte, round_trip, split,
                          flops_per_sec, working_set=0, op="numpy"):
    """_numpy_offload_why, with its decision recorded in the receipt when the
    work was big enough to be considered at all."""
    _WHY["v"] = ""
    r = _numpy_offload_why(nbytes, flops_per_byte, round_trip, split,
                           flops_per_sec, working_set)
    if _WHY["v"]:
        _decide(op, r, _WHY["v"], nbytes)
    return r


def _numpy_offload_why(nbytes, flops_per_byte, round_trip, split,
                       flops_per_sec, working_set=0):
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
    if K < 2:
        _offload_warn("distributed work", "no other machine is in the cluster")
    if K < 2 or nbytes < _spill_min():
        return False
    if _FORCE:
        _WHY["v"] = "forced (PIPEDPEER_DISTRIBUTE=force)"
        return True

    # Does it fit here at all? Everything below compares how long local takes
    # against how long remote takes, which quietly assumes local is possible.
    # When the working set does not fit, local is not slower - it is the OOM
    # killer, or hours of swap. Shipping is then the only answer that runs,
    # whatever the arithmetic says about speed.
    if not _fits_locally(working_set):
        _log("%.0f MB working set against %.0f MB free: this does not fit here, "
             "so it goes to the cluster whatever the transfer costs"
             % (working_set / 1e6, _local_free_bytes() / 1e6))
        _WHY["v"] = ("%.0f MB working set does not fit in %.0f MB free here"
                     % (working_set / 1e6, _local_free_bytes() / 1e6))
        return True

    # Deciding is not free: the bandwidth probe ships 64 MB. A script doing one
    # 33 MB matmul spent 2.5 s on a LAN measuring the link, to conclude it
    # should not use it - and on the phone-tethered link this project also runs
    # over, 64 MB is half a minute. The probe cost more than the work it was
    # declining.
    #
    # It is also avoidable. Bandwidth only ever appears in est_transfer, and
    # only as a divisor, so a lower measurement can only make shipping look
    # worse. Ask first at a bandwidth no real link will beat: if the answer is
    # still "stay local" there, it is "stay local" at the real speed too, and
    # there is nothing to measure.
    # A peer with an accelerator is not this cluster's CPU BLAS, and pricing
    # it as though it were refuses offloads that would pay for themselves.
    remote_flops = flops_per_sec
    if _cluster_has_gpu():
        remote_flops = flops_per_sec * _GPU_SPEEDUP

    if not _offload_wins(nbytes, flops_per_byte, round_trip, split,
                         flops_per_sec, K, _OPTIMISTIC_BW, remote_flops):
        _WHY["v"] += " (even on a 10 Gbit/s link)"
        return False
    bw = _measure_bandwidth()
    if not bw:
        _WHY["v"] = "could not measure the link to the cluster"
        return False
    if not split:
        bw = _per_peer_bw(bw)
    return _offload_wins(nbytes, flops_per_byte, round_trip, split,
                         flops_per_sec, K, bw, remote_flops)


def _per_peer_bw(bw):
    """What one peer gets of the measured bandwidth.

    The probe's 8 items spread over up to 8 peers in parallel, so it measures
    the cluster's aggregate; a single-worker offload (svd, eig) uses one link.
    In the V1 rehearsal 4 peers measured 12.8 MB/s together while one carried
    0.8, and the svd went out on the aggregate figure."""
    return bw / max(1, min(8, int(_NUM_SHARDS) - 1))


# Faster than any link this runs over: 10 Gbit/s. Used only to rule offloads
# out without measuring, so being generous costs a probe, never a wrong answer.
_OPTIMISTIC_BW = 1.25e9


def _offload_wins(nbytes, flops_per_byte, round_trip, split, flops_per_sec, K, bw,
                  remote_flops_per_sec=None):
    """Whether offloading beats local at this bandwidth.

    Local and remote are priced separately: the same work runs at this
    machine's rate here and at the peer's rate there, and on a cluster with an
    accelerator those are not the same number.
    """
    if remote_flops_per_sec is None or remote_flops_per_sec <= 0:
        remote_flops_per_sec = flops_per_sec
    est_local = nbytes * flops_per_byte / flops_per_sec
    est_transfer = nbytes * round_trip / bw
    _WHY["v"] = "here ~%.2fs, transfer ~%.2fs" % (est_local, est_transfer)
    if not split:
        # Single-worker offload (svd, eig) is not a race against local: it
        # relocates the work so a weak orchestrator is freed entirely, and is
        # worth it when shipping the matrix costs far less than computing it
        # here. Charging the remote compute against it as well made this
        # condition unsatisfiable - est_remote equals est_local on a cluster
        # of like machines, so it read "transfer + local < half of local".
        return est_transfer < est_local * 0.5
    est_remote = (nbytes * flops_per_byte / remote_flops_per_sec) / K * 1.3
    _WHY["v"] = ("here ~%.2fs, there ~%.2fs transfer + %.2fs compute on %d nodes"
                 % (est_local, est_transfer, est_remote, K))
    return est_local > est_transfer + est_remote


def _groupby_worker_src():
    return ("import pandas as _pd\n"
            "def run(sub):\n"
            "    return sub.groupby(_by, as_index=_as_index, sort=False, dropna=_dropna, observed=True).agg(_spec, *_args, **_kwargs)\n")


def _groupby_observed(gb):
    """The observed= a groupby really runs with. pandas before 2.1 defaults
    to False, 2.1-2.x stores no_default and treats it as False, 3.x defaults
    to True; an explicit True or False from the call wins."""
    obs = getattr(gb, "observed", False)
    if obs is True or obs is False:
        return obs
    import pandas as _pd
    return int(_pd.__version__.split(".")[0]) >= 3


def _add_unobserved(df, keys, as_index, dropna, spec, args, kw, res):
    """Add the category combinations no row has, as observed=False shows them.

    Every partial runs with observed=True: with observed=False each node
    would emit every category and the concat would repeat them. The missing
    rows come from aggregating an empty frame of the same dtypes, which
    yields every combination with its empty-group value (0 for sum and
    count, NaN for mean) - the value pandas itself gives an empty group.

    A plain key beside a categorical one contributes its observed values to
    the product. An empty frame has none, so such a key is made categorical
    over its observed values for the empty aggregation and cast back after."""
    import pandas as _pd
    e = df.iloc[0:0].copy()
    plain = {}
    for k in keys:
        if not isinstance(df[k].dtype, _pd.CategoricalDtype):
            if not dropna and df[k].isna().any():
                raise ValueError("NaN in a plain key beside a categorical one")
            plain[k] = df[k].dtype
            e[k] = _pd.Categorical([], categories=df[k].dropna().unique())
    empty = e.groupby(keys, as_index=True, sort=False, dropna=dropna,
                      observed=False).agg(spec, *args, **kw)
    if plain:
        empty.index = _pd.MultiIndex.from_arrays(
            [empty.index.get_level_values(k).astype(plain[k]) if k in plain
             else empty.index.get_level_values(k) for k in keys], names=keys)
    if not as_index:
        res = res.set_index(keys)
        if not isinstance(empty, _pd.DataFrame):      # size() and the like
            empty = empty.to_frame(res.columns[0])
    missing = empty[~empty.index.isin(res.index)]
    if len(missing):
        res = _pd.concat([res, missing])
    if not as_index:
        res = res.reset_index()
    return res


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
    # --offload ships bucket 0 too: this machine keeps no share.
    first = 0 if _OFFLOAD_HERE else 1
    local = df[buckets == 0]
    if first and len(local):
        parts.append(local.groupby(keys, as_index=as_index, sort=False,
                                   dropna=dropna, observed=True).agg(spec, *args, **kw))
    remote = [df[buckets == b] for b in range(first, K) if (buckets == b).any()]
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
    if (any(isinstance(df[k].dtype, _pd.CategoricalDtype) for k in keys)
            and not _groupby_observed(gb)):
        res = _add_unobserved(gb.obj, keys, as_index, dropna, spec, args, kw, res)
    if as_index:
        return res.sort_index()
    return res.reset_index(drop=True).sort_values(by=keys, kind="stable").reset_index(drop=True)


def _groupby_gate(gb):
    """True when a DataFrameGroupBy.agg should shuffle: big enough to pay for
    the transfer, keys are plain or categorical columns (no index/array
    keys)."""
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
    if not _should_spill(nbytes, 4, op="pandas.groupby"):
        return False
    for n in gb._grouper.names:
        if not (isinstance(n, str) and n in df.columns):
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
        if b == 0 and not _OFFLOAD_HERE:   # --offload ships bucket 0 too
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
    return _should_spill(nbytes, 2, op="pandas.merge")


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
            hreq = urllib.request.Request(_URL + "/health")
            with _daemon_open(hreq, 2) as r:
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
    import io
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

    def _local(idx):
        want = set(idx)
        for i, (start, end) in enumerate(_csv_ranges(str(path), header)):
            if i in want:
                with open(str(path), "rb") as f:
                    f.seek(start)
                    raw = f.read(end - start)
                yield _pd.read_csv(io.BytesIO(raw), names=names, header=None, **parse_kw)
    return _PartitionedFrame(all_keys, cols, metas, str(path), _local)


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

    def _local(idx):
        for i in sorted(set(idx)):
            buf = io.BytesIO()
            _pq.write_table(pf.read_row_group(i), buf)
            buf.seek(0)
            yield _pd.read_parquet(buf, **kw)
    return _PartitionedFrame(all_keys, list(metas[0]["dtypes"]), metas, str(path), _local)


class _PartitionedFrame:
    """Proxy over an out-of-core file: chunks parsed on the cluster (cached
    content-addressed in the warm workers' _CACHE), metadata local. groupby
    and merge run distributed; anything else materializes the chunks back into
    one local DataFrame (the documented fallback)."""

    def __init__(self, keys, cols, meta, path, local=None):
        self._keys = keys
        self._cols = list(cols)
        self._meta = meta
        self._path = path
        # local(idx) re-parses chunks idx from the source file here, in
        # order, one at a time: the fallback when the cluster cannot serve them.
        self._local = local

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

    def _map(self, what, func_src, extra=None, idx=None):
        """Run func_src's run(df) on each chunk in idx (default: all), on the
        node that cached it; one result per chunk, in order.

        A node can lose its cache (restart, eviction, the pool routing a key
        to a peer that never parsed it): the daemon then answers HTTP 500.
        That must not fail the user's script, so the chunks are re-parsed
        here from the source file, one at a time, with a visible warning."""
        import base64
        import json
        import pickle
        import urllib.request
        idx = list(range(len(self._keys))) if idx is None else list(idx)
        req = {"func_src": func_src, "func_name": "run",
               "items": list(range(len(idx))), "starmap": False,
               "cache_keys": [self._keys[i] for i in idx], "no_split": True}
        if extra is not None:
            req["extra_b64"] = base64.b64encode(pickle.dumps(extra)).decode()
        try:
            r = urllib.request.Request(_URL + "/v1/pool/map", data=json.dumps(req).encode(),
                                       headers={"Content-Type": "application/json",
                                                "X-Pipedpeer-Store": _STORE,
                                                "X-Pipedpeer-Offload": _OFFLOAD})
            with _daemon_open(r, 3600) as resp:
                rs = json.loads(resp.read())["results"]
            if len(rs) != len(idx):
                raise RuntimeError("got %d results for %d chunks" % (len(rs), len(idx)))
            return [pickle.loads(base64.b64decode(x["pickle"])) for x in rs]
        except Exception as e:
            if self._local is None:
                raise
            _record("ooc-" + what, len(idx), False, str(e)[:300])
            _offload_warn("%s of %s" % (what, self._path), "the cluster failed (%s)" % str(e)[:200])
            _log("WARNING: %s on the cluster failed (%s); re-reading %d chunk(s) "
                 "of %s on this machine instead - slower, same result"
                 % (what, str(e)[:300], len(idx), self._path))
        ns = dict(extra or {})
        ns["_CACHE"] = {}
        exec(func_src, ns)
        return [ns["run"](df) for df in self._local(idx)]

    def _chunk(self, i):
        """Fetch one chunk (from its node's cache) as a local DataFrame."""
        return self._map("chunk fetch", "def run(df):\n    return df\n", idx=[i])[0]

    def _materialize(self):
        """Fetch all chunks and concat into one local DataFrame."""
        import pandas as _pd
        return _pd.concat(self._map("materialize", "def run(df):\n    return df\n"),
                          ignore_index=True)

    def _combine_agg(self, by, spec, args, kw, gb_kw):
        """Combine-safe agg over the chunks: each chunk node aggregates its
        rows (partials are tiny), the origin re-aggregates the partials —
        exact because sum/count/size/min/max/first/last/any/all compose, and
        mean recombines as sum(sum)/sum(count)."""
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
        parts = self._map("groupby", src, extra)
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
    res = _pd.concat(pf._map("merge", ("import pandas as _pd\n"
                                       "def run(df):\n"
                                       "    return _pd.merge(df, _right, **_merge_kw)\n"),
                             {"_right": right, "_merge_kw": merge_kw}),
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


`

// shimInstall: import hooks, executor patch, SIGTERM, _install, joblib.
const shimInstall = `class _PatchOnImport:
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


def _make_cluster_executor(base):
    """A ProcessPoolExecutor whose map() spills to the cluster.

    Subclassing the real executor matters. The shim used to bind _ClusterPool
    onto concurrent.futures.ProcessPoolExecutor wholesale, but _ClusterPool
    takes processes= rather than max_workers= and has neither submit() nor
    shutdown(), so constructing an executor by keyword or calling submit() on
    one raised inside any job that touched the futures API. map() and
    submit() are intercepted, everything else is the stock executor, and
    map()'s real signature (several iterables, an iterator result) is
    honoured. submit() queues in the pool's _SubmitBatcher when there is a
    cluster and the call has no keyword arguments; otherwise it is the stock
    submit.
    """

    class _ClusterExecutor(base):
        def __init__(self, max_workers=None, *a, **kw):
            super().__init__(max_workers, *a, **kw)
            self._pp_workers = max_workers
            self._pp_pool = None
            self._pp_closed = False

        def _pp(self):
            if self._pp_pool is None:
                # Lazy: the stock executor spawns its own workers on first
                # stock submit, so an executor never pays for two pools
                # unless it uses both paths.
                self._pp_pool = _ClusterPool(processes=self._pp_workers)
            return self._pp_pool

        def submit(self, fn, /, *args, **kwargs):
            if kwargs or not _cluster_ready():
                return super().submit(fn, *args, **kwargs)
            if self._pp_closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            return self._pp()._submits().submit(fn, args)

        def map(self, fn, *iterables, timeout=None, chunksize=1):
            if not iterables:
                return iter(())
            if len(iterables) == 1:
                items, starmap = list(iterables[0]), False
            else:
                items, starmap = list(zip(*iterables)), True
            return iter(self._pp()._run(fn, items, starmap))

        def shutdown(self, wait=True, **kw):
            self._pp_closed = True
            pool, self._pp_pool = self._pp_pool, None
            if pool is not None:
                if wait:
                    pool.close()      # drains queued submits first
                else:
                    threading.Thread(target=pool.close, daemon=True).start()
            super().shutdown(wait, **kw)

    _ClusterExecutor.__name__ = base.__name__
    _ClusterExecutor.__qualname__ = base.__qualname__
    return _ClusterExecutor


def _quiet_sigterm(signum, frame):
    """Exit 143 at once, workers first, without a traceback.

    A cancel SIGTERMs the whole sandbox. The script is pid 1 there, and pid 1
    ignores a signal it has no handler for, while its Pool workers die: for
    the 2 s before SIGKILL the Pool respawned them and printed ~50 lines of
    tracebacks under "cancelled". os._exit skips what the default action
    skips too (finally blocks, atexit)."""
    import multiprocessing
    try:
        for p in multiprocessing.active_children():
            p.kill()
    except Exception as e:
        os.write(2, ("[pipedpeer] SIGTERM: could not stop pool workers: %s\n" % e).encode())
    for f in (sys.stdout, sys.stderr):
        try:
            f.flush()
        except Exception as e:
            os.write(2, ("[pipedpeer] SIGTERM: could not flush output: %s\n" % e).encode())
    os._exit(128 + signum)


def _install_sigterm():
    # Only over the default action: a handler the environment or the script
    # set is theirs. A forked worker inherits this one and exits as quietly.
    import signal
    try:
        if signal.getsignal(signal.SIGTERM) == signal.SIG_DFL:
            signal.signal(signal.SIGTERM, _quiet_sigterm)
    except ValueError as e:      # not the main thread: nothing to install
        _log("SIGTERM handler not installed: %s" % e)


def _install():
    _install_sigterm()
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
    _cf.ProcessPoolExecutor = _make_cluster_executor(_cf.ProcessPoolExecutor)


def _install_joblib():
    try:
        import joblib.parallel as _jp
    except ImportError:
        return
    if not (_URL and _ENABLED) or int(_NUM_SHARDS) < 2:
        _log("joblib interception skipped (no cluster peers)")
        return

    def _jb_has_shared_accumulator(tasks):
        # sklearn's _accumulate_prediction-style tasks ship as
        # (bound_method, X, all_proba_list, threading.Lock): they mutate the
        # shared list in place (the lock is the tell) and return None. Shipped
        # to a remote worker the mutation lands on a copy and is lost, so any
        # call carrying a lock in its args must run in-process.
        for t in tasks:
            if any(type(x).__module__ == "_thread" and type(x).__name__ == "lock"
                   for x in t[1]):
                return True
        return False

    # The remote side of a joblib call: each item is one delayed (func, args,
    # kwargs) task. The runner marks PIPEDPEER_JB_NESTED so worker-side joblib
    # calls (RF fit inside a GridSearchCV task) run inline instead of
    # recursing into the mesh.
    #
    # Arrays several tasks share travel once per chunk, in the globals frame,
    # and each task carries a reference instead. sklearn hands every tree the
    # whole training set, so a chunk of three trees used to ship X three
    # times - 77 MB for 01_sklearn_rf's 25 MB X - and the transfer, not the
    # trees, set the pace.
    jb_payload = {
        "src": ("import os\n"
                "def _pp_unshare(x):\n"
                "    if type(x) is tuple and len(x) == 2 and x[0] == '__pp_shared__':\n"
                "        return globals()[x[1]]\n"
                "    return x\n"
                "def run(item):\n"
                "    os.environ['PIPEDPEER_JB_NESTED'] = '1'\n"
                "    f, a, k = item\n"
                "    a = tuple(_pp_unshare(x) for x in a)\n"
                "    k = {n: _pp_unshare(v) for n, v in k.items()}\n"
                "    return f(*a, **k)\n"),
        "name": "run",
        "gvars": {},
    }

    def _jb_share(chunk):
        """Move large arrays out of a chunk's tasks into its globals."""
        keys, shared = {}, {}

        def ref(x):
            nb = getattr(x, "nbytes", 0)
            if not isinstance(nb, int) or nb < (1 << 20):
                return x
            key = keys.get(id(x))
            if key is None:
                key = keys[id(x)] = "_pp_shared_%d" % len(keys)
                shared[key] = x
            return ("__pp_shared__", key)

        items = [(i, (f, tuple(ref(x) for x in a), {n: ref(v) for n, v in k.items()}))
                 for i, (f, a, k) in chunk]
        return items, shared

    def _jb_remote(chunk):
        items, shared = _jb_share(chunk)
        return _ClusterPool._remote_chunk(dict(jb_payload, gvars=shared), items, False, "joblib")

    _ORIG_PARALLEL_INIT = _jp.Parallel.__init__
    _ORIG_PARALLEL_CALL = _jp.Parallel.__call__

    def _parallel_init(self, *args, **kw):
        _ORIG_PARALLEL_INIT(self, *args, **kw)
        # n_jobs None, 0 or 1 asked for no parallelism, and a worker that is
        # already running a task from the mesh runs its own calls inline.
        self._pp_cluster = (not os.environ.get("PIPEDPEER_JB_NESTED")
                            and self.n_jobs not in (None, 0, 1))

    def _parallel_call(self, iterable):
        """joblib's Parallel, with the same split as Pool.map.

        This used to be a backend whose submit() sent each batch to the
        cluster and waited for it before joblib could submit the next. Auto
        batching never grows past one task against a synchronous submit, so
        a forest of 80 trees became 80 round trips one after another - and a
        peer runs one request at a time - so the cluster fitted one tree at
        a time while this machine alone fits one per core. Seven times
        slower, on every n_jobs=-1 in sklearn.

        Now the whole call is one map: the first few tasks run here on
        joblib's own backend (threads for sklearn, which shares memory and
        releases the GIL), which measures them; the rest race between this
        machine and multi-task chunks sent to peers, exactly like Pool.map.
        Generators and return_as other than a list keep stock joblib.
        """
        if (not getattr(self, "_pp_cluster", False)
                or getattr(self, "return_as", "list") != "list"):
            return _ORIG_PARALLEL_CALL(self, iterable)
        tasks = list(iterable)

        def run_local(vals):
            if not vals:
                return []
            return _ORIG_PARALLEL_CALL(self, vals)

        n = len(tasks)
        _add_stat("mapped_items", n)
        width = _usable_cores()
        head_n = max(4, min(width, n // 2))
        if _OFFLOAD_HERE and n:
            # --offload: no measuring head here; every task ships.
            head_n = 0
            if _jb_has_shared_accumulator(tasks):
                _offload_warn("a joblib call", "its tasks share an accumulator, "
                              "so they must run in one process")
        if n <= head_n or _jb_has_shared_accumulator(tasks):
            _record_local(n)
            return run_local(tasks)
        head, tail = tasks[:head_n], tasks[head_n:]
        t0 = time.monotonic()
        head_results = run_local(head)
        _record_local(len(head))
        cost = (time.monotonic() - t0) * max(1, min(len(head), width)) / max(1, len(head))

        import pickle
        try:
            # The task as it would travel: its large arrays go once per chunk
            # in the globals frame, so the probe leaves them out. Pickling the
            # whole first task copied the full training set just to throw it
            # away. An argument that still cannot travel is caught per chunk.
            pickle.dumps(_jb_share([(0, tail[0])])[0][0][1])
        except Exception:
            # A lambda or a closure: plain pickle has no name to send it by.
            _decide("joblib", False, "the task cannot be pickled to send")
            _offload_warn("a joblib call", "its tasks cannot be pickled to send")
            _record_local(len(tail))
            return head_results + run_local(tail)
        if not _FORCE and len(tail) * cost < _POOL_MIN_WORK:
            _decide("joblib", False, "%d tasks x %.3fs = %.2fs of work, under the %.2fs worth a round trip"
                    % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
            _record_local(len(tail))
            return head_results + run_local(tail)
        _decide("joblib", True, "forced" if _FORCE else
                "%d tasks x %.3fs = %.2fs of work, over the %.2fs worth a round trip"
                % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
        _log("joblib: %d tasks, %.3fs each; racing this machine against the cluster"
             % (len(tail), cost))
        return head_results + _race_items(
            tail, cost, width, run_local, _jb_remote)

    try:
        _jp.Parallel.__init__ = _parallel_init
        _jp.Parallel.__call__ = _parallel_call
        _log("joblib interception installed")
    except Exception as e:
        _log("joblib interception failed: %s" % e)
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
