package nixgen

// ShimSitecustomize is a sitecustomize.py auto-imported by Python before the
// user's first line (sitecustomize is always imported at interpreter startup).
// It patches the parallel primitives to route through the local pipedpeer
// daemon — no code changes required on the user's script.
//
// The design (see handoff.md §4/D1): interception only, no SDK. It patches
// multiprocessing.Pool, concurrent.futures.ProcessPoolExecutor and
// joblib.Parallel, leaving thread pools alone (shared memory).
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
    return bool(_URL) and _ENABLED and shards >= 2


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


class _ClusterPool:
    """drop-in replacement for multiprocessing.Pool.

    Starts as a real local pool; the first chunks run on local cores, which is
    the measurement. Spills to the cluster only when measured per-item cost x
    remaining clearly exceeds the dispatch cost. Local cores never stop pulling,
    so a remote node is an additional consumer, never a subtracter.
    """

    def __init__(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        import multiprocessing.pool as _mp
        self._remote = _cluster_ready()
        self._requested = processes
        self._init = (initializer, initargs, maxtasksperchild)
        self._procs = _pool_width(processes)
        self._ctx = _mp.Pool(
            processes=self._procs,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
        )
        self._pending = 0
        self._measure_items = 4
        self._spilled = False
        self._threads = None
        self._batcher = None
        atexit.register(self.close)

    def _resize(self):
        """Re-size the local pool to what the machine can run right now.

        A width chosen when the pool was built can be wrong by the time the
        next map arrives: another job may have taken the memory, or released
        it. multiprocessing.Pool cannot grow or shrink, so this rebuilds it
        between calls; within a single map the width is fixed.
        """
        want = _pool_width(self._requested, self._workers_memory())
        if want == self._procs:
            return
        if self._batcher is not None and self._batcher._busy:
            return                        # a submitted batch is using this pool
        import multiprocessing.pool as _mp
        initializer, initargs, maxtasksperchild = self._init
        try:
            new = _mp.Pool(processes=want, initializer=initializer,
                           initargs=initargs, maxtasksperchild=maxtasksperchild)
        except Exception:
            return                        # keep the working pool
        old, self._ctx, self._procs = self._ctx, new, want
        if self._threads is not None:
            try:
                self._threads.terminate()
            except Exception:
                pass
            self._threads = None          # rebuilt at the new width on demand
        _log("local pool resized to %d workers" % want)
        try:
            old.terminate()
        except Exception:
            pass

    def _workers_memory(self):
        """Private memory held by this pool's own worker processes.

        _resize measures free memory with those workers already running, so
        without this they counted against the pool that owns them: on a 3 GB
        orchestrator the first map always found less room than the pool was
        sized with, shrank 3 workers to 2 and paid for a rebuild doing it.
        """
        total = 0
        try:
            page = os.sysconf("SC_PAGE_SIZE")
        except (ValueError, OSError):
            return 0
        for proc in getattr(self._ctx, "_pool", None) or []:
            try:
                with open("/proc/%d/statm" % proc.pid) as f:
                    fields = f.read().split()
                total += max(0, int(fields[1]) - int(fields[2])) * page
            except (OSError, ValueError, IndexError, AttributeError):
                pass
        return total

    # ---- the four Pool workhorses ----
    #
    # apply and apply_async are single calls. With a cluster they queue in a
    # _SubmitBatcher, so a loop of them is sent as one starmap through _run.
    # Without one, or with keyword arguments (a starmap item is positional
    # only), they run locally - on whichever local pool can take the
    # callable: a lambda cannot be sent to a worker process, and handing one
    # to the process pool raises PicklingError.
    def apply(self, func, args=(), kwds=None):
        # kwds defaults to None here and to {} in multiprocessing, and it is
        # passed straight through to func(*args, **kwds). So pool.apply(f, (x,))
        # - the ordinary call, with no keyword arguments - raised "argument
        # after ** must be a mapping, not NoneType" from inside the worker,
        # which reads as a bug in the user's function.
        return self.apply_async(func, args, kwds).get()

    def apply_async(self, func, args=(), kwds=None, callback=None, error_callback=None):
        if self._remote and not kwds:
            return _BatchResult(self._submits().submit(
                func, args, callback, error_callback))
        return self._local_ctx(func).apply_async(
            func, args=args, kwds=kwds or {},
            callback=callback, error_callback=error_callback)

    def _submits(self):
        if self._batcher is None:
            self._batcher = _SubmitBatcher(self)
        return self._batcher

    def map(self, func, iterable, chunksize=None):
        return self._run(func, list(iterable))

    def starmap(self, func, iterable, chunksize=None):
        return self._run(func, list(iterable), starmap=True)

    def imap(self, func, iterable, chunksize=None):
        return self._stream(func, iterable, False)

    def imap_unordered(self, func, iterable, chunksize=None):
        return self._stream(func, iterable, False)

    def _stream(self, func, iterable, starmap):
        """imap semantics: pull the source in batches and yield as each lands.

        The old version handed the whole iterable to map() first. That holds
        every item and every result in memory at once, and turns a generator
        with no end - which imap explicitly supports - into a hang. Batches
        are sized to be worth distributing while staying bounded: too small
        and every batch falls under the dispatch floor and stays local.
        """
        batch, out = [], _imap_batch(self._procs)
        for item in iterable:
            batch.append(item)
            if len(batch) >= out:
                for r in self._run(func, batch, starmap):
                    yield r
                batch = []
        if batch:
            for r in self._run(func, batch, starmap):
                yield r

    def _run(self, func, items, starmap=False, catch=False):
        _add_stat("mapped_items", len(items))
        self._resize()
        if not self._remote or len(items) <= self._measure_items:
            return self._local(func, items, starmap, catch)
        # Measure the first few locally, then decide whether to spill the rest.
        # The batch is as wide as the pool: measuring four items on a twenty
        # core machine left sixteen cores idle for the length of one item, a
        # serial bubble in front of every intercepted map.
        head_n = max(self._measure_items, min(self._procs, len(items) // 2))
        head, tail = items[:head_n], items[head_n:]
        t0 = time.monotonic()
        head_results = self._local(func, head, starmap, catch)
        wall = time.monotonic() - t0
        if not tail:
            return head_results
        # Per-item cost comes from the batch we just ran, not from executing an
        # item a third time. The batch occupied min(items, procs) lanes, so the
        # wall clock covers that many items at once.
        lanes = max(1, min(len(head), self._procs))
        cost = wall * lanes / max(len(head), 1)
        if cost <= 0:
            return head_results + self._local(func, tail, starmap, catch)

        payload = _func_payload(func) or _func_pickle(func)
        if payload is not None and payload.get("pickled"):
            _log("kernel %r has no source to send; shipping it by value"
                 % getattr(func, "__name__", "?"))
        if payload is None:
            _decide("pool.map", False, "the function cannot be sent, by source or by value")
            _record("pool", len(tail), False, "unshippable")
            _log("kernel %r cannot ship at all, by source or by value; "
                 "staying local" % getattr(func, "__name__", "?"))
            return head_results + self._local(func, tail, starmap, catch)
        if not _FORCE and len(tail) * cost < _POOL_MIN_WORK:
            _log("tail is %.2fs of work, below the %.2fs dispatch floor; staying local"
                 % (len(tail) * cost, _POOL_MIN_WORK))
            _decide("pool.map", False, "%d items x %.3fs = %.2fs of work, under the %.2fs worth a round trip"
                    % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
            return head_results + self._local(func, tail, starmap, catch)
        _decide("pool.map", True, "forced" if _FORCE else
                "%d items x %.3fs = %.2fs of work, over the %.2fs worth a round trip"
                % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
        return head_results + self._race(func, tail, starmap, cost, payload, catch)

    def _local_ctx(self, func):
        """The pool that can actually run func on this machine.

        A lambda, or a function closing over a local, does not merely fail to
        reach a peer - it fails to reach this machine's own worker
        PROCESSES, because multiprocessing pickles a callable by reference
        and there is no name for a worker to resolve. Stock
        multiprocessing.Pool raises PicklingError for exactly these, so the
        measurement head died before anything could be dispatched and the
        by-value work below was unreachable.

        Threads share the interpreter, so nothing has to be pickled to reach
        them. The trade is worth naming plainly: for CPU-bound work the GIL
        makes these threads take turns, so the LOCAL half stops being
        parallel. The remote half is unaffected and is where the win is; the
        alternative is the call raising, which is what it did before.
        """
        if _process_safe(func):
            return self._ctx
        if self._threads is None:
            import multiprocessing.dummy as _dummy
            self._threads = _dummy.Pool(self._procs)
            _log("kernel %r cannot cross a process boundary, so local work "
                 "runs on threads; the cluster still runs it in parallel"
                 % getattr(func, "__name__", "?"))
        return self._threads

    def _local(self, func, items, starmap, catch=False):
        _log("local %d items" % len(items))
        _record_local(len(items))
        ctx = self._local_ctx(func)
        if catch:
            return ctx.starmap(_apply_catching, [(func, v) for v in items])
        if starmap:
            return ctx.starmap(func, items)
        return ctx.map(func, items)

    def _race(self, func, items, starmap, per_item_cost, payload, catch=False):
        self._spilled = True

        def run_local(vals):
            ctx = self._local_ctx(func)
            if catch:
                return ctx.starmap(_apply_catching, [(func, v) for v in vals])
            if starmap:
                # _apply carries func as an argument, so a process pool would
                # have to pickle it as data; route by the same test either way.
                return ctx.starmap(_apply, [(func, v) for v in vals])
            return ctx.map(func, vals)

        return _race_items(items, per_item_cost, self._procs, run_local,
                           lambda chunk: self._remote_chunk(payload, chunk, starmap))

    @staticmethod
    def _remote_chunk(payload, chunk, starmap, kind="pool"):
        """Dispatch one chunk to the cluster; returns [(orig_idx, result), ...]
        or None on failure (local already has the work covered). chunk is a list
        of (orig_idx, item) pairs, payload is the _func_payload triple.

        Everything here — including serialisation — sits inside the try. When
        it did not, an unpicklable argument raised out of this method, killed
        the dispatch thread and printed a traceback, which is the one thing
        interception promises never to do."""
        import pickle
        idxs = [p[0] for p in chunk]
        vals = [p[1] for p in chunk]
        if starmap:
            vals = [v if isinstance(v, (list, tuple)) else (v,) for v in vals]
        t0 = time.monotonic()
        try:
            globals_pickle = pickle.dumps(payload.get("gvars") or {})
            items = [pickle.dumps(v) for v in vals]
            header = {
                "items_frames": len(items),
                "globals": True,
                "starmap": starmap,
            }
            # Exactly one of the two, never both: the runner prefers func_src
            # and would silently ignore a callable sent beside it, so a
            # header carrying both would test the wrong path while looking
            # like it tested this one.
            if payload.get("pickled"):
                header["func"] = payload["pickled"]
            else:
                header["func_src"] = payload["src"]
                header["func_name"] = payload["name"]
            # Admission control: the daemon 503s rather than OOM when it
            # cannot spare this, and micro-chunks when it is over budget.
            #
            # The callable counts towards it. A lambda closing over a large
            # array carries that array inside its own payload rather than in
            # the globals frame, so sizing globals+items alone would report a
            # few kilobytes for a request holding hundreds of megabytes.
            header["required_mem"] = _pool_required(
                globals_pickle, items, header.get("func", ""))
            if _FORCE:
                header["force"] = True
            info = {}
            blobs = _pool_send(header, globals_pickle, items, 600, info)
            if len(blobs) != len(idxs):
                raise RuntimeError("got %d results for %d items" % (len(blobs), len(idxs)))
            out = [(idxs[i], pickle.loads(b)) for i, b in enumerate(blobs)]
        except Exception as e:
            _record(kind, len(idxs), False, str(e), (time.monotonic() - t0) * 1000)
            _log("remote failed (%s); local covers it" % e)
            return None
        ms = (time.monotonic() - t0) * 1000
        receipt = info.get("receipt")
        _record(kind, len(out), True, "", ms, receipt)
        out = _RemotePairs(out)
        out.elsewhere = sum(int(p.get("items", 0)) for p in (receipt or {}).get("parts", [])
                            if str(p.get("where", "")).startswith("peer:"))
        if payload.get("pickled"):
            # Counted here rather than where the tier is chosen, so it counts
            # items that were actually SENT by value. Counting the tail at
            # decision time made it larger than dispatched_items - the race
            # keeps half the tail local - and a receipt whose subset exceeds
            # its superset is exactly the kind of number this file exists to
            # not produce.
            with _STATS_LOCK:
                _STATS["shipped_pickled"] += len(out)
        where = ""
        if receipt:
            peers = sorted({p.get("where", "")[5:] for p in receipt.get("parts", [])
                            if str(p.get("where", "")).startswith("peer:")})
            where = (" on " + ", ".join(peers)) if peers else " on the origin node"
        _log("remote ok: %d items in %.0f ms%s" % (len(out), ms, where))
        return out

    def close(self):
        if self._batcher is not None:
            self._batcher.drain()     # queued calls finish before the pool goes
        try:
            self._ctx.close()
            self._ctx.terminate()
        except Exception:
            pass
        if self._threads is not None:
            try:
                self._threads.close()
                self._threads.terminate()
            except Exception:
                pass
            self._threads = None

    def terminate(self):
        self.close()

    def join(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class _RemotePairs(list):
    """A chunk's [(orig_idx, result), ...], and how many ran on another machine.

    A daemon with no free peer runs a chunk itself, on this machine; those
    results are not the cluster's, and counting them as remote_used made
    the receipt and /ui claim work that never left. No receipt, no claim."""
    elsewhere = 0


def _race_items(items, per_item_cost, width, run_local, run_remote):
    """Run items on this machine and the cluster at once, first result wins.

    run_local(values) returns their results in order; run_remote(chunk) takes
    [(index, value), ...] and returns [(index, result), ...], or None when the
    chunk failed. width is the local lane count. Shared by Pool.map and joblib,
    so both get the same never-slower-than-local guarantee.
    """
    # D2: local and remote each pull ~half the tail concurrently; when local
    # finishes its share and a remote chunk is still in flight, an idle
    # local core speculatively re-runs it — first result wins per item.
    # Worst case (remote dead/absent) local eventually does everything, so
    # this is never slower than a plain local Pool; a working remote halves
    # the tail's wall time.
    n = len(items)
    # A sentinel, not None: a kernel that returns None would otherwise
    # leave its slot looking unfilled, and local_run would re-run it
    # forever.
    unset = object()
    slots = [unset] * n
    lock = threading.Lock()
    # A chunk has to be wide enough to fill the node that receives it. The
    # adaptive size targets half a second of work taken one item at a
    # time, which for an item costing about a second collapses to a single
    # item per round trip: the peer then runs one item on one core and
    # sixteen sit idle while the origin waits out the latency. Sizing to
    # at least the local pool width uses this machine as the stand-in for
    # a peer's core count, which is the best estimate the shim has.
    #
    # Capped at half the tail, because ownership alternates: one chunk
    # would be dealt to the local side and the cluster handed nothing.
    chunk_size = max(1, min(max(_adaptive_chunk(per_item_cost), width),
                            (n + 1) // 2))
    chunks = _chunk(list(enumerate(items)), chunk_size)  # [(orig_idx, item), ...]

    # One queue both sides pull from, so each takes chunks as fast as it
    # finishes them. Ownership used to be dealt alternately up front, which
    # fixed this machine's share at half however many peers there were: a
    # three-worker cluster ran 00_pool no faster than a one-worker one,
    # because the half left here took as long as the whole job did elsewhere.
    queue = list(chunks)
    remote_taken = []
    claimed = {}       # id(chunk) -> when a peer took it; None once it failed
    remote_secs = []   # how long finished remote chunks took

    def take(remote=False):
        # Popping and recording a remote claim happen under one lock: split,
        # local could find the queue empty and the claim not yet listed, and
        # return with that chunk's slots still empty.
        with lock:
            # Once every item has its result the race is over: a peer
            # thread that comes back for more must not send anything, or
            # its request lands in the middle of the script's next call.
            if not queue or done.is_set():
                return None
            chunk = queue.pop(0)
            if remote:
                remote_taken.append(chunk)
                claimed[id(chunk)] = time.monotonic()
            return chunk

    def fill(pairs):
        """Run user func over (idx, item) pairs and first-wins into slots."""
        idxs = [p[0] for p in pairs]
        vals = [p[1] for p in pairs]
        res = run_local(vals)
        won = 0
        with lock:
            for i, v in zip(idxs, res):
                if slots[i] is unset:
                    slots[i] = v
                    won += 1
        # Counted only where the local result is the one kept: these items
        # were missing from the receipt, which then summed to less than
        # the map. A speculative re-run that lost is not counted twice.
        if won:
            _record_local(won)

    # Remote threads: each pulls a chunk, dispatches it, first-wins into
    # slots, and pulls the next. Several are in flight at once: one at a time
    # capped the whole cluster's throughput at a single chunk, so a peer sat
    # idle between round trips no matter how many cores it had. A failed
    # chunk stops that thread pulling; local re-runs what it held.
    def deliver_loop():
        while True:
            chunk = take(remote=True)
            if chunk is None:
                return
            t0 = time.monotonic()
            res = run_remote(chunk)
            if res is None:
                with lock:
                    claimed[id(chunk)] = None
                return
            won = 0
            with lock:
                for orig_i, v in res:
                    if slots[orig_i] is unset:
                        slots[orig_i] = v
                        won += 1
            with lock:
                remote_secs.append(time.monotonic() - t0)
            # ponytail: the receipt says how many of a chunk's items ran
            # elsewhere, not which, so a mixed chunk that won only in part is
            # counted pro rata; per-item placement on the wire would make it exact.
            el = min(len(res), getattr(res, "elsewhere", len(res)))
            used = won if el == len(res) else won * el // max(1, len(res))
            if used:
                _add_stat("remote_used", used)

    def local_secs(chunk):
        return per_item_cost * len(chunk) / max(1, min(width, len(chunk)))

    def worth_rerunning(chunk, now):
        """Re-run a peer's chunk here only when that beats waiting for it.

        Re-running everything a peer held the moment local ran dry made a
        fast origin redo the cluster's work every time, so a peer's answer
        rarely won and adding peers changed nothing. A chunk that failed is
        re-run at once. Once some peer answers have come back, local takes a
        chunk when its own run would finish before the peer is expected to,
        or the peer is well overdue. Before any have, it gives the peer
        twice the chunk's local time plus 0.7 s - the most this costs over
        never dispatching at all.
        """
        start = claimed.get(id(chunk))
        if start is None:
            return True
        here = local_secs(chunk)
        if not remote_secs:
            return now > start + 2 * here + 0.7
        expected = sum(remote_secs) / len(remote_secs)
        return now + here < start + expected or now > start + 1.5 * expected + 0.2

    def remote_run():
        inflight = max(1, min(_REMOTE_INFLIGHT, len(chunks) - 1))
        # Daemons, unlike the local side: once the race is done a chunk still
        # in flight is a loser whose answer is thrown away, and as a non-daemon
        # it held the interpreter open at exit for as long as a slow peer took
        # (up to the 600 s request timeout). t_local below is what keeps a
        # live race - and the receipt of what it kept - alive at exit.
        threads = [threading.Thread(target=deliver_loop, daemon=True) for _ in range(inflight)]
        for t in threads:
            t.start()

    # Local pulls until the queue is empty, then re-runs any chunk a peer
    # still holds (straggler tail). done is set once local has given every
    # item a chance, so the caller can always return a complete result.
    # An item that raises here used to kill this thread before done was
    # set, so the caller waited forever instead of seeing the exception
    # a stock Pool.map raises. The error is carried back and re-raised.
    failed = []

    def local_run():
        try:
            while True:
                chunk = take()
                if chunk is None:
                    break
                fill(chunk)
            while True:
                now = time.monotonic()
                with lock:
                    open_chunks = [c for c in remote_taken
                                   if any(slots[p[0]] is unset for p in c)]
                    rerun = [p for c in open_chunks if worth_rerunning(c, now)
                             for p in c if slots[p[0]] is unset]
                if not open_chunks:
                    break
                if rerun:
                    fill(rerun)  # idle local cores re-run the stragglers
                else:
                    time.sleep(0.02)
        except BaseException as e:
            failed.append(e)
        finally:
            done.set()  # also stops take() handing peers new chunks

    done = threading.Event()
    # Explicitly not daemons: a thread inherits daemon from its creator,
    # and a batch of submits runs on the daemon batcher thread, so a live
    # race would be killed at exit and its results never reach the caller.
    t_local = threading.Thread(target=local_run, daemon=False)
    t_remote = threading.Thread(target=remote_run, daemon=False)
    t_local.start()
    t_remote.start()
    done.wait()  # local guarantees a complete result
    if failed:
        raise failed[0]
    return slots


def _cgroup_stat(path, key):
    """One counter from a cgroup's memory.stat, or 0 if it cannot be read."""
    try:
        with open(path + "/memory.stat") as f:
            for line in f:
                name, _, value = line.partition(" ")
                if name == key:
                    return int(value)
    except (OSError, ValueError):
        pass
    return 0


def _cgroup_headroom():
    """What this process's cgroup still allows, or None if it is uncapped.

    The daemon runs under a memory ceiling so that a runaway pool cannot take
    the whole machine down with it. Without reading that ceiling here, the
    pool sizes itself against the machine's free memory, overshoots the cap,
    and the kernel kills a worker - a job failure caused by the very
    protection meant to prevent one. Reading it turns "killed" into "used
    fewer workers".
    """
    try:
        with open("/proc/self/cgroup") as f:
            for line in f:
                if line.startswith("0::"):
                    own = line[3:].strip()
                    break
            else:
                return None
    except OSError:
        return None

    # The limit can be set on any ancestor, and the binding one is the
    # smallest remaining headroom along the whole chain.
    best = None
    path = "/sys/fs/cgroup" + own
    while True:
        try:
            with open(path + "/memory.max") as f:
                raw = f.read().strip()
            if raw != "max":
                with open(path + "/memory.current") as f:
                    used = int(f.read().strip())
                # memory.current counts page cache, which the kernel reclaims
                # before it kills anything. Left in, every file a job reads
                # shrinks the next job's pool: measured 2.0GB of a worker's
                # 2.15GB "used" was inactive cache, against 33MB of real
                # memory, and the pool fell from 5 workers to 1 over five warm
                # runs. inactive_file is the part reclaimed first; current
                # minus it is the "working set" container runtimes report.
                used -= _cgroup_stat(path, "inactive_file")
                room = int(raw) - max(0, used)
                if best is None or room < best:
                    best = room
        except (OSError, ValueError):
            pass
        if path == "/sys/fs/cgroup" or "/" not in path[len("/sys/fs/cgroup"):]:
            break
        path = path.rsplit("/", 1)[0]
    return best


def _avail_bytes():
    """Memory this machine could actually give a new process.

    Not SC_AVPHYS_PAGES: that counts only wholly free pages and ignores the
    page cache, which the kernel hands back on demand. On any box with a warm
    cache it reads several times too low - measured 2GB free against 21GB
    available here - and every limit derived from it collapses. MemAvailable
    is the kernel's own answer to this question.

    Bounded by the cgroup's remaining headroom where there is one, because the
    machine having memory free does not mean this process is allowed to use
    it.
    """
    avail = 0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
                    break
    except (OSError, ValueError, IndexError):
        pass
    if not avail:
        try:
            avail = os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        except (ValueError, OSError):
            return 0
    room = _cgroup_headroom()
    if room is not None and room < avail:
        return max(0, room)
    return avail


def _usable_cores():
    """Cores this process may actually run on.

    os.cpu_count() is the machine's total, which overcounts whenever the job is
    pinned (taskset, a container's cpuset) or throttled (a cgroup cpu.max
    quota): the pool then starts more workers than there are cores to run
    them, and they only time-slice each other.
    """
    try:
        n = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        n = os.cpu_count() or 1
    try:
        with open("/proc/self/cgroup") as f:
            own = next((l[3:].strip() for l in f if l.startswith("0::")), None)
    except OSError:
        own = None
    if own is not None:
        # A quota on any ancestor binds, so walk up and keep the smallest.
        path = "/sys/fs/cgroup" + own
        while True:
            try:
                with open(path + "/cpu.max") as f:
                    quota, period = f.read().split()[:2]
                if quota != "max":
                    n = min(n, max(1, -(-int(quota) // int(period))))
            except (OSError, ValueError):
                pass
            if path in ("/sys/fs/cgroup", "/sys/fs/cgroup/"):
                break
            path = os.path.dirname(path)
    return max(1, n)


def _imap_batch(procs):
    """Items to buffer before running a batch of a lazy imap."""
    override = os.environ.get("PIPEDPEER_IMAP_BATCH", "").strip()
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            pass
    return max(64, procs * 32)


def _pool_width(requested, reclaim=0):
    """How many local workers to run.

    A hand-picked process count is a guess about the machine the author had
    in front of them, so by default it is ignored in favour of what this
    machine can actually run: every core, bounded by free memory. Picking the
    number is the work this is supposed to remove.

    Some counts encode correctness rather than speed, though - a rate limit,
    a resource that is not safe to touch twice, a model that only fits a few
    times over - and those break rather than slow down when overridden.
    PIPEDPEER_RESPECT_POOL_SIZE=1 hands the decision back.
    """
    cores = _usable_cores()
    if os.environ.get("PIPEDPEER_RESPECT_POOL_SIZE") == "1" and requested:
        return max(1, int(requested))
    n = cores
    avail = _avail_bytes()
    if avail > 0:
        # reclaim is what an existing pool's workers hold: rebuilding the
        # pool frees it, so it is memory a new width can count on.
        avail += reclaim
        # Nothing here knows the per-worker working set before the job runs,
        # so assume a conservative 256MB rather than none: a box with many
        # cores and little free memory must not fan out to all of them.
        n = min(n, max(1, int(avail * 0.4) // (256 << 20)))
    if requested and int(requested) != n:
        _log("using %d workers, not the %d requested (cores=%d); "
             "set PIPEDPEER_RESPECT_POOL_SIZE=1 to keep your own count"
             % (n, int(requested), cores))
    return max(1, n)


def _is_library_module(mod):
    """True when the worker's closure holds this module too. The job's own
    files travel with it but are not importable from a worker, so a function
    defined in one has to ship as source; a stdlib or site-packages module is
    present on both sides and can simply be imported."""
    if mod is None:
        return False
    f = getattr(mod, "__file__", None)
    if not f:
        return True                       # builtin module: always present
    try:
        f = os.path.realpath(f)
        for marker in ("site-packages", "dist-packages", "/nix/store/"):
            if marker in f:
                return True
        # Anywhere under the stdlib dir, not only directly in it: packages
        # (collections, re, logging, multiprocessing) and lib-dynload C
        # extensions live in subdirectories. Missing them registered half
        # the stdlib for by-value pickling, and cloudpickle then recursed
        # until it gave up on any kernel that touched a module.
        stdlib = os.path.dirname(os.path.realpath(os.__file__))
        return f.startswith(stdlib + os.sep)
    except Exception:
        return False


def _code_names(code):
    """Every global name a code object reads, comprehensions included."""
    import types
    names = set(code.co_names)
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names |= _code_names(const)
    return names


def _func_payload(func):
    """Rebuild instructions for func as {"src", "name", "gvars"}, or None.

    A worker runs the closure's python with neither the shim nor the job's
    workspace on its path, so a by-reference pickle — __main__.work, or a
    sibling module — names something that does not exist there and every
    chunk fails. Shipping the source instead is what the numpy and pandas
    paths have always done; this generalises it to user kernels: the
    function's own source, the source of any helper or class that travels
    with the job, an import line for every library module it names, and a
    pickled frame of the module-level data it reads.

    Returns None when the callable cannot be rebuilt this way — lambdas,
    closures, nested defs, methods, decorated functions, partials, anything
    written in C. Those go to _func_pickle instead, which ships them by
    value; only what neither can carry stays local, and the receipt counts
    that so "slow" is visible rather than mysterious.
    """
    import inspect
    import pickle
    import textwrap
    import types

    if not isinstance(func, types.FunctionType):
        return None
    if func.__closure__:
        return None
    if "." in getattr(func, "__qualname__", func.__name__):
        return None                       # method or nested def
    if not func.__name__.isidentifier():
        # A lambda bound to a module-level name reaches here: no closure, no
        # dot in its qualname. Its source is the assignment that creates it,
        # which compiles happily, and the worker then looks up "<lambda>" in
        # the namespace that exec produced and dies with a KeyError - once
        # per chunk, on the far side, where the traceback is hardest to see.
        # By value is the only way one of these travels.
        return None

    imports, sources, gvars = [], [], {}
    seen = set()
    pending = [func]
    while pending:
        fn = pending.pop()
        if fn.__name__ in seen:
            continue
        seen.add(fn.__name__)
        try:
            src = textwrap.dedent(inspect.getsource(fn))
        except (OSError, TypeError):
            return None
        if src.lstrip().startswith("@"):
            return None                   # decorator is not in co_names
        sources.append(src)
        modvars = vars(sys.modules.get(fn.__module__, None)) if sys.modules.get(fn.__module__) else {}
        for name in _code_names(fn.__code__):
            if name in seen or name not in modvars:
                continue
            val = modvars[name]
            if isinstance(val, types.ModuleType):
                seen.add(name)
                imports.append("import %s as %s" % (val.__name__, name))
                continue
            travels = not _is_library_module(sys.modules.get(getattr(val, "__module__", None)))
            if isinstance(val, types.FunctionType) and travels:
                pending.append(val)       # helper defined alongside the kernel
                continue
            seen.add(name)
            if isinstance(val, type) and travels:
                try:
                    sources.append(textwrap.dedent(inspect.getsource(val)))
                except (OSError, TypeError):
                    return None
                continue
            gvars[name] = val             # data, or an importable library object

    src = "\n".join(imports + sources)
    try:
        compile(src, "<pipedpeer-kernel>", "exec")
        pickle.dumps(gvars)
    except Exception:
        return None
    return {"src": src, "name": func.__name__, "gvars": gvars}


def _process_safe(func):
    """True when the standard library can send func to a worker process.

    multiprocessing pickles a callable by reference: it stores the module and
    qualified name and expects the worker to look them up. That works for a
    module-level function, a bound method of a module-level class, or a
    partial over one - and not for a lambda or a closure, which have no name
    to resolve.

    Asked by trying it rather than by inspecting the callable's shape,
    because the shapes that fail are not a list anyone can keep correct.
    """
    import pickle
    try:
        pickle.dumps(func)
        return True
    except Exception:
        return False


def _func_pickle(func):
    """Ship func by value with cloudpickle, as {"pickled": b64}, or None.

    The second tier, tried only where source shipping has already given up.
    Source is preferred because it is the smaller and older path: it sends a
    few hundred bytes of text and depends on nothing but the interpreter.
    cloudpickle sends the callable itself - which is what makes it work for a
    lambda, a closure, a bound method or a decorated function, where there is
    no name a worker could resolve and often no source to read.

    Only the sender needs cloudpickle to build this; the worker loads it with
    plain pickle, which reaches into cloudpickle for the reconstruction
    helpers. Both ends run the same shipped store path, so they are running
    the same cloudpickle and there is no version to negotiate.

    Returns None when the callable genuinely cannot travel - an open file
    handle, a live socket, a running generator, a lock - and the caller then
    keeps the work local, as before.
    """
    import base64

    try:
        import cloudpickle
    except ImportError:
        # An environment built before cloudpickle was added to every flake.
        # Nothing is wrong with the callable, so say which it is: the fix is
        # to rebuild the environment, not to rewrite the kernel.
        _log("cloudpickle is not in this environment, so %r cannot ship by "
             "value; rebuild the environment to enable it"
             % getattr(func, "__name__", "?"))
        return None

    # A function defined in the job's own files is not importable on a
    # worker: the workspace does not travel with the closure. cloudpickle
    # pickles a module-level function BY REFERENCE by default, which would
    # produce a payload naming a module the worker has never heard of, and
    # every chunk would fail there rather than here. Registering those
    # modules by value makes it send the code instead.
    #
    # Library modules are deliberately left by reference: they exist on both
    # sides, and sending numpy by value would be absurd.
    try:
        for mod in list(sys.modules.values()):
            if mod is not None and not _is_library_module(mod):
                try:
                    cloudpickle.register_pickle_by_value(mod)
                except Exception:
                    pass    # not every module can be registered; skip it
    except Exception:
        pass

    try:
        blob = cloudpickle.dumps(func)
    except Exception as e:
        _log("kernel %r cannot ship by value either (%s)"
             % (getattr(func, "__name__", "?"), e))
        return None
    return {"pickled": base64.b64encode(blob).decode()}


def _apply(func, item):
    """Run func over one starmap item (a tuple of args)."""
    return func(*item) if isinstance(item, tuple) else func(item)


class _Raised(object):
    """A call's exception, returned in place of its result."""

    def __init__(self, exc):
        self.exc = exc


def _apply_catching(func, item):
    """_apply, with the call's exception returned instead of raised.

    For batched submits: one raising call used to fail its whole map, and
    the batch was then re-run call by call - so every call that had already
    finished ran a second time, side effects and all. Carried as a value,
    the error reaches its own future and nothing runs twice. Only local
    runs need it: a call raising on a peer fails that chunk, and local
    re-runs the chunk through here."""
    try:
        return _apply(func, item)
    except Exception as e:
        return _Raised(e)


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
    if not (_URL and _ENABLED):
        return False
    K = int(_NUM_SHARDS)
    if K < 2 or nbytes < _spill_min():
        return False
    if _FORCE:
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
    import pickle
    import struct
    import urllib.request
    items = _maybe_compress(items, header)
    body = json.dumps(header).encode() + b"\n"
    if globals_pickle is not None:
        body += struct.pack(">I", len(globals_pickle)) + globals_pickle
    for it in items:
        body += struct.pack(">I", len(it)) + it
    req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                 headers={"Content-Type": "application/vnd.pipedpeer.frames",
                                          "X-Pipedpeer-Store": _STORE,
                                          "X-Pipedpeer-Submitter": _SUBMITTER})
    with _daemon_open(req, timeout, opener) as resp:
        data = resp.read()
    nl = data.find(b"\n")
    hdr = json.loads(data[:nl])
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


def _weighted_batches(weights, base_batch, world):
    """Each rank's per-step batch, together adding up to the ring's.

    The ring processes base_batch*world samples per step however the shares
    fall; only the split between ranks changes. Rounding is absorbed by the
    largest rank so the total is exact - let it drift and the step counts
    drift with it.
    """
    total = max(world, int(base_batch) * world)
    # A zero share is a rank that has left the ring: no batch at all.
    b = [max(1, int(round(w * total))) if w > 0 else 0 for w in weights]
    drift = total - sum(b)
    if drift:
        k = b.index(max(b))
        b[k] = max(1, b[k] + drift)
    return b


class _WeightedShardSampler:
    """A rank's share of the dataset, sized by what placement measured.

    Two invariants, both of which fail quietly if they are only approximated:

    Every rank must run the SAME NUMBER of steps. Each step ends at a barrier,
    so a rank with fewer steps leaves the others averaging without it - the
    daemon tolerates that, which is why the failure does not hang but simply
    makes the last step of every epoch short a rank. Sizing shard and batch
    independently and rounding each does not give equal counts: measured
    62/31/7 shares produced 40, 40 and 39 steps. So the step count is chosen
    first, from the whole dataset, and each rank takes exactly steps*batch
    samples.

    Samples are owned by consistent hashing, so when a rank leaves (its
    share drops to zero) only ITS samples move, to the ranks left; nobody
    else's shard is reshuffled. Each sample ranks the live ranks by weighted
    rendezvous hash and goes to the first one that still has room: hashing
    alone gives shards of only roughly the right size, and the step count
    needs them exact.

    The remainder below one global batch is dropped, as drop_last does in any
    DDP setup, and shuffling makes it a different remainder each epoch.

    Shuffling is seeded from the epoch alone, so every rank computes the same
    owners and their shards stay disjoint - the same contract
    DistributedSampler keeps, and the reason set_epoch exists.
    """

    def __init__(self, n, weights, rank, shuffle, base_batch, world):
        if n <= 0:
            raise ValueError("empty dataset")
        self.n = n
        self.rank = rank
        self.shuffle = shuffle
        self.epoch = 0
        self.weights = list(weights)
        self._owned = None  # (epoch, indices) cache
        self.batches = _weighted_batches(weights, base_batch, world)
        self.batch = self.batches[rank]
        self.steps = n // sum(self.batches)
        if self.steps < 1:
            raise ValueError(
                "%d samples do not make one step of %d across the ring"
                % (n, sum(self.batches)))

    def set_epoch(self, epoch):
        self.epoch = epoch

    def __len__(self):
        return self.steps * self.batch

    def __iter__(self):
        # torch is imported per-function everywhere in this file, never as a
        # module global - importing it at class scope would cost every script
        # the import whether or not it trains.
        if self._owned is None or self._owned[0] != self.epoch:
            order = None
            if self.shuffle:
                import torch as _th_local
                g = _th_local.Generator()
                g.manual_seed(self.epoch)
                order = _th_local.randperm(self.n, generator=g).tolist()
            quotas = [self.steps * b for b in self.batches]
            owners = _hash_owners(self.n, self.weights, quotas, order)
            self._owned = (self.epoch, owners[self.rank])
        return iter(self._owned[1])


def _hash_owners(n, weights, quotas, order=None):
    """Split samples 0..n-1 between ranks: exactly quotas[r] each.

    Weighted rendezvous hashing: every (sample, rank) pair gets a fixed
    pseudo-random score, scaled by the rank's share; a sample prefers ranks
    in score order and takes the first with room. A rank with share 0 is out.
    Removing a rank therefore moves only the samples it held (plus the few a
    full survivor overflows), which is what lets the ring lose a member
    without re-slicing everyone's data.

    order is the sequence samples claim room in (the epoch's shuffle);
    samples left when every quota is full are the dropped remainder.
    Returns one list per rank, in claim order.

    Runs every epoch, so it is mostly numpy. Until the first rank is full
    every sample simply gets its top rank, so that prefix is assigned in
    one vectorised step from a running argmax (no n x ranks matrix). Only
    the samples after the first overflow - about sqrt(n) for proportional
    quotas - take the per-sample loop, with their full preference order.
    The assignment is the same as the plain greedy loop's.
    """
    import numpy as _np
    live = [r for r, w in enumerate(weights) if w > 0 and quotas[r] > 0]
    owners = [[] for _ in weights]
    if not live:
        return owners
    idx = _np.arange(n, dtype=_np.uint64)
    top = _hash_score(idx, live[0], weights[live[0]])
    best = _np.zeros(n, dtype=_np.intp)
    for j in range(1, len(live)):
        s = _hash_score(idx, live[j], weights[live[j]])
        better = s > top              # strict: a tie keeps the earlier rank, as argsort did
        best[better] = j
        top = _np.where(better, s, top)
    del idx, top
    seq = _np.arange(n) if order is None else _np.asarray(order, dtype=_np.intp)
    first = best[seq]
    room = [quotas[r] for r in live]
    # Before the first overflow every claim succeeds, so after head claims
    # the total quota is used up at the latest.
    head = min(sum(room), len(seq))
    for j in range(len(live)):
        pos = _np.flatnonzero(first == j)
        if len(pos) > room[j]:
            head = min(head, int(pos[room[j]]))
    for j, r in enumerate(live):
        mine = seq[:head][first[:head] == j]
        owners[r] = mine.tolist()
        room[j] -= len(mine)
    left = sum(room)
    rest = seq[head:]
    if left == 0 or not len(rest):
        return owners
    ridx = rest.astype(_np.uint64)
    scores = _np.stack([_hash_score(ridx, r, weights[r]) for r in live])
    prefs = _np.argsort(-scores, axis=0).T.tolist()  # per sample: live slots, best first
    for s, pref in zip(rest.tolist(), prefs):
        if left == 0:
            break
        for j in pref:
            if room[j]:
                room[j] -= 1
                left -= 1
                owners[live[j]].append(s)
                break
    return owners


def _hash_score(idx, r, w):
    """Weighted rendezvous score of rank r for samples idx (uint64 array):
    splitmix64 of (sample, rank), stable across processes and runs unlike
    Python's salted hash(), scaled by the rank's share w."""
    import numpy as _np
    x = idx * _np.uint64(0x9E3779B97F4A7C15) + _np.uint64((r + 1) * 0x632BE59BD9B4E019 % (1 << 64))
    x ^= x >> _np.uint64(30)
    x *= _np.uint64(0xBF58476D1CE4E5B9)
    x ^= x >> _np.uint64(27)
    x *= _np.uint64(0x94D049BB133111EB)
    x ^= x >> _np.uint64(31)
    u = ((x >> _np.uint64(11)).astype(_np.float64) + 0.5) / float(1 << 53)
    return w / -_np.log(u)


def _push_window(values, v, cap):
    """Append v, keeping at most cap entries. The oldest goes first."""
    values.append(v)
    while len(values) > cap:
        del values[0]


def _recent_mean_ms(recent, total_sec, count):
    """What a step is costing now, in milliseconds.

    Recent rather than cumulative. A cumulative mean moves toward a new value
    at (N-k)/N, so a rank throttled at step 200 of 400 still reports half its
    old speed - and one that RECOVERS waits just as long to be given work
    back, which defeats the point of refitting shares at all. The pool's rate
    model has used a sliding window since it was written, for this reason.

    Falls back to the cumulative figure until the window has anything in it.
    """
    if recent:
        return 1000.0 * sum(recent) / len(recent)
    if count <= 0:
        return 0.0
    return 1000.0 * total_sec / count


def _parse_weights(raw, world):
    """Each rank's share of a step, or None when the shares are equal.

    Equal shares are what every ring did before weights existed, and treating
    them as a special case keeps that path byte-for-byte unchanged rather than
    routing it through arithmetic that happens to reduce to it.
    """
    if not raw or world < 2:
        return None
    try:
        w = [float(x) for x in raw.split(",")]
    except ValueError:
        return None
    if len(w) != world or any(x <= 0 for x in w):
        return None
    total = sum(w)
    if total <= 0:
        return None
    w = [x / total for x in w]
    # Within a couple of percent of even is even. Placement measures live
    # machines, so identical hardware still returns slightly different
    # numbers, and resharding for that is churn.
    if max(w) - min(w) < 0.02:
        return None
    return w


def _local_free_bytes():
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
    local = df[buckets == 0]
    if len(local):
        parts.append(local.groupby(keys, as_index=as_index, sort=False,
                                   dropna=dropna, observed=True).agg(spec, *args, **kw))
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
                                                "X-Pipedpeer-Store": _STORE})
            with _daemon_open(r, 3600) as resp:
                rs = json.loads(resp.read())["results"]
            if len(rs) != len(idx):
                raise RuntimeError("got %d results for %d chunks" % (len(rs), len(idx)))
            return [pickle.loads(base64.b64decode(x["pickle"])) for x in rs]
        except Exception as e:
            if self._local is None:
                raise
            _record("ooc-" + what, len(idx), False, str(e)[:300])
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
    import numpy as _np_ddp
    # Gradients ship as float16 unless asked otherwise. Averaging is still
    # done in float64; only the wire is narrowed.
    _FP16_GRADS = os.environ.get("PIPEDPEER_DDP_FP32") != "1"
    _WORLD = int(os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
    _RANK = int(os.environ.get("PIPEDPEER_RANK", "0"))
    # Each rank's share of a step, measured by placement. Equal shares are the
    # same thing as no shares, and are treated as such.
    _WEIGHTS = _parse_weights(os.environ.get("PIPEDPEER_DDP_WEIGHTS", ""), _WORLD)

    def _live_ranks(weights=None, world=None):
        """Ranks still training. _WORLD is the rank-id space: a dead rank
        keeps its index with a zero share, so after one dies and one rejoins
        _WORLD is 3 while 2 ranks train. Messages count ranks with this."""
        w = _WEIGHTS if weights is None else weights
        n = _WORLD if world is None else world
        if not w:
            return n
        return sum(1 for x in w if x > 0) or n
    # The number of samples behind the gradient being sent, read from the
    # batch the model was last given. It decides how the daemon weighs this
    # rank's contribution, and it has to be the real figure rather than the
    # configured one: a final short batch is smaller than the rest, and a
    # rank whose loader ran dry contributes nothing at all.
    _BATCH_N = [0]
    # Whether the measured shares were actually used. The run announces them
    # before the script starts, and a script that indexes its own tensors
    # ("X[rank::world]") never reaches the sampler that would apply them - so
    # without this the run claims a split it did not perform.
    _WEIGHTS_APPLIED = [False]
    _WARNED_WEIGHTS_UNUSED = [False]
    # A change to the ring (new shares, a rank gone, a rank joining) waiting
    # for an epoch boundary. Resharding mid-epoch would have ranks re-slice
    # data they are part-way through, so samples would be trained on twice or
    # skipped; the boundary is where the sampler already re-slices. A dict:
    # weights, world, and join/from while a rank is on its way in.
    _PENDING = [None]
    # Bumped at every ring change; a hashed sampler built under an older one
    # is rebuilt at its loader's next epoch.
    _RING_GEN = [0]
    # Every DataLoader epoch this rank has started, across all loaders. A
    # ring change happens at one of these, and a rank joining mid-run skips
    # exactly the ones the ring had already done.
    _ITERS = [0]
    # Set when a loader is sharded by hash: only such a ring can take a rank
    # mid-run, and the daemon is told so with every sync.
    _SHARDED_HASH = [False]
    # A rank started to join a running ring (pipedpeer saw a machine come
    # back, or a new one): it skips to the ring's epoch and loads the ring's
    # model and optimizer before its first step.
    _JOIN = [os.environ.get("PIPEDPEER_DDP_JOIN") == "1"]
    _JOIN_AT = [None]
    # The learning rates the ring had when this rank took its model, per
    # optimizer, put back once the skipped epochs are behind it.
    _JOIN_LR = [None]
    # Every optimizer the script built, so a model can be handed to a joiner.
    _OPTS = []
    # Set when the daemon has told this rank the ring is better off without
    # it.
    _DROPPED = [False]
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
    # A receipt for training, for the same reason pool work has one: a run
    # that distributed nothing and a run that distributed everything look
    # identical from the outside, and so do a fast ring and one that spent
    # 95% of its time waiting.
    # barrier_sec is the opening round's wait for every rank to start: not
    # sync, and counted as sync it advised averaging every 32 steps (V4 80).
    _DDP_STATS = {"syncs": 0, "sync_sec": 0.0, "sent_bytes": 0, "recv_bytes": 0,
                  "barrier_sec": 0.0}
    # Server-side averaging. Off with PIPEDPEER_DDP_BLACKBOARD=1, which falls
    # back to every rank receiving every rank's payload - needed only against
    # a daemon too old to average.
    _REDUCE_OK = os.environ.get("PIPEDPEER_DDP_BLACKBOARD", "") != "1"
    # The start barrier: the opening weight broadcast waits this long (seconds)
    # for every rank, then the ring starts without the ranks still missing.
    try:
        _START_WAIT = max(1.0, float(os.environ.get("PIPEDPEER_DDP_START_WAIT", "") or 300))
    except ValueError:
        _log("ddp: PIPEDPEER_DDP_START_WAIT=%r is not a number of seconds; using 300"
             % os.environ.get("PIPEDPEER_DDP_START_WAIT"))
        _START_WAIT = 300.0

    # Averaging less often than every step trades accuracy for speed, so it
    # is off unless asked for: PIPEDPEER_DDP_SYNC_EVERY=auto.
    #
    # The temptation to make it the default is strong, and measurement is why
    # it is resisted. On two machines over a home connection an
    # 800k-parameter model costs ~0.5s a step to sync against ~50ms to
    # compute, so 91% of the run is moving gradients and the job takes 54s
    # against 2.9s on one machine. Auto-tuning to average every ~20 steps cuts
    # that to 5.4s - and moved the final loss from 0.0603 to 0.0756, because
    # an 87-step run averaged 7 times is barely distributed training at all.
    #
    # A 10x speed-up that quietly makes the model worse is exactly the kind of
    # result this project exists to stop shipping. So the default stays exact,
    # the diagnosis is printed either way, and the user decides.
    _SYNC_BUDGET = float(os.environ.get("PIPEDPEER_DDP_SYNC_BUDGET", "0.25"))
    _SYNC_EVERY_MAX = int(os.environ.get("PIPEDPEER_DDP_SYNC_EVERY_MAX", "32"))
    _SYNC_AUTO = os.environ.get("PIPEDPEER_DDP_SYNC_EVERY", "").lower() == "auto"
    _SYNC_TUNED = [None if _SYNC_AUTO else _SYNC_EVERY]
    _WARNED_SYNC_BOUND = [False]
    # Said once. The condition holds for every step of the run, and a warning
    # repeated 88 times is a warning nobody reads.
    _WARNED_SAME_WORK = [False]
    # Said when it changes rather than every step: a ring that has lost a rank
    # stays lost, and one line per step is a line nobody reads.
    _LAST_RANKS = [0]

    # Gradients as signed bytes rather than half floats: half the bytes again
    # on a link where bytes are the whole cost. What quantisation drops is
    # carried forward in _ERR and added to the next step's gradient, so the
    # error is delayed rather than discarded - which is what keeps this from
    # being a slow bias towards zero. Off by default because it is still a
    # change to the arithmetic; PIPEDPEER_DDP_INT8=1 turns it on.
    _INT8_GRADS = os.environ.get("PIPEDPEER_DDP_INT8", "") == "1"
    _ERR = {}
    # Compute time is measured as the interval between consecutive optimizer
    # steps minus whatever was spent syncing in it - not as the duration of
    # step() itself, which is a rounding error next to the forward and
    # backward passes it follows and would make N wildly too large.
    _STEP_SEC = [0.0, 0]  # total compute seconds, count
    # The last few steps, for the daemon's refit. Separate from the cumulative
    # figure above, which is right for tuning how often to sync - a stable
    # long-run average - and wrong for noticing that this machine slowed down
    # a moment ago. A cumulative mean over a whole run moves toward a new
    # value at (N-k)/N, so a rank that was throttled at step 200 of 400 still
    # reports half its old speed, and one that RECOVERS waits just as long to
    # be given work back. The pool's rate model has used a sliding window
    # since it was written, for the same reason.
    _STEP_RECENT = []
    _STEP_WINDOW = 20
    _STEP_MARK = [None, 0.0]  # monotonic at last step, sync_sec at last step

    def _apply_rebalance(reply):
        """Take the daemon's verdict on this ring.

        Neither answer is acted on here. New shares wait for an epoch
        boundary, where the sampler re-slices anyway; a drop waits for the
        same place, because leaving mid-epoch would strand the other ranks at
        a barrier expecting a gradient that is no longer coming.
        """
        w = reply.get("weights")
        try:
            world = int(reply.get("world") or _WORLD)
        except (TypeError, ValueError):
            _log("ddp: ignoring a ring size the daemon sent as %r; keeping %d"
                 % (reply.get("world"), _WORLD))
            world = _WORLD
        if w and len(w) == world and world >= _WORLD:
            try:
                w = [float(x) for x in w]
            except (TypeError, ValueError):
                _log("ddp: ignoring shares the daemon sent as %r" % (w,))
                w = None
            # A zero share is a rank that has left the ring. The daemon sends
            # six decimals, so thirds sum to 0.999999: renormalised here, the
            # same way on every rank.
            if w and abs(sum(w) - 1.0) < 1e-4 and all(x >= 0 for x in w) and w[_RANK] > 0:
                total = sum(w)
                p = dict(_PENDING[0] or {})
                p["weights"] = [x / total for x in w]
                p["world"] = world
                if reply.get("join") is not None:
                    p["join"] = int(reply["join"])
                    p["from"] = int(reply.get("from", 0))
                else:
                    # A note states the whole ring, and names a join for as
                    # long as one is pending. One without it means the join
                    # ended (the joiner died, or took its model), so an
                    # earlier note's join must not survive to the boundary.
                    p.pop("join", None)
                    p.pop("from", None)
                _PENDING[0] = p
        if reply.get("down") is not None:
            _log("ddp: %s" % reply.get("why", "rank %s left the ring" % reply.get("down")))
        elif reply.get("join") is not None:
            _log("ddp: %s" % reply.get("why", "rank %s joins the ring" % reply.get("join")))
        if reply.get("drop") == _RANK and not _DROPPED[0]:
            _DROPPED[0] = True
            _log("ddp: leaving the ring — %s. This rank finishes its own work "
                 "locally; the others carry on without waiting for it."
                 % reply.get("why", "the ring is faster without this rank"))

    def _mean_sync_ms():
        """What one sync has cost this rank on average, in milliseconds.

        The daemon needs it to decide whether a rank pays for itself: every
        rank in the ring costs a gradient exchange per step, and a rank whose
        compute contribution is smaller than the sync it adds is making the
        run slower. Placement cannot know this - it runs before a single byte
        of the model has moved.
        """
        if _DDP_STATS["syncs"] <= 0:
            return 0.0
        return 1000.0 * _DDP_STATS["sync_sec"] / _DDP_STATS["syncs"]

    def _mean_step_ms():
        """What a step is costing this rank now, in milliseconds.

        Reported so the lead daemon can compare ranks: same model, same batch,
        whatever hardware this rank has - which makes it the one number that
        compares a GPU against a CPU, and the placement probe cannot, being an
        integer loop on the CPU.

        Recent rather than cumulative, because the daemon uses it to notice a
        machine that has changed. Falls back to the cumulative figure until
        the window has anything in it.
        """
        return _recent_mean_ms(_STEP_RECENT, _STEP_SEC[0], _STEP_SEC[1])

    def _tuned_sync_every():
        """How often to average, from measured sync and step times.

        Chosen so sync is at most _SYNC_BUDGET of the wall clock:
            sync/N <= budget * (step + sync/N)  =>  N >= sync*(1-budget)/(step*budget)
        """
        if _DDP_STATS["syncs"] < 3 or _STEP_SEC[1] < 3:
            return _SYNC_TUNED[0] or _SYNC_EVERY
        sync = _DDP_STATS["sync_sec"] / _DDP_STATS["syncs"]
        step = _STEP_SEC[0] / _STEP_SEC[1]
        if step <= 0 or sync <= 0:
            return _SYNC_TUNED[0] or _SYNC_EVERY
        want = sync * (1.0 - _SYNC_BUDGET) / (step * _SYNC_BUDGET)
        n = max(1, min(_SYNC_EVERY_MAX, int(want + 0.5)))

        if not _SYNC_AUTO:
            # Diagnose regardless. A run that spends nine tenths of itself
            # moving gradients should say so, once, rather than leave the
            # user to conclude the system is simply slow.
            if n > 1 and not _WARNED_SYNC_BOUND[0]:
                _WARNED_SYNC_BOUND[0] = True
                _log("ddp: sync-bound — %.0f ms per sync against %.0f ms of compute, "
                     "so %.0f%% of this run is moving gradients. "
                     "PIPEDPEER_DDP_SYNC_EVERY=auto would average every %d steps "
                     "instead of every one, which is faster and trains a slightly "
                     "different model." % (sync * 1000, step * 1000,
                                           100.0 * sync / (sync + step), n))
            return _SYNC_EVERY

        if _SYNC_TUNED[0] is None:
            _SYNC_TUNED[0] = n
            if n > 1:
                _log("ddp: averaging every %d steps — a sync costs %.0f ms against "
                     "%.0f ms of compute. This is local SGD: faster, and not the "
                     "same arithmetic as exact DDP." % (n, sync * 1000, step * 1000))
        return _SYNC_TUNED[0]

    def _ddp_report():
        """The receipt for a training run, and its verdict.

        Distributing is not automatically worth it, and the failure is silent:
        the loss comes out right either way. Each rank computes 1/world of the
        data, so one machine would have taken world x the compute; the ring
        takes compute + sync. It pays for itself only while

            sync < compute x (world - 1)

        which is a number this run has measured rather than a rule of thumb.
        Measured on two machines over a home link it predicts both outcomes:
        fp16 sync 607ms against 557ms of compute, ring slower (71.1s vs 55.0s);
        int8 sync 319ms, ring faster (53.0s vs 55.0s). On a GPU, where the same
        step takes 25ms, no encoding gets sync under 25ms and one card wins by
        20x - which is worth being told rather than discovering from a
        stopwatch.
        """
        if _DDP_STATS["barrier_sec"] >= 1:
            _log("ddp: waited %.0f s at the start for every rank to be ready "
                 "(not counted as sync)" % _DDP_STATS["barrier_sec"])
        if not _DDP_STATS["syncs"]:
            return
        sync_total = _DDP_STATS["sync_sec"]
        _log("ddp: %d sync(s), %.1fs total (%.0f ms each), %.1f MiB sent, %.1f MiB received"
             % (_DDP_STATS["syncs"], sync_total,
                1000.0 * sync_total / _DDP_STATS["syncs"],
                _DDP_STATS["sent_bytes"] / 1048576.0,
                _DDP_STATS["recv_bytes"] / 1048576.0))
        live = _live_ranks()
        if live < 2 or _STEP_SEC[1] < 2:
            return
        compute = _STEP_SEC[0]
        # Machines, not ranks. Two ranks on one laptop are two identities and
        # one set of cores, and multiplying by the rank count there says the
        # machine is twice as fast as itself: measured, a two-rank ring on one
        # machine called itself worth forming on a run that took 191s against
        # 92.8s for that machine alone. A daemon too old to send the count
        # cannot tell us, so the old assumption stands in that case and is the
        # only place it still applies.
        try:
            _machines = int(os.environ.get("PIPEDPEER_DDP_MACHINES", "") or live)
        except ValueError:
            _machines = live
        _machines = max(1, min(_machines, live))
        alone = compute * _machines       # every machine's share, done by one
        together = compute + sync_total
        if together <= 0:
            return
        speedup = alone / together
        # "alone" is this rank doing every shard at its own speed, which is
        # what can be measured from here - not what the fastest machine in the
        # ring would manage. On a mixed cluster the slower rank's estimate is
        # generous, so the comparison is stated as what it is rather than as a
        # speed-up against the best single node. Measured: this said 1.24x on
        # a run that beat a single fast machine by 1.02x.
        if speedup >= 1.05:
            _log("ddp: the ring paid for itself — %.1fs of compute and %.1fs of "
                 "sync across %d ranks on %d machine(s), against about %.1fs for "
                 "THIS machine to have done the whole dataset (%.2fx). A faster "
                 "machine on its own may still beat that."
                 % (compute, sync_total, live, _machines, alone, speedup))
        elif _machines < live:
            _log("ddp: the ring did NOT pay for itself — %d ranks on %d "
                 "machine(s), so the extra ranks brought no extra hardware: "
                 "%.1fs of compute and %.1fs of sync against about %.1fs for "
                 "this machine alone. Co-located ranks divide one machine and "
                 "pay the sync on top; they are for a second accelerator, not "
                 "a second machine."
                 % (live, _machines, compute, sync_total, alone))
        else:
            _log("ddp: the ring did NOT pay for itself — %.1fs of compute and "
                 "%.1fs of sync across %d ranks, against about %.1fs for THIS "
                 "machine alone. Syncing costs more than the extra machines save.%s"
                 % (compute, sync_total, live, alone,
                    "" if _INT8_GRADS else
                    " PIPEDPEER_DDP_INT8=1 halves the bytes on the wire."))

    import atexit as _atexit_ddp
    _atexit_ddp.register(_ddp_report)
    _SEQ = [0]
    _STEPN = {}
    if _BACKEND == "daemon" and not _SYNC_URL:
        _BACKEND = "gloo"  # no sync endpoint handed down; old transport

    def _count_sync(sec, sent, recv):
        """Book one sync round trip. The first is the opening round, which is
        also the start barrier: its time is mostly waiting for ranks to start,
        so it is kept apart from sync time."""
        if _SEQ[0] == 1:
            _DDP_STATS["barrier_sec"] += sec
        else:
            _DDP_STATS["syncs"] += 1
            _DDP_STATS["sync_sec"] += sec
        _DDP_STATS["sent_bytes"] += sent
        _DDP_STATS["recv_bytes"] += recv

    def _barrier_progress():
        """Say what this rank is waiting for while the start barrier holds it:
        two and a half silent minutes read as a hang (V4 80). Returns an event
        to set when the wait is over."""
        import threading
        import time as _tw
        done = threading.Event()
        hosts = os.environ.get("PIPEDPEER_DDP_HOSTS", "").split(",")

        def missing():
            import json
            import urllib.parse
            import urllib.request
            try:
                req = urllib.request.Request(
                    "%s?group=%s&seq=1" % (_SYNC_URL, urllib.parse.quote(_GROUP)))
                if _TOKEN:
                    req.add_header("X-Pipedpeer-Token", _TOKEN)
                with urllib.request.urlopen(req, timeout=5) as resp:
                    arrived = set(json.loads(resp.read()).get("arrived") or [])
            except Exception as e:
                _log("ddp: could not ask the lead which ranks are ready (%s)" % e)
                arrived = {_RANK}
            names = ["rank %d%s" % (r, " on " + hosts[r] if r < len(hosts) and hosts[r] else "")
                     for r in range(_WORLD) if r not in arrived]
            return ", ".join(names) or "the other ranks"

        def run():
            t0 = _tw.monotonic()
            if done.wait(2):
                return
            limit = ("%d min" % (_START_WAIT // 60) if _START_WAIT % 60 == 0
                     else "%.0f s" % _START_WAIT)
            _log("ddp: waiting for %s to be ready (closure still arriving, or the "
                 "script not started yet), up to %s" % (missing(), limit))
            while not done.wait(30):
                _log("ddp: still waiting for %s (%.0f s of up to %s)"
                     % (missing(), _tw.monotonic() - t0, limit))
        threading.Thread(target=run, daemon=True).start()
        return done

    def _daemon_exchange(payload):
        """One allreduce round trip through the lead rank's daemon.

        Payloads travel as a length-prefixed frame rather than base64 in
        JSON. A gradient is the largest thing this system sends per step and
        it is sent every step; base64 inflated it by a third both on the wire
        and in the daemon's memory, where a full model per rank is already
        held until the slowest rank arrives. The daemon never looks inside
        the payload, so the encoding bought nothing.
        """
        import json
        import struct
        import time as _t
        import urllib.request
        _t0 = _t.monotonic()
        _SEQ[0] += 1
        header = json.dumps({"group": _GROUP, "seq": _SEQ[0], "rank": _RANK,
                             "world": _WORLD}).encode()
        body = header + b"\n" + struct.pack(">I", len(payload)) + payload
        req = urllib.request.Request(
            _SYNC_URL, data=body,
            headers={"Content-Type": "application/vnd.pipedpeer.ddp"})
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        with urllib.request.urlopen(req, timeout=240) as resp:
            data = resp.read()
        nl = data.find(b"\n")
        if nl < 0:
            raise RuntimeError("ddp sync returned no header: %r" % data[:200])
        count = json.loads(data[:nl]).get("blob_frames", 0)
        rest = data[nl + 1:]
        out = []
        for _ in range(count):
            n = struct.unpack(">I", rest[:4])[0]
            out.append(rest[4:4 + n])
            rest = rest[4 + n:]
        if len(out) != _WORLD:
            raise RuntimeError("ddp sync returned %d of %d blobs" % (len(out), _WORLD))
        # Timed because a training run that is slower distributed than local
        # is the failure mode that matters most here, and without a number
        # for where the step went there is nothing to act on. Measured on two
        # machines over a WAN-ish link: 87 steps at 0.69s of sync each, on a
        # model whose whole compute was 33ms a step.
        _count_sync(_t.monotonic() - _t0, len(body), len(data))
        return out

    def _pack(arrs):
        """Flatten arrays into one contiguous buffer of a single dtype.

        Raw bytes rather than pickle, because the daemon has to be able to
        average them: a reply carrying one model instead of one per rank is
        the difference between distributed training being worth doing on a
        normal link and being 21x slower than one machine. Dropping pickle
        also takes Python's serialiser out of every step.
        """
        if len(arrs) == 1:
            return arrs[0].reshape(-1), [arrs[0].shape], [arrs[0].size]
        flat = _np_ddp.concatenate([a.reshape(-1) for a in arrs])
        return flat, [a.shape for a in arrs], [a.size for a in arrs]

    def _unpack(flat, shapes, sizes, dtypes):
        out, off = [], 0
        for shape, size, dt in zip(shapes, sizes, dtypes):
            out.append(flat[off:off + size].reshape(shape).astype(dt, copy=False))
            off += size
        return out

    def _daemon_reduce(arrs, wire_dtype, kind="grads"):
        """One averaged allreduce through the daemon, in reduce mode."""
        import json
        import struct
        import time as _t
        import urllib.error
        import urllib.request
        flat, shapes, sizes = _pack(arrs)
        flat = _np_ddp.ascontiguousarray(flat.astype(wire_dtype, copy=False))
        _t0 = _t.monotonic()
        _SEQ[0] += 1
        opening = kind == "weights" and _SEQ[0] == 1
        header = json.dumps({"group": _GROUP, "seq": _SEQ[0], "rank": _RANK,
                             "world": _WORLD, "dtype": wire_dtype.name,
                             "count": int(flat.size), "kind": kind,
                             "start_wait": _START_WAIT,
                             "sync_every": int(_SYNC_TUNED[0] or 0),
                             "step_ms": _mean_step_ms(),
                             "sync_ms": _mean_sync_ms(),
                             "samples": int(_BATCH_N[0]),
                             "sharded": bool(_SHARDED_HASH[0])}).encode()
        payload = flat.tobytes()
        body = header + b"\n" + struct.pack(">I", len(payload)) + payload
        req = urllib.request.Request(
            _SYNC_URL, data=body,
            headers={"Content-Type": "application/vnd.pipedpeer.ddp.reduce"})
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        waiting = _barrier_progress() if opening else None
        try:
            # The opening round is the start barrier and may wait the whole
            # start wait for a rank whose closure is still arriving.
            with urllib.request.urlopen(req, timeout=(_START_WAIT + 60 if opening else 240)) as resp:
                data = resp.read()
        except urllib.error.HTTPError as e:
            if e.code != 410 or not opening:
                raise
            # Exit status 75 (EX_TEMPFAIL) tells pipedpeer this rank missed the
            # start rather than failed, so it can be started again as a joiner.
            _log("ddp: rank %d was not ready when the ring started training, and the "
                 "ring went on without it; stopping this rank (it can join later)" % _RANK)
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(75)
        finally:
            if waiting is not None:
                waiting.set()
        nl = data.find(b"\n")
        if nl < 0:
            raise RuntimeError("ddp reduce returned no header: %r" % data[:200])
        # Adopt the group's agreed interval. Ranks measure their own link and
        # arrive at different numbers, and averaging at different points in
        # each rank's step sequence is not local SGD - it is ranks combining
        # models that have taken different numbers of local steps, which shows
        # up as a worse final loss and nothing else.
        _reply = json.loads(data[:nl])
        agreed = _reply.get("sync_every", 0)
        if agreed and agreed != _SYNC_TUNED[0]:
            _SYNC_TUNED[0] = int(agreed)
        _ranks = int(_reply.get("ranks", 0) or 0)
        # Not for the opening round: a rank missing there missed the start
        # barrier, and the lead's note that follows says so in its own words.
        if _ranks and _ranks < _WORLD and _LAST_RANKS[0] != _ranks and not opening:
            _LAST_RANKS[0] = _ranks
            _log("ddp: this step averaged %d of %d ranks — the others did not answer "
                 "in time. The run continues on the ranks that did; a smaller average "
                 "is a smaller step in the same direction, and every rank applies the "
                 "identical result, so nothing drifts." % (_ranks, _WORLD))
        if (_WEIGHTS is not None and not _WEIGHTS_APPLIED[0]
                and not _WARNED_WEIGHTS_UNUSED[0]):
            _WARNED_WEIGHTS_UNUSED[0] = True
            _log("ddp: this run measured unequal shares (%s) but the script "
                 "splits its own data, so they were not applied - every rank "
                 "took an equal slice and the ring runs at the slowest rank's "
                 "pace. To use them: give each rank weights[rank] of the data "
                 "instead of X[rank::world]."
                 % ", ".join("%.0f%%" % (100 * w) for w in _WEIGHTS))
        _apply_rebalance(_reply)
        if _reply.get("same_work") and not _WARNED_SAME_WORK[0]:
            _WARNED_SAME_WORK[0] = True
            _log("ddp: every rank produced the SAME gradients, so every rank is "
                 "training on the same data. Averaging identical gradients gives "
                 "exactly the single-process result, so this run is correct and "
                 "pure overhead - the extra machines are doing the same work "
                 "twice.\n"
                 "    Sharding is automatic for a DataLoader. A script that slices "
                 "its tensors by hand has to do it itself, e.g.\n"
                 "      import os\n"
                 "      r = int(os.environ.get('PIPEDPEER_RANK', 0))\n"
                 "      w = int(os.environ.get('PIPEDPEER_WORLD_SIZE', 1))\n"
                 "      X, y = X[r::w], y[r::w]")
        rest = data[nl + 1:]
        n = struct.unpack(">I", rest[:4])[0]
        mean = _np_ddp.frombuffer(rest[4:4 + n], dtype=wire_dtype)
        _count_sync(_t.monotonic() - _t0, len(body), len(data))
        return _unpack(mean, shapes, sizes, [a.dtype for a in arrs])

    def _daemon_reduce_int8(arrs, key):
        """Average gradients as signed bytes, carrying the rounding error.

        Each rank scales its own values into [-127, 127] and sends the scale
        in the header; the daemon decodes, sums in float64 and returns the
        mean the same way. Every rank gets identical bytes back, so quantising
        the reply adds noise to the update without letting the ranks drift
        apart - they still hold exactly the same model, which is the property
        that matters.

        What rounding drops is kept in _ERR and added to the next step's
        gradient. Without that, small gradients round to zero every step and
        never move the model at all; with it they accumulate until they are
        large enough to survive a round, which is the standard error-feedback
        trick and the reason this is usable rather than merely smaller.
        """
        import json
        import struct
        import time as _t
        import urllib.request

        flat, shapes, sizes = _pack(arrs)
        flat = flat.astype("float32", copy=True)
        prev = _ERR.get(key)
        if prev is not None and prev.shape == flat.shape:
            flat += prev

        # A scale per tensor, not one for the model. Layers differ in gradient
        # magnitude by an order of magnitude or more, so a single scale lets
        # the largest set the step size for every other: measured on the demo,
        # hidden layers near 1e-2 alongside an output layer near 3e-1 were left
        # with four quantisation levels between them, and the final loss went
        # from 0.087 to 0.127.
        q = _np_ddp.empty(flat.size, dtype="int8")
        dequant = _np_ddp.empty(flat.size, dtype="float32")
        scales = []
        off = 0
        for n in sizes:
            seg = flat[off:off + n]
            peak = float(_np_ddp.abs(seg).max()) if n else 0.0
            sc = peak / 127.0 if peak > 0 else 1.0
            scales.append(sc)
            qs = _np_ddp.clip(_np_ddp.round(seg / sc), -127, 127).astype("int8")
            q[off:off + n] = qs
            dequant[off:off + n] = qs.astype("float32") * sc
            off += n

        # Whatever quantisation lost, minus nothing: carried to the next step.
        _ERR[key] = flat - dequant

        _t0 = _t.monotonic()
        _SEQ[0] += 1
        header = json.dumps({"group": _GROUP, "seq": _SEQ[0], "rank": _RANK,
                             "world": _WORLD, "dtype": "int8", "kind": "grads",
                             "count": int(q.size), "scale": scales[0],
                             "scales": scales, "counts": [int(n) for n in sizes],
                             "sync_every": int(_SYNC_TUNED[0] or 0),
                             "step_ms": _mean_step_ms(),
                             "sync_ms": _mean_sync_ms(),
                             "samples": int(_BATCH_N[0]),
                             "sharded": bool(_SHARDED_HASH[0])}).encode()
        payload = q.tobytes()
        body = header + b"\n" + struct.pack(">I", len(payload)) + payload
        req = urllib.request.Request(
            _SYNC_URL, data=body,
            headers={"Content-Type": "application/vnd.pipedpeer.ddp.reduce"})
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        with urllib.request.urlopen(req, timeout=240) as resp:
            data = resp.read()
        nl = data.find(b"\n")
        if nl < 0:
            raise RuntimeError("ddp reduce returned no header: %r" % data[:200])
        reply = json.loads(data[:nl])
        agreed = reply.get("sync_every", 0)
        if agreed and agreed != _SYNC_TUNED[0]:
            _SYNC_TUNED[0] = int(agreed)
        if (_WEIGHTS is not None and not _WEIGHTS_APPLIED[0]
                and not _WARNED_WEIGHTS_UNUSED[0]):
            _WARNED_WEIGHTS_UNUSED[0] = True
            _log("ddp: this run measured unequal shares (%s) but the script "
                 "splits its own data, so they were not applied - every rank "
                 "took an equal slice and the ring runs at the slowest rank's "
                 "pace. To use them: give each rank weights[rank] of the data "
                 "instead of X[rank::world]."
                 % ", ".join("%.0f%%" % (100 * w) for w in _WEIGHTS))
        _apply_rebalance(reply)
        if reply.get("same_work") and not _WARNED_SAME_WORK[0]:
            _WARNED_SAME_WORK[0] = True
            _log("ddp: every rank produced the SAME gradients — every rank is "
                 "training on the same data, so this run is correct and pure "
                 "overhead. Shard it: X, y = X[rank::world], y[rank::world]")
        rest = data[nl + 1:]
        n = struct.unpack(">I", rest[:4])[0]
        raw_mean = _np_ddp.frombuffer(rest[4:4 + n], dtype="int8").astype("float32")
        out_scales = reply.get("scales") or [float(reply.get("scale", 1.0)) or 1.0]
        if len(out_scales) == len(sizes):
            mean = _np_ddp.empty(raw_mean.size, dtype="float32")
            off = 0
            for i, seg_n in enumerate(sizes):
                mean[off:off + seg_n] = raw_mean[off:off + seg_n] * out_scales[i]
                off += seg_n
        else:
            mean = raw_mean * out_scales[0]
        _count_sync(_t.monotonic() - _t0, len(body), len(data))
        return _unpack(mean, shapes, sizes, [a.dtype for a in arrs])

    def _daemon_allreduce(tensors, half=None, kind="grads"):
        """Average tensors across ranks in place, through the daemon channel.

        Gradients cross the wire as float16 by default. This is the dominant
        cost of a step on any link slower than a datacentre LAN - the demo is
        bounded by sync time, not compute - and halving the bytes halves it.
        The averaging itself is still done in float64 and written back at the
        tensor's own dtype, so the loss of precision is confined to transport,
        which is the same trade PyTorch's own fp16 compression hook makes.
        PIPEDPEER_DDP_FP32=1 turns it off; weight broadcasts never use it.
        """
        import pickle
        if _DROPPED[0]:
            # Told to leave. Posting anyway would have the others average a
            # rank the daemon has stopped expecting; not posting is what lets
            # them complete without waiting. This rank keeps training on its
            # own shard, which is harmless - its gradients simply stop
            # reaching anybody.
            return
        if half is None:
            half = _FP16_GRADS
        arrs = [t.detach().cpu().numpy() for t in tensors]
        wire = arrs
        if half:
            # Only float32; float64 tensors keep their range, and integer
            # buffers would be corrupted outright.
            wire = [a.astype("float16") if a.dtype == _np_ddp.float32 else a for a in arrs]
        # Reduce mode when the payload is uniform enough for the daemon to
        # average it, which is the ordinary case: one dtype across every
        # gradient. Anything mixed falls back to the blackboard, where the
        # daemon does not need to understand what it is holding.
        wire_dtypes = {a.dtype for a in wire}
        if _REDUCE_OK and _INT8_GRADS and kind == "grads" and len(wire_dtypes) == 1:
            means = _daemon_reduce_int8(arrs, key=id(tensors[0]))
            for i, t in enumerate(tensors):
                t.data.copy_(_th.from_numpy(
                    _np_ddp.ascontiguousarray(means[i].astype(arrs[i].dtype))).to(t.device))
            return
        if _REDUCE_OK and len(wire_dtypes) == 1 and next(iter(wire_dtypes)).name in (
                "float16", "float32", "float64"):
            means = _daemon_reduce(wire, next(iter(wire_dtypes)), kind)
            for i, t in enumerate(tensors):
                t.data.copy_(_th.from_numpy(
                    _np_ddp.ascontiguousarray(means[i].astype(arrs[i].dtype))).to(t.device))
            return
        blobs = _daemon_exchange(pickle.dumps(wire, protocol=pickle.HIGHEST_PROTOCOL))
        peers = [pickle.loads(b) for b in blobs]
        for i, t in enumerate(tensors):
            acc = peers[0][i].astype("float64", copy=True)
            for pr in peers[1:]:
                acc += pr[i]
            acc /= len(peers)
            t.data.copy_(_th.from_numpy(acc.astype(arrs[i].dtype)).to(t.device))

    def _daemon_broadcast(tensors):
        """Overwrite tensors with rank 0's values, through the daemon channel.

        Never reduced precision: this is how every rank agrees on the model
        it is training, and a rounding difference here would compound over
        the run rather than average out as a gradient's would."""
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
    # Keyed by id() rather than the loader itself: the decision is cheap to
    # recompute but must not be repeated per epoch, and a DataLoader is not
    # reliably hashable once a user subclasses it.
    _SHARDED = {}
    import itertools

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
        _OPTS.append(weakref.ref(self))
        _orig_step = self.step

        def _step(_self, *sa, **skw):
            if _BACKEND == "daemon":
                n = _STEPN.get(id(self), 0)
                _STEPN[id(self)] = n + 1
                params = [p for g in self.param_groups for p in g["params"]]
                if self not in _STEPPED:
                    _STEPPED.add(self)
                    # The opening weight broadcast, not a gradient: every rank
                    # starts from the same seed, so these are identical by
                    # construction and must not be read as duplicated work.
                    _daemon_allreduce([p.data for p in params], half=False, kind="weights")
                    _log("ddp daemon-channel sync ready (rank %d/%d, every %d step%s)"
                         % (_RANK, _WORLD, _SYNC_EVERY, "" if _SYNC_EVERY == 1 else "s"))
                import time as _t_step
                _now = _t_step.monotonic()
                if _STEP_MARK[0] is not None:
                    _elapsed = _now - _STEP_MARK[0]
                    _synced = _DDP_STATS["sync_sec"] - _STEP_MARK[1]
                    if _elapsed > _synced >= 0:
                        _STEP_SEC[0] += _elapsed - _synced
                        _STEP_SEC[1] += 1
                        _push_window(_STEP_RECENT, _elapsed - _synced, _STEP_WINDOW)
                _STEP_MARK[0], _STEP_MARK[1] = _now, _DDP_STATS["sync_sec"]

                every = _tuned_sync_every()
                try:
                    if every == 1:
                        grads = [p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad
                                 for p in params if p.grad is not None]
                        if grads:
                            _daemon_allreduce(grads)
                        return _orig_step(*sa, **skw)
                    out = _orig_step(*sa, **skw)
                    if (n + 1) % every == 0:
                        _daemon_allreduce([p.data for p in params], kind="weights")
                    return out
                finally:
                    # This step's gradient has been sent with the batch behind
                    # it; the next forward pass records its own. Left set, the
                    # first batch of the run would be reported for every step
                    # and a short final batch never seen.
                    _BATCH_N[0] = 0
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

        # A bound method, as the optimizer's own step is: an LR scheduler
        # wraps opt.step through its __func__, and a plain function made
        # every script with a scheduler fail at the scheduler's constructor.
        import types as _types
        self.step = _types.MethodType(_step, self)

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

    def _shard(self):
        """Give this rank a disjoint slice of the data.

        Averaging gradients over ranks that all read the SAME data is not
        data-parallel training: every rank recomputes the same work and the
        averaged gradient equals the single-process one, so N machines buy
        nothing but a slower step. Sharding is what makes the ring worth
        having, and it only happened before if the user had built a
        DistributedSampler themselves - which the bundled demo did not.

        The sampler swap is preferred because a skipped batch is still a
        loaded batch: the dataset would be read in full on every rank.
        DataLoader forbids assigning .sampler after construction, but the
        BatchSampler it wraps is an ordinary object, so batched loaders take
        that path and only the unbatched ones fall back to striding.
        """
        ds = getattr(self, "dataset", None)
        if ds is None:
            return None
        shuffle = isinstance(getattr(self, "sampler", None),
                             _th.utils.data.RandomSampler)
        bs = getattr(self, "batch_sampler", None)
        can_rebatch = (bs is not None and hasattr(bs, "sampler")
                       and isinstance(getattr(bs, "batch_size", None), int)
                       and bs.batch_size > 0)

        # Unequal shares need the batch to grow with the shard, not just the
        # shard. Every rank must take the same NUMBER of steps or the ring
        # deadlocks: each step ends at a barrier, so a rank with three times
        # the data and the same batch size runs three times as many steps and
        # the others are left waiting at a sync that never completes. Scaling
        # both keeps shard/batch - the step count - identical for everyone.
        #
        # So weights are only honoured where the batch size can be changed
        # too. Where it cannot, equal shards are the safe answer and the
        # reason is said out loud rather than discovered as a hang.
        if _WEIGHTS is not None:
            if not can_rebatch:
                _log("ddp: this loader's batch size cannot be changed, so the "
                     "measured shares (%s) cannot be used - every rank would "
                     "run a different number of steps and the ring would hang "
                     "at the first sync. Falling back to equal shards."
                     % ", ".join("%.2f" % w for w in _WEIGHTS))
            else:
                try:
                    sampler = _WeightedShardSampler(
                        len(ds), _WEIGHTS, _RANK, shuffle, bs.batch_size, _WORLD)
                except Exception as e:
                    _log("ddp: cannot shard this dataset by measured share (%s); "
                         "falling back to equal shards" % e)
                else:
                    bs.sampler = sampler
                    bs.batch_size = sampler.batch
                    sampler.gen = _RING_GEN[0]
                    _SHARDED_HASH[0] = True
                    _WEIGHTS_APPLIED[0] = True
                    _log("ddp: rank %d takes %d of %d samples (share %.0f%%), "
                         "batch %d, %d steps an epoch - the step count is the "
                         "same on every rank, so none waits on another"
                         % (_RANK, len(sampler), len(ds), 100 * _WEIGHTS[_RANK],
                            sampler.batch, sampler.steps))
                    return sampler

        # Equal shares take the same hashed sampler, so a rank that dies
        # later hands over only its own samples; DistributedSampler's strided
        # slices would all move.
        if can_rebatch:
            try:
                sampler = _WeightedShardSampler(
                    len(ds), [1.0 / _WORLD] * _WORLD, _RANK, shuffle, bs.batch_size, _WORLD)
            except Exception as e:
                _log("ddp: cannot shard this dataset by hash (%s); using "
                     "DistributedSampler, so a rank that dies loses its data" % e)
            else:
                bs.sampler = sampler
                bs.batch_size = sampler.batch
                sampler.gen = _RING_GEN[0]
                _SHARDED_HASH[0] = True
                _log("ddp: sharding %d samples across %d ranks" % (len(ds), _WORLD))
                return sampler

        try:
            sampler = _th.utils.data.distributed.DistributedSampler(
                ds, num_replicas=_WORLD, rank=_RANK, shuffle=shuffle)
        except Exception as e:
            _log("ddp: cannot shard this dataset (%s); every rank reads all of it" % e)
            return None
        if can_rebatch or (bs is not None and hasattr(bs, "sampler")):
            bs.sampler = sampler
            _log("ddp: sharding %d samples across %d ranks" % (len(ds), _WORLD))
            return sampler
        return False        # sharding needed, but only striding is available

    def _ddp_base():
        return _SYNC_URL.rsplit("/sync", 1)[0]

    def _ddp_request(url, body=None, ctype="application/json", timeout=60):
        """One call to the lead daemon; on refusal, the daemon's own words."""
        import urllib.error
        import urllib.request
        req = urllib.request.Request(url, data=body)
        if body is not None:
            req.add_header("Content-Type", ctype)
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
        except (urllib.error.URLError, OSError) as e:
            # The lead daemon is gone or unreachable: status 0, so every
            # caller reports it in its own words instead of a traceback.
            _log("ddp: %s failed: %s" % (url.split("?")[0], getattr(e, "reason", e)))
            return 0, str(getattr(e, "reason", e)).encode()

    def _live_opts():
        return [o for o in (r() for r in _OPTS) if o is not None]

    def _send_snapshot(join, boundary, world, weights):
        """Hand this rank's model and optimizer to a rank joining the ring.

        Taken at the epoch boundary where the ring grows, when every rank
        holds the same weights and optimizer state, so one copy is the ring's.
        """
        import io
        import json
        import struct
        opts = _live_opts()
        state = {
            "params": [[p.detach().cpu() for g in o.param_groups for p in g["params"]]
                       for o in opts],
            "opts": [o.state_dict() for o in opts],
            "stepn": [_STEPN.get(id(o), 0) for o in opts],
        }
        header = json.dumps({"group": _GROUP, "rank": _RANK, "join": join,
                             "boundary": boundary, "seq": _SEQ[0], "world": world,
                             "weights": weights,
                             "sync_every": int(_SYNC_TUNED[0] or 0)}).encode()
        # Header, length and model in one buffer, sent as a view of it: the
        # model used to be copied out of the buffer and then again into the
        # body, three copies of a multi-GB model at the epoch boundary.
        buf = io.BytesIO()
        buf.write(header + b"\n" + b"\0\0\0\0")
        start = buf.tell()
        _th.save(state, buf)
        size = buf.tell() - start
        buf.seek(start - 4)
        buf.write(struct.pack(">I", size))
        code, reply = _ddp_request(_ddp_base() + "/snapshot", buf.getbuffer(),
                                   "application/vnd.pipedpeer.ddp", timeout=240)
        if code != 200:
            raise RuntimeError("HTTP %d: %s" % (code, reply[:300]))
        _log("ddp: handed the model and optimizer to joining rank %d (%.1f MiB)"
             % (join, size / 1048576.0))

    def _drop_joiner(join, world, why):
        """Tell the lead daemon a joining rank will never get its model.

        The ring grew to count it at this boundary, so without this every
        sync would wait out the partial window for a rank that never posts,
        and the refit stays off. Dropped, its share moves back to the ranks
        the ring had."""
        import json
        code, reply = _ddp_request(_ddp_base() + "/down", json.dumps(
            {"group": _GROUP, "rank": join, "world": world,
             "why": "the model could not be handed to it: %s" % why}).encode())
        if code != 200:
            _log("ddp: could not tell the ring to drop joining rank %d (HTTP %d %s); "
                 "every sync waits out the partial window until it does"
                 % (join, code, reply[:200]))

    def _ring_switch(call):
        """Take the ring's pending change at this epoch boundary."""
        nonlocal _WORLD, _WEIGHTS
        p, _PENDING[0] = _PENDING[0], None
        world = int(p.get("world") or _WORLD)
        w = p.get("weights")
        if world > _WORLD:
            if p.get("from") == _RANK and p.get("join") is not None:
                try:
                    _send_snapshot(p["join"], call, world, w)
                except Exception as e:
                    _log("ddp: could not hand the model to joining rank %s (%s); "
                         "the ring carries on and drops it" % (p["join"], e))
                    _drop_joiner(int(p["join"]), world, str(e)[:200])
            _log("ddp: the ring grows from %d to %d ranks at this epoch"
                 % (_live_ranks(), _live_ranks(w, world)))
        _WORLD = world
        if w is not None:
            _WEIGHTS = list(w)
            _WEIGHTS_APPLIED[0] = True
        _RING_GEN[0] += 1

    def _join_ring():
        """Wait for the ring's next epoch boundary and take its model.

        Says it is ready, then waits for the rank handing over the model to
        reach the boundary. Loads the parameters and optimizer state into
        this rank's own optimizers (built by the same script, so the same
        shapes in the same order), takes the ring's sync count, size and
        shares, and returns the epoch it joins at.
        """
        nonlocal _WORLD, _WEIGHTS
        import io
        import json
        import struct
        import urllib.parse
        code, reply = _ddp_request(_ddp_base() + "/join", json.dumps(
            {"group": _GROUP, "rank": _RANK, "world": _WORLD}).encode())
        if code != 200:
            raise RuntimeError("ddp: the ring would not take rank %d: %s"
                               % (_RANK, reply[:300].decode("utf-8", "replace")))
        _log("ddp: rank %d is ready; waiting for the ring's next epoch to join" % _RANK)
        url = _ddp_base() + "/snapshot?group=%s&rank=%d" % (urllib.parse.quote(_GROUP), _RANK)
        while True:
            code, data = _ddp_request(url, timeout=120)
            if code == 200:
                break
            if code != 204:
                raise RuntimeError("ddp: rank %d could not join: HTTP %d %s"
                                   % (_RANK, code, data[:300].decode("utf-8", "replace")))
        nl = data.find(b"\n")
        hdr = json.loads(data[:nl])
        rest = data[nl + 1:]
        n = struct.unpack(">I", rest[:4])[0]
        state = _th.load(io.BytesIO(rest[4:4 + n]), map_location="cpu")
        opts = _live_opts()
        if len(opts) != len(state["opts"]):
            raise RuntimeError(
                "ddp: this rank built %d optimizer(s) before its first epoch and the "
                "ring has %d, so it cannot take the ring's model"
                % (len(opts), len(state["opts"])))
        with _th.no_grad():
            for o, params, sd, stepn in zip(opts, state["params"], state["opts"], state["stepn"]):
                mine = [p for g in o.param_groups for p in g["params"]]
                if len(mine) != len(params):
                    raise RuntimeError("ddp: the ring's model has %d tensors and this "
                                       "rank's %d" % (len(params), len(mine)))
                for p, v in zip(mine, params):
                    p.data.copy_(v.to(p.device))
                o.load_state_dict(sd)
                # The ring broadcast its opening weights long ago; this rank
                # has them now, and a broadcast of its own would be a sync
                # nobody else makes.
                _STEPPED.add(o)
                _STEPN[id(o)] = int(stepn)
        _JOIN_LR[0] = [(weakref.ref(o), [g.get("lr") for g in o.param_groups])
                       for o in opts]
        _SEQ[0] = int(hdr["seq"])
        _WORLD = int(hdr["world"])
        if hdr.get("weights"):
            _WEIGHTS = [float(x) for x in hdr["weights"]]
            _WEIGHTS_APPLIED[0] = True
        if _SYNC_AUTO and hdr.get("sync_every"):
            _SYNC_TUNED[0] = int(hdr["sync_every"])
        _log("ddp: rank %d joins the ring of %d with rank %s's model and optimizer, "
             "skipping the %d epoch(s) the ring already did"
             % (_RANK, _live_ranks(), hdr.get("rank", 0), int(hdr["boundary"])))
        return int(hdr["boundary"])

    def _restore_join_lr():
        """Put back the learning rate the ring handed over.

        The script's own per-epoch code still ran over the skipped epochs, so
        a scheduler stepped once per epoch decayed the loaded rate a second
        time for every one of them - the ring's rate is already decayed for
        those epochs. Its step count now matches the ring's, so only the
        rate is restored."""
        for ref, lrs in _JOIN_LR[0] or ():
            o = ref()
            if o is None:
                continue
            for g, lr in zip(o.param_groups, lrs):
                if lr is not None and g.get("lr") != lr:
                    _log("ddp: learning rate %g restored to the ring's %g after "
                         "the skipped epochs stepped this rank's scheduler"
                         % (g.get("lr"), lr))
                    g["lr"] = lr
        _JOIN_LR[0] = None

    def _iter(self):
        sampler = getattr(self, "sampler", None)
        if _WORLD > 1 and not _NATIVE_DDP:
            call = _ITERS[0]
            _ITERS[0] += 1
            if _JOIN[0]:
                if _JOIN_AT[0] is None:
                    _JOIN_AT[0] = _join_ring()
                if call < _JOIN_AT[0]:
                    # An epoch the ring finished before this rank arrived.
                    _EPOCHS[self] = _EPOCHS.get(self, 0) + 1
                    return iter(())
                _JOIN[0] = False
                _restore_join_lr()
            # An epoch boundary is the one place the ring can change safely:
            # the sampler re-slices here anyway, so no rank is part-way
            # through a shard when the boundaries move.
            if _PENDING[0] is not None:
                _ring_switch(call)
            if not isinstance(sampler, _th.utils.data.distributed.DistributedSampler):
                shard = _SHARDED.get(id(self))
                if shard is None:
                    shard = self._pp_shard()
                    _SHARDED[id(self)] = shard
                if shard is False:
                    # No sampler to swap: hand out every _WORLD-th batch. The
                    # slices are still disjoint, which is what correctness
                    # needs; only the loading is wasteful.
                    epoch = _EPOCHS.get(self, 0)
                    _EPOCHS[self] = epoch + 1
                    return itertools.islice(_ORIG_ITER(self), _RANK, None, _WORLD)
                sampler = shard
            # A loader sharded before the ring last changed is resliced at its
            # own next epoch, whichever loader took the change.
            if (isinstance(sampler, _WeightedShardSampler)
                    and getattr(sampler, "gen", 0) != _RING_GEN[0]):
                new = _WEIGHTS if _WEIGHTS is not None else [1.0 / _WORLD] * _WORLD
                bs = getattr(self, "batch_sampler", None)
                try:
                    reshared = _WeightedShardSampler(
                        sampler.n, new, _RANK, sampler.shuffle,
                        sum(sampler.batches) // len(sampler.batches), _WORLD)
                except Exception as e:
                    sampler.gen = _RING_GEN[0]
                    _log("ddp: cannot take the ring's new shares (%s); keeping "
                         "the ones in use" % e)
                else:
                    reshared.gen = _RING_GEN[0]
                    if bs is not None:
                        bs.sampler = reshared
                        bs.batch_size = reshared.batch
                    _SHARDED[id(self)] = reshared
                    sampler = reshared
                    _log("ddp: rank %d of %d now takes %d samples, batch %d"
                         % (_RANK, _live_ranks(), len(reshared), reshared.batch))

            if hasattr(sampler, "set_epoch"):
                sampler.set_epoch(_EPOCHS.get(self, 0))
                _EPOCHS[self] = _EPOCHS.get(self, 0) + 1
        return _ORIG_ITER(self)

    def _ddp_init(self, *a, **kw):
        _NATIVE_DDP.append(True)
        _log("native DistributedDataParallel detected; shim sync disabled")
        return _ORIG_DDP(self, *a, **kw)

    def _batch_pre_hook(module, args):
        """Record how many samples this step's forward pass is over.

        A global pre-hook, not a patch of Module.forward. Assigning to
        nn.Module.forward intercepts nothing: every real module defines its
        own forward, and Python resolves the subclass's first - measured, a
        patched Module.forward saw not one call for an ordinary model. So the
        sample count was always zero on the wire, which made the weighted
        gradient average fall back to equal weights (wrong once batches are
        proportional) and left the mid-run refit with no rates to work with.

        Only the first module of a step is recorded. The hook fires for every
        layer too, and an inner layer's leading dimension is not the batch
        once a model reshapes.
        """
        if _WORLD <= 1 or _BATCH_N[0]:
            return
        for a in args:
            if hasattr(a, "shape") and getattr(a, "ndim", 0) >= 1:
                try:
                    _BATCH_N[0] = int(a.shape[0])
                except Exception:
                    pass
                return

    try:
        _th.nn.modules.module.register_module_forward_pre_hook(_batch_pre_hook)
    except Exception as e:
        _log("ddp: cannot observe batch sizes (%s); gradients will be averaged "
             "as equals, which is only right when the batches are" % e)

    _ORIG_DDP = _th.nn.parallel.DistributedDataParallel.__init__
    _th.nn.parallel.DistributedDataParallel.__init__ = _ddp_init
    _th.optim.Optimizer.__init__ = _opt_init
    _th.nn.Module.forward = _forward
    _th.utils.data.DataLoader._pp_shard = _shard
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
        if n <= head_n or _jb_has_shared_accumulator(tasks):
            _record_local(n)
            return run_local(tasks)
        head, tail = tasks[:head_n], tasks[head_n:]
        t0 = time.monotonic()
        head_results = run_local(head)
        _record_local(len(head))
        cost = (time.monotonic() - t0) * max(1, min(len(head), width)) / len(head)

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
