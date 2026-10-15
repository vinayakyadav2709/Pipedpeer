"""_ClusterPool: the multiprocessing.Pool drop-in (racing, pacing and payloads live in pool_*.py)."""
import atexit
import os
import time

from pipedpeer_shim import core
from pipedpeer_shim.core import (
    _BatchResult, _OFFLOAD_HERE, _POOL_MIN_WORK, _SubmitBatcher, _cluster_ready, _log, _offload_warn,
)
from pipedpeer_shim.stats import (
    _STATS, _STATS_LOCK, _add_stat, _decide, _record, _record_local,
)
from pipedpeer_shim.transfer import (
    _dumps, _loads, _pool_required, _pool_send,
)
from pipedpeer_shim.pool_func import (
    _apply,
    _apply_catching,
    _func_payload,
    _func_pickle,
    _process_safe,
    _timed,
)
from pipedpeer_shim.pool_pacing import (
    _FUNC_KEY,
    _TLS,
    _flight_opener,
    _fn_fp,
    _relay_steps,
    _stop_pool,
)
from pipedpeer_shim.pool_race import (
    _RemotePairs,
    _race_items,
)
from pipedpeer_shim.pool_resources import (
    _imap_batch,
    _pool_width,
)


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
        want = _pool_width(self._requested, self._workers_memory(), self._procs)
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
            _stop_pool(old)
        except Exception as e:
            _log("stopping the old local pool after a resize failed: %s" % e)

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
        if _OFFLOAD_HERE and self._remote and items:
            # --offload: no measuring head on this machine; all of it ships.
            payload = _func_payload(func) or _func_pickle(func)
            if payload is not None:
                _decide("pool.map", True, "--offload keeps this machine's share at 0")
                return self._race(func, items, starmap, 0, payload, catch)
            _offload_warn("a pool.map", "its function cannot be sent, by source or by value")
        if not self._remote or len(items) <= self._measure_items:
            return self._local(func, items, starmap, catch)
        # Measure the first few locally, then decide whether to spill the rest.
        # The batch is as wide as the pool: measuring four items on a twenty
        # core machine left sixteen cores idle for the length of one item, a
        # serial bubble in front of every intercepted map.
        head_n = max(self._measure_items, min(self._procs, len(items) // 2))
        head, tail = items[:head_n], items[head_n:]
        head_results, cost = self._measure(func, head, starmap, catch)
        if not tail:
            return head_results

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
        if not core._FORCE and len(tail) * cost < _POOL_MIN_WORK:
            _log("tail is %.2fs of work, below the %.2fs dispatch floor; staying local"
                 % (len(tail) * cost, _POOL_MIN_WORK))
            _decide("pool.map", False, "%d items x %.3fs = %.2fs of work, under the %.2fs worth a round trip"
                    % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
            return head_results + self._local(func, tail, starmap, catch)
        _decide("pool.map", True, "forced" if core._FORCE else
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

    def _measure(self, func, items, starmap, catch):
        """Run the head locally; return (results, per-item cost in seconds).

        Each item is timed inside the worker, around the call alone. The
        batch's wall clock used to be the measure, and it counted pool
        dispatch, pickling and scheduler waits as work: on a loaded laptop
        a trivial x + 1 measured 14-20 ms per item and shipped. The cost is
        the 25th percentile of the samples, so one item that waited on a
        busy core does not set the estimate either."""
        _log("local %d items" % len(items))
        _record_local(len(items))
        pairs = self._local_ctx(func).starmap(
            _timed, [(func, v, starmap, catch) for v in items])
        secs = sorted(dt for _, dt in pairs)
        return [r for r, _ in pairs], (secs[len(secs) // 4] if secs else 0.0)

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
        flight = getattr(_TLS, "flight", None)
        try:
            gvars = payload.get("gvars") or {}
            if payload.get("pickled"):
                # The callable rides in the globals frame as raw bytes, not
                # as base64 in the JSON header: a third bigger, and parsed as
                # one JSON string on every hop. The worker pops it from there.
                gvars = dict(gvars, **{_FUNC_KEY: payload["pickled"]})
            globals_pickle = pickle.dumps(gvars)
            items = [_dumps(v) for v in vals]
            header = {
                "items_frames": len(items),
                "globals": True,
                "starmap": starmap,
                "fn_fp": _fn_fp(payload),
                "oob": True,       # results may come back out of band; _loads reads both
            }
            # Exactly one of the two, never both: the runner prefers func_src
            # and would silently ignore a callable sent beside it, so a
            # header carrying both would test the wrong path while looking
            # like it tested this one.
            if not payload.get("pickled"):
                header["func_src"] = payload["src"]
                header["func_name"] = payload["name"]
            # Admission control: the daemon 503s rather than OOM when it
            # cannot spare this, and micro-chunks when it is over budget.
            #
            # The callable counts towards it: a lambda closing over a large
            # array carries that array inside itself, and it rides in
            # globals_pickle (above), so sizing globals+items includes it.
            header["required_mem"] = _pool_required(globals_pickle, items)
            if core._FORCE:
                header["force"] = True
            info = {}
            blobs = _pool_send(header, globals_pickle, items, 600, info,
                               opener=_flight_opener(flight))
            if len(blobs) != len(idxs):
                raise RuntimeError("got %d results for %d items" % (len(blobs), len(idxs)))
            out = [(idxs[i], _loads(b)) for i, b in enumerate(blobs)]
        except Exception as e:
            if flight is not None and flight.cancelled:
                # Local already holds every result: a cancelled loser, not a failure.
                _log("cancelled a chunk of %d items: this machine finished them first" % len(idxs))
                return None
            _record(kind, len(idxs), False, str(e), (time.monotonic() - t0) * 1000)
            _log("remote failed (%s); local covers it" % e)
            return None
        ms = (time.monotonic() - t0) * 1000
        receipt = info.get("receipt")
        _record(kind, len(out), True, "", ms, receipt)
        _relay_steps(receipt)
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
            _stop_pool(self._ctx)
        except Exception as e:
            _log("pool exit: stopping the local pool failed: %s" % e)
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
