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
          "parts": []}
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
    for p in (receipt or {}).get("parts", []):
        if str(p.get("where", "")).startswith("peer:"):
            elsewhere += int(p.get("items", 0))
        else:
            here += int(p.get("items", 0))
    with _STATS_LOCK:
        _claim_receipt()
        if ok:
            _STATS["dispatched_items"] += items
            _STATS["remote_items"] += elsewhere
            _STATS["local_items"] += here
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
        _STATS["parts"].append(entry)


def _record_local(items):
    with _STATS_LOCK:
        _claim_receipt()
        _STATS["local_items"] += items


@atexit.register
def _write_receipt():
    path = os.environ.get("PIPEDPEER_RECEIPT")
    if not path or _RECEIPT_OWNER[0] != os.getpid():
        return
    import json
    try:
        with _STATS_LOCK:
            body = json.dumps(_STATS)
        with open(path, "w") as f:
            f.write(body)
    except Exception:
        pass


class _ClusterPool:
    """drop-in replacement for multiprocessing.Pool.

    Starts as a real local pool; the first chunks run on local cores, which is
    the measurement. Spills to the cluster only when measured per-item cost x
    remaining clearly exceeds the dispatch cost. Local cores never stop pulling,
    so a remote node is an additional consumer, never a subtracter.
    """

    def __init__(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        import multiprocessing.pool as _mp
        # NUM_SHARDS counts this node too, so "1" means there is nobody to
        # spill to. Dispatching then posts work to our own daemon, which runs
        # it through a single serialised warm worker while the local pool sits
        # idle — strictly slower than doing it here.
        try:
            shards = int(_NUM_SHARDS)
        except ValueError:
            shards = 0
        self._remote = bool(_URL) and _ENABLED and shards >= 2
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
        atexit.register(self.close)

    def _resize(self):
        """Re-size the local pool to what the machine can run right now.

        A width chosen when the pool was built can be wrong by the time the
        next map arrives: another job may have taken the memory, or released
        it. multiprocessing.Pool cannot grow or shrink, so this rebuilds it
        between calls; within a single map the width is fixed.
        """
        want = _pool_width(self._requested)
        if want == self._procs:
            return
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

    # ---- the four Pool workhorses ----
    #
    # apply and apply_async are single calls: there is no tail to spill, so
    # they run locally. They still have to ask which local pool can take the
    # callable - a lambda cannot be sent to a worker process, and handing one
    # to the process pool raises PicklingError. map and imap reach the same
    # routing through _run; these two are the paths that bypassed it.
    def apply(self, func, args=(), kwds=None):
        # kwds defaults to None here and to {} in multiprocessing, and it is
        # passed straight through to func(*args, **kwds). So pool.apply(f, (x,))
        # - the ordinary call, with no keyword arguments - raised "argument
        # after ** must be a mapping, not NoneType" from inside the worker,
        # which reads as a bug in the user's function.
        return self._local_ctx(func).apply(func, args=args, kwds=kwds or {})

    def apply_async(self, func, args=(), kwds=None, callback=None, error_callback=None):
        return self._local_ctx(func).apply_async(
            func, args=args, kwds=kwds or {},
            callback=callback, error_callback=error_callback)

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

    def _run(self, func, items, starmap=False):
        self._resize()
        if not self._remote or len(items) <= self._measure_items:
            return self._local(func, items, starmap)
        # Measure the first few locally, then decide whether to spill the rest.
        # The batch is as wide as the pool: measuring four items on a twenty
        # core machine left sixteen cores idle for the length of one item, a
        # serial bubble in front of every intercepted map.
        head_n = max(self._measure_items, min(self._procs, len(items) // 2))
        head, tail = items[:head_n], items[head_n:]
        t0 = time.monotonic()
        head_results = self._local(func, head, starmap)
        wall = time.monotonic() - t0
        if not tail:
            return head_results
        # Per-item cost comes from the batch we just ran, not from executing an
        # item a third time. The batch occupied min(items, procs) lanes, so the
        # wall clock covers that many items at once.
        lanes = max(1, min(len(head), self._procs))
        cost = wall * lanes / max(len(head), 1)
        if cost <= 0:
            return head_results + self._local(func, tail, starmap)

        payload = _func_payload(func) or _func_pickle(func)
        if payload is not None and payload.get("pickled"):
            _log("kernel %r has no source to send; shipping it by value"
                 % getattr(func, "__name__", "?"))
        if payload is None:
            _record("pool", len(tail), False, "unshippable")
            _log("kernel %r cannot ship at all, by source or by value; "
                 "staying local" % getattr(func, "__name__", "?"))
            return head_results + self._local(func, tail, starmap)
        if not _FORCE and len(tail) * cost < _POOL_MIN_WORK:
            _log("tail is %.2fs of work, below the %.2fs dispatch floor; staying local"
                 % (len(tail) * cost, _POOL_MIN_WORK))
            return head_results + self._local(func, tail, starmap)
        return head_results + self._race(func, tail, starmap, cost, payload)

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

    def _local(self, func, items, starmap):
        _log("local %d items" % len(items))
        _record_local(len(items))
        ctx = self._local_ctx(func)
        if starmap:
            return ctx.starmap(func, items)
        return ctx.map(func, items)

    def _race(self, func, items, starmap, per_item_cost, payload):
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
        chunk_size = max(1, min(max(_adaptive_chunk(per_item_cost), self._procs),
                                (n + 1) // 2))
        chunks = _chunk(list(enumerate(items)), chunk_size)  # [(orig_idx, item), ...]

        # Interleave chunk ownership so neither side is strictly first.
        local_chunks = chunks[::2]
        remote_chunks = chunks[1::2]

        def fill(pairs):
            """Run user func over (idx, item) pairs and first-wins into slots."""
            idxs = [p[0] for p in pairs]
            vals = [p[1] for p in pairs]
            ctx = self._local_ctx(func)
            if starmap:
                # _apply carries func as an argument, so a process pool would
                # have to pickle it as data; route by the same test either way.
                res = ctx.starmap(_apply, [(func, v) for v in vals])
            else:
                res = ctx.map(func, vals)
            with lock:
                for i, v in zip(idxs, res):
                    if slots[i] is None:
                        slots[i] = v

        # Remote thread: dispatch remote chunks, first-wins into slots.
        # Chunks go out concurrently. Dispatching them one at a time capped
        # the whole cluster's throughput at a single chunk, so a peer sat idle
        # between round trips no matter how many cores it had.
        def deliver(chunk):
            res = self._remote_chunk(payload, chunk, starmap)
            if res is None:
                return
            with lock:
                for orig_i, v in res:
                    if slots[orig_i] is None:
                        slots[orig_i] = v

        def remote_run():
            if not remote_chunks:
                return
            width = max(1, min(_REMOTE_INFLIGHT, len(remote_chunks)))
            if width == 1:
                for chunk in remote_chunks:
                    deliver(chunk)
                return
            import concurrent.futures as _cfut
            with _cfut.ThreadPoolExecutor(max_workers=width) as ex:
                list(ex.map(deliver, remote_chunks))

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

    def _remote_chunk(self, payload, chunk, starmap):
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
            _record("pool", len(idxs), False, str(e), (time.monotonic() - t0) * 1000)
            _log("remote failed (%s); local covers it" % e)
            return None
        ms = (time.monotonic() - t0) * 1000
        receipt = info.get("receipt")
        _record("pool", len(out), True, "", ms, receipt)
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
                room = int(raw) - used
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


def _imap_batch(procs):
    """Items to buffer before running a batch of a lazy imap."""
    override = os.environ.get("PIPEDPEER_IMAP_BATCH", "").strip()
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            pass
    return max(64, procs * 32)


def _pool_width(requested):
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
    cores = os.cpu_count() or 1
    if os.environ.get("PIPEDPEER_RESPECT_POOL_SIZE") == "1" and requested:
        return max(1, int(requested))
    n = cores
    avail = _avail_bytes()
    if avail > 0:
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
        return os.path.dirname(f) == os.path.dirname(os.path.realpath(os.__file__))
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
                                     200e9, working)

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
# Always on, and safe to be: every intercept gates on the latency cost
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


def _maybe_compress(items, header):
    """Compress item frames when it pays for itself.

    Whether it does depends entirely on the payload. A pandas frame of
    strings or categoricals shrinks by most of its size; a dense float array
    is already incompressible and every byte of CPU spent on it is waste. So
    try, keep the result only if it is meaningfully smaller, and record the
    choice in the header rather than guessing again on the other side.

    The daemon relays these bytes without looking inside them, so this is
    settled entirely between the shim and the worker - no wire change.
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


def _pool_send(header, globals_pickle, items, timeout, out_header=None):
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
    with _daemon_open(req, timeout) as resp:
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


def _weighted_batches(weights, base_batch, world):
    """Each rank's per-step batch, together adding up to the ring's.

    The ring processes base_batch*world samples per step however the shares
    fall; only the split between ranks changes. Rounding is absorbed by the
    largest rank so the total is exact - let it drift and the step counts
    drift with it.
    """
    total = max(world, int(base_batch) * world)
    b = [max(1, int(round(w * total))) for w in weights]
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

    Contiguous, not strided. Striding by rank only produces disjoint,
    exhaustive shards when every rank takes the same stride, which is exactly
    what stops being true here. Cumulative offsets do, for any shares.

    The remainder below one global batch is dropped, as drop_last does in any
    DDP setup, and shuffling makes it a different remainder each epoch.

    Shuffling is seeded from the epoch alone, so every rank permutes the same
    way and their slices stay disjoint - the same contract DistributedSampler
    keeps, and the reason set_epoch exists.
    """

    def __init__(self, n, weights, rank, shuffle, base_batch, world):
        if n <= 0:
            raise ValueError("empty dataset")
        self.n = n
        self.rank = rank
        self.shuffle = shuffle
        self.epoch = 0
        self.batches = _weighted_batches(weights, base_batch, world)
        self.batch = self.batches[rank]
        self.steps = n // sum(self.batches)
        if self.steps < 1:
            raise ValueError(
                "%d samples do not make one step of %d across the ring"
                % (n, sum(self.batches)))

    def set_epoch(self, epoch):
        self.epoch = epoch

    def _bounds(self):
        start = sum(self.steps * b for b in self.batches[:self.rank])
        return start, start + self.steps * self.batch

    def __len__(self):
        return self.steps * self.batch

    def __iter__(self):
        # torch is imported per-function everywhere in this file, never as a
        # module global - importing it at class scope would cost every script
        # the import whether or not it trains.
        import torch as _th_local
        order = list(range(self.n))
        if self.shuffle:
            g = _th_local.Generator()
            g.manual_seed(self.epoch)
            order = [order[i] for i in _th_local.randperm(self.n, generator=g).tolist()]
        start, end = self._bounds()
        return iter(order[start:end])


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
        return False
    bw = _measure_bandwidth()
    if not bw:
        return False
    return _offload_wins(nbytes, flops_per_byte, round_trip, split,
                         flops_per_sec, K, bw, remote_flops)


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
    if not split:
        # Single-worker offload (svd, eig) is not a race against local: it
        # relocates the work so a weak orchestrator is freed entirely, and is
        # worth it when shipping the matrix costs far less than computing it
        # here. Charging the remote compute against it as well made this
        # condition unsatisfiable - est_remote equals est_local on a cluster
        # of like machines, so it read "transfer + local < half of local".
        return est_transfer < est_local * 0.5
    est_remote = (nbytes * flops_per_byte / remote_flops_per_sec) / K * 1.3
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
    import numpy as _np_ddp
    # Gradients ship as float16 unless asked otherwise. Averaging is still
    # done in float64; only the wire is narrowed.
    _FP16_GRADS = os.environ.get("PIPEDPEER_DDP_FP32") != "1"
    _WORLD = int(os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
    _RANK = int(os.environ.get("PIPEDPEER_RANK", "0"))
    # Each rank's share of a step, measured by placement. Equal shares are the
    # same thing as no shares, and are treated as such.
    _WEIGHTS = _parse_weights(os.environ.get("PIPEDPEER_DDP_WEIGHTS", ""), _WORLD)
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
    # Shares the daemon has refitted from what the ring actually did, waiting
    # for an epoch boundary. Resharding mid-epoch would have ranks re-slice
    # data they are part-way through, so samples would be trained on twice or
    # skipped; the boundary is where the sampler already re-slices.
    _PENDING_WEIGHTS = [None]
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
    _DDP_STATS = {"syncs": 0, "sync_sec": 0.0, "sent_bytes": 0, "recv_bytes": 0}
    # Server-side averaging. Off with PIPEDPEER_DDP_BLACKBOARD=1, which falls
    # back to every rank receiving every rank's payload - needed only against
    # a daemon too old to average.
    _REDUCE_OK = os.environ.get("PIPEDPEER_DDP_BLACKBOARD", "") != "1"

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
        if w and len(w) == _WORLD:
            try:
                w = [float(x) for x in w]
            except (TypeError, ValueError):
                w = None
            if w and abs(sum(w) - 1.0) < 1e-6 and all(x > 0 for x in w):
                _PENDING_WEIGHTS[0] = w
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
        if not _DDP_STATS["syncs"]:
            return
        sync_total = _DDP_STATS["sync_sec"]
        _log("ddp: %d sync(s), %.1fs total (%.0f ms each), %.1f MiB sent, %.1f MiB received"
             % (_DDP_STATS["syncs"], sync_total,
                1000.0 * sync_total / _DDP_STATS["syncs"],
                _DDP_STATS["sent_bytes"] / 1048576.0,
                _DDP_STATS["recv_bytes"] / 1048576.0))
        if _WORLD < 2 or _STEP_SEC[1] < 2:
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
            _machines = int(os.environ.get("PIPEDPEER_DDP_MACHINES", "") or _WORLD)
        except ValueError:
            _machines = _WORLD
        _machines = max(1, min(_machines, _WORLD))
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
                 % (compute, sync_total, _WORLD, _machines, alone, speedup))
        elif _machines < _WORLD:
            _log("ddp: the ring did NOT pay for itself — %d ranks on %d "
                 "machine(s), so the extra ranks brought no extra hardware: "
                 "%.1fs of compute and %.1fs of sync against about %.1fs for "
                 "this machine alone. Co-located ranks divide one machine and "
                 "pay the sync on top; they are for a second accelerator, not "
                 "a second machine."
                 % (_WORLD, _machines, compute, sync_total, alone))
        else:
            _log("ddp: the ring did NOT pay for itself — %.1fs of compute and "
                 "%.1fs of sync across %d ranks, against about %.1fs for THIS "
                 "machine alone. Syncing costs more than the extra machines save.%s"
                 % (compute, sync_total, _WORLD, alone,
                    "" if _INT8_GRADS else
                    " PIPEDPEER_DDP_INT8=1 halves the bytes on the wire."))

    import atexit as _atexit_ddp
    _atexit_ddp.register(_ddp_report)
    _SEQ = [0]
    _STEPN = {}
    if _BACKEND == "daemon" and not _SYNC_URL:
        _BACKEND = "gloo"  # no sync endpoint handed down; old transport

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
        _DDP_STATS["syncs"] += 1
        _DDP_STATS["sync_sec"] += _t.monotonic() - _t0
        _DDP_STATS["sent_bytes"] += len(body)
        _DDP_STATS["recv_bytes"] += len(data)
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
        import urllib.request
        flat, shapes, sizes = _pack(arrs)
        flat = _np_ddp.ascontiguousarray(flat.astype(wire_dtype, copy=False))
        _t0 = _t.monotonic()
        _SEQ[0] += 1
        header = json.dumps({"group": _GROUP, "seq": _SEQ[0], "rank": _RANK,
                             "world": _WORLD, "dtype": wire_dtype.name,
                             "count": int(flat.size), "kind": kind,
                             "sync_every": int(_SYNC_TUNED[0] or 0),
                             "step_ms": _mean_step_ms(),
                             "sync_ms": _mean_sync_ms(),
                             "samples": int(_BATCH_N[0])}).encode()
        payload = flat.tobytes()
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
        if _ranks and _ranks < _WORLD and _LAST_RANKS[0] != _ranks:
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
        _DDP_STATS["syncs"] += 1
        _DDP_STATS["sync_sec"] += _t.monotonic() - _t0
        _DDP_STATS["sent_bytes"] += len(body)
        _DDP_STATS["recv_bytes"] += len(data)
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
                             "samples": int(_BATCH_N[0])}).encode()
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
        _DDP_STATS["syncs"] += 1
        _DDP_STATS["sync_sec"] += _t.monotonic() - _t0
        _DDP_STATS["sent_bytes"] += len(body)
        _DDP_STATS["recv_bytes"] += len(data)
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
        _orig_step = self.step

        def _step(*sa, **skw):
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
                    _WEIGHTS_APPLIED[0] = True
                    _log("ddp: rank %d takes %d of %d samples (share %.0f%%), "
                         "batch %d, %d steps an epoch - the step count is the "
                         "same on every rank, so none waits on another"
                         % (_RANK, len(sampler), len(ds), 100 * _WEIGHTS[_RANK],
                            sampler.batch, sampler.steps))
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

    def _iter(self):
        sampler = getattr(self, "sampler", None)
        if _WORLD > 1 and not _NATIVE_DDP:
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
            # An epoch boundary is the one place shares can change safely:
            # the sampler re-slices here anyway, so no rank is part-way
            # through a shard when the boundaries move.
            if _PENDING_WEIGHTS[0] is not None and isinstance(
                    sampler, _WeightedShardSampler):
                new = _PENDING_WEIGHTS[0]
                _PENDING_WEIGHTS[0] = None
                bs = getattr(self, "batch_sampler", None)
                try:
                    reshared = _WeightedShardSampler(
                        sampler.n, new, _RANK, sampler.shuffle,
                        sum(sampler.batches) // _WORLD, _WORLD)
                except Exception as e:
                    _log("ddp: cannot take the refitted shares (%s); keeping "
                         "the ones in use" % e)
                else:
                    if bs is not None:
                        bs.sampler = reshared
                        bs.batch_size = reshared.batch
                    _SHARDED[id(self)] = reshared
                    sampler = reshared
                    _log("ddp: rank %d reshared to %d samples, batch %d - the "
                         "daemon refitted the ring from what it measured"
                         % (_RANK, len(reshared), reshared.batch))

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
    one raised inside any job that touched the futures API. Everything except
    map() is now the stock executor; only map() is intercepted, and its real
    signature (several iterables, an iterator result) is honoured.
    """

    class _ClusterExecutor(base):
        def __init__(self, max_workers=None, *a, **kw):
            super().__init__(max_workers, *a, **kw)
            self._pp_workers = max_workers
            self._pp_pool = None

        def map(self, fn, *iterables, timeout=None, chunksize=1):
            if not iterables:
                return iter(())
            if len(iterables) == 1:
                items, starmap = list(iterables[0]), False
            else:
                items, starmap = list(zip(*iterables)), True
            if self._pp_pool is None:
                # Lazy: the stock executor spawns its own workers on first
                # submit, so a map-only executor never pays for two pools.
                self._pp_pool = _ClusterPool(processes=self._pp_workers)
            return iter(self._pp_pool._run(fn, items, starmap))

        def shutdown(self, wait=True, **kw):
            if self._pp_pool is not None:
                self._pp_pool.close()
                self._pp_pool = None
            super().shutdown(wait, **kw)

    _ClusterExecutor.__name__ = base.__name__
    _ClusterExecutor.__qualname__ = base.__qualname__
    return _ClusterExecutor


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
    _cf.ProcessPoolExecutor = _make_cluster_executor(_cf.ProcessPoolExecutor)


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
