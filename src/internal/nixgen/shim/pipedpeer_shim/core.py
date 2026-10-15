"""Shared shim state: environment, logging, --offload, the submit batcher."""
import os
import sys
import threading
import time


_ENABLED = os.environ.get("PIPEDPEER_SHIM") == "1"
_URL = os.environ.get("PIPEDPEER_DAEMON_URL", "")
_STORE = os.environ.get("PIPEDPEER_STORE_PATH", "")
_NODE_ID = os.environ.get("PIPEDPEER_NODE_ID", "")
_NUM_SHARDS = os.environ.get("PIPEDPEER_CLOSURE_NODES", "0")  # closure node count; map shards use PIPEDPEER_NUM_SHARDS
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
        from pipedpeer_shim.stats import _record_local  # stats imports core
        _record_local(len(group))
        pending = [(entry, ctx.apply_async(fn, entry[1])) for entry in group]
        for entry, ar in pending:
            try:
                r = ar.get()
            except Exception as e:
                _settle(entry, None, e)
            else:
                _settle(entry, r, None)




class _Raised(object):
    """A call's exception, returned in place of its result."""

    def __init__(self, exc):
        self.exc = exc
