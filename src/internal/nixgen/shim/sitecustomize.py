# Auto-imported by Python at startup when on PYTHONPATH.
# Patches parallel primitives to route through the local pipedpeer daemon.
#
# The code lives in the pipedpeer_shim package beside this file; this file
# holds only the install step (import hooks, executor patch, SIGTERM, joblib).
# A package, not loose modules: this dir is on PYTHONPATH ahead of
# site-packages and shares one import namespace with the job's own files, so
# a top-level core.py or stats.py here would collide with theirs.
import functools
import os
import site
import sys
import threading
import time

# This file shadows the Nix-supplied sitecustomize.py (both are named
# sitecustomize and only the first on PYTHONPATH is imported). The Nix one is
# what adds NIX_PYTHONPATH — the python-with-packages env's site-packages, where
# numpy/torch/etc. live — so without this the shim would silently strip the
# env's packages and every import would fail. Do exactly what Nix's does.
if "NIX_PYTHONPATH" in os.environ:
    _nix_paths = os.environ.pop("NIX_PYTHONPATH", None)
    if _nix_paths:
        functools.reduce(lambda k, p: site.addsitedir(p, k), _nix_paths.split(":"), site._init_pathinfo())

from pipedpeer_shim import core
from pipedpeer_shim.core import (
    _OFFLOAD_HERE, _POOL_MIN_WORK, _cluster_ready, _log, _offload_warn,
    _usable_cores, _warn,
)
from pipedpeer_shim.stats import (
    _add_stat, _decide, _record_local,
)
from pipedpeer_shim.numpy_ops import (
    _install_numpy,
)
from pipedpeer_shim.pandas_ops import (
    _install_io, _install_pandas,
)
from pipedpeer_shim.pool import (
    _ClusterPool, _func_payload, _func_pickle, _is_library_module, _race_items,
)
from pipedpeer_shim.ddp import (
    _PENDING_PATCHES, _install_ddp,
)


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
            # Not a silent slowdown: B1 (a crash here) went unseen for weeks.
            _warn("%s interception failed, so this script runs on this "
                  "machine only: %s" % (name, exc))


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
    if not core._ENABLED:
        return
    sys.meta_path.insert(0, _PatchOnImport())
    _defer("numpy", _install_numpy)
    _defer("torch", _install_ddp)
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
    if not (core._URL and core._ENABLED) or int(core._NUM_SHARDS) < 2:
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
    #
    # A task function defined in the job's own files (cpu.py's mandel_tile in
    # __main__) pickles by reference, and a worker's __main__ is its runner
    # script, so every remote task died with "no attribute mandel_tile". It
    # travels the way Pool.map's kernel does (ADR-0017): source, else
    # cloudpickle, once per chunk in the globals frame; the task names it.
    jb_payload = {
        "src": ("import os, pickle\n"
                "def _pp_unshare(x):\n"
                "    if type(x) is tuple and len(x) == 2 and x[0] == '__pp_shared__':\n"
                "        return globals()[x[1]]\n"
                "    return x\n"
                "def _pp_func(f):\n"
                "    if not (type(f) is tuple and len(f) == 2 and f[0] == '__pp_func__'):\n"
                "        return f\n"
                "    p = globals()[f[1]]\n"
                "    if type(p) is dict:\n"
                "        if p.get('pickled'):\n"
                "            p = pickle.loads(p['pickled'])\n"
                "        else:\n"
                "            ns = dict(p['gvars'])\n"
                "            exec(p['src'], ns)\n"
                "            p = ns[p['name']]\n"
                "        globals()[f[1]] = p\n"
                "    return p\n"
                "def run(item):\n"
                "    os.environ['PIPEDPEER_JB_NESTED'] = '1'\n"
                "    f, a, k = item\n"
                "    f = _pp_func(f)\n"
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

        fkeys = {}

        def fref(f):
            # ponytail: only f's own module is checked; a partial or wrapper
            # around a __main__ function still goes by reference and fails
            # remotely (local covers it).
            if _is_library_module(sys.modules.get(getattr(f, "__module__", None))):
                return f
            key = fkeys.get(id(f))
            if key is None:
                p = _func_payload(f) or _func_pickle(f)
                if p is None:
                    _log("joblib task %r cannot travel as source or by value; sent by name"
                         % getattr(f, "__name__", f))
                    return f
                key = fkeys[id(f)] = "_pp_func_%d" % len(fkeys)
                shared[key] = p
            return ("__pp_func__", key)

        items = [(i, (fref(f), tuple(ref(x) for x in a), {n: ref(v) for n, v in k.items()}))
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
        if not core._FORCE and len(tail) * cost < _POOL_MIN_WORK:
            _decide("joblib", False, "%d tasks x %.3fs = %.2fs of work, under the %.2fs worth a round trip"
                    % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
            _record_local(len(tail))
            return head_results + run_local(tail)
        _decide("joblib", True, "forced" if core._FORCE else
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
