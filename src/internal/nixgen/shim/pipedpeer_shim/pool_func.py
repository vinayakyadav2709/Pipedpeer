"""Function payloads: shipping a callable by value, and chunking."""
import os
import sys
import time

from pipedpeer_shim.core import (
    _Raised, _log,
)


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
    """Ship func by value with cloudpickle, as {"pickled": bytes}, or None.

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
    return {"pickled": blob}


def _apply(func, item):
    """Run func over one starmap item (a tuple of args)."""
    return func(*item) if isinstance(item, tuple) else func(item)


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


def _timed(func, item, starmap, catch):
    """One item the way _local runs it, and the seconds the call took."""
    t0 = time.perf_counter()
    if catch:
        r = _apply_catching(func, item)
    elif starmap:
        r = func(*item)
    else:
        r = func(item)
    return r, time.perf_counter() - t0


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
