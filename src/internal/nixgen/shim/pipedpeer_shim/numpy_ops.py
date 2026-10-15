"""numpy matmul offload and the numpy offload cost model."""
import os
import time

from pipedpeer_shim import core
from pipedpeer_shim import transfer
from pipedpeer_shim.core import (
    _TOKEN, _log, _offload_warn, _spill_min,
)
from pipedpeer_shim.stats import (
    _WHY, _decide,
)
from pipedpeer_shim.transfer import (
    _BW_CACHE, _BW_TTL, _pool_required, _pool_send, _pool_send_within,
)


def _np_dispatch(blocks, other):
    """Run [np.matmul(block, other) for block in blocks] over the cluster via
    the warm-worker /v1/pool/map path. Blocks and the fixed operand ship as
    raw pickle frames; the daemon splits the blocks across peers. Falls back
    to local on any failure so an absent cluster never breaks math."""
    import pickle
    try:
        # The worker runs the closure's python with no shim on its path, so the
        # function ships as source (pickling by reference would resolve
        # a shim module the worker cannot import and fail). The
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
        if core._FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, items, 1200)
        return [pickle.loads(p) for p in blobs]
    except Exception as e:
        _log("numpy remote failed (%s); local fallback" % e)
        import numpy as _np
        return [_np.matmul(b, other) for b in blocks]


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
                and a.shape[0] >= 8 and core._URL and core._ENABLED
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
        if _mm_gate(a, b):
            try:
                return _mm_dispatch(a, b)
            except Exception as e:
                _mm_no_fallback(a, b, e)
                _log("numpy matmul fallback (%s)" % e)
        return _orig_matmul(a, b, *args, **kw)

    def _dot(a, b, *args, **kw):
        if _mm_gate(a, b):
            try:
                return _mm_dispatch(a, b)
            except Exception as e:
                _log("numpy dot fallback (%s)" % e)
        return _orig_dot(a, b, *args, **kw)

    def _tensordot(a, b, axes=2, *args, **kw):
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
        if core._FORCE:
            header["force"] = True
        blobs = _pool_send_within(header, globals_pickle, items, deadline)
        return pickle.loads(blobs[0])

    def _linalg_deadline(a):
        # Past the time a peer like this machine needs (ship it, compute it,
        # ship U/S/Vh back), the offload has already lost; half again as
        # slack, and never under 30 s.
        est_local = a.nbytes * max(8, a.shape[0] // 4) / 1.5e9
        bw = _per_peer_bw(_BW_CACHE["bw"] or _OPTIMISTIC_BW)
        return max(transfer._LINALG_MIN_DEADLINE, 1.5 * (est_local + a.nbytes * 3 / bw))

    def _linalg(name):
        orig = _orig_linalg[name]
        def wrapped(a, *args, **kw):
            if (a.ndim == 2 and core._URL and core._ENABLED
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
    if not core._URL:
        return False
    try:
        import json as _json
        import urllib.request
        req = urllib.request.Request(core._URL.rstrip("/") + "/v1/nodes")
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
    if not (core._URL and core._ENABLED):
        return False
    K = int(core._NUM_SHARDS)
    if K < 2:
        _offload_warn("distributed work", "no other machine is in the cluster")
    if K < 2 or nbytes < _spill_min():
        return False
    if core._FORCE:
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
    bw = transfer._measure_bandwidth()
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
    return bw / max(1, min(8, int(core._NUM_SHARDS) - 1))


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
