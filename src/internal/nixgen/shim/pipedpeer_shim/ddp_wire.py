"""DDP wire pieces: exit codes, int8 codec, frame reader, shard cuts, sync thread."""

# C3 exit codes a rank ends with (the CLI reads them): the start barrier
# missed, the ring aborted or its lead lost, a rank released because one
# machine alone is faster (normal, not a failure).
_EXIT_MISSED_START = 75
_EXIT_ABORTED = 76
_EXIT_RELEASED = 77
_SKIP_END = object()    # next()'s default when a loader runs out mid-skip

# The "samples" a pad step reports: a repeated sample that only keeps the
# step counts equal, weighed as nothing by the daemon (0 still means "not
# reported", read as 1).
_PAD_SAMPLES = -1

# int8 gradients get one scale per block of this many values, never spanning
# two tensors (R1 #3, the block size SparseLoCo/INTELLECT-1 use): a scale per
# tensor lets one large value flatten the rest of a big layer to a few levels.
_INT8_BLOCK = 4096

def _int8_blocks(sizes, block=_INT8_BLOCK):
    """Block lengths for int8 scales: each tensor cut into block-sized
    pieces, the last one shorter. Empty tensors contribute nothing."""
    out = []
    for n in sizes:
        n = int(n)
        out.extend([block] * (n // block))
        if n % block:
            out.append(n % block)
    return out

def _int8_encode(flat, counts):
    """Quantise a float32 vector to int8, one scale per block in counts.

    Returns (q, scales, dequant): the bytes to send, each block's scale
    (peak/127, or 1 for an all-zero block) and what the receiver will decode,
    so the caller can carry flat - dequant forward as error feedback.

    float32 throughout, one model-sized temporary: the float64 version made
    four model-sized float64 arrays a step (FIX-DDPCOPY, ~8.9M params). The
    scales are float32 values, so the receiver's q*scale (float64) rounds to
    exactly the dequant returned here and the carried error stays exact."""
    import numpy as _np
    if not flat.size:
        return flat.astype(_np.int8), [], flat.astype(_np.float32)
    starts = _np.concatenate(([0], _np.cumsum(counts)[:-1])).astype(_np.intp)
    peak = _np.maximum.reduceat(_np.abs(flat), starts)
    # Test the scale, not the peak: a subnormal peak (< ~1.8e-43) over 127
    # underflows to 0, and a 0 scale is refused by the daemon.
    scales = (peak / _np.float32(127)).astype(_np.float32)
    scales = _np.where(scales > 0, scales, _np.float32(1)).astype(_np.float32)
    per = _np.repeat(scales, counts)
    tmp = _np.divide(flat, per, dtype=_np.float32)
    _np.rint(tmp, out=tmp)
    _np.clip(tmp, -127, 127, out=tmp)
    q = tmp.astype(_np.int8)
    tmp *= per                          # the dequantised values, in place
    return q, scales.tolist(), tmp

def _int8_decode(raw, scales, counts):
    """The float32 values int8 bytes stand for: one scale per block when the
    sender gave one per block, else a single scale for everything. float32
    throughout (FIX-DDPCOPY); every rank decodes the same bytes the same way,
    so the ranks still hold identical models."""
    import numpy as _np
    vals = raw.astype(_np.float32)
    if scales and len(scales) == len(counts):
        vals *= _np.repeat(_np.asarray(scales, dtype=_np.float32), counts)
    else:
        vals *= _np.float32(scales[0]) if scales else _np.float32(1)
    return vals

def _read_frames(buf, count):
    """count length-prefixed frames (4-byte big-endian length, then the
    bytes) from buf, as memoryview slices: one pass, no copy per frame.
    Slicing the remainder after each frame copied it every time, which is
    quadratic in the ring size on a blackboard reply holding every rank's
    model."""
    import struct
    mv = memoryview(buf)
    out, off = [], 0
    for i in range(count):
        if off + 4 > len(mv):
            raise RuntimeError("ddp reply ends before frame %d of %d" % (i + 1, count))
        n = struct.unpack_from(">I", mv, off)[0]
        off += 4
        if off + n > len(mv):
            raise RuntimeError("ddp frame %d of %d claims %d bytes, %d left"
                               % (i + 1, count, n, len(mv) - off))
        out.append(mv[off:off + n])
        off += n
    return out

def _shard_cuts(counts, n):
    """The butterfly's shards (rank k's daemon averages shard k): n ranges
    (lo, hi, block_lo, block_hi) of a flat vector, cut on block boundaries
    (counts are the int8 blocks) so each shard keeps whole blocks and their
    scales, and every shard's average is the hub's, value for value. None
    when there are fewer blocks than shards."""
    import itertools
    nb = len(counts)
    if n < 2 or nb < n:
        return None
    ends = [0] + list(itertools.accumulate(int(c) for c in counts))
    cut = [nb * k // n for k in range(n + 1)]
    return [(ends[cut[k]], ends[cut[k + 1]], cut[k], cut[k + 1]) for k in range(n)]

class _ShardLost(Exception):
    """A butterfly shard's daemon did not answer."""

class _SyncThread:
    """The one background thread overlapped syncs run on: U26's delayed
    gradients, and what U33's streaming fragments reuse. Generic over what
    is sent: submit any callable. Calls run one at a time, in order, so the
    sync sequence numbers they take are in the same order on every rank."""
    NAME = "pipedpeer-ddp-sync"

    def __init__(self):
        self._pool = None

    def submit(self, fn, *a, **kw):
        if self._pool is None:
            from concurrent.futures import ThreadPoolExecutor
            self._pool = ThreadPoolExecutor(1, thread_name_prefix=self.NAME)
        return self._pool.submit(fn, *a, **kw)

    def drain(self):
        """Wait for everything queued, leaving each result with its future:
        a sync made outside the thread must not take a sequence number while
        one inside it is in flight. A no-op on the thread itself."""
        import threading
        if self._pool is None or threading.current_thread().name.startswith(self.NAME):
            return
        self._pool.submit(lambda: None).result()

class _DelayedGrads:
    """One-step-delayed gradient averaging (hivemind delay_grad_averaging,
    DiLoCoX's one-step delay): step t's gradients are averaged on the sync
    thread while step t+1 computes, and step t+1 applies them. Every step's
    average is applied exactly once - by the next step, or by take() at the
    end of a loader's pass, so the model after an epoch has every update."""

    def __init__(self, thread):
        self.thread = thread
        self.pending = None     # (params, future of the averaged grads)

    def swap(self, params, grads, reduce):
        """Queue reduce(grads) for these params and return the previous
        step's (params, averaged grads), or None at a pass's first step."""
        prev = self.take()
        self.pending = (params, self.thread.submit(reduce, grads))
        return prev

    def take(self):
        p, self.pending = self.pending, None
        return None if p is None else (p[0], p[1].result())

class _PassEnd:
    """A DataLoader iterator that calls on_end once, when its pass ends."""

    def __init__(self, it, on_end):
        self._it, self._end = it, on_end

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._it)
        except StopIteration:
            end, self._end = self._end, None
            if end is not None:
                end()
            raise

    def __len__(self):
        return len(self._it)

    def __getattr__(self, name):
        return getattr(self._it, name)
