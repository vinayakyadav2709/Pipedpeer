"""Transparent PyTorch DDP: batch weights, shard sampler, sync."""
import os
import sys
import time
import weakref

from pipedpeer_shim import record
from pipedpeer_shim.core import (
    _TOKEN, _log, _warn,
)

# C3 exit codes a rank ends with (the CLI reads them): the start barrier
# missed, the ring aborted or its lead lost, a rank released because one
# machine alone is faster (normal, not a failure).
_EXIT_MISSED_START = 75
_EXIT_ABORTED = 76
_EXIT_RELEASED = 77

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
    scales = _np.where(peak > 0, peak / _np.float32(127), _np.float32(1)).astype(_np.float32)
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


# DiLoCo (--ddp-sync fast, PIPEDPEER_DDP_SYNC_MODE=fast): each rank trains on
# its own for H steps, then the ranks average how far they moved (the
# pseudo-gradient theta_global - theta_local, weighted by the samples each
# trained on) and every rank applies that average to the last synced model
# with an outer Nesterov step (Douillard et al. 2023; by default lr 1 and no
# momentum, which is plain averaging). The schedule: exact per-step sync
# first (post-local SGD, Lin et al. 2020), then a fixed H (or, with
# PIPEDPEER_DILOCO_H=0, H adapted to how far the ranks drift apart,
# AdaComm-style, Wang & Joshi 2019), a sync on every epoch's last step, then
# back to exact over the last tenth of the run when its length is known. The
# script's batch, learning rate and epochs are never touched.


def _env_num(name, default, kind=float):
    """A numeric tuning variable, or default (with a log line) when unset
    or not a number."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return kind(raw)
    except ValueError:
        sys.stderr.write("[pipedpeer] %s=%r is not a number; using %r\n" % (name, raw, default))
        return default


# Defaults from DILOCO-TUNE (docs/benchmarks/diloco.md): plain model averaging
# (outer lr 1, no momentum) every 64 steps after a 10-step exact warm-up. The
# paper's outer Nesterov 0.7/0.9 amplifies the step about 7x at our intervals
# and failed the 1% bar at every H we can reach (up to one sync per epoch);
# the drift-adaptive interval collapsed to H=2 on the transformer. Tuning
# variables, for scripts/bench-diloco.sh: PIPEDPEER_DILOCO_OUTER_LR,
# PIPEDPEER_DILOCO_MOMENTUM, PIPEDPEER_DILOCO_H (the fixed interval; 0 = the
# drift-adaptive one), PIPEDPEER_DILOCO_WARMUP (exact-sync steps before local
# training; 0 = the rule in _diloco_warmup) and PIPEDPEER_DILOCO_UNWEIGHTED=1
# (average the ranks as equals instead of by samples).
_DILOCO_OUTER_LR = _env_num("PIPEDPEER_DILOCO_OUTER_LR", 1.0)
_DILOCO_MOMENTUM = _env_num("PIPEDPEER_DILOCO_MOMENTUM", 0.0)
_DILOCO_FIXED_H = _env_num("PIPEDPEER_DILOCO_H", 64, int)
_DILOCO_WARMUP = _env_num("PIPEDPEER_DILOCO_WARMUP", 0, int)
_DILOCO_UNWEIGHTED = os.environ.get("PIPEDPEER_DILOCO_UNWEIGHTED") == "1"
_DILOCO_H_MAX = 64
# With PIPEDPEER_DILOCO_H=0 the local phase starts at 2 and doubles or halves
# at each sync; H=1 is exact sync, which only the warm-up and the final
# tightening use, so every local phase ends with a round that also syncs the
# optimizer state.
_DILOCO_H_START = _DILOCO_FIXED_H or 2
_DILOCO_H_FLOOR = 2
# Every Kth outer step the inner optimizer's state (Adam's exp_avg and
# exp_avg_sq, SGD's momentum) is averaged in the same round as the weights.
_DILOCO_STATE_EVERY = 10
# Drift = the ranks' pseudo-gradient disagreement (the daemon's relative
# spread) over the mean gradient disagreement of the warm-up's last rounds.
# Below GROW the ranks agree better than single steps did: H doubles. Above
# SHRINK they disagree more than single steps did, so they are drifting into
# different models: H halves. Calibration knobs: U31B's benchmark tunes them.
_DILOCO_DRIFT_GROW = 0.7
_DILOCO_DRIFT_SHRINK = 1.0
_DILOCO_REF_WINDOW = 10
_DILOCO_TIGHTEN = 0.1


def _diloco_warmup(total):
    """Exact-sync steps before local training: a tenth of the run, between
    10 and 500 steps, when its length is known; else 10 (DILOCO-TUNE: 10
    and 50 ended equal, 10 syncs 3-4x less on a short run)."""
    import math
    if _DILOCO_WARMUP > 0:
        return _DILOCO_WARMUP
    if not total:
        return 10
    return min(500, max(10, int(math.ceil(_DILOCO_TIGHTEN * total))))


def _diloco_cap(n, total):
    """The largest interval allowed after step n of total: never past the
    end, and shrinking linearly to 1 over the last tenth. None when the
    length is unknown."""
    import math
    if not total:
        return None
    left = max(0, total - n)
    window = max(1, int(math.ceil(_DILOCO_TIGHTEN * total)))
    cap = max(1, left)
    if left <= window:
        cap = min(cap, max(1, int(_DILOCO_H_MAX * left / window)))
    return cap


def _diloco_next_interval(h, drift, ref, n, total):
    """The interval until the next sync, decided after a sync that ended
    step n with the given drift (None: not measured this round)."""
    if _DILOCO_FIXED_H:
        h = _DILOCO_FIXED_H
    elif drift is not None and ref is not None:
        if ref > 0:
            excess = drift / ref
        else:
            excess = 0.0 if drift == 0 else float("inf")
        if excess < _DILOCO_DRIFT_GROW:
            h = h * 2
        elif excess > _DILOCO_DRIFT_SHRINK:
            h = h // 2
    if _DILOCO_FIXED_H:   # a fixed interval may exceed the adaptive ceiling
        h = max(1, h)
    else:
        h = min(_DILOCO_H_MAX, max(_DILOCO_H_FLOOR, h))
    cap = _diloco_cap(n, total)
    return h if cap is None else min(h, cap)


def _diloco_epoch_cap(h, step_in_epoch, epoch_steps):
    """h capped so a sync lands on the epoch's last step. A run ends at an
    epoch's end, so it ends synced: every rank holds the global model, even
    when the run length is unknown. Without this the last interval (up to 64
    steps, a third of a short run) trained each rank on its own shard alone.
    epoch_steps None or 0: not known, h unchanged."""
    if not epoch_steps:
        return h
    left = epoch_steps - step_in_epoch
    if left <= 0:
        left = epoch_steps    # the step that ended an epoch: the next one
    return max(1, min(h, left))


def _diloco_note(total):
    """What the schedule can do near the end, for the log."""
    if total:
        return ("ddp: DiLoCo tightens back to exact sync over the last %d of %d steps"
                % (max(1, int(_DILOCO_TIGHTEN * total + 0.999)), total))
    return ("ddp: DiLoCo sees no run length (only a OneCycleLR states one), so no "
            "tightening near the end: the ranks' last sync can be up to the "
            "interval before the final step")


def _outer_nesterov(theta, buf, delta, lr=_DILOCO_OUTER_LR, mu=_DILOCO_MOMENTUM):
    """torch.optim.SGD(nesterov=True) on one tensor, with the averaged
    pseudo-gradient as the gradient. Returns the new (theta, buf)."""
    buf = delta.copy() if buf is None else mu * buf + delta
    return (theta - lr * (delta + mu * buf)).astype(theta.dtype, copy=False), buf


def _opt_moments(opt, params):
    """The optimizer's per-parameter float state shaped like each parameter
    (exp_avg, exp_avg_sq, momentum_buffer...), in parameter then key order:
    the same list on every rank running the same script."""
    out = []
    for p in params:
        st = opt.state.get(p) or {}
        for k in sorted(st):
            v = st[k]
            if (hasattr(v, "is_floating_point") and v.is_floating_point()
                    and tuple(v.shape) == tuple(p.shape)):
                out.append(v)
    return out


def _np_copy(t, dtype=None):
    """A tensor as a numpy array of its own: float64 stays float64, other
    floats (bfloat16 included, which numpy cannot hold) become float32. Never
    a view of the tensor - a CPU tensor's .numpy() shares its memory, and a
    snapshot that moves with training is no snapshot."""
    import numpy as _np
    import torch as _th
    if dtype is None:
        dtype = _np.float64 if t.dtype == _th.float64 else _np.float32
    src = t.detach().cpu()
    if src.dtype not in (_th.float32, _th.float64):
        src = src.float()
    return _np.array(src.numpy(), dtype=dtype, copy=True)


class _DiLoCo:
    """One optimizer's DiLoCo state. theta is the model at the last sync
    (None outside a local phase); every field moves the same way on every
    rank, from the same replies, which is what keeps their schedules in
    step."""

    def __init__(self):
        self.theta = None
        self.buf = None
        self.h = 1
        self.next_sync = 0
        self.outer = 0
        self.samples = 0
        self.refs = []
        self.ref = None
        self.started = False

    def begin(self, params, h, done):
        """Start a local phase after step done: the model now is the global
        one, the outer momentum starts from zero."""
        self.theta = [_np_copy(p) for p in params]
        self.buf = None
        self.h = h
        self.next_sync = done + h
        self.samples = 0
        self.started = True


def _diloco_round(st, opt, params, reduce, force_state=False):
    """One outer step. reduce(deltas, extras, samples) averages across ranks
    in one round and returns (means of deltas + extras, drift), or None when
    this rank is not syncing. Every Kth outer step (or when forced) the
    optimizer state rides in the same round as extras. Writes the new global
    model into params and the averaged state into the optimizer; returns the
    round's drift, or None when it carried state (the spread then mixes in
    the moments) or did not sync."""
    import numpy as _np
    import torch as _th
    deltas = [g - _np_copy(p, g.dtype) for g, p in zip(st.theta, params)]
    st.outer += 1
    with_state = force_state or st.outer % _DILOCO_STATE_EVERY == 0
    moments = _opt_moments(opt, params) if with_state else []
    got = reduce(deltas, [_np_copy(m) for m in moments], st.samples)
    st.samples = 0
    if got is None:
        return None
    means, drift = got
    bufs = st.buf or [None] * len(deltas)
    new = [_outer_nesterov(t, b, _np.asarray(d, dtype=t.dtype))
           for t, b, d in zip(st.theta, bufs, means[:len(deltas)])]
    st.theta = [t for t, _ in new]
    st.buf = [b for _, b in new]
    with _th.no_grad():
        for p, t in zip(params, st.theta):
            p.copy_(_th.from_numpy(t).to(p.device, p.dtype))
        for m, v in zip(moments, means[len(deltas):]):
            # A copy: the reply's bytes are read-only, which torch warns about.
            m.copy_(_th.from_numpy(_np.array(v, copy=True)).to(m.device, m.dtype))
    return None if with_state else drift


def _weighted_batches(weights, base_batch, world):
    """Each rank's per-step batch, together adding up to the script's.

    The script's batch is the ring's batch: the ranks split it by share, so a
    step averages over the same samples one machine would have, at the
    script's learning rate and with the script's step count (Decisions:
    training keeps the user's maths). Every live rank takes at least one
    sample, so a batch smaller than the ring grows to one per rank. Rounding
    is absorbed by the largest rank so the total is exact - let it drift and
    the step counts drift with it.
    """
    live = sum(1 for w in weights if w > 0) or world
    total = max(live, int(base_batch))
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

    The remainder below one global batch is trained, as one machine trains
    its last partial batch (unless the script's loader says drop_last): it
    is one extra step, split by share (_split_tail). A live rank with no
    sample in it repeats one of its own as a pad, so every rank still takes
    the same number of steps, and reports it as weighing nothing
    (_PAD_SAMPLES), so the sample-weighted average over that step is exactly
    the mean over the remainder - what one machine computes.

    Shuffling is seeded from the epoch alone, so every rank computes the same
    owners and their shards stay disjoint - the same contract
    DistributedSampler keeps, and the reason set_epoch exists.
    """

    def __init__(self, n, weights, rank, shuffle, base_batch, world, drop_last=False):
        if n <= 0:
            raise ValueError("empty dataset")
        self.n = n
        self.rank = rank
        self.shuffle = shuffle
        self.epoch = 0
        self.weights = list(weights)
        self.base = base_batch  # the script's batch, kept for a reshare
        self._owned = None  # (epoch, indices) cache
        self.batches = _weighted_batches(weights, base_batch, world)
        self.batch = self.batches[rank]
        self.steps = n // sum(self.batches)
        if self.steps < 1 and drop_last:
            raise ValueError(
                "%d samples do not make one step of %d across the ring"
                % (n, sum(self.batches)))
        self.drop_last = drop_last
        left = 0 if drop_last else n - self.steps * sum(self.batches)
        self.tails, pads = _split_tail(self.weights, self.batches, left)
        self.tail = self.tails[rank]
        # This rank's last batch of an epoch is a pad (one repeated sample
        # that weighs nothing); total_steps counts the partial step.
        self.pad = rank in pads
        self.total_steps = self.steps + (1 if left else 0)

    def set_epoch(self, epoch):
        self.epoch = epoch

    def __len__(self):
        return self.steps * self.batch + self.tail + (1 if self.pad else 0)

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
            quotas = [self.steps * b + t for b, t in zip(self.batches, self.tails)]
            owners = _hash_owners(self.n, self.weights, quotas, order)
            mine = owners[self.rank]
            if self.pad:
                mine = mine + [mine[0] if mine else 0]
            self._owned = (self.epoch, mine)
        return iter(self._owned[1])


def _split_tail(weights, batches, left):
    """Split the last partial step's left samples between the live ranks.

    Returns (per-rank counts, the ranks that pad). By share, each piece no
    larger than the rank's full batch so it stays a single batch (the
    excess moves to ranks with room; the full batches total more than left,
    so there always is). With fewer samples than live ranks, the largest
    shares take one each and the rest pad. Every rank computes the same
    answer from the same shares."""
    tails = [0] * len(weights)
    live = [r for r, w in enumerate(weights) if w > 0]
    if left <= 0 or not live:
        return tails, set()
    by_share = sorted(live, key=lambda r: (-weights[r], r))
    if left >= len(live):
        tails = _weighted_batches(weights, left, len(weights))
        extra = 0
        for r in live:
            if tails[r] > batches[r]:
                extra += tails[r] - batches[r]
                tails[r] = batches[r]
        for r in by_share:
            take = min(batches[r] - tails[r], extra)
            tails[r] += take
            extra -= take
    else:
        for r in by_share[:left]:
            tails[r] = 1
    return tails, {r for r in live if tails[r] == 0}


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


# ---- transparent PyTorch DDP (PIPEDPEER_DDP=1) ----
# pipedpeer run --ddp K starts K ranks, each with PIPEDPEER_RANK /
# PIPEDPEER_WORLD_SIZE / PIPEDPEER_DDP_SYNC (the lead daemon's sync URL) /
# PIPEDPEER_DDP=1 set, so plain single-process training code runs
# data-parallel with zero changes: at the first optimizer.step the ranks
# compare their models and average the weights once, then average gradients
# every step through the lead daemon, broadcast buffers on first forward, and
# split the script's batch and dataset between them. Native torch DDP (the
# user wraps the model themselves) is detected and left to its own sync.
def _install_ddp():
    if os.environ.get("PIPEDPEER_DDP") != "1":
        return
    try:
        import torch as _th
    except ImportError:
        return

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
    # --ddp N (C2): the user sized the ring, so the lead never shrinks it.
    _FIXED = os.environ.get("PIPEDPEER_DDP_FIXED") == "1"
    # Each rank's machine name, by rank, for messages.
    _HOSTS = os.environ.get("PIPEDPEER_DDP_HOSTS", "").split(",")

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
    # [the sampler of the loader being iterated, steps taken this epoch].
    _EPOCH = [None]
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
    # True from a skipped epoch's placeholder batch until the joiner's next
    # epoch starts; with it the stdout the script had before.
    _CATCHUP = [False]
    _CATCHUP_OUT = [None]
    # Every optimizer the script built, so a model can be handed to a joiner.
    _OPTS = []
    # Set when the daemon has told this rank the ring is better off without
    # it.
    _DROPPED = [False]
    # Every sync is one POST to the lead rank's daemon on the same port every
    # other byte of pipedpeer traffic uses: no sockets of our own, no
    # MASTER_PORT, nothing new to firewall (ADR-0010).
    _SYNC_URL = os.environ.get("PIPEDPEER_DDP_SYNC", "")
    _GROUP = os.environ.get("PIPEDPEER_DDP_GROUP", "ddp")
    # 1 = average gradients every step (exact DDP semantics). N>1 = local
    # SGD: train locally, average weights every Nth step — the knob for
    # high-latency links where a per-step round trip dominates.
    _SYNC_RAW = os.environ.get("PIPEDPEER_DDP_SYNC_EVERY", "1").strip().lower()
    _SYNC_AUTO = _SYNC_RAW == "auto"
    _SYNC_EVERY = 1 if _SYNC_AUTO else max(1, int(_SYNC_RAW or "1"))
    # run --ddp-sync fast (C2, Decisions): DiLoCo with an adaptive interval
    # (_DiLoCo, _diloco_round). Replaces the interval above when set.
    _FAST = os.environ.get("PIPEDPEER_DDP_SYNC_MODE", "").strip().lower() == "fast"
    _DILOCO = {}            # id(optimizer) -> _DiLoCo
    _TOTAL = [None]         # the run's optimizer steps, when a OneCycleLR says
    _DRIFT = [None]         # the last reduce reply's drift, None if it had none
    _TOLD_FAST = [False]
    # The butterfly (U26): every rank's sync URL as this rank reaches it, in
    # rank order, when the CLI built a ring of 3+ machines that all reach
    # each other. Shard k of each gradient goes to rank k's daemon (shard 0
    # to the lead, with the ring's control), so the lead's link carries
    # 2(N-1)/N of a gradient instead of N-1. A shard whose daemon stops
    # answering is averaged on the lead from then on (_PEER_DEAD).
    _PEERS = [u for u in os.environ.get("PIPEDPEER_DDP_PEERS", "").split(",") if u]
    _PEER_DEAD = set()
    _SHARD_POOL = []
    # Ranks sharing this rank's node (multi-GPU, not the lead's node): they
    # average on their own daemon first, which sends the lead one sync for
    # the machine.
    try:
        _LOCAL = [int(r) for r in os.environ.get("PIPEDPEER_DDP_LOCAL", "").split(",") if r]
    except ValueError:
        _warn("ddp: PIPEDPEER_DDP_LOCAL=%r is not a list of ranks; syncing directly with the lead"
              % os.environ.get("PIPEDPEER_DDP_LOCAL"))
        _LOCAL = []
    _LOCAL_URL = os.environ.get("PIPEDPEER_DAEMON_URL", "").rstrip("/")
    # One-step-delayed overlap (opt-in: each gradient lands a step late):
    # step t's average travels while step t+1 computes.
    _OVERLAP = os.environ.get("PIPEDPEER_DDP_OVERLAP") == "1"
    _SYNC_THREAD = _SyncThread()
    _DELAYED = {}           # id(optimizer) -> (_DelayedGrads, its own step)
    _TOLD_OVERLAP = [False]
    # Seconds this rank's training thread spent blocked on syncs: what a
    # step's compute time is net of (an overlapped sync costs only its wait).
    _EXPOSED = [0.0]
    # A receipt for training, for the same reason pool work has one: a run
    # that distributed nothing and a run that distributed everything look
    # identical from the outside, and so do a fast ring and one that spent
    # 95% of its time waiting.
    # barrier_sec is the opening round's wait for every rank to start: not
    # sync, and counted as sync it advised averaging every 32 steps (V4 80).
    _DDP_STATS = {"syncs": 0, "sync_sec": 0.0, "sent_bytes": 0, "recv_bytes": 0,
                  "barrier_sec": 0.0}
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
    _SYNC_BUDGET = 0.25
    _SYNC_EVERY_MAX = 32
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
    # being a slow bias towards zero; the lead does the same for its reply.
    # On by default (R1 #1: 8-bit with error feedback is a mild compressor
    # that keeps per-step semantics); PIPEDPEER_DDP_INT8=0 sends fp16.
    _INT8_GRADS = os.environ.get("PIPEDPEER_DDP_INT8", "") != "0"
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
        if reply.get("release") is not None:
            _take_release(reply, w, world)
            return
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

    def _take_release(reply, w, world):
        """D2: one rank alone beats the split. Every rank takes the change at
        the next epoch boundary (_ring_switch): the kept rank's share becomes
        the whole dataset and it stops syncing, the others exit 77. Rank 0
        says so in one plain line, since it is the output the user watches."""
        try:
            keep = int(reply["release"])
            w = [float(x) for x in w]
        except (TypeError, ValueError, KeyError) as e:
            _log("ddp: ignoring a release note the daemon sent as %r (%s)" % (reply, e))
            return
        if len(w) != world or keep < 0 or keep >= world or sum(w) <= 0:
            _log("ddp: ignoring a release note with shares %r for a ring of %d" % (w, world))
            return
        total = sum(w)
        _PENDING[0] = {"weights": [x / total for x in w], "world": world, "release": keep}
        if _RANK == 0:
            who = _HOSTS[keep] if keep < len(_HOSTS) and _HOSTS[keep] else "rank %d" % keep
            n = int(reply.get("release_ranks") or _live_ranks())
            # Shown without -v: it changes where the run trains.
            _warn("one machine is faster alone: %s takes about %.0f ms a step by itself "
                  "against %.0f ms split across %d machines, so it trains on alone from "
                  "the next epoch. To keep the split: --ddp %d"
                  % (who, float(reply.get("solo_ms") or 0), float(reply.get("split_ms") or 0),
                     n, n))

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
                     "--ddp-sync fast would train locally and sync about every %d "
                     "steps (DiLoCo), which is faster and trains a slightly "
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
                 "machine alone. Syncing costs more than the extra machines save."
                 % (compute, sync_total, live, alone))

    import atexit as _atexit_ddp
    _atexit_ddp.register(_ddp_report)
    _SEQ = [0]
    _STEPN = {}
    _WARNED_NO_SYNC = [False]
    # When the sync now in flight started (monotonic), 0 when none is: the
    # lead watch probes only while a rank is waiting on the lead.
    _INFLIGHT = [0.0]
    _WATCHING = [False]
    _LEAD_PROBE_SEC = 8

    def _count_sync(sec, sent, recv):
        """Book one sync round trip. The first is the opening round, which is
        also the start barrier: its time is mostly waiting for ranks to start,
        so it is kept apart from sync time."""
        if _SEQ[0] == 1:
            _DDP_STATS["barrier_sec"] += sec
        else:
            _DDP_STATS["syncs"] += 1
            _DDP_STATS["sync_sec"] += sec
            record.step("ddp_sync", sec, sent + recv)
        _DDP_STATS["sent_bytes"] += sent
        _DDP_STATS["recv_bytes"] += recv
        import threading
        if not threading.current_thread().name.startswith(_SyncThread.NAME):
            _EXPOSED[0] += sec

    def _lead_name():
        if _HOSTS and _HOSTS[0]:
            return _HOSTS[0]
        import urllib.parse
        return urllib.parse.urlparse(_SYNC_URL).hostname or "(unknown)"

    def _stop(code, reason):
        """End this rank now with a C3 code, after the one line that says why.

        os._exit rather than an exception: the ring is gone, and a script's
        own except/finally could wait on a sync that will never answer. The
        CLI stops every other rank when it sees 76."""
        sys.stdout.flush()
        sys.stderr.write("pipedpeer: training stopped: %s\n" % reason)
        sys.stderr.flush()
        os._exit(code)

    def _watch_lead():
        """Notice a lead that vanished mid-sync within about 30 s.

        A sync may legitimately wait minutes (the start barrier, a slow
        rank), and a machine that drops off the network never closes the
        connection, so the POST alone would sit out its whole timeout. While a
        sync is in flight a thread asks the lead's daemon for the round's
        status; three unanswered checks in a row end the rank (exit 76)."""
        if _WATCHING[0]:
            return
        _WATCHING[0] = True
        import threading
        import urllib.error
        import urllib.parse
        import urllib.request

        def run():
            fails = 0
            while True:
                time.sleep(_LEAD_PROBE_SEC)
                since = _INFLIGHT[0]
                if not since or time.monotonic() - since < _LEAD_PROBE_SEC:
                    fails = 0
                    continue
                req = urllib.request.Request("%s?group=%s&seq=0" % (
                    _SYNC_URL, urllib.parse.quote(_GROUP)))
                if _TOKEN:
                    req.add_header("X-Pipedpeer-Token", _TOKEN)
                try:
                    urllib.request.urlopen(req, timeout=5).close()
                    fails = 0
                except urllib.error.HTTPError:
                    fails = 0  # it answered, whatever it said
                except Exception as e:
                    fails += 1
                    _log("ddp: the lead machine %s did not answer a check (%s), %d of 3"
                         % (_lead_name(), getattr(e, "reason", e), fails))
                    if fails >= 3 and _INFLIGHT[0]:
                        _stop(_EXIT_ABORTED, "the lead machine %s stopped answering (%s)"
                              % (_lead_name(), getattr(e, "reason", e)))
        threading.Thread(target=run, daemon=True, name="pipedpeer-ddp-lead").start()

    def _post(req, timeout, opening=False, lost=False):
        """Send one sync to the lead daemon and return the reply body.

        A refusal is reported in the daemon's own words: a bare "HTTP 400"
        hid why the demo's 3-rank run died. 409 with abort is the ring ending
        for every rank (C3 exit 76); 410 on the opening round is this rank
        missing the start barrier (exit 75); no answer at all is the lead
        lost (exit 76). lost=True (a butterfly shard's daemon, not the lead):
        no answer raises _ShardLost instead, and the lead watch is left to
        the lead's own syncs."""
        import json
        import urllib.error
        import urllib.request
        if not lost:
            _watch_lead()
            _INFLIGHT[0] = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "replace")
            except Exception as re:
                body = "(no body: %s)" % re
            try:
                info = json.loads(body)
            except ValueError:
                info = None
            if not isinstance(info, dict):
                info = {}
            why = info.get("error") or body[:300]
            if e.code == 409 and info.get("abort"):
                _stop(_EXIT_ABORTED, why)
            if e.code == 410 and opening:
                # This rank missed the start rather than failed, so pipedpeer
                # can start it again as a joiner.
                _log("ddp: rank %d was not ready when the ring started training, and the "
                     "ring went on without it; stopping this rank (it can join later)" % _RANK)
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(_EXIT_MISSED_START)
            raise RuntimeError("ddp sync refused by the lead daemon (HTTP %d): %s"
                               % (e.code, why)) from None
        except (urllib.error.URLError, OSError) as e:
            if lost:
                raise _ShardLost(str(getattr(e, "reason", e))) from None
            _stop(_EXIT_ABORTED, "the lead machine %s stopped answering (%s)"
                  % (_lead_name(), getattr(e, "reason", e)))
        finally:
            if not lost:
                _INFLIGHT[0] = 0.0

    def _header(kind, **extra):
        """A sync's JSON header: who this rank is and what it measured."""
        import json
        h = {"group": _GROUP, "seq": _SEQ[0], "rank": _RANK, "world": _WORLD,
             "kind": kind, "sync_every": int(_SYNC_TUNED[0] or 0),
             "step_ms": _mean_step_ms(), "sync_ms": _mean_sync_ms(),
             "samples": int(_BATCH_N[0]), "sharded": bool(_SHARDED_HASH[0]),
             "fixed": _FIXED}
        h.update(extra)
        return json.dumps(h).encode()

    def _on_reply(reply, opening):
        """Everything a reduce reply says besides the numbers."""
        # Adopt the group's agreed interval. Ranks measure their own link and
        # arrive at different numbers, and averaging at different points in
        # each rank's step sequence is not local SGD - it is ranks combining
        # models that have taken different numbers of local steps, which
        # shows up as a worse final loss and nothing else.
        _DRIFT[0] = reply.get("drift")
        agreed = reply.get("sync_every", 0)
        if agreed and agreed != _SYNC_TUNED[0]:
            _SYNC_TUNED[0] = int(agreed)
        ranks = int(reply.get("ranks", 0) or 0)
        # Not for the opening round: a rank missing there missed the start
        # barrier, and the lead's note that follows says so in its own words.
        if ranks and ranks < _WORLD and _LAST_RANKS[0] != ranks and not opening:
            _LAST_RANKS[0] = ranks
            _log("ddp: this step averaged %d of %d ranks — the others did not answer "
                 "in time. The run continues on the ranks that did; a smaller average "
                 "is a smaller step in the same direction, and every rank applies the "
                 "identical result, so nothing drifts." % (ranks, _WORLD))
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

    def _barrier_progress():
        """Say what this rank is waiting for while the start barrier holds it:
        two and a half silent minutes read as a hang (V4 80). Returns an event
        to set when the wait is over."""
        import threading
        import time as _tw
        done = threading.Event()
        hosts = _HOSTS

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
        _SYNC_THREAD.drain()
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
        data = _post(req, 240)
        nl = data.find(b"\n")
        if nl < 0:
            raise RuntimeError("ddp sync returned no header: %r" % data[:200])
        count = json.loads(data[:nl]).get("blob_frames", 0)
        out = _read_frames(memoryview(data)[nl + 1:], count)
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

    def _daemon_reduce(arrs, wire_dtype, kind="grads", manifest=None, **more):
        """One averaged allreduce through the daemon, in reduce mode. more
        goes into the header (samples= overrides the last batch's count)."""
        flat, shapes, sizes = _pack(arrs)
        flat = _np_ddp.ascontiguousarray(flat.astype(wire_dtype, copy=False))
        _SYNC_THREAD.drain()
        _t0 = time.monotonic()
        _SEQ[0] += 1
        opening = kind == "weights" and _SEQ[0] == 1
        extra = {"dtype": wire_dtype.name, "count": int(flat.size),
                 "start_wait": _START_WAIT}
        extra.update(more)
        if opening and manifest is not None:
            # The lead compares every rank's parameter list before averaging
            # anything: ranks that built different models stop together with
            # both described, instead of a byte count from the accumulator.
            extra["manifest"] = manifest
        waiting = _barrier_progress() if opening else None
        try:
            # The opening round is the start barrier and may wait the whole
            # start wait for a rank whose closure is still arriving.
            reply, mean, _, sent, recv = _reduce_exchange(
                kind, flat, extra, _int8_blocks(sizes), opening=opening)
        finally:
            if waiting is not None:
                waiting.set()
        _on_reply(reply, opening)
        _count_sync(time.monotonic() - _t0, sent, recv)
        return _unpack(mean, shapes, sizes, [a.dtype for a in arrs])

    def _reduce_post(url, kind, fields, payload, opening=False, lost=False):
        """One reduce contribution to url. Returns (reply header, mean
        bytes, bytes sent, bytes received)."""
        import json
        import struct
        import urllib.request
        # payload: any contiguous buffer; joined once, never copied twice.
        payload = memoryview(payload).cast("B")
        body = b"".join((_header(kind, **fields), b"\n", struct.pack(">I", payload.nbytes), payload))
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/vnd.pipedpeer.ddp.reduce"})
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        # A shard's daemon answers within the daemon's partial wait (45 s)
        # when it is up; one that is not should cost the ring that, not 240 s.
        data = _post(req, _START_WAIT + 60 if opening else (90 if lost else 240),
                     opening=opening, lost=lost)
        nl = data.find(b"\n")
        if nl < 0:
            raise RuntimeError("ddp reduce returned no header: %r" % data[:200])
        return (json.loads(data[:nl]), _read_frames(memoryview(data)[nl + 1:], 1)[0],
                len(body), len(data))

    def _butterfly_on(kind, opening):
        """Every rank decides this from shared state alone (the CLI's peer
        list, the ring's size and shares, the sync mode), so all send a round
        the same way. Gradients only: DiLoCo's drift needs the whole vector
        in one place."""
        return (kind == "grads" and not opening and not _FAST
                and len(_PEERS) == _WORLD >= 3 and _live_ranks() == _WORLD)

    def _local_on(kind, opening):
        """Same-machine pre-averaging: gradients only (a forwarded sum hides
        each rank's spread from DiLoCo's drift), while every local rank is
        still training."""
        if kind != "grads" or opening or _FAST or len(_LOCAL) < 2 or not _LOCAL_URL:
            return False
        if _RANK not in _LOCAL or max(_LOCAL) >= _WORLD:
            return False
        return _WEIGHTS is None or all(r < len(_WEIGHTS) and _WEIGHTS[r] > 0 for r in _LOCAL)

    def _reduce_exchange(kind, flat, fields, blocks, opening=False, scales=None):
        """Average flat across the ring: one POST to the lead (the hub), one
        per shard to every rank's daemon (the butterfly), or one to this
        machine's daemon, which averages the ranks here and sends the lead
        one sync for the machine. scales: flat is int8 with one scale per
        block. Returns (the lead's reply header, the mean as flat's dtype,
        the reply's scales, bytes sent, bytes received)."""
        fields.setdefault("samples", int(_BATCH_N[0]))
        cuts = _shard_cuts(blocks, _WORLD) if _butterfly_on(kind, opening) else None
        if cuts is None:
            url = _SYNC_URL
            if _local_on(kind, opening):
                url = _LOCAL_URL + "/v1/ddp/sync"
                fields = dict(fields, local=_LOCAL, upstream=_SYNC_URL)
            reply, raw, sent, recv = _reduce_post(url, kind, fields, flat, opening)
            return (reply, _np_ddp.frombuffer(raw, dtype=flat.dtype), reply.get("scales"),
                    sent, recv)

        def shard(k):
            lo, hi, blo, bhi = cuts[k]
            f = dict(fields, count=int(hi - lo), shard=k, shards=_WORLD)
            if scales is not None:
                f.update(scales=scales[blo:bhi], counts=blocks[blo:bhi], scale=scales[blo])
            part = flat[lo:hi]
            if k == 0 or k in _PEER_DEAD:
                return _reduce_post(_SYNC_URL, kind, f, part)
            try:
                return _reduce_post(_PEERS[k], kind, f, part, lost=True)
            except _ShardLost as e:
                # Every other rank finds the same daemon silent and does the
                # same, so they meet on the lead under this shard's key.
                _PEER_DEAD.add(k)
                _warn("ddp: rank %d's machine did not answer its share of the sync (%s); "
                      "the lead averages that share from now on" % (k, e))
                return _reduce_post(_SYNC_URL, kind, f, part)

        if not _SHARD_POOL:
            from concurrent.futures import ThreadPoolExecutor
            _SHARD_POOL.append(ThreadPoolExecutor(
                max(1, _WORLD - 1), thread_name_prefix="pipedpeer-ddp-shard"))
        futs = [_SHARD_POOL[0].submit(shard, k) for k in range(1, _WORLD)]
        got = [shard(0)] + [f.result() for f in futs]
        mean = _np_ddp.concatenate([_np_ddp.frombuffer(g[1], dtype=flat.dtype) for g in got])
        rscales = None
        if scales is not None:
            rscales = []
            for g in got:
                rscales.extend(g[0].get("scales") or [float(g[0].get("scale", 1.0)) or 1.0])
        return (got[0][0], mean, rscales, sum(g[2] for g in got), sum(g[3] for g in got))

    def _daemon_reduce_int8(arrs, key, kind="grads", **more):
        """Average gradients as signed bytes, carrying the rounding error.

        Each rank scales its values into [-127, 127], one scale per block of
        _INT8_BLOCK values, and sends the scales in the header; the daemon
        decodes, sums in float64 and returns the mean the same way. Every
        rank gets identical bytes back, so the ranks hold exactly the same
        model, which is the property that matters.

        What rounding drops is kept in _ERR and added to the next step's
        gradient; the lead keeps its reply's rounding the same way. Without
        that, small gradients round to zero every step and never move the
        model at all; with it they accumulate until they are large enough to
        survive a round - the standard error-feedback trick, and the reason
        this is usable rather than merely smaller.
        """
        flat, shapes, sizes = _pack(arrs)
        counts = _int8_blocks(sizes)
        if int(more.get("samples", _BATCH_N[0])) < 0:
            # A pad step weighs nothing: send zeros and keep the carried
            # error for the next real gradient.
            q = _np_ddp.zeros(flat.size, dtype="int8")
            scales = [1.0] * len(counts)
        else:
            # Our own buffer, written in place below: _pack's concatenation
            # already is one; a lone tensor's array is the gradient itself.
            flat = flat.astype("float32", copy=len(arrs) == 1)
            prev = _ERR.get(key)
            if prev is not None and prev.shape == flat.shape:
                flat += prev
            q, scales, dequant = _int8_encode(flat, counts)
            # Whatever quantisation lost: carried to the next step.
            flat -= dequant
            _ERR[key] = flat

        _SYNC_THREAD.drain()
        _t0 = time.monotonic()
        _SEQ[0] += 1
        fields = dict(dtype="int8", count=int(q.size), scale=scales[0] if scales else 1.0,
                      scales=scales, counts=counts, **more)
        reply, raw_mean, rscales, sent, recv = _reduce_exchange(
            kind, q, fields, counts, scales=scales)
        _on_reply(reply, False)
        mean = _int8_decode(raw_mean, rscales or [float(reply.get("scale", 1.0)) or 1.0], counts)
        _count_sync(time.monotonic() - _t0, sent, recv)
        return _unpack(mean, shapes, sizes, [a.dtype for a in arrs])

    def _daemon_allreduce(tensors, half=None, kind="grads", manifest=None, key=None,
                          samples=None):
        """Average tensors across ranks in place, through the daemon channel.

        Gradients cross the wire as float16 by default. This is the dominant
        cost of a step on any link slower than a datacentre LAN - the demo is
        bounded by sync time, not compute - and halving the bytes halves it.
        The averaging itself is still done in float64 and written back at the
        tensor's own dtype, so the loss of precision is confined to transport,
        which is the same trade PyTorch's own fp16 compression hook makes.
        PIPEDPEER_DDP_FP32=1 turns it off; weight broadcasts never use it.

        key: what the int8 rounding error is carried under - the optimizer,
        not id(tensors[0]): zero_grad(set_to_none=True) makes a new gradient
        tensor every step, so a tensor's id lost the error each step and left
        a model-sized array behind in _ERR. samples: the batch behind these
        gradients, when the caller read it earlier (an overlapped sync runs
        after the next step has reset _BATCH_N).
        """
        import pickle
        more = {} if samples is None else {"samples": int(samples)}
        if _DROPPED[0] or _live_ranks() <= 1:
            # A ring of one (the others released, or every other share
            # gone) has nobody to average with: it stops syncing entirely.
            # Told to leave. Posting anyway would have the others average a
            # rank the daemon has stopped expecting; not posting is what lets
            # them complete without waiting. This rank keeps training on its
            # own shard, which is harmless - its gradients simply stop
            # reaching anybody.
            return
        if half is None:
            half = _FP16_GRADS
        arrs = [t.detach().cpu().numpy() for t in tensors]
        # Only float32 narrows to float16; float64 tensors keep their range,
        # and integer buffers would be corrupted outright.
        f16 = _np_ddp.dtype("float16")
        # Reduce mode when the payload is uniform enough for the daemon to
        # average it, which is the ordinary case: one dtype across every
        # gradient. Anything mixed falls back to the blackboard, where the
        # daemon does not need to understand what it is holding.
        wire_dtypes = {f16 if half and a.dtype == _np_ddp.float32 else a.dtype for a in arrs}
        if _INT8_GRADS and kind == "grads" and len(wire_dtypes) == 1:
            # Checked before any float16 copy is made: int8 never uses one
            # (it cost ~0.25 s a step at 8.9M params, FIX-DDPCOPY).
            means = _daemon_reduce_int8(arrs, key=key if key is not None else id(tensors[0]),
                                        **more)
            for i, t in enumerate(tensors):
                t.data.copy_(_th.from_numpy(
                    _np_ddp.ascontiguousarray(means[i].astype(arrs[i].dtype, copy=False))))
            return
        wire = arrs
        if half:
            wire = [a.astype("float16") if a.dtype == _np_ddp.float32 else a for a in arrs]
        if len(wire_dtypes) == 1 and next(iter(wire_dtypes)).name in (
                "float16", "float32", "float64"):
            means = _daemon_reduce(wire, next(iter(wire_dtypes)), kind, manifest, **more)
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

    def _diloco_reduce(deltas, extras, samples):
        """_diloco_round's reduce. Pseudo-gradients go as int8 with error
        feedback; a round carrying optimizer state goes whole as float32:
        exp_avg_sq spans orders of magnitude within a block, so int8 would
        round its small entries to zero (huge Adam steps there) and the
        reply's error feedback could push them below zero. That round sends
        the carried int8 error at full width and clears it."""
        if _DROPPED[0] or _live_ranks() <= 1:
            return None
        _DRIFT[0] = None
        if _DILOCO_UNWEIGHTED:
            samples = 0         # the daemon weighs a rank that gives none as 1
        if extras or not _INT8_GRADS:
            err = _ERR.pop("delta", None)
            wire, off = [], 0
            for d in deltas:
                d = d.astype("float32")
                if err is not None and err.size >= off + d.size:
                    d += err[off:off + d.size].reshape(d.shape)
                off += d.size
                wire.append(d)
            wire += [e.astype("float32", copy=False) for e in extras]
            means = _daemon_reduce(wire, _np_ddp.dtype("float32"), "delta",
                                   samples=int(samples))
        else:
            means = _daemon_reduce_int8(deltas, "delta", "delta", samples=int(samples))
        return means, _DRIFT[0]

    def _fast_epoch_cap(h):
        ep = _EPOCH[0]
        if ep is None:
            return h
        return _diloco_epoch_cap(h, ep[1], int(getattr(ep[0], "total_steps", 0) or 0))

    def _fast_step(opt, params, n, orig, sa, skw):
        """Step n of opt under --ddp-sync fast: exact sync through the
        warm-up and after the final tightening, else local steps and a
        DiLoCo round every H. Every branch depends only on the step count,
        the run length and reply data, which every rank shares."""
        st = _DILOCO.get(id(opt))
        if st is None:
            st = _DILOCO[id(opt)] = _DiLoCo()
        done = n + 1
        total = _TOTAL[0]
        if st.theta is None:
            grads = [p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad
                     for p in params if p.grad is not None]
            _DRIFT[0] = None
            if grads:
                _daemon_allreduce(grads, key=("grads", id(opt)))
            drift = _DRIFT[0]
            out = orig(*sa, **skw)
            if st.started:
                return out          # tightened to exact for the end of the run
            if done < _diloco_warmup(total):
                if drift is not None:
                    _push_window(st.refs, drift, _DILOCO_REF_WINDOW)
                return out
            # The post-local switch, made once.
            if st.ref is None and st.refs:
                st.ref = sum(st.refs) / len(st.refs)
            cap = _diloco_cap(done, total)
            h = _DILOCO_H_START if cap is None else min(_DILOCO_H_START, cap)
            if h > 1:
                h = _fast_epoch_cap(h)
            if h <= 1 and cap is not None and cap <= 1:
                st.started = True   # too near the end to train locally
                return out
            st.begin(params, h, done)
            _SYNC_TUNED[0] = h
            if st.ref is None:
                _log("ddp: the lead daemon reports no drift between ranks (an older "
                     "build), so DiLoCo keeps syncing every %d steps" % h)
            _log("ddp: exact sync warm-up done after %d steps (drift reference %s); "
                 "training locally, syncing every %d steps"
                 % (done, "%.3g" % st.ref if st.ref is not None else "none", h))
            return out
        out = orig(*sa, **skw)
        st.samples += max(0, int(_BATCH_N[0]))  # a pad step adds no samples
        if done < st.next_sync:
            return out
        cap = _diloco_cap(done, total)
        last = cap is not None and cap <= 1
        drift = _diloco_round(st, opt, params, _diloco_reduce, force_state=last)
        if last:
            st.theta = None
            _SYNC_TUNED[0] = 1
            _log("ddp: DiLoCo back to exact sync for the last %d step(s)" % max(0, total - done))
            return out
        h = _fast_epoch_cap(_diloco_next_interval(st.h, drift, st.ref, done, total))
        if h != st.h:
            _log("ddp: DiLoCo syncs every %d steps now (was %d; drift %s against %s)"
                 % (h, st.h, "%.3g" % drift if drift is not None else "not measured",
                    "%.3g" % st.ref if st.ref is not None else "no reference"))
        st.h, st.next_sync = h, done + h
        _SYNC_TUNED[0] = h
        return out

    def _diloco_settle():
        """Before the ring changes: end every local phase with a round that
        also syncs the optimizer state, so every rank holds the same model
        and optimizer (the snapshot a joiner takes is the ring's), then
        restart the local phase from there with a fresh outer momentum on
        every rank, the joiner included."""
        for o in _live_opts():
            st = _DILOCO.get(id(o))
            if st is None or st.theta is None:
                continue
            params = [p for g in o.param_groups for p in g["params"]]
            _diloco_round(st, o, params, _diloco_reduce, force_state=True)
            st.theta, st.started = None, False
            _log("ddp: DiLoCo synced model and optimizer state for the ring change")

    def _delayed_step(opt, params, orig, sa, skw):
        """An exact-mode step with PIPEDPEER_DDP_OVERLAP=1: send this step's
        gradients on the sync thread and apply the previous step's average,
        which travelled while this step computed. A pass's first step only
        sends; _flush_delayed applies the last one when the pass ends."""
        entry = _DELAYED.get(id(opt))
        if entry is None:
            entry = _DELAYED[id(opt)] = (_DelayedGrads(_SYNC_THREAD), orig)
        with_grad = [p for p in params if p.grad is not None]
        # Copies, taken now: the script reuses or frees p.grad next step.
        copies = [(p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad)
                  .detach().to("cpu", copy=True) for p in with_grad]
        samples = int(_BATCH_N[0])

        def reduce(grads):
            _daemon_allreduce(grads, key=("grads", id(opt)), samples=samples)
            return grads
        t0 = time.monotonic()
        prev = entry[0].swap(with_grad, copies, reduce)
        _EXPOSED[0] += time.monotonic() - t0
        if not _TOLD_OVERLAP[0]:
            _TOLD_OVERLAP[0] = True
            _log("ddp: overlapping each gradient sync with the next step's compute "
                 "(PIPEDPEER_DDP_OVERLAP=1): every gradient is applied one step late")
        if prev is None:
            return None
        # A parameter with a gradient now but none last step has no average
        # to apply; left as is, the optimizer would apply its local gradient.
        had = {id(p) for p in prev[0]}
        for p in with_grad:
            if id(p) not in had:
                p.grad = None
        return _apply_delayed(prev, orig, sa, skw, restore=False)

    def _apply_delayed(prev, orig, sa, skw, restore):
        """Run the optimizer's own step with an averaged gradient in .grad.
        restore: put the script's gradients back afterwards (a flush runs
        outside the script's step, and its next backward may add to them)."""
        ps, avg = prev
        saved = [p.grad for p in ps] if restore else None
        with _th.no_grad():
            for p, g in zip(ps, avg):
                g = g.to(p.device)
                if p.grad is None or p.grad.is_sparse or p.grad.shape != g.shape:
                    p.grad = g.to(p.dtype)
                else:
                    p.grad.copy_(g)
        try:
            return orig(*sa, **skw)
        finally:
            if restore:
                for p, g in zip(ps, saved):
                    p.grad = g

    def _flush_delayed():
        """Apply every optimizer's pending average now: at the end of a
        loader's pass (so the model after the loop has every update), and
        before an epoch starts or the ring changes."""
        for d, orig in list(_DELAYED.values()):
            t0 = time.monotonic()
            prev = d.take()
            _EXPOSED[0] += time.monotonic() - t0
            if prev is not None:
                _apply_delayed(prev, orig, (), {}, restore=True)

    _STEPPED = weakref.WeakSet()
    # Modules holding buffers (BatchNorm running stats), in first-forward
    # order: the same order on every rank running the same script, which is
    # how a joiner matches the ring's buffers to its own.
    _BUF_SEEN = weakref.WeakSet()
    _BUF_MODS = []
    _JOIN_BUFS = [None]

    def _buffer_list():
        return [b for m in (r() for r in _BUF_MODS) if m is not None
                for b in m._buffers.values() if b is not None]

    def _load_join_buffers():
        """Take the ring's buffers once the catch-up batches are behind.

        Not at the join itself: this rank has run no forward pass then, so it
        cannot know its modules, and the catch-up batches would move the
        running stats again afterwards."""
        theirs, _JOIN_BUFS[0] = _JOIN_BUFS[0], None
        if theirs is None:
            return
        mine = _buffer_list()
        if len(mine) != len(theirs) or any(a.shape != b.shape for a, b in zip(mine, theirs)):
            _warn("ddp: the ring's %d buffer(s) do not match this rank's %d; keeping "
                 "this rank's own (BatchNorm running stats may differ)"
                 % (len(theirs), len(mine)))
            return
        with _th.no_grad():
            for a, b in zip(mine, theirs):
                a.copy_(b.to(a.device))
        _log("ddp: took the ring's %d buffer(s) after the catch-up batches" % len(mine))
    _ORIG_FWD = _th.nn.Module.forward
    _FWD = weakref.WeakSet()
    _ORIG_ITER = _th.utils.data.DataLoader.__iter__
    _EPOCHS = weakref.WeakKeyDictionary()
    # Keyed by id() rather than the loader itself: the decision is cheap to
    # recompute but must not be repeated per epoch, and a DataLoader is not
    # reliably hashable once a user subclasses it.
    _SHARDED = {}
    import itertools

    # The modules a step's forward pass starts from (the first module the
    # pre-hook sees each step), for naming parameters in the manifest.
    _ROOTS = weakref.WeakSet()

    def _manifest(params):
        """What the lead compares at the opening barrier: every parameter's
        name, shape and dtype, and where this rank runs."""
        names = {}
        for m in list(_ROOTS):
            try:
                for name, p in m.named_parameters():
                    names.setdefault(id(p), name)
            except Exception as e:
                _log("ddp: cannot name the parameters of %s (%s)" % (type(m).__name__, e))
        device = ""
        if params:
            dev = params[0].device
            device = dev.type
            if dev.type == "cuda":
                try:
                    device = "cuda, " + _th.cuda.get_device_name(dev)
                except Exception as e:
                    _log("ddp: cannot name this rank's GPU (%s)" % e)
        return {"names": [names.get(id(p), "") for p in params],
                "shapes": [list(p.shape) for p in params],
                "dtypes": [str(p.dtype).replace("torch.", "") for p in params],
                "device": device,
                "host": _HOSTS[_RANK] if _RANK < len(_HOSTS) else ""}

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
            if _CATCHUP[0]:
                # A joiner's placeholder batch for an epoch the ring already
                # trained: no update, no sync, no step counted.
                _BATCH_N[0] = 0
                return None
            if not _SYNC_URL:
                if not _WARNED_NO_SYNC[0]:
                    _WARNED_NO_SYNC[0] = True
                    _warn("ddp: no sync endpoint was handed to rank %d, so it trains its "
                          "own slice without averaging with the others" % _RANK)
                return _orig_step(*sa, **skw)
            if _live_ranks() <= 1:
                # Released down to this rank, or every other share gone: a
                # ring of one trains like the script alone, syncing nothing.
                return _orig_step(*sa, **skw)
            n = _STEPN.get(id(self), 0)
            _STEPN[id(self)] = n + 1
            params = [p for g in self.param_groups for p in g["params"]]
            if self not in _STEPPED:
                _STEPPED.add(self)
                # The opening weight broadcast, not a gradient: every rank
                # starts from the same seed, so these are identical by
                # construction and must not be read as duplicated work.
                _daemon_allreduce([p.data for p in params], half=False, kind="weights",
                                  manifest=_manifest(params))
                _log("ddp daemon-channel sync ready (rank %d/%d, every %d step%s)"
                     % (_RANK, _WORLD, _SYNC_EVERY, "" if _SYNC_EVERY == 1 else "s"))
                if _FAST and not _TOLD_FAST[0]:
                    _TOLD_FAST[0] = True
                    _log(_diloco_note(_TOTAL[0]))
                    if _RANK == 0:
                        # Shown without -v: the ranks no longer run the
                        # script's exact arithmetic.
                        every = ("%d" % _DILOCO_FIXED_H if _DILOCO_FIXED_H
                                 else "%d-%d" % (_DILOCO_H_START, _DILOCO_H_MAX))
                        _warn("training with DiLoCo, sync every %s steps after %d "
                              "exact ones — --ddp-sync exact for per-step sync"
                              % (every, _diloco_warmup(_TOTAL[0])))
            ep = _EPOCH[0]
            if ep is not None:
                ep[1] += 1
                if ep[0].pad and ep[1] == ep[0].total_steps:
                    # This rank's last batch of the epoch is a repeated sample
                    # standing in so every rank takes the same number of
                    # steps: it weighs nothing in the average.
                    _BATCH_N[0] = _PAD_SAMPLES
            _now = time.monotonic()
            if _STEP_MARK[0] is not None:
                _elapsed = _now - _STEP_MARK[0]
                _synced = _EXPOSED[0] - _STEP_MARK[1]
                if _elapsed > _synced >= 0:
                    _STEP_SEC[0] += _elapsed - _synced
                    _STEP_SEC[1] += 1
                    _push_window(_STEP_RECENT, _elapsed - _synced, _STEP_WINDOW)
            _STEP_MARK[0], _STEP_MARK[1] = _now, _EXPOSED[0]

            if _FAST:
                try:
                    return _fast_step(self, params, n, _orig_step, sa, skw)
                finally:
                    _BATCH_N[0] = 0
            every = _tuned_sync_every()
            try:
                if every == 1:
                    if _OVERLAP and _ITERS[0] and not _JOIN[0]:
                        return _delayed_step(self, params, _orig_step, sa, skw)
                    grads = [p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad
                             for p in params if p.grad is not None]
                    if grads:
                        _daemon_allreduce(grads, key=("grads", id(self)))
                    return _orig_step(*sa, **skw)
                out = _orig_step(*sa, **skw)
                if (n + 1) % every == 0:
                    # Weights average at full width: a rounding difference in
                    # a weight compounds over the run instead of averaging
                    # out as a gradient's does (R1 #2).
                    _daemon_allreduce([p.data for p in params], half=False, kind="weights")
                return out
            finally:
                # This step's gradient has been sent with the batch behind
                # it; the next forward pass records its own. Left set, the
                # first batch of the run would be reported for every step
                # and a short final batch never seen.
                _BATCH_N[0] = 0

        # A bound method, as the optimizer's own step is: an LR scheduler
        # wraps opt.step through its __func__, and a plain function made
        # every script with a scheduler fail at the scheduler's constructor.
        import types as _types
        self.step = _types.MethodType(_step, self)

    def _forward(self, *args, **kw):
        if (_WORLD > 1 and _SYNC_URL and not _NATIVE_DDP and not _CATCHUP[0]
                and self not in _FWD and _live_ranks() > 1):
            # Only sync once training is underway: the optimizer's first
            # step is the rendezvous.
            if _STEPPED:
                _FWD.add(self)
                bufs = [b for b in self._buffers.values()
                        if b is not None and b.is_floating_point() and b.numel()]
                if bufs:
                    _daemon_broadcast(bufs)
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
                        len(ds), _WEIGHTS, _RANK, shuffle, bs.batch_size, _WORLD,
                        drop_last=bool(getattr(bs, "drop_last", False)))
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
                    len(ds), [1.0 / _WORLD] * _WORLD, _RANK, shuffle, bs.batch_size, _WORLD,
                    drop_last=bool(getattr(bs, "drop_last", False)))
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

    def _diloco_shared(o):
        st = _DILOCO.get(id(o))
        if st is None:
            return {}
        return {"refs": list(st.refs), "ref": st.ref, "started": st.started}

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
            "buffers": [b.detach().cpu() for b in _buffer_list()],
        }
        header = json.dumps({"group": _GROUP, "rank": _RANK, "join": join,
                             "boundary": boundary, "seq": _SEQ[0], "world": world,
                             "weights": weights,
                             "sync_every": int(_SYNC_TUNED[0] or 0),
                             # The DiLoCo schedule's shared state, per
                             # optimizer, so the joiner decides as the ring.
                             "diloco": [_diloco_shared(o) for o in opts]}).encode()
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
        if _FAST:
            _diloco_settle()
        p, _PENDING[0] = _PENDING[0], None
        world = int(p.get("world") or _WORLD)
        w = p.get("weights")
        if p.get("release") is not None and p["release"] != _RANK:
            _log("ddp: rank %d released at this epoch: rank %s trains on alone"
                 % (_RANK, p["release"]))
            import atexit
            try:
                # The receipt and the ring's report, which os._exit would skip.
                atexit._run_exitfuncs()
            except Exception as e:
                _log("ddp: exit handlers failed while releasing rank %d (%s)" % (_RANK, e))
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(_EXIT_RELEASED)
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
        state = _th.load(io.BytesIO(_read_frames(memoryview(data)[nl + 1:], 1)[0]),
                         map_location="cpu")
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
        for o, d in zip(opts, hdr.get("diloco") or []):
            if d:
                st = _DILOCO[id(o)] = _DiLoCo()
                st.refs = [float(x) for x in d.get("refs") or []]
                st.ref = d.get("ref")
                st.started = bool(d.get("started"))
        _JOIN_BUFS[0] = state.get("buffers")
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

    class _CatchupOut:
        """The joiner's stdout during catch-up: each line is tagged, so the
        placeholder epochs' prints are not read as training."""

        def __init__(self, inner):
            self._inner = inner
            self._bol = True

        def write(self, s):
            out = []
            for part in s.splitlines(True):
                if self._bol:
                    out.append("[rank %d catch-up, not trained] " % _RANK)
                out.append(part)
                self._bol = part.endswith("\n")
            self._inner.write("".join(out))
            return len(s)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    def _begin_catchup():
        if not _CATCHUP[0]:
            _CATCHUP[0] = True
            _CATCHUP_OUT[0] = sys.stdout
            sys.stdout = _CatchupOut(sys.stdout)

    def _end_catchup():
        if _CATCHUP[0]:
            _CATCHUP[0] = False
            if isinstance(sys.stdout, _CatchupOut):
                sys.stdout.flush()
                sys.stdout = _CATCHUP_OUT[0]

    def _catchup_batch(loader):
        """One real batch for an epoch the ring already did.

        An empty epoch left the script's loop variables unbound, so a joiner
        died on the first print(loss) after its loop. One batch binds them;
        backward and the optimizer step are no-ops while _CATCHUP is set, so
        nothing trains, syncs or counts twice."""
        for batch in _ORIG_ITER(loader):
            yield batch
            break

    _ORIG_BACKWARD = _th.Tensor.backward

    def _backward(self, *a, **kw):
        if _CATCHUP[0]:
            return None
        return _ORIG_BACKWARD(self, *a, **kw)

    def _catchup_noop(fn):
        def wrapped(*a, **kw):
            if _CATCHUP[0]:
                return None
            return fn(*a, **kw)
        return wrapped

    def _iter(self):
        it = _iter_inner(self)
        if _OVERLAP and _WORLD > 1 and not _NATIVE_DDP and not _CATCHUP[0]:
            # The pass's last average lands when the pass ends, not a step
            # into the next one, so whatever follows the loop (evaluation, a
            # save) sees the model with every update.
            return _PassEnd(it, _flush_delayed)
        return it

    def _iter_inner(self):
        sampler = getattr(self, "sampler", None)
        if _OVERLAP and _WORLD > 1 and not _NATIVE_DDP:
            # A loop broken off early never ended its pass: its last average
            # lands here, before the ring may change below.
            _flush_delayed()
        if _WORLD > 1 and not _NATIVE_DDP:
            call = _ITERS[0]
            _ITERS[0] += 1
            if _JOIN[0]:
                if _JOIN_AT[0] is None:
                    _JOIN_AT[0] = _join_ring()
                if call < _JOIN_AT[0]:
                    # An epoch the ring finished before this rank arrived.
                    _EPOCHS[self] = _EPOCHS.get(self, 0) + 1
                    _begin_catchup()
                    return _catchup_batch(self)
                _end_catchup()
                _load_join_buffers()
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
                        sampler.n, new, _RANK, sampler.shuffle, sampler.base, _WORLD,
                        drop_last=sampler.drop_last)
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
            # Steps are counted from here, to know when this epoch's last
            # (possibly pad) batch is the one being stepped.
            # ponytail: one counter, for the loader iterated last; a loop that
            # interleaves two sharded loaders' steps would need one each.
            _EPOCH[0] = [sampler, 0] if isinstance(sampler, _WeightedShardSampler) else None
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
        if _WORLD <= 1:
            return
        if module._buffers and module not in _BUF_SEEN:
            _BUF_SEEN.add(module)
            _BUF_MODS.append(weakref.ref(module))
        if _BATCH_N[0] or _CATCHUP[0]:
            return
        _ROOTS.add(module)
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
        _warn("ddp: cannot observe batch sizes (%s); gradients will be averaged "
             "as equals, which is only right when the batches are" % e)

    if _FAST:
        # The run length for DiLoCo's final tightening: a OneCycleLR is the
        # one common place a script states it in optimizer steps (a plain
        # epoch loop's count is not observable).
        _OCL = getattr(_th.optim.lr_scheduler, "OneCycleLR", None)
        if _OCL is not None:
            _ORIG_OCL_INIT = _OCL.__init__

            def _ocl_init(self, *a, **kw):
                _ORIG_OCL_INIT(self, *a, **kw)
                _TOTAL[0] = int(self.total_steps)
                _log("ddp: the OneCycleLR says this run is %d steps" % _TOTAL[0])
            _OCL.__init__ = _ocl_init

    _ORIG_DDP = _th.nn.parallel.DistributedDataParallel.__init__
    _th.nn.parallel.DistributedDataParallel.__init__ = _ddp_init
    _th.optim.Optimizer.__init__ = _opt_init
    _th.nn.Module.forward = _forward
    _th.utils.data.DataLoader._pp_shard = _shard
    _th.utils.data.DataLoader.__iter__ = _iter
    if _JOIN[0]:
        _th.Tensor.backward = _backward
        # A GradScaler finds no gradients after a no-op backward and asserts;
        # its step, update and unscale_ sit out the catch-up batch too.
        for cls in {getattr(getattr(_th, "amp", None), "GradScaler", None),
                    getattr(getattr(_th.cuda, "amp", None), "GradScaler", None)} - {None}:
            for name in ("step", "update", "unscale_"):
                if name in cls.__dict__:
                    setattr(cls, name, _catchup_noop(cls.__dict__[name]))
    _log("ddp interception installed")


_PENDING_PATCHES = {}


