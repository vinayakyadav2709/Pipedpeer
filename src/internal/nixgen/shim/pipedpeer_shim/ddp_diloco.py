"""DDP: DiLoCo schedule, outer step and round."""
import os
import sys

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
_DILOCO_ENV_H = _env_num("PIPEDPEER_DILOCO_H", None, int)
# The interval in use; _install_ddp sets it from the ring size (_diloco_every).
_DILOCO_FIXED_H = 64 if _DILOCO_ENV_H is None else _DILOCO_ENV_H
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
# DILOCO-MANY (docs/benchmarks/diloco.md "Ranks"): plain averaging lands
# further from one device as ranks are added (each trains on a smaller slice
# of the batch), so the default interval depends on the ring size: rows of
# (up to this many ranks, H). Past the last row no interval passed: the
# daemon never picks fast sync there, and an explicit --ddp-sync fast uses
# the last row's H and says so. Same table as dilocoRanks in the daemon's
# ddprebalance.go (TestDiLoCoRanksMatchShim).
_DILOCO_RANKS = ((2, 32), (3, 16), (4, 4))

def _diloco_every(world):
    """(H, validated) for a ring of world ranks: PIPEDPEER_DILOCO_H when set
    (the user's choice), else the _DILOCO_RANKS row; past the last row its H
    and validated False."""
    if _DILOCO_ENV_H is not None:
        return _DILOCO_ENV_H, True
    for up_to, h in _DILOCO_RANKS:
        if world <= up_to:
            return h, True
    return _DILOCO_RANKS[-1][1], False

def _diloco_use_every(h):
    """Set the interval every schedule function reads (0 = adaptive). Every
    rank must hold the same one: they sync at the same steps."""
    global _DILOCO_FIXED_H, _DILOCO_H_START
    _DILOCO_FIXED_H = h
    _DILOCO_H_START = h or 2

def _sync_mode(env, overlap):
    """The rank's sync mode from PIPEDPEER_DDP_SYNC_MODE: "fast" or "exact"
    when the user named one with --ddp-sync, else "auto" (U32): exact until
    the lead measures DiLoCo faster and says so. Never "auto" with the opt-in
    overlap, whose step-late gradient a mid-run switch would strand."""
    mode = (env or "").strip().lower()
    if mode in ("fast", "exact"):
        return mode
    return "exact" if overlap else "auto"

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

# Time-bounded rounds, for ranks of unequal speed (a phone next to a laptop).
# A local round gets a budget of SLACK x its steps x the ring's median step
# time (the lead daemon's steps_ms, from every rank's last report). A rank
# still short of the round's end when its budget runs out skips the round's
# remaining batches and syncs with what it trained; the average is weighted
# by samples, so the round is the mean over the work actually done. A fast
# rank then waits at most one budget a round instead of the slowest rank's
# whole interval. The skipped batches are not trained this epoch; the
# epoch-boundary refit shrinks a slow rank's share, so skips fade after it.
# Skipped data costs accuracy (PHONE-DDP sim, H 64, 1 laptop + 10 phones that
# each stall 3x in a tenth of rounds: slack 2 saves ~30% of the time, drops
# ~3% of samples and adds +0.1-2% val loss against equal-step rounds; 1.25 on
# misjudged shares dropped ~10% for +6%). Accuracy outranks speed, so the
# default 4 is a stall guard only: it trips on a rank 4x slower than the
# ring's median for a round, not on the ordinary spread the refit absorbs.
# PIPEDPEER_DILOCO_ROUND_SLACK=0 gives equal-step rounds (the A/B baseline).
_DILOCO_ROUND_SLACK = _env_num("PIPEDPEER_DILOCO_ROUND_SLACK", 4.0)

def _round_budget(steps_ms, steps, slack=None):
    """Seconds a local round of the given steps may take: slack x steps x the
    median of the ranks' step times (ms, by rank). None when nothing is known
    or the budget is off."""
    slack = _DILOCO_ROUND_SLACK if slack is None else slack
    try:
        vals = sorted(float(v) for v in (steps_ms or {}).values() if float(v) > 0)
    except (TypeError, ValueError, AttributeError):
        return None
    if slack <= 0 or not vals or steps < 1:
        return None
    k = len(vals)
    med = vals[k // 2] if k % 2 else (vals[k // 2 - 1] + vals[k // 2]) / 2
    return slack * steps * med / 1000.0

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
        # The round's clock: set when its first batch is handed out (not at
        # the sync, so an evaluation between epochs is not counted), with
        # the step it started after and its budget in seconds (None: none).
        self.t0 = None
        self.n0 = 0
        self.budget = None
        self.skipped = 0    # batches skipped over the run, for the report

    def begin(self, params, h, done):
        """Start a local phase after step done: the model now is the global
        one, the outer momentum starts from zero."""
        self.theta = [_np_copy(p) for p in params]
        self.buf = None
        self.h = h
        self.next_sync = done + h
        self.samples = 0
        self.started = True
        self.t0 = None

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

