"""Transparent PyTorch DDP: batch weights, shard sampler, sync."""
import os
import sys
import weakref

from pipedpeer_shim.core import (
    _TOKEN, _log,
)


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
    # True from a skipped epoch's placeholder batch until the joiner's next
    # epoch starts; with it the stdout the script had before.
    _CATCHUP = [False]
    _CATCHUP_OUT = [None]
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
    _SYNC_RAW = os.environ.get("PIPEDPEER_DDP_SYNC_EVERY", "1").strip().lower()
    _SYNC_AUTO = _SYNC_RAW == "auto"
    _SYNC_EVERY = 1 if _SYNC_AUTO else max(1, int(_SYNC_RAW or "1"))
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
            _log("ddp: the ring's %d buffer(s) do not match this rank's %d; keeping "
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
            if _CATCHUP[0]:
                # A joiner's placeholder batch for an epoch the ring already
                # trained: no update, no sync, no step counted.
                _BATCH_N[0] = 0
                return None
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
        if _WORLD > 1 and not _NATIVE_DDP and not _CATCHUP[0] and self not in _FWD:
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
            "buffers": [b.detach().cpu() for b in _buffer_list()],
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
        if _WORLD <= 1:
            return
        if module._buffers and module not in _BUF_SEEN:
            _BUF_SEEN.add(module)
            _BUF_MODS.append(weakref.ref(module))
        if _BATCH_N[0] or _CATCHUP[0]:
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


