"""DDP: the weighted batch split, shard sampler and rank weights."""

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
