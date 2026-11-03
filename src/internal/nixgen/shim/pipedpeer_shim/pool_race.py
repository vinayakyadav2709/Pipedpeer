"""Racing chunks between this machine and the cluster."""
import threading
import time

from pipedpeer_shim import record
from pipedpeer_shim.core import (
    _OFFLOAD_HERE, _log, _offload_warn,
)
from pipedpeer_shim.stats import (
    _add_stat, _record_local,
)
from pipedpeer_shim.pool_func import (
    _chunk,
)
from pipedpeer_shim.pool_pacing import (
    _RACE_REPACE_S,
    _Flight,
    _TLS,
    _chunk_cap,
    _chunk_floor,
    _cluster_pacing,
    _fac2_size,
    _race_lost,
    _race_won,
    _remote_paused,
)


class _RemotePairs(list):
    """A chunk's [(orig_idx, result), ...], and how many ran on another machine.

    A daemon with no free peer runs a chunk itself, on this machine; those
    results are not the cluster's, and counting them as remote_used made
    the receipt and /ui claim work that never left. No receipt, no claim."""
    elsewhere = 0


def _offload_items(items, per_item_cost, width, run_local, run_remote):
    """_race_items under --offload: every chunk goes to the cluster and no
    local core races it. A chunk the cluster could not take runs here, said
    out loud, so the run still finishes."""
    from concurrent.futures import ThreadPoolExecutor
    pacing = _cluster_pacing()
    size = max(1, min(_chunk_floor(per_item_cost, width, pacing), len(items)))
    chunks = _chunk(list(enumerate(items)), size)
    slots = [None] * len(items)
    with ThreadPoolExecutor(max(1, min(pacing["inflight"], len(chunks)))) as ex:
        answers = list(ex.map(run_remote, chunks))
    for chunk, res in zip(chunks, answers):
        if res is None:
            _offload_warn("%d item(s)" % len(chunk), "the cluster could not take them")
            vals = run_local([p[1] for p in chunk])
            _record_local(len(chunk))
            res = list(zip([p[0] for p in chunk], vals))
        else:
            used = min(len(res), getattr(res, "elsewhere", len(res)))
            if used:
                _add_stat("remote_used", used)
        for i, v in res:
            slots[i] = v
    return slots


def _race_items(items, per_item_cost, width, run_local, run_remote):
    """Run items on this machine and the cluster at once, first result wins.

    run_local(values) returns their results in order; run_remote(chunk) takes
    [(index, value), ...] and returns [(index, result), ...], or None when the
    chunk failed. width is the local lane count. Shared by Pool.map and joblib,
    so both get the same never-slower-than-local guarantee.
    """
    # D2: local and remote pull from one queue concurrently; when local runs
    # dry and a remote chunk is still in flight, an idle local core
    # speculatively re-runs it, first result wins per item, and the losing
    # remote copy is cancelled. Worst case (remote dead/absent) local does
    # everything, so this is never slower than a plain local Pool.
    if _OFFLOAD_HERE:
        return _offload_items(items, per_item_cost, width, run_local, run_remote)
    n = len(items)
    # A sentinel, not None: a kernel that returns None would otherwise
    # leave its slot looking unfilled, and local_run would re-run it
    # forever.
    unset = object()
    slots = [unset] * n
    lock = threading.Lock()
    t_start = time.monotonic()

    # Pacing comes from the daemon: how many chunks to keep in flight (each
    # node's depth, summed: one running and one queued per node on a LAN, not
    # a global 4) and how long a part should be. Chunks are cut as they are
    # taken, FAC2-style: each round hands out half of what is left, so the
    # first chunks amortise start-up and the last are small and the tail
    # short. The floor keeps a chunk wide enough to fill the cores that get it
    # and long enough to pay for its round trip; the cap keeps it short
    # enough that most of the map stays queued, for whoever is free.
    #
    # The pacing is read again every _RACE_REPACE_S while the race runs
    # (repace, from remote_run): the peers holding this closure are whoever
    # holds it NOW. A peer that finished importing after the job started (the
    # first job after a join) or a machine that joined mid-map raises the
    # depth, and remote_run starts a dispatch thread per new slot. Read once
    # at the start, a map never used anyone who arrived after it began.
    pace = {}

    def repace(pacing):
        peers = max(0, int(pacing.get("peers", 0)))
        with lock:
            pace["peers"] = peers
            # No peer holds the closure: a chunk would run on our own
            # daemon, beside the local pool. Hold until one does. A daemon
            # that did not say (an older build) is sent to as before.
            hold = not peers and pacing.get("known", True)
            pace["inflight"] = 0 if hold else max(1, pacing["inflight"])
            pace["pullers"] = 1 + max(1, pacing["inflight"])
            # Threads beyond one per peer only prefetch; they stop in the
            # tail, when the queue can no longer fill everyone (bind late).
            pace["base"] = max(1, peers)
            pace["floor_r"] = _chunk_floor(per_item_cost, width, pacing)
            pace["floor_l"] = _chunk_floor(per_item_cost, width, dict(pacing, cores=0))
            pace["cap_r"] = _chunk_cap(per_item_cost, max(width, pacing.get("cores") or 0), pacing)
            pace["cap_l"] = _chunk_cap(per_item_cost, width, pacing)

    queue = list(enumerate(items))      # [(orig_idx, item), ...]
    pos = [0]
    seq = [0]
    remote_taken = []
    claimed = {}       # id(chunk) -> when a peer took it; None once it failed
    remote_secs = []   # how long finished remote chunks took
    flights = {}       # id(chunk) -> _Flight while its request is out

    def take(remote=False, slot=0):
        # Popping and recording a remote claim happen under one lock: split,
        # local could find the queue empty and the claim not yet listed, and
        # return with that chunk's slots still empty.
        with lock:
            # Once every item has its result the race is over: a peer
            # thread that comes back for more must not send anything, or
            # its request lands in the middle of the script's next call.
            left = n - pos[0]
            if not left or done.is_set():
                return None
            floor = pace["floor_r"] if remote else pace["floor_l"]
            if remote and (_remote_paused() or slot >= pace["inflight"]
                           or (slot >= pace["base"] and left < pace["inflight"] * floor)):
                return None
            size = _fac2_size(left, pace["pullers"], floor)
            cap = pace["cap_r"] if remote else pace["cap_l"]
            if cap is not None:
                # Never under the floor, nor under 4 rounds of the local
                # pool: a chunk is a barrier for the cores that run it.
                size = min(size, max(cap, floor, 4 * width))
            chunk = queue[pos[0]:pos[0] + size]
            pos[0] += size
            k = seq[0]
            seq[0] += 1
            if remote:
                remote_taken.append(chunk)
                claimed[id(chunk)] = time.monotonic()
        record.step("queue", time.monotonic() - t_start, chunk=k)
        return chunk

    def cut_losers():
        """Cancel remote chunks whose every item already has a result."""
        with lock:
            losers = [flights.pop(cid) for cid in list(flights)
                      if all(slots[p[0]] is not unset for p in by_id[cid])]
        for f in losers:
            f.cancel()

    by_id = {}

    def fill(pairs):
        """Run user func over (idx, item) pairs and first-wins into slots."""
        idxs = [p[0] for p in pairs]
        vals = [p[1] for p in pairs]
        t0 = time.monotonic()
        res = run_local(vals)
        record.step("compute", time.monotonic() - t0, items=len(pairs))
        won = 0
        with lock:
            for i, v in zip(idxs, res):
                if slots[i] is unset:
                    slots[i] = v
                    won += 1
        # Counted only where the local result is the one kept: these items
        # were missing from the receipt, which then summed to less than
        # the map. A speculative re-run that lost is not counted twice.
        if won:
            _record_local(won)
        cut_losers()

    # Remote threads: each pulls a chunk, dispatches it, first-wins into
    # slots, and pulls the next. A failed chunk stops that thread pulling;
    # local re-runs what it held. A cancelled one (local won it) lost a race.
    def deliver_loop(j):
        try:
            deliver(j)
        finally:
            with lock:
                live.discard(j)

    def deliver(j):
        while True:
            chunk = take(remote=True, slot=j)
            if chunk is None:
                return
            flight = _Flight()
            with lock:
                by_id[id(chunk)] = chunk
                flights[id(chunk)] = flight
            _TLS.flight = flight
            t0 = time.monotonic()
            try:
                res = run_remote(chunk)
            finally:
                _TLS.flight = None
                with lock:
                    flights.pop(id(chunk), None)
            secs = time.monotonic() - t0
            if res is None:
                if flight.cancelled:
                    _race_lost(secs)
                    continue
                with lock:
                    claimed[id(chunk)] = None
                    broken[0] += 1   # this slot stays closed; local re-runs the chunk
                return
            won = 0
            with lock:
                for orig_i, v in res:
                    if slots[orig_i] is unset:
                        slots[orig_i] = v
                        won += 1
                remote_secs.append(secs)
            if won:
                _race_won()
            else:
                _race_lost(secs)
            # ponytail: the receipt says how many of a chunk's items ran
            # elsewhere, not which, so a mixed chunk that won only in part is
            # counted pro rata; per-item placement on the wire would make it exact.
            el = min(len(res), getattr(res, "elsewhere", len(res)))
            used = won if el == len(res) else won * el // max(1, len(res))
            if used:
                _add_stat("remote_used", used)

    def local_secs(chunk):
        return per_item_cost * len(chunk) / max(1, min(width, len(chunk)))

    def worth_rerunning(chunk, now):
        """Re-run a peer's chunk here only when that beats waiting for it.

        Re-running everything a peer held the moment local ran dry made a
        fast origin redo the cluster's work every time, so a peer's answer
        rarely won and adding peers changed nothing. A chunk that failed is
        re-run at once. Once some peer answers have come back, local takes a
        chunk when its own run would finish before the peer is expected to,
        or the peer is well overdue. Before any have, it gives the peer
        twice the chunk's local time plus 0.7 s - the most this costs over
        never dispatching at all.
        """
        start = claimed.get(id(chunk))
        if start is None:
            return True
        here = local_secs(chunk)
        if not remote_secs:
            return now > start + 2 * here + 0.7
        expected = sum(remote_secs) / len(remote_secs)
        return now + here < start + expected or now > start + 1.5 * expected + 0.2

    live = set()     # dispatch slots with a thread running
    broken = [0]     # slots closed by a failed chunk

    def open_slots():
        """Start a dispatch thread for each slot the pacing allows that has
        none. A slot whose chunk failed stays closed, as before the pacing
        was re-read: a failing cluster must not be retried every second."""
        with lock:
            want = min(pace["inflight"] - broken[0], n) - len(live)
            new = []
            if pos[0] < n and not done.is_set() and not _remote_paused():
                free = (j for j in range(pace["inflight"]) if j not in live)
                new = [j for _, j in zip(range(max(0, want)), free)]
            live.update(new)
        # Daemons, unlike the local side: once the race is done a chunk still
        # in flight is a loser whose answer is thrown away, and as a non-daemon
        # it held the interpreter open at exit for as long as a slow peer took
        # (up to the 600 s request timeout). t_local below is what keeps a
        # live race - and the receipt of what it kept - alive at exit.
        for j in new:
            threading.Thread(target=deliver_loop, args=(j,), daemon=True).start()

    def remote_run():
        open_slots()
        while not done.wait(_RACE_REPACE_S):
            before = (pace["peers"], pace["inflight"])
            repace(_cluster_pacing(max_age=_RACE_REPACE_S))
            if (pace["peers"], pace["inflight"]) != before:
                _log("map in progress: %d peer(s) hold this closure now; %d chunk(s) in flight"
                     % (pace["peers"], pace["inflight"]))
            open_slots()

    # Local pulls until the queue is empty, then re-runs any chunk a peer
    # still holds (straggler tail). done is set once local has given every
    # item a chance, so the caller can always return a complete result.
    # An item that raises here used to kill this thread before done was
    # set, so the caller waited forever instead of seeing the exception
    # a stock Pool.map raises. The error is carried back and re-raised.
    failed = []

    def local_run():
        try:
            while True:
                chunk = take()
                if chunk is None:
                    break
                fill(chunk)
            while True:
                now = time.monotonic()
                with lock:
                    open_chunks = [c for c in remote_taken
                                   if any(slots[p[0]] is unset for p in c)]
                    rerun = [p for c in open_chunks if worth_rerunning(c, now)
                             for p in c if slots[p[0]] is unset]
                if not open_chunks:
                    break
                if rerun:
                    fill(rerun)  # idle local cores re-run the stragglers
                else:
                    time.sleep(0.02)
        except BaseException as e:
            failed.append(e)
        finally:
            done.set()  # also stops take() handing peers new chunks
            with lock:
                left = list(flights.values())
                flights.clear()
            for f in left:   # every request still out lost the race
                f.cancel()

    done = threading.Event()
    repace(_cluster_pacing())
    # t_local is explicitly not a daemon: a thread inherits daemon from its
    # creator, and a batch of submits runs on the daemon batcher thread, so
    # a live race would be killed at exit and its results never reach the
    # caller. t_remote only paces; it must not hold the exit for a pacing
    # request still out when the race ends.
    t_local = threading.Thread(target=local_run, daemon=False)
    t_remote = threading.Thread(target=remote_run, daemon=True)
    t_local.start()
    t_remote.start()
    done.wait()  # local guarantees a complete result
    if failed:
        raise failed[0]
    return slots
