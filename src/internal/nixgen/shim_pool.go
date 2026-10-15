package nixgen

// shimPoolA: _ClusterPool, racing, pool width, function payloads, chunking.
const shimPoolA = `class _ClusterPool:
    """drop-in replacement for multiprocessing.Pool.

    Starts as a real local pool; the first chunks run on local cores, which is
    the measurement. Spills to the cluster only when measured per-item cost x
    remaining clearly exceeds the dispatch cost. Local cores never stop pulling,
    so a remote node is an additional consumer, never a subtracter.
    """

    def __init__(self, processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        import multiprocessing.pool as _mp
        self._remote = _cluster_ready()
        self._requested = processes
        self._init = (initializer, initargs, maxtasksperchild)
        self._procs = _pool_width(processes)
        self._ctx = _mp.Pool(
            processes=self._procs,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
        )
        self._pending = 0
        self._measure_items = 4
        self._spilled = False
        self._threads = None
        self._batcher = None
        atexit.register(self.close)

    def _resize(self):
        """Re-size the local pool to what the machine can run right now.

        A width chosen when the pool was built can be wrong by the time the
        next map arrives: another job may have taken the memory, or released
        it. multiprocessing.Pool cannot grow or shrink, so this rebuilds it
        between calls; within a single map the width is fixed.
        """
        want = _pool_width(self._requested, self._workers_memory())
        if want == self._procs:
            return
        if self._batcher is not None and self._batcher._busy:
            return                        # a submitted batch is using this pool
        import multiprocessing.pool as _mp
        initializer, initargs, maxtasksperchild = self._init
        try:
            new = _mp.Pool(processes=want, initializer=initializer,
                           initargs=initargs, maxtasksperchild=maxtasksperchild)
        except Exception:
            return                        # keep the working pool
        old, self._ctx, self._procs = self._ctx, new, want
        if self._threads is not None:
            try:
                self._threads.terminate()
            except Exception:
                pass
            self._threads = None          # rebuilt at the new width on demand
        _log("local pool resized to %d workers" % want)
        try:
            old.terminate()
        except Exception:
            pass

    def _workers_memory(self):
        """Private memory held by this pool's own worker processes.

        _resize measures free memory with those workers already running, so
        without this they counted against the pool that owns them: on a 3 GB
        orchestrator the first map always found less room than the pool was
        sized with, shrank 3 workers to 2 and paid for a rebuild doing it.
        """
        total = 0
        try:
            page = os.sysconf("SC_PAGE_SIZE")
        except (ValueError, OSError):
            return 0
        for proc in getattr(self._ctx, "_pool", None) or []:
            try:
                with open("/proc/%d/statm" % proc.pid) as f:
                    fields = f.read().split()
                total += max(0, int(fields[1]) - int(fields[2])) * page
            except (OSError, ValueError, IndexError, AttributeError):
                pass
        return total

    # ---- the four Pool workhorses ----
    #
    # apply and apply_async are single calls. With a cluster they queue in a
    # _SubmitBatcher, so a loop of them is sent as one starmap through _run.
    # Without one, or with keyword arguments (a starmap item is positional
    # only), they run locally - on whichever local pool can take the
    # callable: a lambda cannot be sent to a worker process, and handing one
    # to the process pool raises PicklingError.
    def apply(self, func, args=(), kwds=None):
        # kwds defaults to None here and to {} in multiprocessing, and it is
        # passed straight through to func(*args, **kwds). So pool.apply(f, (x,))
        # - the ordinary call, with no keyword arguments - raised "argument
        # after ** must be a mapping, not NoneType" from inside the worker,
        # which reads as a bug in the user's function.
        return self.apply_async(func, args, kwds).get()

    def apply_async(self, func, args=(), kwds=None, callback=None, error_callback=None):
        if self._remote and not kwds:
            return _BatchResult(self._submits().submit(
                func, args, callback, error_callback))
        return self._local_ctx(func).apply_async(
            func, args=args, kwds=kwds or {},
            callback=callback, error_callback=error_callback)

    def _submits(self):
        if self._batcher is None:
            self._batcher = _SubmitBatcher(self)
        return self._batcher

    def map(self, func, iterable, chunksize=None):
        return self._run(func, list(iterable))

    def starmap(self, func, iterable, chunksize=None):
        return self._run(func, list(iterable), starmap=True)

    def imap(self, func, iterable, chunksize=None):
        return self._stream(func, iterable, False)

    def imap_unordered(self, func, iterable, chunksize=None):
        return self._stream(func, iterable, False)

    def _stream(self, func, iterable, starmap):
        """imap semantics: pull the source in batches and yield as each lands.

        The old version handed the whole iterable to map() first. That holds
        every item and every result in memory at once, and turns a generator
        with no end - which imap explicitly supports - into a hang. Batches
        are sized to be worth distributing while staying bounded: too small
        and every batch falls under the dispatch floor and stays local.
        """
        batch, out = [], _imap_batch(self._procs)
        for item in iterable:
            batch.append(item)
            if len(batch) >= out:
                for r in self._run(func, batch, starmap):
                    yield r
                batch = []
        if batch:
            for r in self._run(func, batch, starmap):
                yield r

    def _run(self, func, items, starmap=False, catch=False):
        _add_stat("mapped_items", len(items))
        self._resize()
        if _OFFLOAD_HERE and self._remote and items:
            # --offload: no measuring head on this machine; all of it ships.
            payload = _func_payload(func) or _func_pickle(func)
            if payload is not None:
                _decide("pool.map", True, "--offload keeps this machine's share at 0")
                return self._race(func, items, starmap, 0, payload, catch)
            _offload_warn("a pool.map", "its function cannot be sent, by source or by value")
        if not self._remote or len(items) <= self._measure_items:
            return self._local(func, items, starmap, catch)
        # Measure the first few locally, then decide whether to spill the rest.
        # The batch is as wide as the pool: measuring four items on a twenty
        # core machine left sixteen cores idle for the length of one item, a
        # serial bubble in front of every intercepted map.
        head_n = max(self._measure_items, min(self._procs, len(items) // 2))
        head, tail = items[:head_n], items[head_n:]
        t0 = time.monotonic()
        head_results = self._local(func, head, starmap, catch)
        wall = time.monotonic() - t0
        if not tail:
            return head_results
        # Per-item cost comes from the batch we just ran, not from executing an
        # item a third time. The batch occupied min(items, procs) lanes, so the
        # wall clock covers that many items at once.
        lanes = max(1, min(len(head), self._procs))
        cost = wall * lanes / max(len(head), 1)
        if cost <= 0:
            return head_results + self._local(func, tail, starmap, catch)

        payload = _func_payload(func) or _func_pickle(func)
        if payload is not None and payload.get("pickled"):
            _log("kernel %r has no source to send; shipping it by value"
                 % getattr(func, "__name__", "?"))
        if payload is None:
            _decide("pool.map", False, "the function cannot be sent, by source or by value")
            _record("pool", len(tail), False, "unshippable")
            _log("kernel %r cannot ship at all, by source or by value; "
                 "staying local" % getattr(func, "__name__", "?"))
            return head_results + self._local(func, tail, starmap, catch)
        if not _FORCE and len(tail) * cost < _POOL_MIN_WORK:
            _log("tail is %.2fs of work, below the %.2fs dispatch floor; staying local"
                 % (len(tail) * cost, _POOL_MIN_WORK))
            _decide("pool.map", False, "%d items x %.3fs = %.2fs of work, under the %.2fs worth a round trip"
                    % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
            return head_results + self._local(func, tail, starmap, catch)
        _decide("pool.map", True, "forced" if _FORCE else
                "%d items x %.3fs = %.2fs of work, over the %.2fs worth a round trip"
                % (len(tail), cost, len(tail) * cost, _POOL_MIN_WORK))
        return head_results + self._race(func, tail, starmap, cost, payload, catch)

    def _local_ctx(self, func):
        """The pool that can actually run func on this machine.

        A lambda, or a function closing over a local, does not merely fail to
        reach a peer - it fails to reach this machine's own worker
        PROCESSES, because multiprocessing pickles a callable by reference
        and there is no name for a worker to resolve. Stock
        multiprocessing.Pool raises PicklingError for exactly these, so the
        measurement head died before anything could be dispatched and the
        by-value work below was unreachable.

        Threads share the interpreter, so nothing has to be pickled to reach
        them. The trade is worth naming plainly: for CPU-bound work the GIL
        makes these threads take turns, so the LOCAL half stops being
        parallel. The remote half is unaffected and is where the win is; the
        alternative is the call raising, which is what it did before.
        """
        if _process_safe(func):
            return self._ctx
        if self._threads is None:
            import multiprocessing.dummy as _dummy
            self._threads = _dummy.Pool(self._procs)
            _log("kernel %r cannot cross a process boundary, so local work "
                 "runs on threads; the cluster still runs it in parallel"
                 % getattr(func, "__name__", "?"))
        return self._threads

    def _local(self, func, items, starmap, catch=False):
        _log("local %d items" % len(items))
        _record_local(len(items))
        ctx = self._local_ctx(func)
        if catch:
            return ctx.starmap(_apply_catching, [(func, v) for v in items])
        if starmap:
            return ctx.starmap(func, items)
        return ctx.map(func, items)

    def _race(self, func, items, starmap, per_item_cost, payload, catch=False):
        self._spilled = True

        def run_local(vals):
            ctx = self._local_ctx(func)
            if catch:
                return ctx.starmap(_apply_catching, [(func, v) for v in vals])
            if starmap:
                # _apply carries func as an argument, so a process pool would
                # have to pickle it as data; route by the same test either way.
                return ctx.starmap(_apply, [(func, v) for v in vals])
            return ctx.map(func, vals)

        return _race_items(items, per_item_cost, self._procs, run_local,
                           lambda chunk: self._remote_chunk(payload, chunk, starmap))

    @staticmethod
    def _remote_chunk(payload, chunk, starmap, kind="pool"):
        """Dispatch one chunk to the cluster; returns [(orig_idx, result), ...]
        or None on failure (local already has the work covered). chunk is a list
        of (orig_idx, item) pairs, payload is the _func_payload triple.

        Everything here — including serialisation — sits inside the try. When
        it did not, an unpicklable argument raised out of this method, killed
        the dispatch thread and printed a traceback, which is the one thing
        interception promises never to do."""
        import pickle
        idxs = [p[0] for p in chunk]
        vals = [p[1] for p in chunk]
        if starmap:
            vals = [v if isinstance(v, (list, tuple)) else (v,) for v in vals]
        t0 = time.monotonic()
        try:
            globals_pickle = pickle.dumps(payload.get("gvars") or {})
            items = [pickle.dumps(v) for v in vals]
            header = {
                "items_frames": len(items),
                "globals": True,
                "starmap": starmap,
            }
            # Exactly one of the two, never both: the runner prefers func_src
            # and would silently ignore a callable sent beside it, so a
            # header carrying both would test the wrong path while looking
            # like it tested this one.
            if payload.get("pickled"):
                header["func"] = payload["pickled"]
            else:
                header["func_src"] = payload["src"]
                header["func_name"] = payload["name"]
            # Admission control: the daemon 503s rather than OOM when it
            # cannot spare this, and micro-chunks when it is over budget.
            #
            # The callable counts towards it. A lambda closing over a large
            # array carries that array inside its own payload rather than in
            # the globals frame, so sizing globals+items alone would report a
            # few kilobytes for a request holding hundreds of megabytes.
            header["required_mem"] = _pool_required(
                globals_pickle, items, header.get("func", ""))
            if _FORCE:
                header["force"] = True
            info = {}
            blobs = _pool_send(header, globals_pickle, items, 600, info)
            if len(blobs) != len(idxs):
                raise RuntimeError("got %d results for %d items" % (len(blobs), len(idxs)))
            out = [(idxs[i], pickle.loads(b)) for i, b in enumerate(blobs)]
        except Exception as e:
            _record(kind, len(idxs), False, str(e), (time.monotonic() - t0) * 1000)
            _log("remote failed (%s); local covers it" % e)
            return None
        ms = (time.monotonic() - t0) * 1000
        receipt = info.get("receipt")
        _record(kind, len(out), True, "", ms, receipt)
        out = _RemotePairs(out)
        out.elsewhere = sum(int(p.get("items", 0)) for p in (receipt or {}).get("parts", [])
                            if str(p.get("where", "")).startswith("peer:"))
        if payload.get("pickled"):
            # Counted here rather than where the tier is chosen, so it counts
            # items that were actually SENT by value. Counting the tail at
            # decision time made it larger than dispatched_items - the race
            # keeps half the tail local - and a receipt whose subset exceeds
            # its superset is exactly the kind of number this file exists to
            # not produce.
            with _STATS_LOCK:
                _STATS["shipped_pickled"] += len(out)
        where = ""
        if receipt:
            peers = sorted({p.get("where", "")[5:] for p in receipt.get("parts", [])
                            if str(p.get("where", "")).startswith("peer:")})
            where = (" on " + ", ".join(peers)) if peers else " on the origin node"
        _log("remote ok: %d items in %.0f ms%s" % (len(out), ms, where))
        return out

    def close(self):
        if self._batcher is not None:
            self._batcher.drain()     # queued calls finish before the pool goes
        try:
            self._ctx.close()
            self._ctx.terminate()
        except Exception:
            pass
        if self._threads is not None:
            try:
                self._threads.close()
                self._threads.terminate()
            except Exception:
                pass
            self._threads = None

    def terminate(self):
        self.close()

    def join(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


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
    size = max(1, min(max(_adaptive_chunk(per_item_cost), width), len(items)))
    chunks = _chunk(list(enumerate(items)), size)
    slots = [None] * len(items)
    with ThreadPoolExecutor(max(1, min(_REMOTE_INFLIGHT, len(chunks)))) as ex:
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
    # D2: local and remote each pull ~half the tail concurrently; when local
    # finishes its share and a remote chunk is still in flight, an idle
    # local core speculatively re-runs it — first result wins per item.
    # Worst case (remote dead/absent) local eventually does everything, so
    # this is never slower than a plain local Pool; a working remote halves
    # the tail's wall time.
    if _OFFLOAD_HERE:
        return _offload_items(items, per_item_cost, width, run_local, run_remote)
    n = len(items)
    # A sentinel, not None: a kernel that returns None would otherwise
    # leave its slot looking unfilled, and local_run would re-run it
    # forever.
    unset = object()
    slots = [unset] * n
    lock = threading.Lock()
    # A chunk has to be wide enough to fill the node that receives it. The
    # adaptive size targets half a second of work taken one item at a
    # time, which for an item costing about a second collapses to a single
    # item per round trip: the peer then runs one item on one core and
    # sixteen sit idle while the origin waits out the latency. Sizing to
    # at least the local pool width uses this machine as the stand-in for
    # a peer's core count, which is the best estimate the shim has.
    #
    # Capped at half the tail, because ownership alternates: one chunk
    # would be dealt to the local side and the cluster handed nothing.
    chunk_size = max(1, min(max(_adaptive_chunk(per_item_cost), width),
                            (n + 1) // 2))
    chunks = _chunk(list(enumerate(items)), chunk_size)  # [(orig_idx, item), ...]

    # One queue both sides pull from, so each takes chunks as fast as it
    # finishes them. Ownership used to be dealt alternately up front, which
    # fixed this machine's share at half however many peers there were: a
    # three-worker cluster ran 00_pool no faster than a one-worker one,
    # because the half left here took as long as the whole job did elsewhere.
    queue = list(chunks)
    remote_taken = []
    claimed = {}       # id(chunk) -> when a peer took it; None once it failed
    remote_secs = []   # how long finished remote chunks took

    def take(remote=False):
        # Popping and recording a remote claim happen under one lock: split,
        # local could find the queue empty and the claim not yet listed, and
        # return with that chunk's slots still empty.
        with lock:
            # Once every item has its result the race is over: a peer
            # thread that comes back for more must not send anything, or
            # its request lands in the middle of the script's next call.
            if not queue or done.is_set():
                return None
            chunk = queue.pop(0)
            if remote:
                remote_taken.append(chunk)
                claimed[id(chunk)] = time.monotonic()
            return chunk

    def fill(pairs):
        """Run user func over (idx, item) pairs and first-wins into slots."""
        idxs = [p[0] for p in pairs]
        vals = [p[1] for p in pairs]
        res = run_local(vals)
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

    # Remote threads: each pulls a chunk, dispatches it, first-wins into
    # slots, and pulls the next. Several are in flight at once: one at a time
    # capped the whole cluster's throughput at a single chunk, so a peer sat
    # idle between round trips no matter how many cores it had. A failed
    # chunk stops that thread pulling; local re-runs what it held.
    def deliver_loop():
        while True:
            chunk = take(remote=True)
            if chunk is None:
                return
            t0 = time.monotonic()
            res = run_remote(chunk)
            if res is None:
                with lock:
                    claimed[id(chunk)] = None
                return
            won = 0
            with lock:
                for orig_i, v in res:
                    if slots[orig_i] is unset:
                        slots[orig_i] = v
                        won += 1
            with lock:
                remote_secs.append(time.monotonic() - t0)
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

    def remote_run():
        inflight = max(1, min(_REMOTE_INFLIGHT, len(chunks) - 1))
        # Daemons, unlike the local side: once the race is done a chunk still
        # in flight is a loser whose answer is thrown away, and as a non-daemon
        # it held the interpreter open at exit for as long as a slow peer took
        # (up to the 600 s request timeout). t_local below is what keeps a
        # live race - and the receipt of what it kept - alive at exit.
        threads = [threading.Thread(target=deliver_loop, daemon=True) for _ in range(inflight)]
        for t in threads:
            t.start()

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

    done = threading.Event()
    # Explicitly not daemons: a thread inherits daemon from its creator,
    # and a batch of submits runs on the daemon batcher thread, so a live
    # race would be killed at exit and its results never reach the caller.
    t_local = threading.Thread(target=local_run, daemon=False)
    t_remote = threading.Thread(target=remote_run, daemon=False)
    t_local.start()
    t_remote.start()
    done.wait()  # local guarantees a complete result
    if failed:
        raise failed[0]
    return slots


def _cgroup_stat(path, key):
    """One counter from a cgroup's memory.stat, or 0 if it cannot be read."""
    try:
        with open(path + "/memory.stat") as f:
            for line in f:
                name, _, value = line.partition(" ")
                if name == key:
                    return int(value)
    except (OSError, ValueError):
        pass
    return 0


def _cgroup_headroom():
    """What this process's cgroup still allows, or None if it is uncapped.

    The daemon runs under a memory ceiling so that a runaway pool cannot take
    the whole machine down with it. Without reading that ceiling here, the
    pool sizes itself against the machine's free memory, overshoots the cap,
    and the kernel kills a worker - a job failure caused by the very
    protection meant to prevent one. Reading it turns "killed" into "used
    fewer workers".
    """
    try:
        with open("/proc/self/cgroup") as f:
            for line in f:
                if line.startswith("0::"):
                    own = line[3:].strip()
                    break
            else:
                return None
    except OSError:
        return None

    # The limit can be set on any ancestor, and the binding one is the
    # smallest remaining headroom along the whole chain.
    best = None
    path = "/sys/fs/cgroup" + own
    while True:
        try:
            with open(path + "/memory.max") as f:
                raw = f.read().strip()
            if raw != "max":
                with open(path + "/memory.current") as f:
                    used = int(f.read().strip())
                # memory.current counts page cache, which the kernel reclaims
                # before it kills anything. Left in, every file a job reads
                # shrinks the next job's pool: measured 2.0GB of a worker's
                # 2.15GB "used" was inactive cache, against 33MB of real
                # memory, and the pool fell from 5 workers to 1 over five warm
                # runs. inactive_file is the part reclaimed first; current
                # minus it is the "working set" container runtimes report.
                used -= _cgroup_stat(path, "inactive_file")
                room = int(raw) - max(0, used)
                if best is None or room < best:
                    best = room
        except (OSError, ValueError):
            pass
        if path == "/sys/fs/cgroup" or "/" not in path[len("/sys/fs/cgroup"):]:
            break
        path = path.rsplit("/", 1)[0]
    return best


def _avail_bytes():
    """Memory this machine could actually give a new process.

    Not SC_AVPHYS_PAGES: that counts only wholly free pages and ignores the
    page cache, which the kernel hands back on demand. On any box with a warm
    cache it reads several times too low - measured 2GB free against 21GB
    available here - and every limit derived from it collapses. MemAvailable
    is the kernel's own answer to this question.

    Bounded by the cgroup's remaining headroom where there is one, because the
    machine having memory free does not mean this process is allowed to use
    it.
    """
    avail = 0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
                    break
    except (OSError, ValueError, IndexError):
        pass
    if not avail:
        try:
            avail = os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        except (ValueError, OSError):
            return 0
    room = _cgroup_headroom()
    if room is not None and room < avail:
        return max(0, room)
    return avail


def _usable_cores():
    """Cores this process may actually run on.

    os.cpu_count() is the machine's total, which overcounts whenever the job is
    pinned (taskset, a container's cpuset) or throttled (a cgroup cpu.max
    quota): the pool then starts more workers than there are cores to run
    them, and they only time-slice each other.
    """
    try:
        n = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        n = os.cpu_count() or 1
    try:
        with open("/proc/self/cgroup") as f:
            own = next((l[3:].strip() for l in f if l.startswith("0::")), None)
    except OSError:
        own = None
    if own is not None:
        # A quota on any ancestor binds, so walk up and keep the smallest.
        path = "/sys/fs/cgroup" + own
        while True:
            try:
                with open(path + "/cpu.max") as f:
                    quota, period = f.read().split()[:2]
                if quota != "max":
                    n = min(n, max(1, -(-int(quota) // int(period))))
            except (OSError, ValueError):
                pass
            if path in ("/sys/fs/cgroup", "/sys/fs/cgroup/"):
                break
            path = os.path.dirname(path)
    return max(1, n)


def _imap_batch(procs):
    """Items to buffer before running a batch of a lazy imap."""
    override = os.environ.get("PIPEDPEER_IMAP_BATCH", "").strip()
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            pass
    return max(64, procs * 32)


def _pool_width(requested, reclaim=0):
    """How many local workers to run.

    A hand-picked process count is a guess about the machine the author had
    in front of them, so by default it is ignored in favour of what this
    machine can actually run: every core, bounded by free memory. Picking the
    number is the work this is supposed to remove.

    Some counts encode correctness rather than speed, though - a rate limit,
    a resource that is not safe to touch twice, a model that only fits a few
    times over - and those break rather than slow down when overridden.
    PIPEDPEER_RESPECT_POOL_SIZE=1 hands the decision back.
    """
    cores = _usable_cores()
    if os.environ.get("PIPEDPEER_RESPECT_POOL_SIZE") == "1" and requested:
        return max(1, int(requested))
    n = cores
    avail = _avail_bytes()
    if avail > 0:
        # reclaim is what an existing pool's workers hold: rebuilding the
        # pool frees it, so it is memory a new width can count on.
        avail += reclaim
        # Nothing here knows the per-worker working set before the job runs,
        # so assume a conservative 256MB rather than none: a box with many
        # cores and little free memory must not fan out to all of them.
        n = min(n, max(1, int(avail * 0.4) // (256 << 20)))
    if requested and int(requested) != n:
        _log("using %d workers, not the %d requested (cores=%d); "
             "set PIPEDPEER_RESPECT_POOL_SIZE=1 to keep your own count"
             % (n, int(requested), cores))
    return max(1, n)


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
    """Ship func by value with cloudpickle, as {"pickled": b64}, or None.

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
    import base64

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
    return {"pickled": base64.b64encode(blob).decode()}


def _apply(func, item):
    """Run func over one starmap item (a tuple of args)."""
    return func(*item) if isinstance(item, tuple) else func(item)


class _Raised(object):
    """A call's exception, returned in place of its result."""

    def __init__(self, exc):
        self.exc = exc


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


`

// shimPoolB: spill cost model, bandwidth probe, compression, _pool_send.
const shimPoolB = `# ---- distributed pandas: cost model, hash-shuffle groupby/merge, OOC ----
# Always on, and safe to be: every intercept gates on the latency cost
# model (_should_spill): a shuffle only fires when transfer + remote estimate
# beats the single-node estimate, so interception is never slower than plain
# pandas (D2). Shuffles bucket rows by hash(key) % K (K = peers+1, bucket 0
# stays local) and ship one bucket per no_split item, so every key's rows land
# complete on one node and the per-node agg/merge is exact — no combiners, any
# agg spec works.
_IN_MERGE = False
_BW_CACHE = {"t": 0.0, "bw": None}
# Floor of an svd/eig offload's wall-clock limit (tests lower it).
_LINALG_MIN_DEADLINE = 30.0
_BW_TTL = 300.0

# The daemon always listens on loopback (http://127.0.0.1:<selfPort>), so its
# requests must never be routed through an http(s)_proxy / no_proxy env proxy
# (GH Actions runners and many sandboxes set these, which would otherwise
# hijack or refuse loopback traffic and make the shim silently fall back to
# local). Build a proxy-less opener and reuse it everywhere.
_DaemonOpener = None
def _daemon_open(req, timeout, opener=None):
    """Send req to the loopback daemon, bypassing any environment HTTP proxy.

    The shared secret is attached here rather than at each call site: there
    are half a dozen of those and a new one that forgot would 401, fall back
    to local work, and look like a slow job rather than a misconfiguration."""
    if _TOKEN and not req.has_header("X-pipedpeer-token"):
        req.add_header("X-Pipedpeer-Token", _TOKEN)
    global _DaemonOpener
    import urllib.error
    import urllib.request
    if _DaemonOpener is None:
        _DaemonOpener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        return (opener or _DaemonOpener).open(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        raise RuntimeError("daemon HTTP %d: %s" % (e.code, e.read())) from e


def _should_spill(nbytes, flops_per_byte, op="pandas"):
    """_should_spill_why, with its decision recorded in the receipt when the
    work was big enough to be considered at all."""
    _WHY["v"] = ""
    r = _should_spill_why(nbytes, flops_per_byte)
    if _WHY["v"]:
        _decide(op, r, _WHY["v"], nbytes)
    return r


def _should_spill_why(nbytes, flops_per_byte):
    """Latency cost model: spill nbytes of work (flops_per_byte per byte) when
    the estimated transfer + remote compute beats local single-node time.
    est_local = nbytes*flops_per_byte/1e9 (~1e9 flops/s effective), est_remote
    = est_local/K*1.3 (parallel speedup, 30% overhead)."""
    if not (_URL and _ENABLED):
        return False
    K = int(_NUM_SHARDS)
    if K < 2:
        _offload_warn("distributed work", "no other machine is in the cluster")
    if K < 2 or nbytes < _spill_min():
        return False
    if _FORCE:
        _WHY["v"] = "forced (PIPEDPEER_DISTRIBUTE=force)"
        return True
    if flops_per_byte < 8 and nbytes <= 512 * 1024 * 1024:
        _WHY["v"] = "too little compute per byte to pay for moving %.0f MB" % (nbytes / 1e6)
        return False
    bw = _measure_bandwidth()
    if not bw:
        _WHY["v"] = "could not measure the link to the cluster"
        return False
    est_transfer = nbytes / bw
    est_local = nbytes * flops_per_byte / 1e9
    est_remote = est_local / K * 1.3
    _WHY["v"] = ("here ~%.2fs, there ~%.2fs transfer + %.2fs compute on %d nodes"
                 % (est_local, est_transfer, est_remote, K))
    return est_local > est_transfer + est_remote


def _measure_bandwidth():
    """Effective bytes/sec to the cluster, cached 300s. Probes through the
    same frames path real work uses (the base64-in-JSON path measured ~30MB/s
    and the cost model then refused every spill; frames ships raw pickle).
    64MB of blobs so the measurement is throughput-bound, not latency-bound.
    None on failure means stay local."""
    now = time.monotonic()
    if now - _BW_CACHE["t"] < _BW_TTL:
        return _BW_CACHE["bw"]
    import pickle
    try:
        blobs = [os.urandom(8 * 1024 * 1024) for _ in range(8)]
        items = [pickle.dumps(b) for b in blobs]
        hdr = {
            "func_src": "def run(x):\n    return len(x)\n",
            "items_frames": len(items),
        }
        t0 = time.monotonic()
        _pool_send(hdr, None, items, 60)
        bw = sum(len(b) for b in blobs) / max(time.monotonic() - t0, 1e-6)
    except Exception:
        bw = None
    _BW_CACHE["t"] = now
    _BW_CACHE["bw"] = bw
    return bw


def _maybe_compress(items, header):
    """Compress item frames when it pays for itself.

    Whether it does depends entirely on the payload. A pandas frame of
    strings or categoricals shrinks by most of its size; a dense float array
    is already incompressible and every byte of CPU spent on it is waste. So
    try, keep the result only if it is meaningfully smaller, and record the
    choice in the header rather than guessing again on the other side.

    The daemon relays these bytes without looking inside them, and carries
    the items_zlib flag across every header it rebuilds when it splits
    or forwards the work, so the choice reaches whichever worker runs it.
    """
    if not items:
        return items
    raw_total = sum(len(i) for i in items)
    if raw_total < 1 << 20:
        return items                  # too small for the CPU to be worth it
    import zlib
    try:
        packed = [zlib.compress(i, 1) for i in items]
    except Exception:
        return items
    if sum(len(p) for p in packed) > raw_total * 0.9:
        return items                  # not worth the decompression on the far side
    header["items_zlib"] = True
    return packed


def _pool_send(header, globals_pickle, items, timeout, out_header=None, opener=None):
    """POST a frames pool/map request: small JSON header line + optional
    globals frame + length-prefixed raw pickle item frames. Returns the raw
    pickle blobs of the results (frames response). Bulk data never touches
    base64 or a giant JSON parse, which is what made the old encoding ~30MB/s."""
    import json
    import pickle
    import struct
    import urllib.request
    items = _maybe_compress(items, header)
    body = json.dumps(header).encode() + b"\n"
    if globals_pickle is not None:
        body += struct.pack(">I", len(globals_pickle)) + globals_pickle
    for it in items:
        body += struct.pack(">I", len(it)) + it
    req = urllib.request.Request(_URL + "/v1/pool/map", data=body,
                                 headers={"Content-Type": "application/vnd.pipedpeer.frames",
                                          "X-Pipedpeer-Store": _STORE,
                                          "X-Pipedpeer-Submitter": _SUBMITTER,
                                          "X-Pipedpeer-Offload": _OFFLOAD})
    try:
        with _daemon_open(req, timeout, opener) as resp:
            data = resp.read()
    except Exception as e:
        # Every caller falls back to running the work here.
        _offload_warn("work the cluster refused", "the cluster could not take it (%s)" % str(e)[:200])
        raise
    nl = data.find(b"\n")
    hdr = json.loads(data[:nl])
    here = sum(int(p.get("items", 0)) for p in (hdr.get("receipt") or {}).get("parts", [])
               if not str(p.get("where", "")).startswith("peer:"))
    if here:
        _offload_warn("%d item(s)" % here, "no other machine could take them "
                      "(none healthy with this environment, or each one failed)")
    if out_header is not None:
        out_header.update(hdr)
    rest = data[nl + 1:]
    out = []
    for _ in range(hdr.get("results_frames", 0)):
        n = struct.unpack(">I", rest[:4])[0]
        out.append(rest[4:4 + n])
        rest = rest[4 + n:]
    return out


def _pool_send_within(header, globals_pickle, items, deadline):
    """_pool_send with a wall-clock limit, raising TimeoutError past it.

    urllib's timeout is per socket operation, so a peer that is slow but not
    silent, or a daemon that tries peer after peer, keeps it waiting for ever:
    02_numpy_heavy.py's svd sat 15 minutes while the daemon gave four
    container peers 300 s each. On expiry the connection is shut, which the
    daemon reads as the requester going away and drops its peer forwards."""
    import socket
    import http.client
    import urllib.request
    conns = []

    class _Tracked(urllib.request.HTTPHandler):
        def http_open(self, req):
            def make(host, **kw):
                c = http.client.HTTPConnection(host, **kw)
                conns.append(c)
                return c
            return self.do_open(make, req)

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _Tracked())
    box = {}

    def send():
        try:
            box["r"] = _pool_send(header, globals_pickle, items, deadline, opener=opener)
        except BaseException as e:
            box["e"] = e

    t = threading.Thread(target=send, daemon=True)
    t.start()
    t.join(deadline)
    if t.is_alive():
        for c in conns:
            try:
                if c.sock is not None:
                    c.sock.shutdown(socket.SHUT_RDWR)
            except OSError as e:
                _log("could not close the timed-out cluster request: %s" % e)
        raise TimeoutError("no answer from the cluster in %.0f s" % deadline)
    if "e" in box:
        raise box["e"]
    return box["r"]


`
