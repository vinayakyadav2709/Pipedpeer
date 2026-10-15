package daemonapi

// chunkFanOut is shared by both worker scripts: it spreads one chunk's items
// across the node's cores instead of walking them one at a time.
//
// A chunk used to run in a single process, so a 16-core peer contributed
// exactly one core to a job. Spilling to it could therefore be slower than
// keeping the work at home — distribution that measurably lost to doing
// nothing, which is the opposite of the point. The kernel arrives as exec'd
// source and is not importable, so it cannot be pickled out to workers; a
// fork context inherits it through module globals instead, which is also why
// this must never use spawn or forkserver.
const chunkFanOut = `
_FUNC = None
_STARMAP = False


def _call_one(item):
    if _STARMAP:
        return _FUNC(*(item if isinstance(item, (list, tuple)) else (item,)))
    return _FUNC(item)


def _mute_stdout():
    # stdout is the warm worker's ack channel; a print() in user code would
    # otherwise land in the middle of the protocol.
    sys.stdout = sys.stderr


def _free_bytes():
    """Memory this node could actually give a new process.

    Not SC_AVPHYS_PAGES: that counts only wholly free pages and ignores the
    page cache, which the kernel reclaims on demand, so on a box with a warm
    cache it reads several times too low and every limit built on it
    collapses. MemAvailable is the kernel's own answer."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    try:
        return os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError):
        return 0


def _fan_width(n_items, required_mem):
    """How many processes to spread a chunk over: bounded by cores, by the
    item count, and by the same 40%-of-free-RAM rule the daemon admits chunks
    against, since running items side by side multiplies the working set.

    The memory bound applies even when the caller sent no estimate. Trusting
    cores alone in that case is how a node with plenty of cores and little
    free RAM gets pushed over: nothing below this enforces a limit, because
    the sandbox carries no cgroup at all, so this arithmetic is the only thing
    standing between a wide chunk and the OOM killer. Unknown per-item cost is
    assumed to be a conservative 256MB rather than zero."""
    if n_items <= 1:
        return 1
    n = os.cpu_count() or 1
    override = os.environ.get("PIPEDPEER_WORKER_PROCS", "").strip()
    if override:
        try:
            return max(1, min(int(override), n_items))
        except ValueError:
            pass
    avail = _free_bytes()
    if avail > 0:
        if required_mem > 0:
            per_item = max(1, required_mem // n_items)
        else:
            per_item = 256 << 20
        n = min(n, max(1, int(avail * 0.4) // per_item))
    return max(1, min(n, n_items))


def run_items(func, items, starmap, req):
    global _FUNC, _STARMAP
    _FUNC, _STARMAP = func, starmap
    # The cache-backed reads keep parsed chunks in this process's _CACHE.
    # Forked children would populate their own copy and the parent would miss
    # on the next fetch, so those paths stay single-process.
    stateful = bool(req.get("cache_keys") or req.get("item_keys"))
    width = 1 if stateful else _fan_width(len(items), int(req.get("required_mem") or 0))
    if width > 1:
        try:
            import multiprocessing
            with multiprocessing.get_context("fork").Pool(width, _mute_stdout) as pool:
                return pool.map(_call_one, items, chunksize=1)
        except Exception as e:
            sys.stderr.write("[pipedpeer] worker pool of %d failed (%r); running %d items single-process\n"
                             % (width, e, len(items)))
    return [_call_one(item) for item in items]
`

// frameCodec is shared by both worker scripts: frames are read through a
// memoryview with offsets (slicing bytes copied the rest of the body per
// frame, O(n^2) in the chunk size), from a bytearray so protocol-5
// out-of-band buffers come out writable, as numpy arrays from pickle are.
//
// An out-of-band frame (R9 a3) is self-describing, so it survives a daemon
// that rebuilds the header without the flag: _OOB_MAGIC, buffer count,
// pickle length, each buffer length (all >I), the pickle, then the buffers.
// Results go out of band only when the request says oob (the submitter can
// read them); otherwise they are plain protocol-5 pickles.
const frameCodec = `
_OOB_MAGIC = b"\x00PP5"
_STEPS = []


def _step(name, t0, nbytes=0):
    dur = time.perf_counter() - t0
    end = time.time_ns()
    _STEPS.append({"step": name, "start_ns": end - int(dur * 1e9),
                   "dur_ns": int(dur * 1e9), "bytes": int(nbytes)})


def read_body(path):
    with open(path, "rb") as f:
        buf = bytearray(os.fstat(f.fileno()).st_size)
        f.readinto(buf)
    return memoryview(buf)


def read_frames(mv, off, n):
    out = []
    for _ in range(n):
        size = struct.unpack_from(">I", mv, off)[0]
        out.append(mv[off + 4:off + 4 + size])
        off += 4 + size
    return out, off


def load_func(ns, req):
    # A by-value callable rides in the globals frame as raw bytes under
    # __pipedpeer_func__ (the shim's _FUNC_KEY); older submitters sent it as
    # base64 in the header's "func".
    blob = ns.pop("__pipedpeer_func__", None)
    if blob is not None:
        return pickle.loads(blob)
    return pickle.loads(base64.b64decode(req["func"]))


def load_obj(b):
    if bytes(b[:4]) == _OOB_MAGIC:
        nbuf, plen = struct.unpack_from(">II", b, 4)
        lens = struct.unpack_from(">%dI" % nbuf, b, 12)
        off = 12 + 4 * nbuf
        p = b[off:off + plen]
        off += plen
        bufs = []
        for n in lens:
            bufs.append(b[off:off + n])
            off += n
        return pickle.loads(p, buffers=bufs)
    return pickle.loads(b)


def dump_parts(obj, oob):
    if not oob:
        return [pickle.dumps(obj, protocol=5)]
    bufs = []
    p = pickle.dumps(obj, protocol=5, buffer_callback=bufs.append)
    if not bufs:
        return [p]
    raws = [b.raw() for b in bufs]
    return [_OOB_MAGIC + struct.pack(">II%dI" % len(raws), len(raws), len(p),
                                     *[r.nbytes for r in raws]), p] + raws


def write_results(path, results, oob):
    t0 = time.perf_counter()
    framed = []
    for r in results:
        parts = dump_parts(r, oob)
        framed.append(struct.pack(">I", sum(len(x) for x in parts)))
        framed.extend(parts)
    nbytes = sum(len(x) for x in framed)
    _step("pickle", t0, nbytes)
    head = {"results_frames": len(results), "steps": _STEPS}
    with open(path, "wb") as f:
        f.write(json.dumps(head).encode() + b"\n")
        f.writelines(framed)
`

// poolRunner is the Python helper executed inside the closure to run a
// pickled function over a batch of items. It is written to a temp file and
// invoked via <storePath>/bin/run pool_runner.py <payload.bin> <out.bin>.
// It is the cold path: each invocation spawns a fresh closure process, which
// costs seconds. The warm worker (below) replaces it for the steady state.
// Accepts both the frames wire format (header line + length-prefixed raw
// pickle frames) and the legacy base64-in-JSON batch.
const poolRunner = `# Runs a pickled callable over a batch, for pipedpeer's cluster pool.
import base64, hashlib, json, os, pickle, struct, sys, time

` + chunkFanOut + frameCodec + `

t0 = time.perf_counter()
data = read_body(sys.argv[1])
nl = data.obj.find(b"\n")
req = json.loads(data.obj if nl < 0 else data.obj[:nl])
frames = bool(req.get("items_frames"))
if frames:
    off = nl + 1
    g = b""
    if req.get("globals"):
        (g,), off = read_frames(data, off, 1)
    raw, off = read_frames(data, off, req["items_frames"])
    # items_zlib: an older submitter compressed each item (kept for it).
    if req.get("items_zlib"):
        import zlib
        raw = [zlib.decompress(r) for r in raw]
    items = [load_obj(r) for r in raw]
else:
    req = json.loads(data.obj)
    g = b""
    items = req["items"]

ns = {"_CACHE": {}}
ns["_CHUNK_DIR"] = req.get("chunk_dir", "")
if g:
    ns.update(load_obj(g))
elif req.get("extra_b64"):
    ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
if req.get("func_src"):
    exec(req["func_src"], ns)
    func = ns[req.get("func_name", "run")]
else:
    func = load_func(ns, req)
starmap = req.get("starmap", False)
if not frames and req.get("items_b64"):
    items = [pickle.loads(base64.b64decode(i)) for i in items]
_step("unpickle", t0, len(data))

if req.get("cache_keys"):
    items = []
    for k in req["cache_keys"]:
        v = ns["_CACHE"].get(k)
        if v is None and ns["_CHUNK_DIR"]:
            p = os.path.join(ns["_CHUNK_DIR"], hashlib.sha256(k.encode()).hexdigest())
            if os.path.exists(p):
                with open(p, "rb") as f:
                    v = pickle.load(f)
                ns["_CACHE"][k] = v
        items.append(v)
    if any(i is None for i in items):
        missing = [k for k, v in zip(req["cache_keys"], items) if v is None]
        raise KeyError("chunk cache miss: %s (dir=%s)" % (missing, ns["_CHUNK_DIR"]))

t0 = time.perf_counter()
results = run_items(func, items, starmap, req)
_step("compute", t0)

if frames:
    write_results(sys.argv[2], results, bool(req.get("oob")))
else:
    out = {"results": [base64.b64encode(pickle.dumps(r)).decode() for r in results]}
    with open(sys.argv[2], "w") as f:
        json.dump(out, f)
`

// warmWorkerScript is the long-lived sibling of poolRunner: one process per
// store path stays up reading JSON-lines from stdin and writing them back to
// stdout. Each line names an in_file (the payload JSON, written to disk by the
// daemon so the full base64 blob never lives in daemon RAM) and an out_file
// for results; the daemon reads the results file when done. This is the "warm
// workers" dispatch model (handoff.md §4/D2): one worker process per node per
// store, tasks streaming as messages.
const warmWorkerScript = `# Persistent pipedpeer cluster worker. One process per store path.
# Reads JSON-lines from stdin: each is {"id": N, "in_file": ..., "out_file": ...}.
# Runs the payload from in_file, writes the results to out_file, then
# answers with a tiny {"id": N, "done": true, "error": ...} line on stdout.
# Kept alive across many /v1/pool/map requests so dispatch never re-spawns
# the closure. _CACHE persists across requests inside this process: func_src
# can read it (via the injected _CACHE name) to store/retrieve chunk data
# keyed by content hash, which is how out-of-core reads keep chunks on nodes.
# in_file/out_file use the frames wire format (header line + length-prefixed
# raw pickle frames) for heavy chunks and legacy base64 JSON for small ones.
import base64, gc, hashlib, json, os, pickle, struct, sys, time, traceback

` + chunkFanOut + frameCodec + `

# ponytail: FIFO-eviction byte budget instead of a true LRU — the point is to
# never OOM the worker from accumulating parsed chunks, and a dict preserves
# insertion order so evicting from the front is O(1). Budget defaults to ~35%
# of free RAM and can be overridden with PIPEDPEER_CACHE_BUDGET. If chunk
# hit-rates ever drop (daemon re-ships the same chunk because it was evicted),
# upgrade to an OrderedDict.move_to_end LRU on __getitem__/__setitem__ —
# same budget math, just reorders instead of evicting the wrong entry.
class _BoundedCache(dict):
    def __init__(self):
        super().__init__()
        self._bytes = 0
        budget = os.environ.get("PIPEDPEER_CACHE_BUDGET", "").strip()
        if budget:
            try:
                self.budget = int(float(budget))
            except ValueError:
                self.budget = self._default_budget()
        else:
            self.budget = self._default_budget()

    @staticmethod
    def _default_budget():
        try:
            avail = os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
            return int(avail * 0.35)
        except (ValueError, OSError):
            return 2 << 30

    @staticmethod
    def _size_of(v):
        try:
            return int(v.memory_usage(deep=True).sum())
        except Exception:
            try:
                return sys.getsizeof(v)
            except Exception:
                return 1024

    def __setitem__(self, k, v):
        if k in self:
            self._bytes -= self._size_of(self[k])
        super().__setitem__(k, v)
        self._bytes += self._size_of(v)
        while self._bytes > self.budget and len(self) > 1:
            oldest = next(iter(self))
            self._bytes -= self._size_of(self[oldest])
            del self[oldest]

_CACHE = _BoundedCache()
_CHUNK_DIR = ""

def load_req(path):
    data = read_body(path)
    nl = data.obj.find(b"\n")
    req = json.loads(data.obj if nl < 0 else data.obj[:nl])
    frames = bool(req.get("items_frames"))
    if not frames:
        req = json.loads(data.obj)
        # Legacy requests carry globals as extra_b64 (base64 pickle); return an
        # empty bytes g so the caller's extra_b64 branch decodes them. Passing
        # extra_b64 here would make pickle.loads(g) choke on a str.
        return req, frames, req["items"], b"", len(data)
    off = nl + 1
    g = b""
    if req.get("globals"):
        (g,), off = read_frames(data, off, 1)
    raw, off = read_frames(data, off, req["items_frames"])
    # items_zlib: an older submitter compressed each item (kept for it).
    if req.get("items_zlib"):
        import zlib
        raw = [zlib.decompress(r) for r in raw]
    return req, frames, [load_obj(r) for r in raw], g, len(data)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
        del _STEPS[:]
        t0 = time.perf_counter()
        req, frames, items, g, nbytes = load_req(msg["in_file"])
        _CHUNK_DIR = req.get("chunk_dir", "")
        ns = {}
        if g:
            ns.update(load_obj(g))
        elif req.get("extra_b64"):
            ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
        ns["_CACHE"] = _CACHE
        ns["_CHUNK_DIR"] = _CHUNK_DIR
        if req.get("func_src"):
            exec(req["func_src"], ns)
            func = ns[req.get("func_name", "run")]
        else:
            func = load_func(ns, req)
        starmap = req.get("starmap", False)
        if not frames and req.get("items_b64"):
            items = [pickle.loads(base64.b64decode(i)) for i in items]
        _step("unpickle", t0, nbytes)
        # cache_keys: item i is a content hash resolved from the process-wide
        # chunk cache (or the on-disk fallback) instead of the payload. A miss
        # is an error: the submitter falls back to local work rather than
        # running with wrong data.
        if req.get("cache_keys"):
            items = []
            for k in req["cache_keys"]:
                v = _CACHE.get(k)
                if v is None and _CHUNK_DIR:
                    p = os.path.join(_CHUNK_DIR, hashlib.sha256(k.encode()).hexdigest())
                    if os.path.exists(p):
                        with open(p, "rb") as f:
                            v = pickle.load(f)
                        _CACHE[k] = v
                items.append(v)
            if any(i is None for i in items):
                missing = [k for k, v in zip(req["cache_keys"], items) if v is None]
                raise KeyError("chunk cache miss: %s (dir=%s)" % (missing, _CHUNK_DIR))
        t0 = time.perf_counter()
        results = run_items(func, items, starmap, req)
        _step("compute", t0)
        if frames:
            write_results(msg["out_file"], results, bool(req.get("oob")))
        else:
            out = {"id": msg["id"], "results":
                   [base64.b64encode(pickle.dumps(r)).decode() for r in results]}
            with open(msg["out_file"], "w") as f:
                json.dump(out, f)
        ack = {"id": msg["id"], "done": True}
    except Exception as e:
        ack = {"id": msg.get("id"), "done": False, "error": str(e) + "\n" + traceback.format_exc()}
    # Explicit release before the next chunk: a long-lived worker must not
    # accumulate partitions while pipelining micro-chunks on a small node.
    for _n in ("req", "items", "results", "g"):
        try:
            exec("del " + _n)
        except NameError:
            pass
    gc.collect()
    sys.stdout.write(json.dumps(ack) + "\n")
    sys.stdout.flush()
`
