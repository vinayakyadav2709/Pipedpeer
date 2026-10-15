package daemonapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Frames wire format. A frames body is a small JSON header line followed by
// length-prefixed raw pickle frames:
//
//	{header json, "items_frames": N}\n[globals frame][item frame]...
//
// frame = [4-byte big-endian length][raw bytes]. The header carries only small
// metadata (function source, flags, counts); all bulk data travels as frames,
// so big items never go through base64 or a giant JSON parse. Both requests
// and responses use it; legacy base64-in-JSON bodies are still accepted and
// emitted when the submitter did not opt in (small payloads keep the old path).
func putFrame(buf []byte, payload []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(payload)))
	return append(buf, payload...)
}

// readFrame pulls one length-prefixed frame off src.
func readFrame(src []byte) ([]byte, []byte, error) {
	if len(src) < 4 {
		return nil, nil, fmt.Errorf("truncated frame header")
	}
	n := int(binary.BigEndian.Uint32(src[:4]))
	src = src[4:]
	if len(src) < n {
		return nil, nil, fmt.Errorf("truncated frame payload (need %d, have %d)", n, len(src))
	}
	return src[:n], src[n:], nil
}

// parseFrames reads the globals frame (when present) and itemCount item frames.
func parseFrames(body []byte, hasGlobals bool, itemCount int) ([]byte, []json.RawMessage, error) {
	rest := body
	var globals []byte
	var err error
	if hasGlobals {
		if globals, rest, err = readFrame(rest); err != nil {
			return nil, nil, err
		}
	}
	items := make([]json.RawMessage, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		var f []byte
		if f, rest, err = readFrame(rest); err != nil {
			return nil, nil, err
		}
		items = append(items, f)
	}
	return globals, items, nil
}

// buildFrames assembles a frames body from a header line and optional globals.
func buildFrames(header []byte, globals []byte, items []json.RawMessage) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, header...)
	buf = append(buf, '\n')
	if globals != nil {
		buf = putFrame(buf, globals)
	}
	for _, it := range items {
		buf = putFrame(buf, it)
	}
	return buf
}

func chunkDirFor(storePath string) string {
	sum := sha256.Sum256([]byte(storePath))
	// Parsed chunks are worth keeping across a warm-worker respawn and can be
	// large; a tmpfs charges RAM for them and drops them on reboot.
	return filepath.Join(userdir.Cache(), "chunkcache", hex.EncodeToString(sum[:8]))
}

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

// poolFramesHeader is the first line of a frames reply.
func poolFramesHeader(n int, node string, parts []provPart) []byte {
	recJSON, _ := json.Marshal(parts) // []provPart always marshals
	return fmt.Appendf(nil, "{\"results_frames\": %d, \"receipt\": {\"node\": %q, \"parts\": %s}}\n",
		n, node, recJSON)
}

const framesContentType = "application/vnd.pipedpeer.frames"

// resultPickle extracts the raw pickle bytes a result carries. Results are
// standardised on raw pickle bytes internally; only the wire edges base64.
func resultPickle(r any) []byte {
	m, ok := r.(map[string]string)
	if !ok {
		return nil
	}
	return []byte(m["pickle"])
}

// --- The peer link (C21): U27 implements, U18 calls from pool.go.
//
// Three things happen to a frames body on its way to a peer, all here so the
// shim never pays for them on the loopback hop:
//   - a globals frame of 1 MiB or more crosses once per peer: later bodies
//     carry only its sha256 and the peer reads it from its own cache;
//   - the whole body becomes one zstd stream when a sample says it shrinks
//     and the link is slow enough for that to pay (Dask's sample test, R9 a1);
//   - the bytes/s every real transfer achieved feed a per-peer EWMA, which
//     replaces the shim's 64 MB probe (R9 a4).
// Everything is additive on the wire (ADR-0006): an older peer never sees a
// hash-only body or a zstd one unless it was sent by a build that knows it.

const (
	// globalsByHashMin is joblib's memmap threshold; below it the frame costs
	// less than the extra round trip a cache miss would.
	globalsByHashMin = 1 << 20
	// compressMinBody: smaller bodies are latency-bound; Dask skips < 10 kB.
	compressMinBody = 64 << 10
	trialSamples    = 5
	trialSampleSize = 16 << 10
	// globalsCacheMax bounds the peer-side globals cache on disk.
	globalsCacheMax = 2 << 30
	// heldPerPeer bounds how many hashes the origin remembers per peer.
	heldPerPeer = 64
	// linkFresh is how long a link sample stays believable: Wi-Fi moves.
	linkFresh = 15 * time.Minute
	linkEWMA  = 0.3
	// linkMinSample: smaller transfers measure latency, not bandwidth.
	linkMinSample = 256 << 10
)

type peerLink struct {
	bps  float64
	at   time.Time
	held map[string]time.Time // globals sha256 -> when sent
}

var links = struct {
	sync.Mutex
	m map[string]*peerLink
}{m: map[string]*peerLink{}}

func linkFor(peerKey string) *peerLink {
	l := links.m[peerKey]
	if l == nil {
		l = &peerLink{held: map[string]time.Time{}}
		links.m[peerKey] = l
	}
	return l
}

// encodePeerBody prepares a frames body for the link to one peer: it drops a
// globals frame the peer already holds (sending its globals_sha256 instead)
// and zstd-compresses the result when that pays on this link. Set
// Content-Encoding to the returned value when it is non-empty. Bodies that are
// not frames pass through untouched.
func encodePeerBody(peerKey string, body []byte) (out []byte, contentEncoding string) {
	body = globalsByHash(peerKey, body)
	bps, known := linkSpeed(peerKey)
	lvl, ok, _ := compressChoice(body, bps, known)
	if !ok {
		return body, ""
	}
	return encoderFor(lvl).EncodeAll(body, make([]byte, 0, len(body)/2)), "zstd"
}

// splitFramesHeader returns the parsed header line and the frames after it,
// or ok=false for a legacy (non-frames) body.
func splitFramesHeader(body []byte) (hdr map[string]json.RawMessage, rest []byte, ok bool) {
	nl := bytes.IndexByte(body, '\n')
	if nl <= 0 {
		return nil, nil, false
	}
	if err := json.Unmarshal(body[:nl], &hdr); err != nil {
		return nil, nil, false
	}
	var n int
	if json.Unmarshal(hdr["items_frames"], &n) != nil || n <= 0 {
		return nil, nil, false
	}
	return hdr, body[nl+1:], true
}

func hdrBool(hdr map[string]json.RawMessage, k string) bool {
	var b bool
	_ = json.Unmarshal(hdr[k], &b) // absent or not a bool reads as false
	return b
}

// joinFrames rebuilds a body from a header and the frames that follow it.
func joinFrames(hdr map[string]json.RawMessage, globals, rest []byte) []byte {
	h, _ := json.Marshal(hdr) // map of RawMessage that came from json always marshals
	n := len(h) + 1 + len(rest)
	if globals != nil {
		n += 4 + len(globals)
	}
	buf := make([]byte, 0, n)
	buf = append(buf, h...)
	buf = append(buf, '\n')
	if globals != nil {
		buf = putFrame(buf, globals)
	}
	return append(buf, rest...)
}

func globalsByHash(peerKey string, body []byte) []byte {
	hdr, rest, ok := splitFramesHeader(body)
	if !ok || !hdrBool(hdr, "globals") {
		return body
	}
	g, after, err := readFrame(rest)
	if err != nil || len(g) < globalsByHashMin {
		return body
	}
	sum := sha256.Sum256(g)
	sha := hex.EncodeToString(sum[:])
	hdr["globals_sha256"], _ = json.Marshal(sha)
	links.Lock()
	l := linkFor(peerKey)
	_, held := l.held[sha]
	if !held {
		if len(l.held) >= heldPerPeer {
			evictOldest(l.held)
		}
	}
	// Marked before the send: a send that fails leaves the peer without it,
	// and that peer's 409 brings it back through forgetPeerGlobals.
	l.held[sha] = time.Now()
	links.Unlock()
	if held {
		hdr["globals"] = json.RawMessage("false")
		return joinFrames(hdr, nil, after)
	}
	return joinFrames(hdr, g, after)
}

func evictOldest(m map[string]time.Time) {
	var oldK string
	var oldT time.Time
	for k, t := range m {
		if oldK == "" || t.Before(oldT) {
			oldK, oldT = k, t
		}
	}
	delete(m, oldK)
}

// forgetPeerGlobals is called when a peer answers that it lacks a globals
// blob it was assumed to hold, before the one resend.
func forgetPeerGlobals(peerKey, sha string) {
	links.Lock()
	defer links.Unlock()
	if l := links.m[peerKey]; l != nil {
		delete(l.held, sha)
	}
	log.Printf("[pool] peer %s lacked globals %.12s; resending them once", peerKey, sha)
}

// missingGlobalsError is decodePoolBody's answer to a hash-only body whose
// globals this node does not hold. The handler replies 409
// {"missing_globals": sha} (poolDecodeStatus) and the origin resends.
type missingGlobalsError struct{ Sha string }

func (e *missingGlobalsError) Error() string {
	return "globals " + e.Sha + " not in this node's cache"
}

// poolDecodeStatus maps a decodePoolBody error to the reply handlePoolMap
// writes: 409 {"missing_globals": sha} on a cache miss, else 400.
func poolDecodeStatus(err error) (int, map[string]string) {
	var m *missingGlobalsError
	if errors.As(err, &m) {
		return http.StatusConflict, map[string]string{"missing_globals": m.Sha}
	}
	return http.StatusBadRequest, map[string]string{"error": "bad pool body: " + err.Error()}
}

// missingGlobalsIn reads a peer's reply: ok when it is the 409 asking for a
// globals blob, so the origin calls forgetPeerGlobals and resends once.
func missingGlobalsIn(status int, body []byte) (sha string, ok bool) {
	if status != http.StatusConflict {
		return "", false
	}
	var r struct {
		Sha string `json:"missing_globals"`
	}
	if json.Unmarshal(body, &r) != nil || r.Sha == "" {
		return "", false
	}
	return r.Sha, true
}

// decodePoolBody undoes encodePeerBody on the receiving daemon, right after
// handlePoolMap reads the body: it unpacks zstd and puts back a globals frame
// sent by hash (or stores one sent in full, for the next chunk). A hash it
// cannot resolve is a *missingGlobalsError.
func decodePoolBody(contentEncoding string, raw []byte) ([]byte, error) {
	body := raw
	switch contentEncoding {
	case "", "identity":
	case "zstd":
		var err error
		if body, err = zstdDecoder().DecodeAll(raw, nil); err != nil {
			return nil, fmt.Errorf("zstd body: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q", contentEncoding)
	}
	hdr, rest, ok := splitFramesHeader(body)
	if !ok {
		return body, nil
	}
	var sha string
	if json.Unmarshal(hdr["globals_sha256"], &sha) != nil || sha == "" {
		return body, nil
	}
	if !validSha256(sha) {
		return nil, fmt.Errorf("bad globals_sha256 %q", sha)
	}
	path := filepath.Join(globalsCacheDir(), sha)
	if hdrBool(hdr, "globals") {
		g, _, err := readFrame(rest)
		if err != nil {
			return nil, err
		}
		storeGlobals(path, sha, g)
		return body, nil
	}
	g, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Printf("[pool] reading cached globals %.12s: %v; asking the origin to resend", sha, err)
		}
		return nil, &missingGlobalsError{Sha: sha}
	}
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil { // LRU order for pruneGlobals
		log.Printf("[pool] touching cached globals %.12s: %v", sha, err)
	}
	hdr["globals"] = json.RawMessage("true")
	return joinFrames(hdr, g, rest), nil
}

func validSha256(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// globalsCacheDir sits beside the per-store chunk caches: globals are
// content-addressed, so one directory serves every store.
func globalsCacheDir() string {
	return filepath.Join(userdir.Cache(), "chunkcache", "globals")
}

// storeGlobals keeps a globals blob for later hash-only bodies. Failing to
// store costs one resend later, so it logs and carries on.
func storeGlobals(path, sha string, g []byte) {
	if _, err := os.Stat(path); err == nil {
		return
	}
	if sum := sha256.Sum256(g); hex.EncodeToString(sum[:]) != sha {
		log.Printf("[pool] globals frame does not match its globals_sha256 %.12s; not caching it", sha)
		return
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Printf("[pool] globals cache %s: %v", dir, err)
		return
	}
	f, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		log.Printf("[pool] globals cache %s: %v", dir, err)
		return
	}
	_, werr := f.Write(g)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		log.Printf("[pool] writing globals %.12s: %v %v", sha, werr, cerr)
		_ = os.Remove(f.Name())
		return
	}
	if err := os.Rename(f.Name(), path); err != nil {
		log.Printf("[pool] storing globals %.12s: %v", sha, err)
		_ = os.Remove(f.Name())
		return
	}
	pruneGlobals(dir, globalsCacheMax)
}

// pruneGlobals drops the least recently used blobs above max bytes, in the
// style of narCache.prune. A wrong eviction costs one resend.
func pruneGlobals(dir string, max int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("[pool] pruning globals cache %s: %v", dir, err)
		return
	}
	type item struct {
		path string
		size int64
		used time.Time
	}
	var items []item
	var total int64
	for _, e := range entries {
		if !validSha256(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // removed under us
		}
		items = append(items, item{filepath.Join(dir, e.Name()), info.Size(), info.ModTime()})
		total += info.Size()
	}
	sort.Slice(items, func(i, j int) bool { return items[i].used.Before(items[j].used) })
	for _, it := range items {
		if total <= max {
			return
		}
		if err := os.Remove(it.path); err != nil {
			log.Printf("[pool] pruning globals %s: %v", it.path, err)
			continue
		}
		total -= it.size
	}
}

// compressChoice is the Dask-style sample test (R9 a1): five 16 KiB samples
// at the fastest level give the ratio and the compressor speed C. Compression
// is kept only when the samples shrink to <= 0.8 and C (scaled for the level)
// is at least 1.5x the link speed B; the level follows B: under 5 MB/s
// better, under 60 MB/s default, else fastest. An unknown link gets fastest.
// trialed is the bytes compressed to decide (the cost on incompressible input).
func compressChoice(body []byte, bps float64, known bool) (lvl zstd.EncoderLevel, ok bool, trialed int) {
	if len(body) < compressMinBody {
		return 0, false, 0
	}
	enc := encoderFor(zstd.SpeedFastest)
	step := (len(body) - trialSampleSize) / (trialSamples - 1)
	var packed int
	dst := make([]byte, 0, trialSampleSize+64)
	start := time.Now()
	for i := 0; i < trialSamples; i++ {
		s := body[i*step : i*step+trialSampleSize]
		dst = enc.EncodeAll(s, dst[:0])
		packed += len(dst)
		trialed += len(s)
	}
	took := time.Since(start).Seconds()
	if float64(packed) > 0.8*float64(trialed) {
		return 0, false, trialed
	}
	if !known {
		return zstd.SpeedFastest, true, trialed
	}
	c := float64(trialed) / max(took, 1e-9)
	// Relative speed of each level against fastest (klauspost README:
	// 318 / 134 / 67 MB/s), so the measured C carries over.
	for _, l := range []struct {
		lvl   zstd.EncoderLevel
		below float64
		speed float64
	}{
		{zstd.SpeedBetterCompression, 5e6, 0.21},
		{zstd.SpeedDefault, 60e6, 0.42},
		{zstd.SpeedFastest, math.Inf(1), 1},
	} {
		if bps < l.below && c*l.speed >= 1.5*bps {
			return l.lvl, true, trialed
		}
	}
	return 0, false, trialed
}

var (
	encMu    sync.Mutex
	encoders = map[zstd.EncoderLevel]*zstd.Encoder{}
	decOnce  sync.Once
	decoder  *zstd.Decoder
)

// encoderFor returns a shared encoder; EncodeAll is safe for concurrent use.
func encoderFor(l zstd.EncoderLevel) *zstd.Encoder {
	encMu.Lock()
	defer encMu.Unlock()
	if e := encoders[l]; e != nil {
		return e
	}
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(l))
	if err != nil {
		panic("zstd encoder with static options: " + err.Error()) // only bad options fail
	}
	encoders[l] = e
	return e
}

// zstdDecoder is shared; DecodeAll is safe for concurrent use. The output is
// capped like an uncompressed body, so a small bomb cannot grow past it.
func zstdDecoder() *zstd.Decoder {
	decOnce.Do(func() {
		d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxPoolBody))
		if err != nil {
			panic("zstd decoder with static options: " + err.Error())
		}
		decoder = d
	})
	return decoder
}

// observeLink feeds one real transfer on the link to a peer into its EWMA:
// bytes that crossed and the time they took. U18 calls it through timedRead
// for replies, and for requests with (time to response headers - the peer's
// reported compute). Small transfers measure latency and are ignored.
func observeLink(peerKey string, n int64, d time.Duration) {
	if n < linkMinSample || d <= 0 {
		return
	}
	bps := float64(n) / d.Seconds()
	links.Lock()
	defer links.Unlock()
	l := linkFor(peerKey)
	if l.bps == 0 || time.Since(l.at) > linkFresh {
		l.bps = bps
	} else {
		l.bps = linkEWMA*bps + (1-linkEWMA)*l.bps
	}
	l.at = time.Now()
}

// timedRead reads a peer's reply body and records how fast it arrived.
func timedRead(peerKey string, r io.Reader) ([]byte, error) {
	start := time.Now()
	b, err := io.ReadAll(r)
	if err == nil {
		observeLink(peerKey, int64(len(b)), time.Since(start))
	}
	return b, err
}

// linkSpeed is the measured bytes/s on the link to a peer (EWMA of real
// transfers); ok=false when there is no recent sample.
func linkSpeed(peerKey string) (bytesPerSec float64, ok bool) {
	links.Lock()
	defer links.Unlock()
	l := links.m[peerKey]
	if l == nil || l.bps == 0 || time.Since(l.at) > linkFresh {
		return 0, false
	}
	return l.bps, true
}

// clusterLinkSpeed is the sum of every fresh per-peer speed: the aggregate
// the shim's cost models expect (numpy_ops divides it per peer). It is the
// additive link_bytes_per_sec field of /v1/pool/stats; ok=false omits it and
// the shim falls back to a small probe.
func clusterLinkSpeed() (bytesPerSec float64, ok bool) {
	links.Lock()
	defer links.Unlock()
	for _, l := range links.m {
		if l.bps > 0 && time.Since(l.at) <= linkFresh {
			bytesPerSec += l.bps
			ok = true
		}
	}
	return bytesPerSec, ok
}
