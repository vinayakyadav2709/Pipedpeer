package daemonapi

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"path/filepath"
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
        except Exception:
            pass
    return [_call_one(item) for item in items]
`

// poolRunner is the Python helper executed inside the closure to run a
// pickled function over a batch of items. It is written to a temp file and
// invoked via <storePath>/bin/run pool_runner.py <payload.bin> <out.bin>.
// It is the cold path: each invocation spawns a fresh closure process, which
// costs seconds. The warm worker (below) replaces it for the steady state.
// Accepts both the frames wire format (header line + length-prefixed raw
// pickle frames) and the legacy base64-in-JSON batch.
const poolRunner = `# Runs a pickled callable over a batch, for pipedpeer's cluster pool.
import base64, hashlib, json, os, pickle, struct, sys

` + chunkFanOut + `

with open(sys.argv[1], "rb") as f:
    data = f.read()

nl = data.find(b"\n")
req = json.loads(data if nl < 0 else data[:nl])
frames = bool(req.get("items_frames"))
if frames:
    rest = data[nl + 1:]
    def read_frame(rest):
        n = struct.unpack(">I", rest[:4])[0]
        return rest[4:4 + n], rest[4 + n:]
    if req.get("globals"):
        g, rest = read_frame(rest)
    else:
        g = b""
    raw = []
    for _ in range(req["items_frames"]):
        f, rest = read_frame(rest)
        raw.append(f)
    # items_zlib: the submitter found this payload compressible and said so,
    # rather than either side guessing. Dense float arrays are sent raw.
    if req.get("items_zlib"):
        import zlib
        raw = [zlib.decompress(r) for r in raw]
    items = [pickle.loads(r) for r in raw]
else:
    req = json.loads(data)
    g = b""
    items = req["items"]

ns = {"_CACHE": {}}
ns["_CHUNK_DIR"] = req.get("chunk_dir", "")
if g:
    ns.update(pickle.loads(g))
elif req.get("extra_b64"):
    ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
if req.get("func_src"):
    exec(req["func_src"], ns)
    func = ns[req.get("func_name", "run")]
else:
    func = pickle.loads(base64.b64decode(req["func"]))
starmap = req.get("starmap", False)
if not frames and req.get("items_b64"):
    items = [pickle.loads(base64.b64decode(i)) for i in items]

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

results = run_items(func, items, starmap, req)

if frames:
    out = b"".join(struct.pack(">I", len(p)) + p for p in [pickle.dumps(r) for r in results])
    with open(sys.argv[2], "wb") as f:
        f.write(b'{"results_frames": %d}\n' % len(results) + out)
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
import base64, gc, hashlib, json, os, pickle, struct, sys, traceback

` + chunkFanOut + `

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
    with open(path, "rb") as f:
        data = f.read()
    nl = data.find(b"\n")
    req = json.loads(data if nl < 0 else data[:nl])
    frames = bool(req.get("items_frames"))
    if not frames:
        req = json.loads(data)
        # Legacy requests carry globals as extra_b64 (base64 pickle); return an
        # empty bytes g so the caller's extra_b64 branch decodes them. Passing
        # extra_b64 here would make pickle.loads(g) choke on a str.
        return req, frames, req["items"], b""
    rest = data[nl + 1:]
    def read_frame(rest):
        n = struct.unpack(">I", rest[:4])[0]
        return rest[4:4 + n], rest[4 + n:]
    g = b""
    if req.get("globals"):
        g, rest = read_frame(rest)
    raw = []
    for _ in range(req["items_frames"]):
        f, rest = read_frame(rest)
        raw.append(f)
    # items_zlib: the submitter found this payload compressible and said so,
    # rather than either side guessing. Dense float arrays are sent raw.
    if req.get("items_zlib"):
        import zlib
        raw = [zlib.decompress(r) for r in raw]
    return req, frames, [pickle.loads(r) for r in raw], g

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
        req, frames, items, g = load_req(msg["in_file"])
        _CHUNK_DIR = req.get("chunk_dir", "")
        ns = {}
        if g:
            ns.update(pickle.loads(g))
        elif req.get("extra_b64"):
            ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
        ns["_CACHE"] = _CACHE
        ns["_CHUNK_DIR"] = _CHUNK_DIR
        if req.get("func_src"):
            exec(req["func_src"], ns)
            func = ns[req.get("func_name", "run")]
        else:
            func = pickle.loads(base64.b64decode(req["func"]))
        starmap = req.get("starmap", False)
        if not frames and req.get("items_b64"):
            items = [pickle.loads(base64.b64decode(i)) for i in items]
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
        results = run_items(func, items, starmap, req)
        if frames:
            out = b"".join(struct.pack(">I", len(p)) + p for p in [pickle.dumps(r) for r in results])
            with open(msg["out_file"], "wb") as f:
                f.write(b'{"results_frames": %d}\n' % len(results) + out)
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
    for _n in ("req", "items", "results"):
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

// --- Contracts for the transfer work (U27 implements; U18 calls from pool.go).
// Stubs keep today's behaviour until U27 lands.

// encodePeerBody prepares a frames body for the link to one peer: U27 makes it
// zstd-compress by measured link speed and drop a globals frame the peer
// already holds. Stub: unchanged body, no Content-Encoding.
func encodePeerBody(peerKey string, body []byte) (out []byte, contentEncoding string) {
	return body, ""
}

// decodePoolBody undoes encodePeerBody on the receiving daemon. Stub: as is.
func decodePoolBody(contentEncoding string, raw []byte) ([]byte, error) {
	return raw, nil
}

// forgetPeerGlobals is called when a peer answers that it lacks a globals
// blob it was assumed to hold, before the one resend. Stub: nothing cached.
func forgetPeerGlobals(peerKey, sha string) {}

// linkSpeed is the measured bytes/s on the link to a peer (passive EWMA of
// real transfers, U27). Stub: unknown.
func linkSpeed(peerKey string) (bytesPerSec float64, ok bool) {
	return 0, false
}
