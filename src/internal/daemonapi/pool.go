package daemonapi

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/userdir"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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

// chunk is the unit of pool work threaded through dispatch. items are opaque
// raw pickle bytes (frames mode) or legacy base64 pickles (itemsB64); globals
// is a raw pickle frame that every worker unpickles into the func_src
// namespace before running (e.g. a fixed matmul operand shared by all items).
type chunk struct {
	pickledFunc, funcSrc, funcName, extraB64 string
	items                                    []json.RawMessage
	globals                                  []byte
	frames                                   bool
	starmap                                  bool
	itemsB64                                 bool
	noSplit                                  bool
	noFanout                                 bool
	force                                    bool
	originLocal                              bool
	prov                                     *provenance
	requiredMem                              int64
	cacheKeys                                []string
	itemKeys                                 []string
	chunkDir                                 string
}

// chunkDirFor returns the on-disk chunk cache dir for a store path. Chunks
// written here survive warm-worker respawns, so cache_keys still resolve after
// a worker dies. The dir is under the daemon state dir, not the (read-only)
// nix store.
// forceRotate spreads whole-chunk forced dispatches across peers: without
// it every sub-minSplit chunk would land on the first peer in spill order.
var forceRotate atomic.Int64

// isLoopback reports whether a request came from this machine, which is how
// the daemon tells a job's own shim apart from a peer forwarding work in.
func isLoopback(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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

type poolRequest struct {
	Func     string `json:"func"`                // base64 pickled callable
	FuncSrc  string `json:"func_src,omitempty"`  // Python source of the callable
	FuncName string `json:"func_name,omitempty"` // name func_src defines
	// ExtraB64 ships pickled globals merged into func_src's namespace (e.g. the
	// fixed right-hand operand of a block matmul).
	ExtraB64 string          `json:"extra_b64,omitempty"`
	Items    json.RawMessage `json:"items"`
	Starmap  bool            `json:"starmap"`
	// ItemsB64 marks Items as base64-pickled objects (numpy arrays etc.) rather
	// than plain JSON scalars, so block-partitioned numeric work can ship.
	ItemsB64 bool `json:"items_b64,omitempty"`
	// CacheKeys resolves items from the worker's persistent chunk cache (warm
	// worker only) instead of the payload: item i is the content hash under
	// which the chunk was previously stored. A miss fails the request; the
	// shim falls back to local work. One per item, empty for payload items.
	CacheKeys []string `json:"cache_keys,omitempty"`
	// ItemKeys gives each item an affinity key (content hash of the raw chunk,
	// or empty for payload items). In noSplit routing the key picks the peer
	// via a stable hash, so a chunk always lands on the same node regardless
	// of which batch/request carried it — cache_keys fetches for the same key
	// then resolve on that same node. Position-based routing (item i → peer
	// i%len(peers)) is used when no keys are present.
	ItemKeys []string `json:"item_keys,omitempty"`
	// NoFanout tells the receiving daemon to run the chunk locally instead of
	// splitting and forwarding it to peers. The origin splits exactly once;
	// every forwarded chunk is terminal (one-hop fan-out).
	NoFanout bool `json:"no_fanout,omitempty"`
	// NoSplit keeps every item as its own routed unit: item i goes to peer i
	// (items beyond the peer count are local), so the origin can pre-partition
	// work (hash-shuffle buckets) and have each part land on exactly one node.
	NoSplit bool `json:"no_split,omitempty"`
	// Force mirrors the shim's PIPEDPEER_DISTRIBUTE=force: the submitter wants
	// distribution demonstrated, so small chunks that would normally stay
	// local still fan out.
	Force bool `json:"force,omitempty"`
	// RequiredMemBytes is the submitter's estimate of the working set this
	// chunk needs; the daemon refuses with 503 when it cannot spare that much,
	// so an overloaded node never OOMs mid-chunk. The shim falls back locally.
	RequiredMemBytes int64 `json:"required_mem,omitempty"`
	// ItemsFrames marks a frames-mode body (see putFrame): the request body is
	// a small JSON header line + optional globals frame + ItemsFrames item
	// frames. The header above it must NOT contain "items" — bulk data lives
	// in the frames so it never touches base64 or a giant JSON parse.
	ItemsFrames int `json:"items_frames,omitempty"`
	// Globals says the body starts with a globals frame every worker unpickles
	// into the func_src namespace before running (fixed operands shared by all
	// items; only valid in frames mode).
	Globals bool `json:"globals,omitempty"`
}

// poolWorker is a warm, long-lived closure subprocess for one store path.
// The closure is imported once and kept up; requests stream over pipes so
// dispatch costs a pipe write instead of a closure spawn (seconds → ms).
type poolWorker struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *bufio.Scanner
	id     int64
	// mu serialises submits on this worker: stdin writes and stdout reads must
	// not interleave when several fan-out goroutines hit the same store.
	mu sync.Mutex
}

// poolManager owns one warm worker per store path and a cold fallback.
// A single worker per store is the D2 model: tasks are serialised at the node
// (the pickled payload is untrusted and the closure python is shared), so one
// process per store bounds both concurrency and process count.
type poolManager struct {
	mu      sync.Mutex
	workers map[string]*poolWorker // storePath → warm worker

	// peerFn returns healthy peer daemon endpoints (host:port) that share the
	// closure, ordered best-first for spill. Nil disables spill (single node).
	// submitter is the requesting job's origin endpoint ("host:port"); the
	// implementation sinks it to the tail so an idle orchestrator never
	// outranks real workers (weak-orchestrator: laptop stays flat).
	peerFn func(storePath, submitter string) []string

	// deadPeers are peers that failed this chunk; later parts skip them and
	// fall through to the next best candidate. Reset per runChunk so a peer
	// that recovers mid-run is tried again.
	deadMu    sync.Mutex
	deadPeers map[string]bool

	// rates is what every peer has actually managed, measured from the parts
	// already sent to it. Sizing shares from advertised core counts alone
	// ignores how loaded a peer is and how slow the link to it is, which are
	// usually the things that decide whether sending it work helps at all.
	rates *rateModel

	// coresFn reports a peer's advertised core count, used only as a prior
	// until that peer has been measured once.
	coresFn func(peer string) int

	stats poolStats
}

// peerCores is the prior for an unmeasured peer.
func (pm *poolManager) peerCores(peer string) int {
	if pm.coresFn == nil {
		return 0
	}
	return pm.coresFn(peer)
}

// provenance records where each part of a chunk actually ran, so the answer
// travels home with the results.
//
// Without it the shim can only say it handed work to a daemon, which is not
// the same claim: a daemon with no eligible peers runs the chunk itself, and
// counting that as distributed work is exactly the sort of comfortable
// half-truth that let a broken dispatch path look healthy. A receipt that
// cannot tell "ran on another machine" from "posted to a socket" is not
// evidence.
type provenance struct {
	mu    sync.Mutex
	parts []provPart
}

// provPart.Where is "local" for work this node ran, or "peer:<host:port>".
type provPart struct {
	Where string `json:"where"`
	Items int    `json:"items"`
	Ms    int64  `json:"ms"`
}

func (p *provenance) record(where string, items int, ms int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parts = append(p.parts, provPart{Where: where, Items: items, Ms: ms})
}

func (p *provenance) snapshot() []provPart {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]provPart(nil), p.parts...)
}

// poolStats is this node's tally of pool work, served at /v1/pool/stats.
//
// Until it existed there was no way to ask a node what it had actually done:
// a run that distributed everything and one that distributed nothing left the
// same job history, and the only trace of a chunk arriving was a log line, so
// anything wanting to check had to shell into the machine and grep. That is
// how a completely broken dispatch path stayed invisible for months, and why
// the chaos test could not tell a surviving cluster from an idle one.
type poolStats struct {
	mu             sync.Mutex
	ChunksReceived int64 `json:"chunks_received"`
	ChunksLocal    int64 `json:"chunks_from_local_shim"`
	ChunksPeer     int64 `json:"chunks_from_peers"`
	ItemsExecuted  int64 `json:"items_executed_here"`
	PartsToPeers   int64 `json:"parts_sent_to_peers"`
	PartsFailed    int64 `json:"parts_failed"`
}

func (p *poolStats) snapshot() map[string]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return map[string]int64{
		"chunks_received":        p.ChunksReceived,
		"chunks_from_local_shim": p.ChunksLocal,
		"chunks_from_peers":      p.ChunksPeer,
		"items_executed_here":    p.ItemsExecuted,
		"parts_sent_to_peers":    p.PartsToPeers,
		"parts_failed":           p.PartsFailed,
	}
}

func (p *poolStats) add(f func(*poolStats)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(p)
}

func newPoolManager() *poolManager {
	return &poolManager{
		workers:   make(map[string]*poolWorker),
		deadPeers: make(map[string]bool),
		rates:     newRateModel(),
	}
}

// releaseHeapAfterWork forces Go to return freed heap to the OS after heavy
// requests (chunk bodies and script output can be hundreds of MB), throttled
// so a burst of micro-chunks doesn't pay a full GC per request. Go keeps freed
// heap resident until the next GC cycle; without this the daemon's RSS would
// linger near the peak of the last big job and starve the other containers on
// a shared-RAM rig.
var lastHeapRelease atomic.Int64

func releaseHeapAfterWork() {
	const minInterval = 30 * time.Second
	now := time.Now().UnixNano()
	prev := lastHeapRelease.Load()
	if now-prev < int64(minInterval) {
		return
	}
	if lastHeapRelease.CompareAndSwap(prev, now) {
		runtime.GC()
		debug.FreeOSMemory()
	}
}

// SetPeerFn installs the function that returns peer daemon endpoints eligible
// to run chunks for a store path. Without it, all work stays local.
func (pm *poolManager) SetPeerFn(fn func(storePath, submitter string) []string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.peerFn = fn
}

// spillPeerCount returns how many healthy peers currently share the store
// path. It drives PIPEDPEER_NUM_SHARDS so the shim knows how many nodes can
// take work. Safe on a nil peerFn (returns 0).
func (pm *poolManager) spillPeerCount(storePath string) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.peerFn == nil {
		return 0
	}
	return len(pm.peerFn(storePath, ""))
}

// handlePoolMap executes a pickled function over a batch of items using the
// local closure, returning per-item results. It is the worker side of the
// sitecustomize cluster pool (see nixgen/shim.go). Each request is one chunk.
//
// It runs in a subprocess of <storePath>/bin/run so it executes in exactly the
// environment the user's script runs in — no SDK, no shared state. Requests to
// a store that already has a warm worker reuse it instead of re-spawning.
// handlePoolStats reports what pool work this node has actually done, so a
// test or a demo can check where the work went without shelling in to grep a
// log file.
func (s *Server) handlePoolStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.pool.stats.snapshot())
}

// maxPoolBody bounds a single /v1/pool/map request. The body is read whole
// into memory before anything looks at it, so without a ceiling a peer -
// or a bug in chunk sizing - can push this daemon into the OOM killer with
// one request, taking every other job on the node with it. The shim's own
// chunks are bounded by the 40%-of-free-RAM rule long before this; the
// ceiling is here for everything that is not the shim behaving.
const maxPoolBody = 2 << 30 // 2 GiB

// poolBodyLimit is the ceiling for this daemon, which is not the same number
// on every node. A fixed 2 GiB is most of a small worker's entire allowance -
// the container beside its host in the lab is capped at exactly that - so one
// request sized to the constant would consume everything the daemon is
// allowed before a single item had been looked at. Half the budget, so there
// is room left to do something with what arrives.
func (s *Server) poolBodyLimit() int64 {
	limit := int64(maxPoolBody)
	if b := cgroups.SelfBudget(); b.Total > 0 && b.Total/2 < limit {
		limit = b.Total / 2
	}
	return limit
}

func (s *Server) handlePoolMap(w http.ResponseWriter, r *http.Request) {
	limit := s.poolBodyLimit()
	rawBody, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
			"error": fmt.Sprintf("pool request body exceeds %d bytes", limit),
		})
		return
	}
	defer releaseHeapAfterWork()
	var req poolRequest
	// Frames bodies: the header is the first line and the frames follow it.
	// Legacy bodies are compact JSON with no literal newline, so the first
	// newline split is a safe discriminator.
	var items []json.RawMessage
	var globals []byte
	frames := false
	if nl := bytes.IndexByte(rawBody, '\n'); nl > 0 {
		var probe struct {
			ItemsFrames int  `json:"items_frames"`
			Globals     bool `json:"globals"`
		}
		if err := json.Unmarshal(rawBody[:nl], &probe); err == nil && probe.ItemsFrames > 0 {
			frames = true
			if err := json.Unmarshal(rawBody[:nl], &req); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
				return
			}
			var err error
			globals, items, err = parseFrames(rawBody[nl+1:], probe.Globals, probe.ItemsFrames)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad frames: " + err.Error()})
				return
			}
		}
	}
	if !frames {
		if err := json.NewDecoder(bytes.NewReader(rawBody)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
			return
		}
		if err := json.Unmarshal(req.Items, &items); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad items: " + err.Error()})
			return
		}
	}

	// The store path / run wrapper comes from the request environment. The
	// submitter embeds it so we need no job context.
	storePath := r.Header.Get("X-Pipedpeer-Store")
	// Where the requesting job was submitted from ("host:port"); sunk to the
	// tail of spill order so an idle orchestrator never outranks workers.
	submitter := r.Header.Get("X-Pipedpeer-Submitter")
	runPath := filepath.Join(storePath, "bin", "run")
	if storePath == "" || !pathExists(runPath) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-Pipedpeer-Store header (or invalid store path)"})
		return
	}

	if req.FuncSrc != "" && req.FuncName == "" {
		req.FuncName = "run" // mirror the runner's default: func_src without func_name
	}
	log.Printf("[pool] received pool/map from %s: items=%d b64=%v cache_keys=%d required_mem=%d forwarded=%q",
		r.RemoteAddr, len(items), req.ItemsB64, len(req.CacheKeys), req.RequiredMemBytes,
		r.Header.Get("X-Pipedpeer-Forwarded"))
	fromLocalShim := isLoopback(r.RemoteAddr) && r.Header.Get("X-Pipedpeer-Forwarded") == ""
	s.pool.stats.add(func(p *poolStats) {
		p.ChunksReceived++
		if fromLocalShim {
			p.ChunksLocal++
		} else {
			p.ChunksPeer++
		}
	})

	// Memory bounding for horizontal scaling on constrained nodes. The safe
	// working set is 40% of free RAM (max_safe_chunk_size); anything larger
	// runs as sequential micro-chunks so an 8-16GB node never OOMs mid-chunk.
	// When even one micro-chunk cannot fit (asymmetric orchestrator: weak
	// local, strong peers), the whole request is forwarded to the best peer —
	// the orchestrator executes 0% heavy compute. 503 only when no peer can
	// take it.
	if req.RequiredMemBytes > 0 {
		available := s.AvailableForJob()
		if available < req.RequiredMemBytes {
			if r.Header.Get("X-Pipedpeer-Forwarded") == "" {
				if peer := s.pool.bestPeer(storePath, submitter); peer != "" {
					log.Printf("[pool] forwarding %d items (required_mem=%d) to peer %s", len(items), req.RequiredMemBytes, peer)
					if s.forwardPoolMap(w, rawBody, peer, storePath) {
						return
					}
				}
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": fmt.Sprintf("insufficient memory: need %d bytes, available %d bytes", req.RequiredMemBytes, available),
			})
			return
		}
		safe := int64(float64(available) * 0.4)
		if safe > 0 && req.RequiredMemBytes > safe && len(items) > 1 {
			ch := &chunk{
				pickledFunc: req.Func, funcSrc: req.FuncSrc, funcName: req.FuncName,
				extraB64: req.ExtraB64, items: items, globals: globals, frames: frames,
				starmap: req.Starmap, itemsB64: req.ItemsB64, noSplit: req.NoSplit,
				noFanout: req.NoFanout, force: req.Force, requiredMem: req.RequiredMemBytes,
				cacheKeys: req.CacheKeys, itemKeys: req.ItemKeys,
				chunkDir: chunkDirFor(storePath),
			}
			s.runMicroChunks(w, runPath, storePath, ch, safe, submitter)
			return
		}
	}

	var results []any
	ch := &chunk{
		pickledFunc: req.Func, funcSrc: req.FuncSrc, funcName: req.FuncName,
		extraB64: req.ExtraB64, items: items, globals: globals, frames: frames,
		starmap: req.Starmap, itemsB64: req.ItemsB64, noSplit: req.NoSplit,
		noFanout: req.NoFanout, force: req.Force, requiredMem: req.RequiredMemBytes,
		cacheKeys: req.CacheKeys, itemKeys: req.ItemKeys,
		chunkDir:    chunkDirFor(storePath),
		originLocal: fromLocalShim,
		prov:        &provenance{},
	}
	if err := os.MkdirAll(ch.chunkDir, 0755); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	switch {
	case ch.noSplit:
		// Hash-shuffle routing: the origin pre-partitioned items into buckets
		// (one per node); each item must land on exactly one node, so bypass
		// the minSplit gate and route per item.
		results, err = s.pool.runChunk(runPath, storePath, ch, submitter)
	case req.NoFanout:
		// One-hop fan-out: the origin already split; this chunk is terminal.
		results, err = s.pool.runLocal(runPath, storePath, ch)
		if err == nil {
			ch.prov.record("local", len(ch.items), 0)
		}
	default:
		results, err = s.pool.runChunk(runPath, storePath, ch, submitter)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if frames {
		// Frames response: small header line + raw pickle result frames. The
		// receipt rides in the header so the submitter learns where the work
		// ran, not merely that it was accepted.
		recJSON, _ := json.Marshal(ch.prov.snapshot())
		hdr := fmt.Sprintf("{\"results_frames\": %d, \"receipt\": {\"node\": %q, \"parts\": %s}}\n",
			len(results), s.nodeID, recJSON)
		w.Header().Set("Content-Type", "application/vnd.pipedpeer.frames")
		w.WriteHeader(http.StatusOK)
		body := make([]byte, 0, 1024)
		body = append(body, hdr...)
		for _, r := range results {
			body = putFrame(body, resultPickle(r))
		}
		w.Write(body)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// resultPickle extracts the raw pickle bytes a result carries. Results are
// standardised on raw pickle bytes internally; only the wire edges base64.
func resultPickle(r any) []byte {
	m, ok := r.(map[string]string)
	if !ok {
		return nil
	}
	return []byte(m["pickle"])
}

// runMicroChunks splits a too-large request into sequential sub-batches sized
// to max_safe_chunk_size (40% of free RAM) and concatenates results in input
// order. Each sub-batch still fans out to peers; only the orchestrator's own
// memory share is bounded, and the worker's explicit GC keeps RSS flat across
// the pipeline.
func (s *Server) runMicroChunks(w http.ResponseWriter, runPath, storePath string, ch *chunk, safe int64, submitter string) {
	n := int((ch.requiredMem + safe - 1) / safe)
	if n > len(ch.items) {
		n = len(ch.items)
	}
	total := int64(len(ch.items))
	var results []any
	for i := 0; i < n; i++ {
		lo := total * int64(i) / int64(n)
		hi := total * int64(i+1) / int64(n)
		if lo == hi {
			continue
		}
		sub := *ch
		sub.items = ch.items[lo:hi]
		var keys []string
		if len(ch.cacheKeys) > 0 {
			keys = ch.cacheKeys[lo:hi]
		}
		sub.cacheKeys = keys
		var ikeys []string
		if len(ch.itemKeys) > 0 {
			ikeys = ch.itemKeys[lo:hi]
		}
		sub.itemKeys = ikeys
		var part []any
		var err error
		switch {
		case sub.noSplit:
			part, err = s.pool.runChunk(runPath, storePath, &sub, submitter)
		case sub.noFanout:
			part, err = s.pool.runLocal(runPath, storePath, &sub)
		default:
			part, err = s.pool.runChunk(runPath, storePath, &sub, submitter)
		}
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		results = append(results, part...)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// forwardPoolMap relays the raw request to a peer (one hop) so a weak
// orchestrator forwards heavy work instead of refusing it. Returns true when
// the peer answered; the status and body are copied verbatim.
func (s *Server) forwardPoolMap(w http.ResponseWriter, rawBody []byte, peer, storePath string) bool {
	fwd, err := http.NewRequest("POST", fmt.Sprintf("http://%s/v1/pool/map", peer), bytes.NewReader(rawBody))
	if err != nil {
		return false
	}
	fwd.Header.Set("Content-Type", "application/json")
	fwd.Header.Set("X-Pipedpeer-Store", storePath)
	fwd.Header.Set("X-Pipedpeer-Forwarded", "1")
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(fwd)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
	return true
}

// runChunk dispatches one chunk, preferring a warm worker and falling back to
// the cold per-request spawn when the warm worker dies or cannot start.
// When healthy peers share the store, items are split across local + peers and
// results are merged in input order. Peers are tried best-first; a peer that
// fails is skipped for the rest of the chunk and the next best takes its part.
// Local is always the last resort, so remote nodes add capacity but never
// remove it (D2: never slower).
//
// With noSplit, items are not re-split: each item is one part routed to its
// own preferred peer (part i prefers peers[i], wrapping past the peer count to
// local), so a pre-partitioned payload (hash-shuffle buckets) lands one item
// per node. Results are still returned in input order.
func (pm *poolManager) runChunk(runPath, storePath string, ch *chunk, submitter string) ([]any, error) {
	// Peel off peers before taking the local worker lock so each local submit
	// serialises only its own sub-chunk.
	var peers []string
	pm.mu.Lock()
	if pm.peerFn != nil {
		peers = pm.peerFn(storePath, submitter)
	}
	pm.mu.Unlock()

	pm.deadMu.Lock()
	pm.deadPeers = make(map[string]bool)
	pm.deadMu.Unlock()

	// One worker per participating node. Local is always included.
	type part struct {
		// c is the chunk with items narrowed to this part's share. Globals and
		// mode flags travel with it so every worker gets the full context.
		c *chunk
		// run is set only for the local part; remote parts POST instead.
		runPath string
		// peers is the candidate list for this part, its own primary peer
		// first (part i prefers peers[i]), the rest as fallback in ranked
		// order. The part tries each in order, skipping dead ones, before
		// going local.
		peers []string
	}
	var parts []part
	items := ch.items
	noSplit := ch.noSplit

	switch {
	case noSplit:
		// One part per item. Item i's peer is chosen by its affinity key when
		// one is present (stable hash over the key, so the same chunk always
		// lands on the same node and later cache_keys fetches resolve there),
		// otherwise positionally (peer i, rotating back through the list when
		// there are more items than peers). Items past the peer count fall
		// through to local via the empty runPath branch below.
		//
		// The affinity index must be computed over a STABLE peer order
		// (lexicographic), not the load-ranked order: the load ranking shifts
		// between the parse and the combine requests, so hash % len would land
		// the same key on different nodes and every cache_keys fetch would miss.
		// The load order is still used for fallback ranking after the stable
		// affinity target.
		stablePeers := append([]string{}, peers...)
		sort.Strings(stablePeers)
		for i := range items {
			sub := *ch
			sub.items = items[i : i+1]
			// Narrow the keys to this part's single item: the remote worker
			// resolves cache_keys from its own _CACHE, so broadcasting the full
			// list makes it demand chunks that live on other peers (miss).
			if len(sub.cacheKeys) > 0 {
				sub.cacheKeys = sub.cacheKeys[i : i+1]
			}
			if len(sub.itemKeys) > 0 {
				sub.itemKeys = sub.itemKeys[i : i+1]
			}
			p := part{c: &sub}
			if len(peers) > 0 {
				target := ""
				if key := affinityKey(ch, i); key != "" {
					target = stablePeers[int(affinityHash(key)%uint32(len(stablePeers)))]
				} else {
					target = peers[i%len(peers)]
				}
				for j, peer := range peers {
					if peer == target {
						ordered := append(append([]string{}, peers[j:]...), peers[:j]...)
						p.peers = ordered
						break
					}
				}
				if p.peers == nil {
					p.peers = []string{target}
				}
			} else {
				p.runPath = runPath
			}
			parts = append(parts, p)
		}
	case len(peers) == 0:
		parts = []part{{c: ch, runPath: runPath}}
	default:
		// Split evenly across local + peers. A small chunk stays local (splitting
		// would cost more than it saves); only fan out once there is real work.
		// force (PIPEDPEER_DISTRIBUTE=force) lowers the floor to a pair so a
		// demo-sized chunk still visibly fans out, and a chunk too small even
		// for that goes to a peer whole — rotated so successive one-task
		// dispatches (joblib) spread across the cluster instead of pinning
		// the first peer.
		minSplit := 8
		if ch.force {
			minSplit = 2
		}
		if len(items) < minSplit {
			// Too small to divide. Send it whole to a peer anyway when it came
			// from a shim on this machine: that shim already kept its own half
			// and handed us this one to place elsewhere, so running it here
			// competes with the pool that is mid-race, and the round trip
			// bought nothing. Rotated so successive small dispatches spread
			// instead of pinning the first peer.
			if ch.force || ch.originLocal {
				j := int(forceRotate.Add(1)) % len(peers)
				ordered := append(append([]string{}, peers[j:]...), peers[:j]...)
				parts = []part{{c: ch, peers: ordered}}
			} else {
				parts = []part{{c: ch, runPath: runPath}}
			}
		} else {
			// Keep a share for this node only when the work came from
			// somewhere else. A chunk posted by a shim on this machine is the
			// half of a race the local pool already declined to run: taking a
			// share of it here would put the daemon's own worker processes on
			// the same cores that pool is saturating, so the machine does the
			// work twice over, oversubscribed, and spilling measures slower
			// than never spilling at all. Every part still falls back to local
			// if its peers fail, so nothing is stranded.
			// Sizes follow measured speed, not headcount. An equal split
			// across a 20-core box and a 4-core box makes the fast one finish
			// and wait, so the chunk takes as long as the slowest
			// participant - which is how adding a machine used to make a job
			// slower. The scheduler equalises finish times instead, and
			// declines any peer that cannot start before the rest are done.
			shares := pm.planShares(items, peers, !ch.originLocal)
			idx := 0
			for _, sh := range shares {
				if sh.Items == 0 || idx >= len(items) {
					continue
				}
				end := min(idx+sh.Items, len(items))
				sub := *ch
				sub.items = items[idx:end]
				idx = end
				if sh.Device.ID == localDeviceID {
					parts = append(parts, part{c: &sub, runPath: runPath})
					continue
				}
				// Preferred peer first, then the rest as fallbacks: a part
				// whose peer dies still runs somewhere rather than failing.
				ordered := []string{sh.Device.ID}
				for _, p := range peers {
					if p != sh.Device.ID {
						ordered = append(ordered, p)
					}
				}
				parts = append(parts, part{c: &sub, peers: ordered})
			}
			// Anything rounding left over goes to the first part rather than
			// being dropped; losing items here is silent and corrupts results.
			if idx < len(items) && len(parts) > 0 {
				last := parts[len(parts)-1]
				extra := *last.c
				extra.items = append(append([]json.RawMessage{}, last.c.items...), items[idx:]...)
				parts[len(parts)-1].c = &extra
			}
		}
	}

	remote := 0
	for _, pt := range parts {
		if len(pt.peers) > 0 {
			remote++
		}
	}
	log.Printf("[pool] chunk: %d items -> %d part(s), %d remote, %d local", len(items), len(parts), remote, len(parts)-remote)
	pm.stats.add(func(p *poolStats) { p.PartsToPeers += int64(remote) })

	// Results are stored per part and concatenated in part order after the
	// wait, so the response is always in input order regardless of which part
	// finished first. The shim maps results back by index, so this order is
	// load-bearing.
	partResults := make([][]any, len(parts))
	errs := make([]error, len(parts))
	var wg sync.WaitGroup

	for i, p := range parts {
		wg.Add(1)
		go func(i int, p part) {
			defer wg.Done()
			var r []any
			var e error
			started := time.Now()
			if p.runPath != "" {
				r, e = pm.runLocal(runPath, storePath, p.c)
				if e == nil {
					d := time.Since(started)
					ch.prov.record("local", len(p.c.items), d.Milliseconds())
					pm.rates.observe(localDeviceID, len(p.c.items), d)
				}
			} else {
				// Walk the ranked peer list; a failure falls through to the
				// next best candidate. If every peer fails, the work is ours.
				for _, peer := range p.peers {
					if pm.peerDead(peer) {
						continue
					}
					r, e = pm.runRemote(peer, storePath, p.c)
					if e == nil {
						d := time.Since(started)
						ch.prov.record("peer:"+peer, len(p.c.items), d.Milliseconds())
						// Timed end to end, so the round trip and the closure
						// transfer are in the number rather than assumed away.
						pm.rates.observe(peer, len(p.c.items), d)
						break
					}
					pm.markPeerDead(peer)
				}
				if e != nil {
					// D2/D3 — a remote node adds capacity, never subtracts.
					r, e = pm.runLocal(runPath, storePath, p.c)
					if e == nil {
						// Deliberately not measured: this ran locally after a
						// peer failed, so the elapsed time includes the failed
						// attempt and would libel this node's own speed.
						ch.prov.record("local", len(p.c.items), time.Since(started).Milliseconds())
					}
				}
			}
			if e != nil {
				errs[i] = e
				pm.stats.add(func(p *poolStats) { p.PartsFailed++ })
				log.Printf("[pool] part %d failed: %v", i, e)
				return
			}
			partResults[i] = r
		}(i, p)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	results := make([]any, 0, len(items))
	for _, r := range partResults {
		results = append(results, r...)
	}
	return results, nil
}

// affinityKey returns the routing key for noSplit item i: the cache key when
// the request resolves cached chunks, else the item's own affinity key, else
// "" for positional routing.
func affinityKey(ch *chunk, i int) string {
	if len(ch.cacheKeys) > 0 && i < len(ch.cacheKeys) {
		return ch.cacheKeys[i]
	}
	if len(ch.itemKeys) > 0 && i < len(ch.itemKeys) {
		return ch.itemKeys[i]
	}
	return ""
}

// affinityHash is a stable FNV-1a hash (NOT Go's randomized maphash): the
// same key must route to the same peer across daemon restarts and requests.
func affinityHash(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// sinkPeerLast moves the submitting machine's endpoint to the tail of the
// spill order (soft exclusion): an idle orchestrator otherwise outranks busy
// workers and eats the compute --remote was supposed to keep off it. It stays
// eligible so it still helps when no worker can take a chunk.
func sinkPeerLast(peers []string, submitter string) []string {
	if submitter == "" {
		return peers
	}
	for i, p := range peers {
		if p == submitter && i < len(peers)-1 {
			out := append(peers[:i:i], peers[i+1:]...)
			return append(out, submitter)
		}
	}
	return peers
}

// bestPeer returns the top-ranked healthy peer sharing a store ("" when none),
// used to forward requests the local node cannot admit (asymmetric
// orchestrator: 0% heavy compute runs locally when peers exist).
func (pm *poolManager) bestPeer(storePath, submitter string) string {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.peerFn == nil {
		return ""
	}
	ps := pm.peerFn(storePath, submitter)
	for _, p := range ps {
		if p != submitter {
			return p
		}
	}
	if len(ps) > 0 {
		return ps[0] // only the submitter shares this store — D2 fallback
	}
	return ""
}

// peerDead reports whether a peer failed earlier in this chunk.
func (pm *poolManager) peerDead(peer string) bool {
	pm.deadMu.Lock()
	defer pm.deadMu.Unlock()
	return pm.deadPeers[peer]
}

// markPeerDead records that a peer failed so later parts skip it.
func (pm *poolManager) markPeerDead(peer string) {
	pm.deadMu.Lock()
	defer pm.deadMu.Unlock()
	pm.deadPeers[peer] = true
}

// runLocal dispatches a sub-chunk to this node's warm worker (or cold path).
func (pm *poolManager) runLocal(runPath, storePath string, ch *chunk) ([]any, error) {
	pm.stats.add(func(p *poolStats) { p.ItemsExecuted += int64(len(ch.items)) })
	pm.mu.Lock()
	worker, ok := pm.workers[storePath]
	if !ok || worker.dead() {
		if ok {
			log.Printf("[pool] warm worker for store %s was dead; respawning", storePath)
			pm.workers[storePath] = nil
		}
		log.Printf("[pool] spawning warm worker for store %s", storePath)
		w, err := pm.spawn(runPath, storePath)
		if err != nil {
			log.Printf("[pool] warm worker spawn failed: %v", err)
			// Cold fallback: one-off spawn keeps the node functional even when
			// persistent workers are unavailable.
			return runPoolChunk(runPath, ch)
		}
		worker = w
		pm.workers[storePath] = worker
	}
	pm.mu.Unlock()

	results, err := worker.submit(ch)
	if err != nil {
		// Worker died mid-flight — drop it and fall back to a cold run for
		// this chunk; the next request will re-warm.
		log.Printf("[pool] warm worker submit failed for store %s: %v", storePath, err)
		pm.mu.Lock()
		delete(pm.workers, storePath)
		pm.mu.Unlock()
		return runPoolChunk(runPath, ch)
	}
	return results, nil
}

// runRemote forwards a sub-chunk to a peer daemon's /v1/pool/map. The peer must
// have the same closure already (peerFn filters for that). Failures are fatal
// for the chunk: the caller returns them rather than silently dropping work.
func (pm *poolManager) runRemote(peer, storePath string, ch *chunk) ([]any, error) {
	// no_fanout: the origin splits exactly once; a peer must never re-split and
	// forward again (one-hop tree, results flow straight back to the origin).
	var body []byte
	if ch.frames {
		sub := *ch
		sub.noFanout = true
		hdr, _ := json.Marshal(map[string]any{
			"func_src": ch.funcSrc, "func_name": ch.funcName, "func": ch.pickledFunc,
			"starmap": ch.starmap, "items_frames": len(ch.items), "globals": ch.globals != nil,
			"required_mem": ch.requiredMem, "no_fanout": true,
			"cache_keys": ch.cacheKeys, "item_keys": ch.itemKeys, "chunk_dir": ch.chunkDir,
		})
		body = buildFrames(hdr, ch.globals, ch.items)
	} else {
		payload := map[string]any{"func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName, "extra_b64": ch.extraB64, "items": ch.items, "starmap": ch.starmap, "items_b64": ch.itemsB64, "no_fanout": true, "chunk_dir": ch.chunkDir}
		if len(ch.cacheKeys) > 0 {
			payload["cache_keys"] = ch.cacheKeys
		}
		if len(ch.itemKeys) > 0 {
			payload["item_keys"] = ch.itemKeys
		}
		body, _ = json.Marshal(payload)
	}
	url := fmt.Sprintf("http://%s/v1/pool/map", peer)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if ch.frames {
		req.Header.Set("Content-Type", "application/vnd.pipedpeer.frames")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Pipedpeer-Store", storePath)

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer %s: %v", peer, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)
	}
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if ch.frames {
		// Frames response: header line + result frames.
		nl := bytes.IndexByte(body, '\n')
		if nl < 0 {
			return nil, fmt.Errorf("peer %s: bad frames response", peer)
		}
		var out struct {
			ResultsFrames int `json:"results_frames"`
		}
		if err := json.Unmarshal(body[:nl], &out); err != nil || out.ResultsFrames <= 0 {
			return nil, fmt.Errorf("peer %s: bad frames response: %v", peer, err)
		}
		var frames [][]byte
		rest := body[nl+1:]
		for i := 0; i < out.ResultsFrames; i++ {
			var f []byte
			f, rest, err = readFrame(rest)
			if err != nil {
				return nil, fmt.Errorf("peer %s: %v", peer, err)
			}
			frames = append(frames, f)
		}
		results := make([]any, 0, len(frames))
		for _, f := range frames {
			results = append(results, map[string]string{"pickle": string(f)})
		}
		return results, nil
	}
	var out struct {
		Results []map[string]string `json:"results"`
	}
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&out); err != nil {
		return nil, err
	}
	results := make([]any, 0, len(out.Results))
	for _, r := range out.Results {
		if _, err := base64.StdEncoding.DecodeString(r["pickle"]); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, nil
}

// spawn starts a warm worker for a store path. bin/run warm_worker.py reads
// JSON-lines from stdin and writes results to stdout.
func (pm *poolManager) spawn(runPath, storePath string) (*poolWorker, error) {
	dir, err := userdir.Scratch("warm-*")
	if err != nil {
		return nil, err
	}
	workerScript := filepath.Join(dir, "warm_worker.py")
	if err := os.WriteFile(workerScript, []byte(warmWorkerScript), 0755); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	cmd := exec.Command(runPath, workerScript)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	w := &poolWorker{
		cmd:    cmd,
		stdin:  bufio.NewWriter(stdin),
		stdout: bufio.NewScanner(stdout),
	}
	w.stdout.Buffer(make([]byte, 0, 64<<10), 16<<20)
	return w, nil
}

func (w *poolWorker) dead() bool {
	return w == nil || w.cmd == nil || w.cmd.ProcessState != nil
}

// submit sends one task over the pipes and waits for its result file. The
// payload (pickled items can be ~100s of MB) is written to a temp file and the
// worker is told the path, so the full blob never passes through the pipe or
// lives in daemon RAM. Frames chunks ship as a header line + length-prefixed
// raw pickle frames; legacy chunks keep the base64 JSON encoding.
func (w *poolWorker) submit(ch *chunk) ([]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.id++

	// Holds the chunk payload: hundreds of megabytes is normal, and a
	// tmpfs would charge the job's own memory for it.
	dir, err := userdir.Scratch("warm-task-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	inPath := filepath.Join(dir, "in.bin")
	outPath := filepath.Join(dir, "out.bin")

	var body []byte
	if ch.frames {
		hdr, _ := json.Marshal(map[string]any{
			"id": w.id, "func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName,
			"starmap": ch.starmap, "items_frames": len(ch.items), "globals": ch.globals != nil,
			"required_mem": ch.requiredMem, "cache_keys": ch.cacheKeys, "item_keys": ch.itemKeys,
			"chunk_dir": ch.chunkDir,
		})
		body = buildFrames(hdr, ch.globals, ch.items)
	} else {
		req := map[string]any{"id": w.id, "func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName, "extra_b64": ch.extraB64, "items": ch.items, "starmap": ch.starmap, "items_b64": ch.itemsB64, "chunk_dir": ch.chunkDir}
		if len(ch.cacheKeys) > 0 {
			req["cache_keys"] = ch.cacheKeys
		}
		if len(ch.itemKeys) > 0 {
			req["item_keys"] = ch.itemKeys
		}
		body, err = json.Marshal(req)
		if err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(inPath, body, 0644); err != nil {
		return nil, err
	}

	msg := map[string]any{"id": w.id, "in_file": inPath, "out_file": outPath}
	line, _ := json.Marshal(msg)
	if _, err := w.stdin.WriteString(string(line) + "\n"); err != nil {
		return nil, err
	}
	if err := w.stdin.Flush(); err != nil {
		return nil, err
	}

	// Read until the ack matching our id arrives. Scan() returns false on EOF
	// (worker exited), which surfaces as an error and lets the caller fall back
	// to a cold run.
	for {
		if !w.stdout.Scan() {
			return nil, fmt.Errorf("warm worker closed: %v", w.stdout.Err())
		}
		var ack struct {
			ID    int64  `json:"id"`
			Done  bool   `json:"done"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(w.stdout.Bytes(), &ack); err != nil {
			continue
		}
		if ack.ID != w.id {
			continue // stale line from a concurrent write; ignore
		}
		if ack.Error != "" {
			return nil, fmt.Errorf("worker: %s", ack.Error)
		}
		if !ack.Done {
			return nil, fmt.Errorf("worker: no done ack")
		}
		outBytes, err := os.ReadFile(outPath)
		if err != nil {
			return nil, err
		}
		if ch.frames {
			nl := bytes.IndexByte(outBytes, '\n')
			if nl < 0 {
				return nil, fmt.Errorf("worker: bad frames output")
			}
			var out struct {
				ResultsFrames int `json:"results_frames"`
			}
			if err := json.Unmarshal(outBytes[:nl], &out); err != nil {
				return nil, err
			}
			rest := outBytes[nl+1:]
			results := make([]any, 0, out.ResultsFrames)
			for i := 0; i < out.ResultsFrames; i++ {
				var f []byte
				f, rest, err = readFrame(rest)
				if err != nil {
					return nil, err
				}
				results = append(results, map[string]string{"pickle": string(f)})
			}
			return results, nil
		}
		var out struct {
			Results []string `json:"results"`
		}
		if err := json.Unmarshal(outBytes, &out); err != nil {
			return nil, err
		}
		results := make([]any, 0, len(out.Results))
		for _, r := range out.Results {
			if _, err := base64.StdEncoding.DecodeString(r); err != nil {
				return nil, err
			}
			results = append(results, map[string]string{"pickle": r})
		}
		return results, nil
	}
}

// stopAll terminates every warm worker; used on server shutdown.
func (pm *poolManager) stopAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, w := range pm.workers {
		if w != nil && w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
	}
	pm.workers = map[string]*poolWorker{}
}

// runPoolChunk is the cold path: one closure spawn per chunk. Kept for the
// fallback when warm workers are unavailable.
func runPoolChunk(runPath string, ch *chunk) ([]any, error) {
	if ch.chunkDir != "" {
		if err := os.MkdirAll(ch.chunkDir, 0755); err != nil {
			return nil, err
		}
	}
	dir, err := userdir.Scratch("pool-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	runnerPath := filepath.Join(dir, "pool_runner.py")
	if err := os.WriteFile(runnerPath, []byte(poolRunner), 0755); err != nil {
		return nil, err
	}

	inPath := filepath.Join(dir, "in.bin")
	outPath := filepath.Join(dir, "out.bin")
	var body []byte
	if ch.frames {
		hdr, _ := json.Marshal(map[string]any{
			"func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName,
			"starmap": ch.starmap, "items_frames": len(ch.items), "globals": ch.globals != nil,
			"required_mem": ch.requiredMem, "cache_keys": ch.cacheKeys, "item_keys": ch.itemKeys,
			"chunk_dir": ch.chunkDir,
		})
		body = buildFrames(hdr, ch.globals, ch.items)
	} else {
		payload := map[string]any{"func": ch.pickledFunc, "func_src": ch.funcSrc, "func_name": ch.funcName, "extra_b64": ch.extraB64, "items": ch.items, "starmap": ch.starmap, "items_b64": ch.itemsB64, "chunk_dir": ch.chunkDir}
		if len(ch.cacheKeys) > 0 {
			payload["cache_keys"] = ch.cacheKeys
		}
		if len(ch.itemKeys) > 0 {
			payload["item_keys"] = ch.itemKeys
		}
		body, _ = json.Marshal(payload)
	}
	if err := os.WriteFile(inPath, body, 0644); err != nil {
		return nil, err
	}

	cmd := exec.Command(runPath, runnerPath, inPath, outPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("pool run failed: %v", err)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		return nil, err
	}
	if ch.frames {
		nl := bytes.IndexByte(outBytes, '\n')
		if nl < 0 {
			return nil, fmt.Errorf("pool run: bad frames output")
		}
		var out struct {
			ResultsFrames int `json:"results_frames"`
		}
		if err := json.Unmarshal(outBytes[:nl], &out); err != nil {
			return nil, err
		}
		rest := outBytes[nl+1:]
		results := make([]any, 0, out.ResultsFrames)
		for i := 0; i < out.ResultsFrames; i++ {
			var f []byte
			f, rest, err = readFrame(rest)
			if err != nil {
				return nil, err
			}
			results = append(results, map[string]string{"pickle": string(f)})
		}
		return results, nil
	}
	var out struct {
		Results []string `json:"results"` // base64-pickled results
	}
	if err := json.Unmarshal(outBytes, &out); err != nil {
		return nil, err
	}

	results := make([]any, 0, len(out.Results))
	for _, r := range out.Results {
		if _, err := base64.StdEncoding.DecodeString(r); err != nil {
			return nil, err
		}
		// Ship the pickled blob back base64-encoded; the shim unpickles it.
		results = append(results, map[string]string{"pickle": r})
	}
	return results, nil
}
