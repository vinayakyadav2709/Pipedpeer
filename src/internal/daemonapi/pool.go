package daemonapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/runrec"
)

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
	itemsZlib                                bool
	noSplit                                  bool
	noFanout                                 bool
	force                                    bool
	originLocal                              bool
	prov                                     *provenance
	requiredMem                              int64
	cacheKeys                                []string
	itemKeys                                 []string
	chunkDir                                 string
	// offload is the machine key `run --offload` keeps distributed work off
	// (X-Pipedpeer-Offload; "" when the run did not ask). See offloadPeers.
	offload string
	// fnFP fingerprints the function and its environment (shim header
	// "fn_fp"): the key, with the node id, of the speed stats kept across runs.
	fnFP string
	// python is the requesting shim's interpreter "major.minor"
	// (X-Pipedpeer-Python); forwarded so a phone can refuse bytecode it
	// cannot load.
	python string
	// modules are the top-level modules the chunk needs (phone.Modules), sent
	// with every runnable check so a phone without torch is not picked.
	modules []string
	// oob mirrors poolRequest.Oob.
	oob bool
	// forwardedBy is the node id that forwarded this request whole
	// (X-Pipedpeer-Forwarded; "1" from older builds). That node could not
	// admit the work, so no part of it goes back there: a chunk is forwarded
	// at most once.
	forwardedBy string
	// ctx is the request this chunk serves. It ends when the requester goes
	// away (its job was killed, or the origin dropped a forward), and that
	// kills the local workers and drops the peer forwards started for it.
	ctx context.Context
}

// context returns the chunk's request context; Background when the chunk was
// built outside a request (tests, direct calls).
func (c *chunk) context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// killProcessGroup SIGKILLs the process group a worker leads (it was started
// with Setpgid), so whatever the user's function forked dies with it. Falls
// back to the process alone when the group is already gone.
func killProcessGroup(p *os.Process) error {
	if p == nil {
		return nil
	}
	if err := syscall.Kill(-p.Pid, syscall.SIGKILL); err != nil {
		return p.Kill()
	}
	return nil
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
	// ItemsZlib marks frame items as zlib-compressed by the shim. The worker
	// decompresses them, so every header the daemon rebuilds for a part has
	// to carry it on; dropped, the worker unpickles compressed bytes and
	// fails with "invalid load key, 'x'".
	ItemsZlib bool `json:"items_zlib,omitempty"`
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
	// FnFP is the shim's function fingerprint (C22), the stats key.
	FnFP string `json:"fn_fp,omitempty"`
	// Oob: the submitter reads out-of-band (pickle-5) result frames; carried
	// on every header the daemon rebuilds for a part.
	Oob bool `json:"oob,omitempty"`
}

// poolWorker is a warm, long-lived closure subprocess for one store path.
// The closure is imported once and kept up; requests stream over pipes so
// dispatch costs a pipe write instead of a closure spawn (seconds → ms).
type poolWorker struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *bufio.Scanner
	id     int64
	// killed is set once the worker was stopped (its request was cancelled,
	// or the server shut down); a killed worker is never reused.
	killed atomic.Bool
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
	// outranks real workers (weak-orchestrator: laptop stays flat). need is
	// the header each peer's runnable check sees (phone.Need; nil = none).
	peerFn func(storePath, submitter string, need http.Header) []string

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

	// machineFn reports a peer's machine key ("" when unknown), so --offload
	// can drop a machine's peers by key rather than by address.
	machineFn func(peer string) string

	// nodeFn maps a peer address to its node id (identity key fingerprint),
	// the key of every per-node measurement: an internet peer's address is a
	// forwarder port that changes. Nil or "" falls back to the address.
	nodeFn func(peer string) string
	// selfID is this node's id, for its own entries and receipt steps.
	selfID string
	// priorFn / observeFn reach the cross-run stats (poolstats.go, C22),
	// keyed by node id and function fingerprint. Nil in bare tests.
	priorFn   func(node, fn string) (itemsPerSec, overheadSec float64, ok bool)
	observeFn func(node, fn string, items int, computeSec, elapsedSec float64)
	bindOnce  sync.Once

	// book is what every node holds right now: the per-node depth gate and
	// the backlog the split counts.
	book flightBook
	// warmMu/warm remember which (node, fn) pairs have run a part: the first
	// carries closure fetch and worker spawn, so it is not a speed sample.
	warmMu sync.Mutex
	warm   map[string]bool
	// replyIDs: peer address -> the node id its last reply named.
	replyIDs map[string]string
	// backups counts tail backups running now; at most one at a time.
	backups atomic.Int32

	stats poolStats
}

// key is the per-node measurement key for a peer address: its node id from
// the node table, else the id the peer named in its last reply, else the
// address.
func (pm *poolManager) key(peer string) string {
	if pm.nodeFn != nil {
		if id := pm.nodeFn(peer); id != "" {
			return id
		}
	}
	pm.warmMu.Lock()
	defer pm.warmMu.Unlock()
	if id := pm.replyIDs[peer]; id != "" {
		return id
	}
	return peer
}

// sawNode remembers the node id a peer named in its reply's receipt.
func (pm *poolManager) sawNode(peer, id string) {
	if id == "" {
		return
	}
	pm.warmMu.Lock()
	defer pm.warmMu.Unlock()
	if pm.replyIDs == nil {
		pm.replyIDs = map[string]string{}
	}
	pm.replyIDs[peer] = id
}

// firstPart reports whether this is the first part (node, fn) has run since
// the daemon started, and marks it seen.
func (pm *poolManager) firstPart(node, fn string) bool {
	pm.warmMu.Lock()
	defer pm.warmMu.Unlock()
	k := node + "\x00" + fn
	if pm.warm[k] {
		return false
	}
	pm.warm[k] = true
	return true
}

// bindPool connects the pool manager to this server's node table and stats
// store, once, on the first pool request. Done here rather than in New so
// the pool files stay the only ones that know about it.
func (s *Server) bindPool() {
	s.pool.bindOnce.Do(func() {
		s.pool.selfID = s.nodeID
		s.pool.priorFn = s.poolPrior
		s.pool.observeFn = s.poolObserve
		s.pool.nodeFn = func(peer string) string {
			s.peersMu.RLock()
			defer s.peersMu.RUnlock()
			for addr, ph := range s.peerHealths {
				if addr == peer || fmt.Sprintf("%s:%d", ph.Host, ph.Port) == peer {
					return ph.NodeID
				}
			}
			return ""
		}
	})
}

// selfMachine is this daemon's machine key; a variable so tests can move it.
var selfMachine = heartbeat.Machine

// offloadPeers applies `run --offload`: it drops every peer on the machine
// whose key is machine, and reports whether this node runs on that machine
// itself, in which case it takes no share either. The machine is matched by
// key, never by address - an internet peer is 127.0.0.1:<forwarder port>. A
// peer that reports no key is kept: unknown is not the same machine.
func (pm *poolManager) offloadPeers(peers []string, machine string) ([]string, bool) {
	if machine == "" {
		return peers, false
	}
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if pm.machineFn != nil && heartbeat.SameMachine(pm.machineFn(p), machine) {
			continue
		}
		out = append(out, p)
	}
	return out, heartbeat.SameMachine(selfMachine(), machine)
}

// dropForwarder removes the node that forwarded this chunk (by node id) from
// its peers: it could not admit the work, so sending it parts would only
// bounce them. "" or an older build's "1" matches no node and drops nothing.
func (pm *poolManager) dropForwarder(peers []string, nodeID string) []string {
	if nodeID == "" {
		return peers
	}
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if pm.key(p) != nodeID {
			out = append(out, p)
		}
	}
	return out
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
// Steps are the daemon's own timed steps for the part (queue, compute), for
// the run record (C24); additive.
type provPart struct {
	Where string        `json:"where"`
	Items int           `json:"items"`
	Ms    int64         `json:"ms"`
	Steps []runrec.Step `json:"steps,omitempty"`
}

func (p *provenance) record(where string, items int, ms int64, steps ...runrec.Step) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parts = append(p.parts, provPart{Where: where, Items: items, Ms: ms, Steps: steps})
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
	// ChunksCancelled counts requests whose requester went away mid-work.
	ChunksCancelled int64 `json:"chunks_cancelled"`
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
		"chunks_cancelled":       p.ChunksCancelled,
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
		warm:      make(map[string]bool),
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
func (pm *poolManager) SetPeerFn(fn func(storePath, submitter string, need http.Header) []string) {
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
	return len(pm.peerFn(storePath, "", nil))
}
