package daemonapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pipedpeer/pipedpeer/internal/cgroups"
	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
	"github.com/pipedpeer/pipedpeer/internal/phone"
	"github.com/pipedpeer/pipedpeer/internal/runrec"
	"github.com/pipedpeer/pipedpeer/internal/schedule"
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
	"syscall"
	"time"
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
//
// With ?store=<path> it also answers how the shim should pace a map on that
// closure (additive, all integers): inflight = the sum over peers of each
// node's depth (the shim keeps that many chunks in flight, not a global 4),
// part_ms = the target length of one part, cores = the peers' cores (a chunk
// must fill them). link_bytes_per_sec (always, when measured) is the sum of
// the fresh per-peer link speeds (U27's cost models divide it per peer).
func (s *Server) handlePoolStats(w http.ResponseWriter, r *http.Request) {
	s.bindPool()
	out := map[string]int64{}
	for k, v := range s.pool.stats.snapshot() {
		out[k] = v
	}
	if bw, ok := clusterLinkSpeed(); ok {
		out["link_bytes_per_sec"] = int64(bw)
	}
	if store := r.URL.Query().Get("store"); store != "" {
		for k, v := range s.pool.pacing(store, r.Header.Get("X-Pipedpeer-Submitter")) {
			out[k] = v
		}
	}
	writeJSON(w, http.StatusOK, out)
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

// poolCancelled reports whether the requester of a pool request went away: the
// shim's job was killed, or the origin dropped the forward it sent here. The
// workers and forwards tied to the request have already been stopped through
// its context by then; this logs it once and counts it.
func (s *Server) poolCancelled(r *http.Request, items int, started time.Time) bool {
	if r.Context().Err() == nil {
		return false
	}
	log.Printf("[pool] cancelled: %d items from %s after %.1fs; requester went away, local workers stopped, peer forwards dropped",
		items, r.RemoteAddr, time.Since(started).Seconds())
	s.pool.stats.add(func(p *poolStats) { p.ChunksCancelled++ })
	return true
}

func (s *Server) handlePoolMap(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	limit := s.poolBodyLimit()
	rawBody, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
			"error": fmt.Sprintf("pool request body exceeds %d bytes", limit),
		})
		return
	}
	defer releaseHeapAfterWork()
	s.bindPool()
	// A peer may have compressed the body for the link (C21).
	if rawBody, err = decodePoolBody(r.Header.Get("Content-Encoding"), rawBody); err != nil {
		code, v := poolDecodeStatus(err)
		writeJSON(w, code, v)
		return
	}
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
	offload := r.Header.Get("X-Pipedpeer-Offload")
	python := r.Header.Get("X-Pipedpeer-Python")
	if storePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-Pipedpeer-Store header"})
		return
	}
	// <store>/bin/run, or a pool-only phone's host python; a by-value
	// function from another python minor is refused (Decisions: func_src rule).
	runPath, err := phone.PoolRunPath(storePath)
	modules := chunkModules(&req, globals, items, frames)
	if err == nil && !phone.CanRun(storePath, phone.Need(python, req.FuncSrc != "", modules...)) {
		err = fmt.Errorf("this node's python cannot run the chunk (python %q, modules %v; see the daemon log)", python, modules)
	}
	if err != nil {
		log.Printf("[pool] refusing pool/map for %s: %v", storePath, err)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cannot run this chunk here: " + err.Error()})
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
	//
	// Forwarding happens at most once: a request that was itself forwarded,
	// or is a part (no_fanout) another node already split, is refused here
	// instead, and its sender falls back to its next peer or runs it.
	forwardedBy := r.Header.Get("X-Pipedpeer-Forwarded")
	if req.RequiredMemBytes > 0 {
		available := s.AvailableForJob()
		if fromLocalShim {
			// The job asking is running here under its own lease; its pool
			// work is that job's memory, not a new promise on top of it.
			available = s.availableForOwnJob()
		}
		if available < req.RequiredMemBytes {
			if forwardedBy == "" && !req.NoFanout {
				if peer := s.pool.bestPeer(storePath, submitter, offload, phone.Need(python, req.FuncSrc != "", modules...)); peer != "" {
					log.Printf("[pool] forwarding %d items (required_mem=%d) to peer %s", len(items), req.RequiredMemBytes, peer)
					if s.forwardPoolMap(r.Context(), w, rawBody, peer, storePath, offload, python) {
						return
					}
					if s.poolCancelled(r, len(items), started) {
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
				starmap: req.Starmap, itemsB64: req.ItemsB64, itemsZlib: req.ItemsZlib, noSplit: req.NoSplit,
				noFanout: req.NoFanout, force: req.Force, requiredMem: req.RequiredMemBytes,
				cacheKeys: req.CacheKeys, itemKeys: req.ItemKeys,
				chunkDir:    chunkDirFor(storePath),
				offload:     offload,
				fnFP:        req.FnFP,
				python:      python,
				modules:     modules,
				oob:         req.Oob,
				forwardedBy: forwardedBy,
				// originLocal stays unset: setting it here would change
				// placement, which is out of this path's remit.
				prov: &provenance{},
				ctx:  r.Context(),
			}
			results, err := s.runMicroChunks(runPath, storePath, ch, safe, submitter)
			if err != nil {
				if s.poolCancelled(r, len(items), started) {
					return
				}
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			s.writePoolReply(w, ch.frames, results, ch.prov)
			return
		}
	}

	var results []any
	ch := &chunk{
		pickledFunc: req.Func, funcSrc: req.FuncSrc, funcName: req.FuncName,
		extraB64: req.ExtraB64, items: items, globals: globals, frames: frames,
		starmap: req.Starmap, itemsB64: req.ItemsB64, itemsZlib: req.ItemsZlib, noSplit: req.NoSplit,
		noFanout: req.NoFanout, force: req.Force, requiredMem: req.RequiredMemBytes,
		cacheKeys: req.CacheKeys, itemKeys: req.ItemKeys,
		chunkDir:    chunkDirFor(storePath),
		offload:     offload,
		fnFP:        req.FnFP,
		python:      python,
		modules:     modules,
		oob:         req.Oob,
		originLocal: fromLocalShim,
		forwardedBy: forwardedBy,
		prov:        &provenance{},
		ctx:         r.Context(),
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
		// Timed, so the origin learns the compute time apart from the trip.
		t0 := time.Now()
		results, err = s.pool.runLocal(runPath, storePath, ch)
		if err == nil {
			d := time.Since(t0)
			ch.prov.record("local", len(ch.items), d.Milliseconds(),
				s.pool.step(s.pool.selfID, "compute", t0, d, 0))
		}
	default:
		results, err = s.pool.runChunk(runPath, storePath, ch, submitter)
	}
	if err != nil {
		if s.poolCancelled(r, len(items), started) {
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.writePoolReply(w, frames, results, ch.prov)
}

// writePoolReply answers a pool request in the shape it was asked in: frames
// for a frames request (the shim reads only results_frames and the frames that
// follow, so a JSON reply to it would look like zero results), JSON otherwise.
func (s *Server) writePoolReply(w http.ResponseWriter, frames bool, results []any, prov *provenance) {
	if !frames {
		writeJSON(w, http.StatusOK, map[string]any{"results": results})
		return
	}
	// Frames response: small header line + raw pickle result frames. The
	// receipt rides in the header so the submitter learns where the work
	// ran, not merely that it was accepted.
	w.Header().Set("Content-Type", framesContentType)
	w.WriteHeader(http.StatusOK)
	body := make([]byte, 0, 1024)
	body = append(body, poolFramesHeader(len(results), s.nodeID, prov.snapshot())...)
	for _, r := range results {
		body = putFrame(body, resultPickle(r))
	}
	w.Write(body)
}

// runMicroChunks splits a too-large request into sequential sub-batches sized
// to max_safe_chunk_size (40% of free RAM) and concatenates results in input
// order. Each sub-batch still fans out to peers; only the orchestrator's own
// memory share is bounded, and the worker's explicit GC keeps RSS flat across
// the pipeline.
func (s *Server) runMicroChunks(runPath, storePath string, ch *chunk, safe int64, submitter string) ([]any, error) {
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
		if err := ch.context().Err(); err != nil {
			return nil, err
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
			if err == nil {
				ch.prov.record("local", len(sub.items), 0)
			}
		default:
			part, err = s.pool.runChunk(runPath, storePath, &sub, submitter)
		}
		if err != nil {
			return nil, err
		}
		results = append(results, part...)
	}
	return results, nil
}

// forwardPoolMap relays the raw request to a peer (one hop) so a weak
// orchestrator forwards heavy work instead of refusing it. Returns true when
// the peer answered. The status and body are copied, except that a frames
// receipt is relabelled so the work the peer ran itself reads as "peer:<peer>":
// to the submitter it ran on another machine, not on this one.
//
// ctx is the incoming request's: when its requester goes away the forward is
// dropped, and the peer stops the work through its own request context.
func (s *Server) forwardPoolMap(ctx context.Context, w http.ResponseWriter, rawBody []byte, peer, storePath, offload, python string) bool {
	hdr := http.Header{}
	hdr.Set("Content-Type", "application/json")
	hdr.Set("X-Pipedpeer-Store", storePath)
	// Our node id, so the peer keeps every part of it off this node; any
	// non-empty value reads as forwarded (older builds send "1").
	fwd := s.nodeID
	if fwd == "" {
		fwd = "1"
	}
	hdr.Set("X-Pipedpeer-Forwarded", fwd)
	if python != "" {
		hdr.Set("X-Pipedpeer-Python", python)
	}
	if offload != "" {
		// The peer may split it again; it must keep off that machine too.
		hdr.Set("X-Pipedpeer-Offload", offload)
	}
	status, ct, body, err := s.pool.postPeer(ctx, peer, rawBody, hdr, 5*time.Minute)
	if err != nil {
		if ctx.Err() == nil { // a cancel is logged once by the caller
			log.Printf("[pool] forward to %s failed: %v", peer, err)
		}
		return false
	}
	if status == http.StatusOK {
		body = relabelForwardedReceipt(body, peer)
	}
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(status)
	w.Write(body)
	return true
}

// postPeer POSTs a pool body to a peer's /v1/pool/map through the link
// encoding (C21: compression, globals the peer already holds) and returns the
// reply. A peer that answers 409 {"missing_globals": sha} lacked a globals
// blob it was assumed to hold: that is forgotten and the body resent once.
func (pm *poolManager) postPeer(ctx context.Context, peer string, raw []byte, hdr http.Header, timeout time.Duration) (status int, contentType string, body []byte, err error) {
	key := pm.key(peer)
	client := &http.Client{Timeout: timeout}
	for attempt := 0; ; attempt++ {
		enc, ce := encodePeerBody(key, raw)
		req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s/v1/pool/map", peer), bytes.NewReader(enc))
		if err != nil {
			return 0, "", nil, err
		}
		req.Header = hdr.Clone()
		if ce != "" {
			req.Header.Set("Content-Encoding", ce)
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, "", nil, err
		}
		body, err = timedRead(key, resp.Body)
		resp.Body.Close()
		if err != nil {
			return 0, "", nil, fmt.Errorf("reading reply: %w", err)
		}
		if sha, miss := missingGlobalsIn(resp.StatusCode, body); miss && attempt == 0 {
			forgetPeerGlobals(key, sha) // logs the resend
			continue
		}
		return resp.StatusCode, resp.Header.Get("Content-Type"), body, nil
	}
}

// relabelForwardedReceipt rewrites a frames reply's receipt so every part the
// peer ran itself ("local") reads "peer:<peer>". The node and the frames are
// kept. A JSON reply carries no receipt and passes through untouched.
func relabelForwardedReceipt(body []byte, peer string) []byte {
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		return body
	}
	var hdr struct {
		ResultsFrames *int `json:"results_frames"`
		Receipt       struct {
			Node  string     `json:"node"`
			Parts []provPart `json:"parts"`
		} `json:"receipt"`
	}
	if err := json.Unmarshal(body[:nl], &hdr); err != nil {
		log.Printf("[pool] forwarded reply from %s has an unreadable first line; receipt left as sent: %v", peer, err)
		return body
	}
	if hdr.ResultsFrames == nil {
		return body // JSON reply
	}
	for i := range hdr.Receipt.Parts {
		if hdr.Receipt.Parts[i].Where == "local" {
			hdr.Receipt.Parts[i].Where = "peer:" + peer
		}
	}
	out := poolFramesHeader(*hdr.ResultsFrames, hdr.Receipt.Node, hdr.Receipt.Parts)
	return append(out, body[nl+1:]...)
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
	ctx := ch.context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Peel off peers before taking the local worker lock so each local submit
	// serialises only its own sub-chunk.
	var peers []string
	pm.mu.Lock()
	if pm.peerFn != nil {
		peers = pm.peerFn(storePath, submitter, phone.Need(ch.python, ch.funcSrc != "", ch.modules...))
	}
	pm.mu.Unlock()
	// --offload: none of the parts below may land on the kept-off machine,
	// and when that is this node, it takes no share of its own.
	peers, offHere := pm.offloadPeers(peers, ch.offload)
	peers = pm.dropForwarder(peers, ch.forwardedBy)
	if offHere && len(peers) == 0 {
		log.Printf("[pool] --offload: no other machine shares this closure; running %d items on this machine", len(ch.items))
	}

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
			if ch.force || ch.originLocal || offHere {
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
			shares := pm.planShares(ch, peers, !ch.originLocal && !offHere)
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
			// The plan can refuse every peer; under --offload there is then no
			// local share to carry the items, so they go to a peer whole.
			if len(parts) == 0 && offHere {
				parts = []part{{c: ch, peers: peers}}
				idx = len(items)
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
					ch.prov.record("local", len(p.c.items), d.Milliseconds(),
						pm.step(pm.selfID, "compute", started, d, i))
					pm.rates.observe(localDeviceID, len(p.c.items), d)
					pm.observeStats(pm.selfID, ch.fnFP, len(p.c.items), d.Seconds(), d.Seconds())
				}
			} else {
				// Walk the ranked peer list; a failure falls through to the
				// next best candidate. If every peer fails, the work is ours.
				//
				// One time budget for the whole walk. A peer that runs out of
				// it is slow, not broken: its copy keeps running, a second copy
				// starts here, and the first to finish wins. Handing the part
				// to the next peer would only start it again from scratch (a
				// one-item svd part tried four peers at 300 s each and hung for
				// over 15 minutes), and dropping the peer's copy threw away
				// minutes of nearly finished work.
				deadline := started.Add(remotePartBudget)
				raced := false
				for _, peer := range p.peers {
					if ctx.Err() != nil {
						break
					}
					if pm.peerDead(peer) {
						continue
					}
					left := time.Until(deadline)
					if left <= 0 {
						log.Printf("[pool] part %d: no time left of %s for another peer; running its %d items here", i, remotePartBudget, len(p.c.items))
						break
					}
					// The node's depth gate: parts beyond what it should hold
					// wait here, in the daemon's queue for that node, not on
					// the wire. The wait is a "queue" step of the record.
					key := pm.key(peer)
					expected := pm.expectedSec(peer, ch.fnFP, len(p.c.items))
					qStart := time.Now()
					release, gerr := pm.book.acquire(ctx, key, pm.depth(peer, ch.fnFP), expected)
					if gerr != nil {
						e = gerr
						break
					}
					waited := time.Since(qStart)
					sent := time.Now()
					var where string
					var compute float64
					if offHere {
						// No local copy races a slow peer: this machine is
						// kept off. A peer that fails hands on to the next.
						where = "peer:" + peer
						r, compute, e = pm.runRemote(peer, storePath, p.c, 0)
					} else {
						fresh := pm.expectedSec("", ch.fnFP, len(p.c.items))
						r, compute, where, raced, e = pm.runRemoteRacing(i, peer, runPath, storePath, p.c, left-waited, expected, fresh)
					}
					release()
					if e == nil {
						d := time.Since(sent)
						key = pm.key(peer) // the reply may have named the node
						if where == "local" {
							key = pm.selfID // the backup copy here won
						}
						steps := []runrec.Step{pm.step(key, "queue", qStart, waited, i)}
						if compute > 0 && where != "local" {
							steps = append(steps, pm.step(key, "compute", sent, time.Duration(compute*float64(time.Second)), i))
						}
						ch.prov.record(where, len(p.c.items), time.Since(started).Milliseconds(), steps...)
						if where != "local" {
							// Timed end to end, so the round trip and the closure
							// transfer are in the number rather than assumed away.
							// A local win is not measured: it started late, after
							// the peer's budget ran out. The queue wait is not
							// the peer's time, so it is left out.
							pm.rates.observe(key, len(p.c.items), d)
							if compute <= 0 {
								compute = d.Seconds()
							}
							pm.observeStats(key, ch.fnFP, len(p.c.items), compute, d.Seconds())
						} else {
							pm.observeLost(peer, ch.fnFP, len(p.c.items), d, expected)
						}
						break
					}
					if ctx.Err() != nil {
						pm.observeLost(peer, ch.fnFP, len(p.c.items), time.Since(sent), expected)
						break // the requester left; the peer did nothing wrong
					}
					if raced {
						break // both copies failed; the local one was the fallback
					}
					pm.markPeerDead(peer)
				}
				if e != nil && ctx.Err() == nil && !raced {
					// D2/D3 — a remote node adds capacity, never subtracts.
					if offHere {
						log.Printf("[pool] --offload: every peer failed part %d (%v); running its %d items on this machine", i, e, len(p.c.items))
					}
					r, e = pm.runLocal(runPath, storePath, p.c)
					if e == nil {
						// Deliberately not measured: this ran locally after a
						// peer failed, so the elapsed time includes the failed
						// attempt and would libel this node's own speed.
						ch.prov.record("local", len(p.c.items), time.Since(started).Milliseconds())
					}
				}
			}
			if err := ctx.Err(); err != nil {
				// Cancelled: no local fallback, no "part failed" noise. The
				// request logs the cancel once.
				errs[i] = err
				return
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
func (pm *poolManager) bestPeer(storePath, submitter, offload string, need http.Header) string {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.peerFn == nil {
		return ""
	}
	ps, _ := pm.offloadPeers(pm.peerFn(storePath, submitter, need), offload)
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
	ctx := ch.context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
			pm.mu.Unlock()
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
	if err != nil && ctx.Err() != nil {
		// The request was cancelled and submit killed the worker mid-chunk:
		// forget it (the next request re-warms) and run nothing in its place.
		pm.dropWorker(storePath, worker)
		return nil, ctx.Err()
	}
	if err != nil {
		// Worker died mid-flight (or could not take the task) — drop it, stop
		// it if it still runs, and fall back to a cold run for this chunk; the
		// next request will re-warm.
		log.Printf("[pool] warm worker submit failed for store %s: %v", storePath, err)
		pm.dropWorker(storePath, worker)
		if worker.killOnce() {
			go func() {
				err := worker.cmd.Wait()
				log.Printf("[pool] warm worker pid %d stopped after a failed submit: %v", worker.cmd.Process.Pid, err)
			}()
		}
		return runPoolChunk(runPath, ch)
	}
	return results, nil
}

// dropWorker forgets a failed warm worker, but only while the map still holds
// that worker: a concurrent request may already have spawned a fresh one for
// the store, and dropping it would leak its process past stopAll.
func (pm *poolManager) dropWorker(storePath string, w *poolWorker) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.workers[storePath] == w {
		delete(pm.workers, storePath)
	}
}

// runRemote forwards a sub-chunk to a peer daemon's /v1/pool/map. The peer must
// have the same closure already (peerFn filters for that). Failures are fatal
// for the chunk: the caller returns them rather than silently dropping work.
// remotePartBudget is how long one part may spend on peers, across every peer
// it tries, before it runs locally. A variable so tests can shorten it.
var remotePartBudget = 5 * time.Minute

// partMem is the required_mem a part sends: the shim's per-node estimate
// (transfer.py _pool_required: 2 x (globals + callable + largest item)) taken
// over this part's own items, never more than the chunk's. Sending the whole
// chunk's figure made a peer refuse a part sized by items it never received.
// Compressed or out-of-band items are not their real size, so those parts keep
// the chunk's figure.
func (ch *chunk) partMem() int64 {
	if ch.requiredMem <= 0 || len(ch.items) == 0 || ch.itemsZlib || ch.oob {
		return ch.requiredMem
	}
	big := 0
	for _, it := range ch.items {
		big = max(big, len(it))
	}
	return min(ch.requiredMem, 2*int64(len(ch.globals)+len(ch.pickledFunc)+big))
}

// runRemote posts a part to one peer and waits up to timeout for its results.
// computeSec is the peer's own account of its compute time (its receipt), 0
// when it sent none.
func (pm *poolManager) runRemote(peer, storePath string, ch *chunk, timeout time.Duration) (results []any, computeSec float64, err error) {
	// no_fanout: the origin splits exactly once; a peer must never re-split and
	// forward again (one-hop tree, results flow straight back to the origin).
	var body []byte
	if ch.frames {
		hdr, _ := json.Marshal(map[string]any{
			"func_src": ch.funcSrc, "func_name": ch.funcName, "func": ch.pickledFunc,
			"starmap": ch.starmap, "items_frames": len(ch.items), "globals": ch.globals != nil,
			"required_mem": ch.partMem(), "no_fanout": true,
			"cache_keys": ch.cacheKeys, "item_keys": ch.itemKeys, "chunk_dir": ch.chunkDir,
			"items_zlib": ch.itemsZlib, "fn_fp": ch.fnFP, "oob": ch.oob,
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
		if ch.fnFP != "" {
			payload["fn_fp"] = ch.fnFP
		}
		body, _ = json.Marshal(payload)
	}
	hdr := http.Header{}
	if ch.frames {
		hdr.Set("Content-Type", framesContentType)
	} else {
		hdr.Set("Content-Type", "application/json")
	}
	hdr.Set("X-Pipedpeer-Store", storePath)
	if ch.python != "" {
		hdr.Set("X-Pipedpeer-Python", ch.python)
	}
	// Tied to the request this chunk serves: a cancel closes the connection,
	// and the peer stops its work when its own request context ends.
	status, _, body, err := pm.postPeer(ch.context(), peer, body, hdr, timeout)
	if err != nil {
		return nil, 0, fmt.Errorf("peer %s: %w", peer, err)
	}
	if status != http.StatusOK {
		return nil, 0, fmt.Errorf("peer %s returned %d", peer, status)
	}
	if ch.frames {
		// Frames response: header line + result frames.
		nl := bytes.IndexByte(body, '\n')
		if nl < 0 {
			return nil, 0, fmt.Errorf("peer %s: bad frames response", peer)
		}
		var out struct {
			ResultsFrames int `json:"results_frames"`
			Receipt       struct {
				Node  string     `json:"node"`
				Parts []provPart `json:"parts"`
			} `json:"receipt"`
		}
		if err := json.Unmarshal(body[:nl], &out); err != nil || out.ResultsFrames <= 0 {
			return nil, 0, fmt.Errorf("peer %s: bad frames response: %v", peer, err)
		}
		pm.sawNode(peer, out.Receipt.Node)
		for _, p := range out.Receipt.Parts {
			computeSec += float64(p.Ms) / 1000
		}
		var frames [][]byte
		rest := body[nl+1:]
		for i := 0; i < out.ResultsFrames; i++ {
			var f []byte
			f, rest, err = readFrame(rest)
			if err != nil {
				return nil, 0, fmt.Errorf("peer %s: %v", peer, err)
			}
			frames = append(frames, f)
		}
		results := make([]any, 0, len(frames))
		for _, f := range frames {
			results = append(results, map[string]string{"pickle": string(f)})
		}
		return results, computeSec, nil
	}
	var out struct {
		Results []map[string]string `json:"results"`
	}
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&out); err != nil {
		return nil, 0, err
	}
	results = make([]any, 0, len(out.Results))
	for _, r := range out.Results {
		if _, err := base64.StdEncoding.DecodeString(r["pickle"]); err != nil {
			return nil, 0, err
		}
		results = append(results, r)
	}
	return results, 0, nil
}

// runRemoteRacing posts part i to peer. A copy also starts here, first to
// finish wins and the other is cancelled (a cancelled local copy kills its
// warm worker, a cancelled remote copy closes the connection and the peer
// stops), in two cases:
//   - the tail backup (R8 step 7): once the part is late (past twice its
//     expected time on the peer, at least 1 s) and a copy here is expected to
//     save more than its own setup, and no other backup is running on this
//     daemon (at most one at a time);
//   - the hard budget: the peer has not answered within budget at all. This
//     is the never-stuck guarantee and ignores the one-backup cap.
//
// expected is the part's predicted seconds on the peer and fresh the predicted
// seconds of a copy here, both 0 when unknown (then only the budget applies).
// where is "peer:<id>" or "local"; raced reports that the local copy was
// started, so the caller must not run the part locally again.
func (pm *poolManager) runRemoteRacing(i int, peer, runPath, storePath string, ch *chunk, budget time.Duration, expected, fresh float64) (results []any, computeSec float64, where string, raced bool, err error) {
	type outcome struct {
		r       []any
		compute float64
		err     error
		where   string
	}
	out := make(chan outcome, 2) // both senders finish even after a winner returns
	parent := ch.context()
	started := time.Now()

	rctx, rcancel := context.WithCancel(parent)
	defer rcancel()
	rc := *ch
	rc.ctx = rctx
	go func() {
		r, c, e := pm.runRemote(peer, storePath, &rc, 0)
		out <- outcome{r, c, e, "peer:" + peer}
	}()

	hard := time.NewTimer(budget)
	defer hard.Stop()
	var lateC <-chan time.Time
	if expected > 0 && fresh > 0 {
		late := time.Duration(max(2*expected, 1) * float64(time.Second))
		if late < budget {
			t := time.NewTimer(late)
			defer t.Stop()
			lateC = t.C
		}
	}
	backup := false
wait:
	for {
		select {
		case o := <-out:
			return o.r, o.compute, o.where, false, o.err
		case <-parent.Done():
			return nil, 0, "", false, parent.Err()
		case <-lateC:
			lateC = nil
			ok, gain := schedule.BackupWorth(expected, time.Since(started).Seconds(), fresh, 0)
			if !ok {
				continue
			}
			if !pm.backups.CompareAndSwap(0, 1) {
				log.Printf("[pool] part %d: late on %s but another backup is running; waiting", i, peer)
				continue
			}
			backup = true
			log.Printf("[pool] part %d: late on %s (expected %.1fs, now %.1fs); a copy here should save %.1fs, starting it, first to finish wins",
				i, peer, expected, time.Since(started).Seconds(), gain)
			break wait
		case <-hard.C:
			log.Printf("[pool] part %d: peer %s has not finished %d items in %s; starting them here too, first to finish wins", i, peer, len(ch.items), budget.Round(100*time.Millisecond))
			break wait
		}
	}
	if backup {
		defer pm.backups.Store(0)
	}

	lctx, lcancel := context.WithCancel(parent)
	defer lcancel()
	lc := *ch
	lc.ctx = lctx
	go func() {
		r, e := pm.runLocal(runPath, storePath, &lc)
		out <- outcome{r, 0, e, "local"}
	}()

	var errs []error
	for range 2 {
		o := <-out
		if o.err == nil {
			log.Printf("[pool] part %d: %s finished first; cancelling the other copy", i, o.where)
			return o.r, o.compute, o.where, true, nil // the deferred cancels stop the loser
		}
		if parent.Err() != nil {
			return nil, 0, "", true, parent.Err()
		}
		log.Printf("[pool] part %d: %s copy failed: %v; waiting for the other", i, o.where, o.err)
		errs = append(errs, o.err)
	}
	return nil, 0, "", true, errors.Join(errs...)
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
	// Own process group, so a cancel kills whatever the user's function forked.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
	return w == nil || w.cmd == nil || w.killed.Load()
}

// kill stops the worker and its process group once; later calls do nothing.
func (w *poolWorker) kill() { w.killOnce() }

// killOnce is kill reporting whether this call stopped the worker, so exactly
// one caller reaps it.
func (w *poolWorker) killOnce() bool {
	if w.killed.Swap(true) || w.cmd == nil || w.cmd.Process == nil {
		return false
	}
	if err := killProcessGroup(w.cmd.Process); err != nil {
		log.Printf("[pool] stopping warm worker pid %d: %v", w.cmd.Process.Pid, err)
	}
	return true
}

// submit sends one task over the pipes and waits for its result file. The
// payload (pickled items can be ~100s of MB) is written to a temp file and the
// worker is told the path, so the full blob never passes through the pipe or
// lives in daemon RAM. Frames chunks ship as a header line + length-prefixed
// raw pickle frames; legacy chunks keep the base64 JSON encoding.
func (w *poolWorker) submit(ch *chunk) ([]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	ctx := ch.context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if w.killed.Load() {
		return nil, fmt.Errorf("warm worker was stopped")
	}
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
			"chunk_dir": ch.chunkDir, "items_zlib": ch.itemsZlib,
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

	// A cancel while this chunk runs kills the worker; the read below then
	// fails and the caller sees the cancel. Reap it so it leaves no zombie.
	stop := context.AfterFunc(ctx, w.kill)
	defer func() {
		if !stop() {
			err := w.cmd.Wait()
			log.Printf("[pool] warm worker pid %d stopped for a cancelled request: %v", w.cmd.Process.Pid, err)
		}
	}()

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
		if w != nil {
			w.kill()
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
			"chunk_dir": ch.chunkDir, "items_zlib": ch.itemsZlib,
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

	ctx := ch.context()
	cmd := exec.CommandContext(ctx, runPath, runnerPath, inPath, outPath)
	// Own process group, killed whole on cancel (see killProcessGroup).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return killProcessGroup(cmd.Process) }
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
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

// chunkModules lists the modules a pool request needs, from its func_src and
// pickles (function, globals, and the first few items: items are uniform, and
// scanning gigabytes per request is not worth it). Compressed items are
// skipped.
func chunkModules(req *poolRequest, globals []byte, items []json.RawMessage, frames bool) []string {
	var pickles [][]byte
	for _, b64 := range []string{req.Func, req.ExtraB64} {
		if b64 == "" {
			continue
		}
		b, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			log.Printf("[pool] module scan: undecodable base64 pickle skipped: %v", err)
			continue
		}
		pickles = append(pickles, b)
	}
	pickles = append(pickles, globals)
	for i, it := range items {
		if i == 4 {
			break
		}
		switch {
		case frames && !req.ItemsZlib:
			pickles = append(pickles, it)
		case !frames && req.ItemsB64:
			var s string
			if json.Unmarshal(it, &s) != nil {
				continue
			}
			if b, err := base64.StdEncoding.DecodeString(s); err == nil {
				pickles = append(pickles, b)
			}
		}
	}
	return phone.Modules(req.FuncSrc, pickles...)
}
