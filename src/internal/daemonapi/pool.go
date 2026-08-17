package daemonapi

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// poolRunner is the Python helper executed inside the closure to run a
// pickled function over a batch of items. It is written to a temp file and
// invoked via <storePath>/bin/run pool_runner.py <payload.json> <out.json>.
// It is the cold path: each invocation spawns a fresh closure process, which
// costs seconds. The warm worker (below) replaces it for the steady state.
const poolRunner = `# Runs a pickled callable over a JSON batch, for pipedpeer's cluster pool.
import base64, json, pickle, sys

with open(sys.argv[1]) as f:
    req = json.load(f)

    if req.get("func_src"):
        ns = {}
        if req.get("extra_b64"):
            ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
        exec(req["func_src"], ns)
        func = ns[req.get("func_name", "run")]
    else:
        func = pickle.loads(base64.b64decode(req["func"]))
items = req["items"]
starmap = req.get("starmap", False)
if req.get("items_b64"):
    items = [pickle.loads(base64.b64decode(i)) for i in items]

results = []
for item in items:
    if starmap:
        args = item if isinstance(item, list) else (item,)
        results.append(func(*args))
    else:
        results.append(func(item))

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
# Runs the payload from in_file, writes the results JSON to out_file, then
# answers with a tiny {"id": N, "done": true, "error": ...} line on stdout.
# Kept alive across many /v1/pool/map requests so dispatch never re-spawns
# the closure. _CACHE persists across requests inside this process: func_src
# can read it (via the injected _CACHE name) to store/retrieve chunk data
# keyed by content hash, which is how out-of-core reads keep chunks on nodes.
import base64, json, pickle, sys

_CACHE = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
        with open(msg["in_file"]) as f:
            req = json.load(f)
        ns = {}
        if req.get("extra_b64"):
            ns.update(pickle.loads(base64.b64decode(req["extra_b64"])))
        ns["_CACHE"] = _CACHE
        if req.get("func_src"):
            exec(req["func_src"], ns)
            func = ns[req.get("func_name", "run")]
        else:
            func = pickle.loads(base64.b64decode(req["func"]))
        items = req["items"]
        starmap = req.get("starmap", False)
        if req.get("items_b64"):
            items = [pickle.loads(base64.b64decode(i)) for i in items]
        # cache_keys: item i is a content hash resolved from the process-wide
        # chunk cache instead of the payload. A miss is an error: the submitter
        # falls back to local work rather than running with wrong data.
        if req.get("cache_keys"):
            items = [ns["_CACHE"].get(k) for k in req["cache_keys"]]
            if any(i is None for i in items):
                raise KeyError("chunk cache miss")
        results = []
        for item in items:
            if starmap:
                args = item if isinstance(item, list) else (item,)
                results.append(func(*args))
            else:
                results.append(func(item))
        out = {"id": msg["id"], "results":
               [base64.b64encode(pickle.dumps(r)).decode() for r in results]}
        with open(msg["out_file"], "w") as f:
            json.dump(out, f)
        ack = {"id": msg["id"], "done": True}
    except Exception as e:
        ack = {"id": msg.get("id"), "done": False, "error": str(e)}
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
	// NoFanout tells the receiving daemon to run the chunk locally instead of
	// splitting and forwarding it to peers. The origin splits exactly once;
	// every forwarded chunk is terminal (one-hop fan-out).
	NoFanout bool `json:"no_fanout,omitempty"`
	// NoSplit keeps every item as its own routed unit: item i goes to peer i
	// (items beyond the peer count are local), so the origin can pre-partition
	// work (hash-shuffle buckets) and have each part land on exactly one node.
	NoSplit bool `json:"no_split,omitempty"`
	// RequiredMemBytes is the submitter's estimate of the working set this
	// chunk needs; the daemon refuses with 503 when it cannot spare that much,
	// so an overloaded node never OOMs mid-chunk. The shim falls back locally.
	RequiredMemBytes int64 `json:"required_mem,omitempty"`
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
	peerFn func(storePath string) []string

	// deadPeers are peers that failed this chunk; later parts skip them and
	// fall through to the next best candidate. Reset per runChunk so a peer
	// that recovers mid-run is tried again.
	deadMu    sync.Mutex
	deadPeers map[string]bool
}

func newPoolManager() *poolManager {
	return &poolManager{workers: make(map[string]*poolWorker), deadPeers: make(map[string]bool)}
}

// SetPeerFn installs the function that returns peer daemon endpoints eligible
// to run chunks for a store path. Without it, all work stays local.
func (pm *poolManager) SetPeerFn(fn func(storePath string) []string) {
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
	return len(pm.peerFn(storePath))
}

// handlePoolMap executes a pickled function over a batch of items using the
// local closure, returning per-item results. It is the worker side of the
// sitecustomize cluster pool (see nixgen/shim.go). Each request is one chunk.
//
// It runs in a subprocess of <storePath>/bin/run so it executes in exactly the
// environment the user's script runs in — no SDK, no shared state. Requests to
// a store that already has a warm worker reuse it instead of re-spawning.
func (s *Server) handlePoolMap(w http.ResponseWriter, r *http.Request) {
	var req poolRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request: " + err.Error()})
		return
	}

	// The store path / run wrapper comes from the request environment. The
	// submitter embeds it so we need no job context.
	storePath := r.Header.Get("X-Pipedpeer-Store")
	runPath := filepath.Join(storePath, "bin", "run")
	if storePath == "" || !pathExists(runPath) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-Pipedpeer-Store header (or invalid store path)"})
		return
	}

	var items []json.RawMessage
	if err := json.Unmarshal(req.Items, &items); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad items: " + err.Error()})
		return
	}

	// Admission control: refuse when the working set cannot fit, so an
	// overloaded node never OOMs mid-chunk. The submitter falls back locally.
	if req.RequiredMemBytes > 0 {
		available := s.AvailableForJob()
		if available < req.RequiredMemBytes {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"error": fmt.Sprintf("insufficient memory: need %d bytes, available %d bytes", req.RequiredMemBytes, available),
			})
			return
		}
	}

	var results []any
	var err error
	switch {
	case req.NoSplit:
		// Hash-shuffle routing: the origin pre-partitioned items into buckets
		// (one per node); each item must land on exactly one node, so bypass
		// the minSplit gate and route per item.
		results, err = s.pool.runChunk(runPath, storePath, req.Func, req.FuncSrc, req.FuncName, req.ExtraB64, items, req.Starmap, req.ItemsB64, true, req.CacheKeys)
	case req.NoFanout:
		// One-hop fan-out: the origin already split; this chunk is terminal.
		results, err = s.pool.runLocal(runPath, storePath, req.Func, req.FuncSrc, req.FuncName, req.ExtraB64, items, req.Starmap, req.ItemsB64, req.CacheKeys)
	default:
		results, err = s.pool.runChunk(runPath, storePath, req.Func, req.FuncSrc, req.FuncName, req.ExtraB64, items, req.Starmap, req.ItemsB64, false, req.CacheKeys)
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
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
func (pm *poolManager) runChunk(runPath, storePath, pickledFunc, funcSrc, funcName, extraB64 string, items []json.RawMessage, starmap, itemsB64, noSplit bool, cacheKeys []string) ([]any, error) {
	// Peel off peers before taking the local worker lock so each local submit
	// serialises only its own sub-chunk.
	var peers []string
	pm.mu.Lock()
	if pm.peerFn != nil {
		peers = pm.peerFn(storePath)
	}
	pm.mu.Unlock()

	pm.deadMu.Lock()
	pm.deadPeers = make(map[string]bool)
	pm.deadMu.Unlock()

	// One worker per participating node. Local is always included.
	type part struct {
		items []json.RawMessage
		// run is set only for the local part; remote parts POST instead.
		runPath string
		// peers is the candidate list for this part, its own primary peer
		// first (part i prefers peers[i]), the rest as fallback in ranked
		// order. The part tries each in order, skipping dead ones, before
		// going local.
		peers []string
	}
	var parts []part

	switch {
	case noSplit:
		// One part per item, part i prefers peers[i] (rotating back through
		// the list when there are more items than peers). Items past the peer
		// count fall through to local via the empty runPath branch below —
		// they simply take the local part of the rotation.
		for i := range items {
			p := part{items: items[i : i+1]}
			if len(peers) > 0 {
				idx := i % len(peers)
				ordered := append(append([]string{}, peers[idx:]...), peers[:idx]...)
				p.peers = ordered
			} else {
				p.runPath = runPath
			}
			parts = append(parts, p)
		}
	case len(peers) == 0:
		parts = []part{{items: items, runPath: runPath}}
	default:
		// Split evenly across local + peers. A small chunk stays local (splitting
		// would cost more than it saves); only fan out once there is real work.
		const minSplit = 8
		if len(items) < minSplit {
			parts = []part{{items: items, runPath: runPath}}
		} else {
			workers := len(peers) + 1
			base := len(items) / workers
			extra := len(items) % workers
			idx := 0
			for i := 0; i < workers; i++ {
				size := base
				if i < extra {
					size++
				}
				if size == 0 {
					continue
				}
				chunk := items[idx : idx+size]
				idx += size
				if i == workers-1 {
					parts = append(parts, part{items: chunk, runPath: runPath}) // local
				} else {
					// Fan out to distinct peers: part i prefers peers[i], so
					// every healthy peer gets work, not just the best one.
					ordered := append(append([]string{}, peers[i:]...), peers[:i]...)
					parts = append(parts, part{items: chunk, peers: ordered})
				}
			}
		}
	}

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
			if p.runPath != "" {
				r, e = pm.runLocal(runPath, storePath, pickledFunc, funcSrc, funcName, extraB64, p.items, starmap, itemsB64, cacheKeys)
			} else {
				// Walk the ranked peer list; a failure falls through to the
				// next best candidate. If every peer fails, the work is ours.
				for _, peer := range p.peers {
					if pm.peerDead(peer) {
						continue
					}
					r, e = pm.runRemote(peer, storePath, pickledFunc, funcSrc, funcName, extraB64, p.items, starmap, itemsB64, cacheKeys)
					if e == nil {
						break
					}
					pm.markPeerDead(peer)
				}
				if e != nil {
					// D2/D3 — a remote node adds capacity, never subtracts.
					r, e = pm.runLocal(runPath, storePath, pickledFunc, funcSrc, funcName, extraB64, p.items, starmap, itemsB64, cacheKeys)
				}
			}
			if e != nil {
				errs[i] = e
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
func (pm *poolManager) runLocal(runPath, storePath, pickledFunc, funcSrc, funcName, extraB64 string, items []json.RawMessage, starmap, itemsB64 bool, cacheKeys []string) ([]any, error) {
	pm.mu.Lock()
	worker, ok := pm.workers[storePath]
	if !ok || worker.dead() {
		if ok {
			pm.workers[storePath] = nil
		}
		w, err := pm.spawn(runPath, storePath)
		if err != nil {
			// Cold fallback: one-off spawn keeps the node functional even when
			// persistent workers are unavailable.
			return runPoolChunk(runPath, pickledFunc, funcSrc, funcName, extraB64, items, starmap, itemsB64, cacheKeys)
		}
		worker = w
		pm.workers[storePath] = worker
	}
	pm.mu.Unlock()

	results, err := worker.submit(pickledFunc, funcSrc, funcName, extraB64, items, starmap, itemsB64, cacheKeys)
	if err != nil {
		// Worker died mid-flight — drop it and fall back to a cold run for
		// this chunk; the next request will re-warm.
		pm.mu.Lock()
		delete(pm.workers, storePath)
		pm.mu.Unlock()
		return runPoolChunk(runPath, pickledFunc, funcSrc, funcName, extraB64, items, starmap, itemsB64, cacheKeys)
	}
	return results, nil
}

// runRemote forwards a sub-chunk to a peer daemon's /v1/pool/map. The peer must
// have the same closure already (peerFn filters for that). Failures are fatal
// for the chunk: the caller returns them rather than silently dropping work.
func (pm *poolManager) runRemote(peer, storePath, pickledFunc, funcSrc, funcName, extraB64 string, items []json.RawMessage, starmap, itemsB64 bool, cacheKeys []string) ([]any, error) {
	// no_fanout: the origin splits exactly once; a peer must never re-split and
	// forward again (one-hop tree, results flow straight back to the origin).
	payload := map[string]any{"func": pickledFunc, "func_src": funcSrc, "func_name": funcName, "extra_b64": extraB64, "items": items, "starmap": starmap, "items_b64": itemsB64, "no_fanout": true}
	if len(cacheKeys) > 0 {
		payload["cache_keys"] = cacheKeys
	}
	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("http://%s/v1/pool/map", peer)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
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
	var out struct {
		Results []map[string]string `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
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
	dir, err := os.MkdirTemp("", "pipedpeer-warm-*")
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
// payload (base64 items can be ~100s of MB) is written to a temp file and the
// worker is told the path, so the full blob never passes through the pipe or
// lives in daemon RAM.
func (w *poolWorker) submit(pickledFunc, funcSrc, funcName, extraB64 string, items []json.RawMessage, starmap, itemsB64 bool, cacheKeys []string) ([]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.id++

	dir, err := os.MkdirTemp("", "pipedpeer-warm-task-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	inPath := filepath.Join(dir, "in.json")
	outPath := filepath.Join(dir, "out.json")

	req := map[string]any{"id": w.id, "func": pickledFunc, "func_src": funcSrc, "func_name": funcName, "extra_b64": extraB64, "items": items, "starmap": starmap, "items_b64": itemsB64}
	if len(cacheKeys) > 0 {
		req["cache_keys"] = cacheKeys
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
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
func runPoolChunk(runPath, pickledFunc, funcSrc, funcName, extraB64 string, items []json.RawMessage, starmap, itemsB64 bool, cacheKeys []string) ([]any, error) {
	dir, err := os.MkdirTemp("", "pipedpeer-pool-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	runnerPath := filepath.Join(dir, "pool_runner.py")
	if err := os.WriteFile(runnerPath, []byte(poolRunner), 0755); err != nil {
		return nil, err
	}

	payload := map[string]any{"func": pickledFunc, "func_src": funcSrc, "func_name": funcName, "extra_b64": extraB64, "items": items, "starmap": starmap, "items_b64": itemsB64}
	if len(cacheKeys) > 0 {
		payload["cache_keys"] = cacheKeys
	}
	inPath := filepath.Join(dir, "in.json")
	outPath := filepath.Join(dir, "out.json")
	inBytes, _ := json.Marshal(payload)
	if err := os.WriteFile(inPath, inBytes, 0644); err != nil {
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
