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

func = pickle.loads(base64.b64decode(req["func"]))
items = req["items"]
starmap = req.get("starmap", False)

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
// stdout. Dispatch then costs a pipe write instead of a closure spawn. This is
// the "warm workers" dispatch model (handoff.md §4/D2): one worker process per
// node per store, tasks streaming as messages.
const warmWorkerScript = `# Persistent pipedpeer cluster worker. One process per store path.
# Reads JSON-lines from stdin, runs each pickled callable over its items,
# writes a JSON-line result to stdout. Kept alive across many /v1/pool/map
# requests so dispatch never re-spawns the closure.
import base64, json, pickle, sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        func = pickle.loads(base64.b64decode(req["func"]))
        items = req["items"]
        starmap = req.get("starmap", False)
        results = []
        for item in items:
            if starmap:
                args = item if isinstance(item, list) else (item,)
                results.append(func(*args))
            else:
                results.append(func(item))
        out = {"id": req["id"],
               "results": [base64.b64encode(pickle.dumps(r)).decode() for r in results]}
    except Exception as e:
        out = {"id": req.get("id"), "error": str(e)}
    sys.stdout.write(json.dumps(out) + "\n")
    sys.stdout.flush()
`

type poolRequest struct {
	Func    string          `json:"func"` // base64 pickled callable
	Items   json.RawMessage `json:"items"`
	Starmap bool            `json:"starmap"`
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
	// closure, used for multi-node spill. Nil disables spill (single node).
	peerFn func(storePath string) []string
}

func newPoolManager() *poolManager {
	return &poolManager{workers: make(map[string]*poolWorker)}
}

// SetPeerFn installs the function that returns peer daemon endpoints eligible
// to run chunks for a store path. Without it, all work stays local.
func (pm *poolManager) SetPeerFn(fn func(storePath string) []string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.peerFn = fn
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

	results, err := s.pool.runChunk(runPath, storePath, req.Func, items, req.Starmap)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// runChunk dispatches one chunk, preferring a warm worker and falling back to
// the cold per-request spawn when the warm worker dies or cannot start.
// When healthy peers share the store, items are split across local + peers and
// results are merged in input order — local is always one of the workers, so a
// remote node adds capacity but never removes it (D2: never slower).
func (pm *poolManager) runChunk(runPath, storePath, pickledFunc string, items []json.RawMessage, starmap bool) ([]any, error) {
	// Peel off peers before taking the local worker lock so each local submit
	// serialises only its own sub-chunk.
	var peers []string
	pm.mu.Lock()
	if pm.peerFn != nil {
		peers = pm.peerFn(storePath)
	}
	pm.mu.Unlock()

	// One worker per participating node. Local is always included.
	type part struct {
		items []json.RawMessage
		// run is set only for the local part; remote parts POST instead.
		runPath string
		peer    string
	}
	var parts []part

	if len(peers) == 0 {
		parts = []part{{items: items, runPath: runPath}}
	} else {
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
					parts = append(parts, part{items: chunk, peer: peers[i]})
				}
			}
		}
	}

	results := make([]any, 0, len(items))
	mu := &sync.Mutex{}
	var wg sync.WaitGroup
	errs := make([]error, len(parts))

	for i, p := range parts {
		wg.Add(1)
		go func(i int, p part) {
			defer wg.Done()
			var r []any
			var e error
			if p.runPath != "" {
				r, e = pm.runLocal(runPath, storePath, pickledFunc, p.items, starmap)
			} else {
				r, e = pm.runRemote(p.peer, storePath, pickledFunc, p.items, starmap)
				if e != nil {
					// A peer that fell out (died, dropped the closure, or is
					// stale) must not fail the chunk: the work is ours to do.
					// D2/D3 — a remote node adds capacity, never subtracts.
					r, e = pm.runLocal(runPath, storePath, pickledFunc, p.items, starmap)
				}
			}
			if e != nil {
				errs[i] = e
				return
			}
			mu.Lock()
			results = append(results, r...)
			mu.Unlock()
		}(i, p)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	return results, nil
}

// runLocal dispatches a sub-chunk to this node's warm worker (or cold path).
func (pm *poolManager) runLocal(runPath, storePath, pickledFunc string, items []json.RawMessage, starmap bool) ([]any, error) {
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
			return runPoolChunk(runPath, pickledFunc, items, starmap)
		}
		worker = w
		pm.workers[storePath] = worker
	}
	pm.mu.Unlock()

	results, err := worker.submit(pickledFunc, items, starmap)
	if err != nil {
		// Worker died mid-flight — drop it and fall back to a cold run for
		// this chunk; the next request will re-warm.
		pm.mu.Lock()
		delete(pm.workers, storePath)
		pm.mu.Unlock()
		return runPoolChunk(runPath, pickledFunc, items, starmap)
	}
	return results, nil
}

// runRemote forwards a sub-chunk to a peer daemon's /v1/pool/map. The peer must
// have the same closure already (peerFn filters for that). Failures are fatal
// for the chunk: the caller returns them rather than silently dropping work.
func (pm *poolManager) runRemote(peer, storePath, pickledFunc string, items []json.RawMessage, starmap bool) ([]any, error) {
	payload := map[string]any{"func": pickledFunc, "items": items, "starmap": starmap}
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

// submit sends one task over the pipes and waits for its result line.
func (w *poolWorker) submit(pickledFunc string, items []json.RawMessage, starmap bool) ([]any, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.id++
	req := map[string]any{"id": w.id, "func": pickledFunc, "items": items, "starmap": starmap}
	body, _ := json.Marshal(req)
	if _, err := w.stdin.WriteString(string(body) + "\n"); err != nil {
		return nil, err
	}
	if err := w.stdin.Flush(); err != nil {
		return nil, err
	}

	// Read until the line matching our id arrives. Scan() returns false on EOF
	// (worker exited), which surfaces as an error and lets the caller fall back
	// to a cold run. A stuck worker is bounded by the work itself.
	for {
		if !w.stdout.Scan() {
			return nil, fmt.Errorf("warm worker closed: %v", w.stdout.Err())
		}
		var out struct {
			ID      int64    `json:"id"`
			Results []string `json:"results"`
			Error   string   `json:"error"`
		}
		if err := json.Unmarshal(w.stdout.Bytes(), &out); err != nil {
			continue
		}
		if out.ID != w.id {
			continue // stale line from a concurrent write; ignore
		}
		if out.Error != "" {
			return nil, fmt.Errorf("worker: %s", out.Error)
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
func runPoolChunk(runPath, pickledFunc string, items []json.RawMessage, starmap bool) ([]any, error) {
	dir, err := os.MkdirTemp("", "pipedpeer-pool-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	runnerPath := filepath.Join(dir, "pool_runner.py")
	if err := os.WriteFile(runnerPath, []byte(poolRunner), 0755); err != nil {
		return nil, err
	}

	payload := map[string]any{"func": pickledFunc, "items": items, "starmap": starmap}
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
