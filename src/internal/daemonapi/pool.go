package daemonapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

// poolRunner is the Python helper executed inside the closure to run a
// pickled function over a batch of items. It is written to a temp file and
// invoked via <storePath>/bin/run pool_runner.py <payload.json> <out.json>.
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

type poolRequest struct {
	Func    string          `json:"func"` // base64 pickled callable
	Items   json.RawMessage `json:"items"`
	Starmap bool            `json:"starmap"`
}

// handlePoolMap executes a pickled function over a batch of items using the
// local closure, returning per-item results. It is the worker side of the
// sitecustomize cluster pool (see nixgen/shim.go). Each request is one chunk.
//
// It runs in a subprocess of <storePath>/bin/run so it executes in exactly the
// environment the user's script runs in — no SDK, no shared state.
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

	results, err := runPoolChunk(runPath, req.Func, items, req.Starmap)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

var poolMux sync.Mutex

func runPoolChunk(runPath, pickledFunc string, items []json.RawMessage, starmap bool) ([]any, error) {
	// Serialise subprocess runs: the closure python is a shared resource and
	// the pickled payload is untrusted input from a peer. Serialising also
	// bounds fan-out concurrency at the node.
	poolMux.Lock()
	defer poolMux.Unlock()

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
