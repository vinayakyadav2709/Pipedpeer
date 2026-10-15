package daemonapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPoolMapMergesInInputOrder is a regression test for the fan-out merge
// bug: results were appended in goroutine completion order, so a part that
// finished late had its results appended after an earlier part — while the
// shim maps results back by input index. When the remote part is deliberately
// slower than the local part, the merged response must still be in input
// order.
func TestPoolMapMergesInInputOrder(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	modDir := t.TempDir()
	os.WriteFile(filepath.Join(modDir, "worker_mod.py"), []byte("def double(x):\n    return x * 2\n"), 0644)
	t.Setenv("PYTHONPATH", modDir+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"))
	emit := `
import base64, pickle, sys
sys.path.insert(0, sys.argv[1])
from worker_mod import double
sys.stdout.write(base64.b64encode(pickle.dumps(double)).decode())
`
	pickled, err := exec.Command(python, "-c", emit, modDir).Output()
	if err != nil {
		t.Fatalf("emit pickle: %v", err)
	}

	// Precompute base64 pickles of every even value the fake peer may return,
	// so the peer handler needs no python subprocess per request.
	emitPickles := `
import base64, json, pickle, sys
out = {}
for v in sys.argv[1:]:
    out[v] = base64.b64encode(pickle.dumps(int(v))).decode()
sys.stdout.write(json.dumps(out))
`
	var args []string
	for i := 2; i <= 24; i += 2 {
		args = append(args, strconv.Itoa(i))
	}
	cmdArgs := append([]string{python, "-c", emitPickles}, args...)
	out, err := exec.Command(cmdArgs[0], cmdArgs[1:]...).Output()
	if err != nil {
		t.Fatalf("emit pickles: %v", err)
	}
	var pickles map[string]string
	if err := json.Unmarshal(out, &pickles); err != nil {
		t.Fatalf("parse pickles: %v", err)
	}

	// Fake peer: answers /v1/pool/map with precomputed doubles, but only after
	// a 2s hold — far longer than the local part needs (6 trivial doubles via
	// a spawned python), so the local part deterministically finishes first.
	// The merge must still come back in input order.
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		var req poolRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var items []int
		if err := json.Unmarshal(req.Items, &items); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		res := make([]map[string]string, 0, len(items))
		for _, it := range items {
			res = append(res, map[string]string{"pickle": pickles[strconv.Itoa(it*2)]})
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": res})
	}))
	defer peerSrv.Close()

	s1 := New("node-" + strings.Repeat("x", 8))
	defer s1.StopWarmWorkers()
	s1.pool.SetPeerFn(func(_, _ string, _ http.Header) []string {
		return []string{strings.TrimPrefix(peerSrv.URL, "http://")}
	})

	// 12 items: split into part 0 (items 1..6 → slow peer) and part 1
	// (items 7..12 → fast local).
	var items []json.RawMessage
	for i := 1; i <= 12; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	results, err := s1.pool.runChunk(runPath, storePath, &chunk{pickledFunc: string(pickled), items: items}, "")
	if err != nil {
		t.Fatalf("runChunk: %v", err)
	}
	if len(results) != 12 {
		t.Fatalf("want 12 results, got %d", len(results))
	}

	// Flow verification: results[i] must be the double of items[i] — the
	// mapping, not just the multiset. Pre-fix this fails (peer part appended
	// after local part despite being first in input order).
	unpickle := `
import pickle, sys
sys.stdout.write(str(pickle.loads(open(sys.argv[1], "rb").read())))
`
	for i, r := range results {
		m, ok := r.(map[string]string)
		if !ok || m["pickle"] == "" {
			t.Fatalf("bad result shape at %d: %#v", i, r)
		}
		blob, err := base64.StdEncoding.DecodeString(m["pickle"])
		if err != nil {
			t.Fatalf("bad pickle at %d: %v", i, err)
		}
		bp := filepath.Join(t.TempDir(), "r.pkl")
		if err := os.WriteFile(bp, blob, 0644); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command(python, "-c", unpickle, bp).Output()
		if err != nil {
			t.Fatalf("unpickle: %v", err)
		}
		var v int
		fmt.Sscan(string(out), &v)
		want := (i + 1) * 2
		if v != want {
			t.Fatalf("result[%d] = %d, want %d (input order broken)", i, v, want)
		}
	}
}

// TestPoolMapNoSplitRoutesPerPeer verifies no_split routing: every item is its
// own part, part i prefers peers[i] (wrapping past the peer count), and no
// item is re-split — the hash-shuffle contract that each bucket lands on
// exactly one node.
// TestPoolStatsRecordsChunkOrigin covers the counters that make distribution
// checkable from outside the process. Without them the only trace of a chunk
// arriving was a log line, so a harness wanting to know whether work had
// reached a node had to shell in and grep — which is why the chaos test could
// not tell a live cluster from an idle one.
func TestPoolStatsRecordsChunkOrigin(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	storePath := fakeRun(t, t.TempDir())
	s := New("node-" + strings.Repeat("s", 8))

	before := s.pool.stats.snapshot()
	if before["chunks_received"] != 0 {
		t.Fatalf("fresh node already reports %d chunks", before["chunks_received"])
	}

	body, _ := json.Marshal(map[string]any{
		"func_src": doubleSrc, "func_name": "double",
		"items": []any{1, 2, 3},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/pool/map", bytes.NewReader(body))
	req.Header.Set("X-Pipedpeer-Store", storePath)
	req.RemoteAddr = "127.0.0.1:5555"
	rec := httptest.NewRecorder()
	s.handlePoolMap(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("pool/map: %d %s", rec.Code, rec.Body.String())
	}

	got := s.pool.stats.snapshot()
	if got["chunks_received"] != 1 {
		t.Errorf("chunks_received = %d, want 1", got["chunks_received"])
	}
	// Loopback and not forwarded: this is a job's own shim, not a peer.
	if got["chunks_from_local_shim"] != 1 || got["chunks_from_peers"] != 0 {
		t.Errorf("origin misattributed: local=%d peers=%d",
			got["chunks_from_local_shim"], got["chunks_from_peers"])
	}
	if got["items_executed_here"] != 3 {
		t.Errorf("items_executed_here = %d, want 3", got["items_executed_here"])
	}

	// And the same tally over HTTP, which is what a harness actually reads.
	statRec := httptest.NewRecorder()
	s.handlePoolStats(statRec, httptest.NewRequest(http.MethodGet, "/v1/pool/stats", nil))
	if statRec.Code != http.StatusOK {
		t.Fatalf("pool/stats: %d", statRec.Code)
	}
	var served map[string]int64
	if err := json.Unmarshal(statRec.Body.Bytes(), &served); err != nil {
		t.Fatalf("decoding stats: %v", err)
	}
	if served["chunks_received"] != 1 || served["items_executed_here"] != 3 {
		t.Errorf("served stats disagree with the counters: %v", served)
	}
}

func TestPoolMapNoSplitRoutesPerPeer(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	emitPickles := `
import base64, json, pickle, sys
out = {}
for v in sys.argv[1:]:
    out[v] = base64.b64encode(pickle.dumps(int(v))).decode()
sys.stdout.write(json.dumps(out))
`
	var args []string
	for i := 2; i <= 12; i += 2 {
		args = append(args, strconv.Itoa(i))
	}
	cmdArgs := append([]string{python, "-c", emitPickles}, args...)
	out, err := exec.Command(cmdArgs[0], cmdArgs[1:]...).Output()
	if err != nil {
		t.Fatalf("emit pickles: %v", err)
	}
	var pickles map[string]string
	if err := json.Unmarshal(out, &pickles); err != nil {
		t.Fatalf("parse pickles: %v", err)
	}

	// Three fake peers; each records which items (and affinity keys) it got.
	type peerRec struct {
		mu    sync.Mutex
		items []int
		keys  []string
	}
	peers := make([]*peerRec, 3)
	srvs := make([]*httptest.Server, 3)
	for i := range peers {
		peers[i] = &peerRec{}
		rec := peers[i]
		srvs[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req poolRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			var items []int
			if err := json.Unmarshal(req.Items, &items); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			rec.mu.Lock()
			rec.items = append(rec.items, items...)
			rec.keys = append(rec.keys, req.ItemKeys...)
			rec.mu.Unlock()
			res := make([]map[string]string, 0, len(items))
			for _, it := range items {
				res = append(res, map[string]string{"pickle": pickles[strconv.Itoa(it*2)]})
			}
			writeJSON(w, http.StatusOK, map[string]any{"results": res})
		}))
		defer srvs[i].Close()
	}

	hosts := make([]string, 3)
	for i := range srvs {
		hosts[i] = strings.TrimPrefix(srvs[i].URL, "http://")
	}

	s1 := New("node-" + strings.Repeat("x", 8))
	defer s1.StopWarmWorkers()
	s1.pool.SetPeerFn(func(_, _ string, _ http.Header) []string { return hosts })

	// 4 items over 3 peers: part 0→peer0, part 1→peer1, part 2→peer2,
	// part 3 wraps to peer0 (3 % 3 == 0).
	items := []json.RawMessage{
		json.RawMessage(`1`), json.RawMessage(`2`),
		json.RawMessage(`3`), json.RawMessage(`4`),
	}
	results, err := s1.pool.runChunk(runPath, storePath, &chunk{items: items, noSplit: true}, "")
	if err != nil {
		t.Fatalf("no_split runChunk: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("want 4 results, got %d", len(results))
	}

	// Flow verification: each item landed on its expected peer, un-split.
	// Peer arrival order is nondeterministic (parts run concurrently), so
	// compare sorted sets.
	want := [][]int{{1, 4}, {2}, {3}}
	for i, rec := range peers {
		rec.mu.Lock()
		got := append([]int(nil), rec.items...)
		rec.mu.Unlock()
		sort.Ints(got)
		w := append([]int(nil), want[i]...)
		sort.Ints(w)
		if !reflect.DeepEqual(got, w) {
			t.Fatalf("peer%d received %v, want %v (routing broken)", i, got, w)
		}
	}

	// Results still map by input index.
	for i, r := range results {
		m, ok := r.(map[string]string)
		if !ok || m["pickle"] == "" {
			t.Fatalf("bad result shape at %d: %#v", i, r)
		}
		blob, err := base64.StdEncoding.DecodeString(m["pickle"])
		if err != nil {
			t.Fatalf("bad pickle at %d: %v", i, err)
		}
		bp := filepath.Join(t.TempDir(), "r.pkl")
		if err := os.WriteFile(bp, blob, 0644); err != nil {
			t.Fatal(err)
		}
		up := `import pickle,sys;sys.stdout.write(str(pickle.loads(open(sys.argv[1],"rb").read())))`
		o, err := exec.Command(python, "-c", up, bp).Output()
		if err != nil {
			t.Fatalf("unpickle: %v", err)
		}
		var v int
		fmt.Sscan(string(o), &v)
		if v != (i+1)*2 {
			t.Fatalf("result[%d] = %d, want %d", i, v, (i+1)*2)
		}
	}

	// Affinity routing: once items carry keys, position stops deciding and a
	// stable hash over the key does. The hash must be taken against a
	// lexicographically sorted peer list, never the load-ranked one peerFn
	// hands back — that ranking shifts between the parse and the combine
	// request, and a key that moves node makes every cache_keys fetch miss.
	// So the peer list is deliberately re-ordered between the two requests
	// below and each key must still land where it landed the first time.
	stable := append([]string(nil), hosts...)
	sort.Strings(stable)
	keys := []string{"chunk-a", "chunk-b", "chunk-c", "chunk-d"}

	calls := 0
	s1.pool.SetPeerFn(func(_, _ string, _ http.Header) []string {
		calls++
		if calls%2 == 0 {
			// A different load ranking, same three peers.
			return []string{hosts[2], hosts[0], hosts[1]}
		}
		return hosts
	})

	routes := make([]map[string]int, 2)
	for req := range routes {
		for _, rec := range peers {
			rec.mu.Lock()
			rec.items = nil
			rec.keys = nil
			rec.mu.Unlock()
		}
		res, err := s1.pool.runChunk(runPath, storePath, &chunk{items: items, itemKeys: keys, noSplit: true}, "")
		if err != nil {
			t.Fatalf("affinity runChunk %d: %v", req, err)
		}
		if len(res) != len(items) {
			t.Fatalf("affinity request %d: want %d results, got %d", req, len(items), len(res))
		}
		routes[req] = map[string]int{}
		for i, rec := range peers {
			rec.mu.Lock()
			for _, k := range rec.keys {
				routes[req][k] = i
			}
			rec.mu.Unlock()
		}
		if len(routes[req]) != len(keys) {
			t.Fatalf("affinity request %d: %d of %d keys reached a peer (%v)", req, len(routes[req]), len(keys), routes[req])
		}
	}
	if !reflect.DeepEqual(routes[0], routes[1]) {
		t.Fatalf("affinity routing moved with the peer ranking: %v then %v (every cache fetch would miss)", routes[0], routes[1])
	}
	for _, k := range keys {
		want := stable[int(affinityHash(k)%uint32(len(stable)))]
		if got := hosts[routes[0][k]]; got != want {
			t.Fatalf("key %q routed to %s, want %s (affinity must hash against the stable peer order)", k, got, want)
		}
	}
}

// TestPoolSpillNextBestOnFailure checks the failover chain: the best peer is
// dead, so its share must land on the next best peer rather than quietly
// falling back to local. The kernel ships as source, as production sends it.
func TestPoolSpillNextBestOnFailure(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	s1 := New("node-" + strings.Repeat("x", 8))
	s2 := New("node-" + strings.Repeat("y", 8))
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	alivePeer := strings.TrimPrefix(srv2.URL, "http://")
	// Best first, but the best is dead (port 1 refuses connections).
	s1.pool.SetPeerFn(func(_, _ string, _ http.Header) []string { return []string{"127.0.0.1:1", alivePeer} })

	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	ch := &chunk{funcSrc: doubleSrc, funcName: "double", items: items}
	results, err := s1.pool.runChunk(runPath, storePath, ch, "")
	if err != nil {
		t.Fatalf("chunk failed: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results, got %d", len(results))
	}
	for i, v := range unpickleInts(t, results) {
		if want := (i + 1) * 2; v != want {
			t.Fatalf("result[%d] = %d, want %d (input order broken)", i, v, want)
		}
	}
	// The alive peer must have executed the failed best peer's share.
	s2.pool.mu.Lock()
	_, s2Ran := s2.pool.workers[storePath]
	s2.pool.mu.Unlock()
	if !s2Ran {
		t.Fatal("next-best peer never executed: failover chain broken")
	}
}

// TestPoolMapFramesRoundTrip exercises the frames wire format end to end:
// a frames request (header line + globals frame + item frames) posted to
// /v1/pool/map must parse, reach a warm worker through the frames in_file,
// and return a frames response (header + raw pickle result frames).
func TestPoolMapFramesRoundTrip(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	store := fakeRun(t, t.TempDir())

	s := New("pool-test-frames")
	defer s.StopWarmWorkers()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	emit := `
import base64, pickle, sys
sys.stdout.write(base64.b64encode(pickle.dumps({"_m": 10})).decode() + "\n" +
                 "\n".join(base64.b64encode(pickle.dumps(v)).decode() for v in (1, 2)))
`
	out, err := exec.Command(python, "-c", emit).Output()
	if err != nil {
		t.Fatalf("emit pickles: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	globals, _ := base64.StdEncoding.DecodeString(lines[0])
	var items []json.RawMessage
	for _, l := range lines[1:] {
		b, _ := base64.StdEncoding.DecodeString(l)
		items = append(items, b)
	}

	hdr := []byte(`{"func_src": "def run(x):\n    return x + _m\n", "func_name": "run", "items_frames": 2, "globals": true}`)
	body := buildFrames(hdr, globals, items)

	req, err := http.NewRequest("POST", srv.URL+"/v1/pool/map", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/vnd.pipedpeer.frames")
	req.Header.Set("X-Pipedpeer-Store", store)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("frames request got %d: %s", resp.StatusCode, string(b))
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	nl := bytes.IndexByte(bodyBytes, '\n')
	if nl < 0 {
		t.Fatal("frames response has no header line")
	}
	var outHdr struct {
		ResultsFrames int `json:"results_frames"`
	}
	if err := json.Unmarshal(bodyBytes[:nl], &outHdr); err != nil || outHdr.ResultsFrames != 2 {
		t.Fatalf("bad frames response header %q: %v", bodyBytes[:nl], err)
	}
	rest := bodyBytes[nl+1:]
	unpickle := `
import pickle, sys
sys.stdout.write(str(pickle.loads(open(sys.argv[1], "rb").read())))
`
	for i, want := range []int{11, 12} {
		f, r, err := readFrame(rest)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		rest = r
		bp := filepath.Join(t.TempDir(), "r.pkl")
		if err := os.WriteFile(bp, f, 0644); err != nil {
			t.Fatal(err)
		}
		got, err := exec.Command(python, "-c", unpickle, bp).Output()
		if err != nil {
			t.Fatalf("unpickle: %v", err)
		}
		var v int
		fmt.Sscan(string(got), &v)
		if v != want {
			t.Fatalf("result[%d] = %d, want %d", i, v, want)
		}
	}
}
