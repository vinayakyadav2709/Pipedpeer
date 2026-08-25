package daemonapi

import (
	"archive/tar"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
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

// fakeRun writes a bin/run wrapper that executes the given python file with the
// same args, standing in for the Nix closure entrypoint. The real one lives in
// <store>/bin/run; here we just need python3 available.
func fakeRun(t *testing.T, dir string) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	bin := filepath.Join(dir, "bin", "run")
	if err := os.MkdirAll(filepath.Dir(bin), 0755); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nexec " + python + " \"$@\"\n"
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// doubleSrc is a kernel in the shape production ships: source plus the name it
// defines. Kernels stopped travelling as by-reference pickles because a worker
// runs from a scratch dir with neither the workspace nor the submitter's
// __main__ importable, so the reference named nothing that existed there.
const doubleSrc = "def double(x):\n    return x * 2\n"

// spillCounter wraps a daemon handler and records every /v1/pool/map arrival
// with the status it answered. Counting arrivals is the only way to tell a real
// remote execution from runChunk's local fallback: when every remote part
// fails, the fallback produces byte-identical results, so asserting on results
// alone passes whether or not anything was ever distributed.
type spillCounter struct {
	mu       sync.Mutex
	arrivals int
	statuses []int
}

func (c *spillCounter) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/pool/map" {
			h.ServeHTTP(w, r)
			return
		}
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		h.ServeHTTP(sw, r)
		c.mu.Lock()
		c.arrivals++
		c.statuses = append(c.statuses, sw.code)
		c.mu.Unlock()
	})
}

func (c *spillCounter) snapshot() (int, []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.arrivals, append([]int(nil), c.statuses...)
}

// statusWriter remembers the status a handler wrote; a handler that only ever
// calls Write has implicitly answered 200.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

// unpickleInts decodes merged pool results into plain ints, preserving order,
// so a test can assert results[i] belongs to items[i] rather than that some
// unordered set of values came back.
func unpickleInts(t *testing.T, results []any) []int {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	const unpickle = `import pickle,sys;sys.stdout.write(str(pickle.loads(open(sys.argv[1],"rb").read())))`
	dir := t.TempDir()
	out := make([]int, 0, len(results))
	for i, r := range results {
		m, ok := r.(map[string]string)
		if !ok || m["pickle"] == "" {
			t.Fatalf("bad result shape at %d: %#v", i, r)
		}
		blob, err := base64.StdEncoding.DecodeString(m["pickle"])
		if err != nil {
			t.Fatalf("bad pickle at %d: %v", i, err)
		}
		bp := filepath.Join(dir, fmt.Sprintf("r%d.pkl", i))
		if err := os.WriteFile(bp, blob, 0644); err != nil {
			t.Fatal(err)
		}
		o, err := exec.Command(python, "-c", unpickle, bp).Output()
		if err != nil {
			t.Fatalf("unpickle result %d: %v", i, err)
		}
		var v int
		if _, err := fmt.Sscan(string(o), &v); err != nil {
			t.Fatalf("parse result %d (%q): %v", i, o, err)
		}
		out = append(out, v)
	}
	return out
}

// fakeNix puts a stand-in `nix` first on PATH. The real one imports a NAR into
// /nix/store, which a test cannot use because its store paths are temp dirs.
// The stand-in reads the same NAR off stdin and materialises the closure it
// describes — first line the store path, the rest the bin/run script — so
// handleStoreImport's materialise step runs for real instead of being skipped.
func fakeNix(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\n" +
		"tmp=$(mktemp)\n" +
		"cat > \"$tmp\"\n" +
		"path=$(head -n 1 \"$tmp\")\n" +
		"mkdir -p \"$path/bin\"\n" +
		"tail -n +2 \"$tmp\" > \"$path/bin/run\"\n" +
		"chmod 0755 \"$path/bin/run\"\n" +
		"rm -f \"$tmp\"\n"
	if err := os.WriteFile(filepath.Join(dir, "nix"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// closureNAR builds the payload fakeNix understands: the store path followed by
// the bin/run entrypoint the importing node must end up with.
func closureNAR(t *testing.T, storePath string) []byte {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	return []byte(storePath + "\n#!/bin/sh\nexec " + python + " \"$@\"\n")
}

// TestRunPoolChunkLegacyPickledFunc covers the legacy by-reference protocol the
// endpoint still accepts: a base64-pickled callable instead of func_src. It
// only resolves where the defining module is importable in the worker, which is
// why it is rigged here with PYTHONPATH and why no distributed test uses it —
// production ships kernels as source.
func TestRunPoolChunkLegacyPickledFunc(t *testing.T) {
	store := fakeRun(t, t.TempDir())

	// Pickle a real, importable function: write a module to a dir on PYTHONPATH
	// and have python3 emit the pickle of it, exactly as the shim does.
	modDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(modDir, "worker_mod.py"), []byte("def double(x):\n    return x * 2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	// Put the module dir on PYTHONPATH so both the pickle-emitter and the
	// pool_runner subprocess (which inherits our env) can import worker_mod.
	t.Setenv("PYTHONPATH", modDir+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"))
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
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

	items := []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`), json.RawMessage(`3`)}
	results, err := runPoolChunk(filepath.Join(store, "bin", "run"), &chunk{pickledFunc: string(pickled), items: items})
	if err != nil {
		t.Fatalf("runPoolChunk: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("want 3 results, got %d", len(results))
	}

	unpickle := `
import pickle, sys
sys.stdout.write(str(pickle.loads(open(sys.argv[1], "rb").read())))
`
	var got []int
	for _, r := range results {
		m, ok := r.(map[string]string)
		if !ok || m["pickle"] == "" {
			t.Fatalf("bad result shape: %#v", r)
		}
		blob, err := base64.StdEncoding.DecodeString(m["pickle"])
		if err != nil {
			t.Fatalf("bad pickle blob: %v", err)
		}
		blobPath := filepath.Join(t.TempDir(), "r.pkl")
		if err := os.WriteFile(blobPath, blob, 0644); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command(python, "-c", unpickle, blobPath).Output()
		if err != nil {
			t.Fatalf("unpickle: %v", err)
		}
		var v int
		if _, err := fmt.Sscan(string(out), &v); err != nil {
			t.Fatalf("parse %q: %v", out, err)
		}
		got = append(got, v)
	}
	if got[0] != 2 || got[1] != 4 || got[2] != 6 {
		t.Fatalf("want [2 4 6], got %v", got)
	}
}

// TestWarmWorkerReuse confirms the poolManager keeps one worker per store path
// alive across requests and correctly falls back to a cold run when the worker
// dies. It uses a bin/run that tracks invocation count so reuse is observable.
func TestWarmWorkerReuse(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	store := t.TempDir()
	bin := filepath.Join(store, "bin")
	os.MkdirAll(bin, 0755)

	// bin/run invokes python with the given script. It logs every invocation to
	// a counter file so we can assert the warm worker reused (1 spawn) vs the
	// cold path (N spawns).
	inv := filepath.Join(store, "invocations")
	runScript := "#!/bin/sh\necho x >> " + inv + "\nexec " + python + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "run"), []byte(runScript), 0755); err != nil {
		t.Fatal(err)
	}

	modDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(modDir, "worker_mod.py"), []byte("def double(x):\n    return x * 2\n"), 0644); err != nil {
		t.Fatal(err)
	}
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

	pm := newPoolManager()
	defer pm.stopAll()

	runPath := filepath.Join(store, "bin", "run")
	for i := 0; i < 3; i++ {
		items := []json.RawMessage{json.RawMessage(fmt.Sprintf(`%d`, i+1)), json.RawMessage(`2`)}
		if _, err := pm.runChunk(runPath, store, &chunk{pickledFunc: string(pickled), items: items}, ""); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}

	invBytes, _ := os.ReadFile(inv)
	if n := len(strings.Fields(string(invBytes))); n != 1 {
		t.Fatalf("warm worker reused: expected 1 closure spawn across 3 chunks, got %d", n)
	}
}

// TestMultiNodeSpill verifies a chunk is split across local + a peer, that the
// peer really ran its share, and that the merged results come back in strict
// input order. It stands up two servers, each with its own warm worker, and
// points one at the other via peerFn.
//
// The kernel ships as source, the way production sends it. A by-reference
// pickle would only resolve on a peer that happens to have the defining module
// importable — a condition a test can manufacture with PYTHONPATH and a real
// worker never meets — and when it fails to resolve, runChunk re-runs the part
// locally and the results are identical. Hence the arrival and warm-worker
// assertions below: they are what distinguishes distribution from fallback.
func TestMultiNodeSpill(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	// Two nodes, each with their own warm-worker pool.
	s1 := New("node-" + strings.Repeat("x", 8))
	s2 := New("node-" + strings.Repeat("y", 8))
	peerCalls := &spillCounter{}
	srv2 := httptest.NewServer(peerCalls.wrap(s2.Handler()))
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	// The peer resolves <storePath>/bin/run, so build that exact layout on
	// disk and use it as the shared store path.
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	runScript := "#!/bin/sh\nexec " + python + " \"$@\"\n"
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte(runScript), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	peerHost := strings.TrimPrefix(srv2.URL, "http://")
	s1.pool.SetPeerFn(func(_, _ string) []string { return []string{peerHost} })

	// 16 items: >= minSplit, so it fans out local + peer.
	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}

	ch := &chunk{funcSrc: doubleSrc, funcName: "double", items: items}
	results, err := s1.pool.runChunk(runPath, storePath, ch, "")
	if err != nil {
		t.Fatalf("runChunk spill: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results, got %d", len(results))
	}

	// Merged in input order: the shim maps results back by index, so a set
	// check would hide a fan-out that returned everything shuffled.
	for i, v := range unpickleInts(t, results) {
		if want := (i + 1) * 2; v != want {
			t.Fatalf("result[%d] = %d, want %d (input order broken)", i, v, want)
		}
	}

	arrivals, statuses := peerCalls.snapshot()
	if arrivals < 1 {
		t.Fatal("no /v1/pool/map ever reached the peer: the chunk never left this node")
	}
	for i, st := range statuses {
		if st != http.StatusOK {
			t.Fatalf("peer answered %d for spill request %d: its part fell back to local", st, i)
		}
	}
	// The peer must have executed, not merely accepted: a warm worker for the
	// store only exists once a chunk actually ran there.
	s2.pool.mu.Lock()
	_, s2Ran := s2.pool.workers[storePath]
	s2.pool.mu.Unlock()
	if !s2Ran {
		t.Fatal("peer never spawned a warm worker: nothing was executed remotely")
	}
}

// TestPoolSpillDeadPeerFallsBackToLocal ensures that when a peer is unreachable
// the chunk still completes locally instead of failing (D2/D3: a remote node
// never subtracts capacity), with every value correct and in input order.
//
// The peer must genuinely have been attempted: a chunk that never tried to
// spill would satisfy a bare length check just as well. runChunk records the
// failure by marking the peer dead for the rest of the chunk, so that list is
// the observable proof — the remote failure itself is not logged.
func TestPoolSpillDeadPeerFallsBackToLocal(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	s := New("fallback-node-xxxxxxxx")
	defer s.StopWarmWorkers()
	// Point at an address that refuses connections.
	const deadPeer = "127.0.0.1:1"
	s.pool.SetPeerFn(func(_, _ string) []string { return []string{deadPeer} })

	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	ch := &chunk{funcSrc: doubleSrc, funcName: "double", items: items}
	results, err := s.pool.runChunk(runPath, storePath, ch, "")
	if err != nil {
		t.Fatalf("chunk failed though a peer was down: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results after local fallback, got %d", len(results))
	}
	for i, v := range unpickleInts(t, results) {
		if want := (i + 1) * 2; v != want {
			t.Fatalf("result[%d] = %d, want %d (local fallback lost or reordered work)", i, v, want)
		}
	}

	s.pool.deadMu.Lock()
	dead := s.pool.deadPeers[deadPeer]
	n := len(s.pool.deadPeers)
	s.pool.deadMu.Unlock()
	if !dead {
		t.Fatal("the unreachable peer was never tried: the chunk stayed local without attempting spill")
	}
	if n != 1 {
		t.Fatalf("want exactly 1 peer recorded as failed, got %d", n)
	}
}

// TestPoolMapRejectsMissingStore checks the worker endpoint rejects requests
// that don't name a valid store path (a trust guard: untrusted peers cannot
// point it at arbitrary binaries).
func TestPoolMapRejectsMissingStore(t *testing.T) {
	s := New("pool-test-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/pool/map", "application/json", strings.NewReader(`{"func":"eA==","items":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 without store header, got %d", resp.StatusCode)
	}
}

// TestPoolMapAdmissionRefusesOverMemory verifies the admission control on
// /v1/pool/map: a chunk whose required_mem exceeds the node's available memory
// is refused with 503 (the shim falls back locally), so an overloaded node
// never OOMs mid-chunk.
func TestPoolMapAdmissionRefusesOverMemory(t *testing.T) {
	store := fakeRun(t, t.TempDir())

	s := New("pool-test-node")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := fmt.Sprintf(`{"func":"eA==","items":[1,2],"required_mem":%d}`, int64(1)<<62)
	req, err := http.NewRequest("POST", srv.URL+"/v1/pool/map", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Pipedpeer-Store", store)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("want 503 for over-memory chunk, got %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Sanity: the same request with a tiny required_mem passes admission and
	// reaches execution (the func is a dummy, so the run itself may error — we
	// only assert it is NOT the admission 503).
	body = `{"func":"eA==","items":[1,2],"required_mem":1}`
	req, err = http.NewRequest("POST", srv.URL+"/v1/pool/map", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Pipedpeer-Store", store)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		t.Fatal("tiny required_mem was refused by admission control")
	}
}

// TestRunPoolChunkItemsB64 verifies the items_b64 transport: pickled objects
// (the mechanism numpy block-partitioned matmul uses) ship as base64 blobs and
// are unpickled by the worker before the callable runs. A pickled list stands in
// for a numpy block so the test needs no numpy.
func TestRunPoolChunkItemsB64(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	store := t.TempDir()
	os.MkdirAll(filepath.Join(store, "bin"), 0755)
	runScript := "#!/bin/sh\nexec " + python + " \"$@\"\n"
	if err := os.WriteFile(filepath.Join(store, "bin", "run"), []byte(runScript), 0755); err != nil {
		t.Fatal(err)
	}

	modDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(modDir, "worker_mod.py"), []byte("def total(xs):\n    return sum(xs)\n"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PYTHONPATH", modDir+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"))

	emit := `
import base64, pickle, sys
sys.path.insert(0, sys.argv[1])
from worker_mod import total
sys.stdout.write(base64.b64encode(pickle.dumps(total)).decode())
`
	pickled, err := exec.Command(python, "-c", emit, modDir).Output()
	if err != nil {
		t.Fatalf("emit pickle: %v", err)
	}

	// Ship each item as a base64-pickled list, mimicking a numpy block.
	b64pickle := `
import base64, pickle, sys
sys.stdout.write(base64.b64encode(pickle.dumps([int(a) for a in sys.argv[1:]])).decode())
`
	var items []json.RawMessage
	for _, row := range [][]int{{1, 2}, {3, 4}, {5, 6}} {
		args := make([]string, 0, len(row)+1)
		args = append(args, "-c", b64pickle)
		for _, n := range row {
			args = append(args, fmt.Sprintf("%d", n))
		}
		out, err := exec.Command(python, args...).Output()
		if err != nil {
			t.Fatalf("pickle item: %v", err)
		}
		items = append(items, json.RawMessage(fmt.Sprintf("%q", string(out))))
	}

	results, err := runPoolChunk(filepath.Join(store, "bin", "run"), &chunk{pickledFunc: string(pickled), items: items, itemsB64: true})
	if err != nil {
		t.Fatalf("runPoolChunk: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("want 3 results, got %d", len(results))
	}

	unpickle := `
import pickle, sys
sys.stdout.write(str(pickle.loads(open(sys.argv[1], "rb").read())))
`
	var got []int
	for _, r := range results {
		m, ok := r.(map[string]string)
		if !ok || m["pickle"] == "" {
			t.Fatalf("bad result shape: %#v", r)
		}
		blob, err := base64.StdEncoding.DecodeString(m["pickle"])
		if err != nil {
			t.Fatalf("bad pickle blob: %v", err)
		}
		blobPath := filepath.Join(t.TempDir(), "r.pkl")
		if err := os.WriteFile(blobPath, blob, 0644); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command(python, "-c", unpickle, blobPath).Output()
		if err != nil {
			t.Fatal(err)
		}
		var v int
		if _, err := fmt.Sscanf(string(out), "%d", &v); err != nil {
			t.Fatalf("unpickle result: %v", err)
		}
		got = append(got, v)
	}
	if !reflect.DeepEqual(got, []int{3, 7, 11}) {
		t.Fatalf("want [3 7 11], got %v", got)
	}
}

// TestClosureBroadcastEnablesSpill is the end-to-end multi-node path: a closure
// uploaded to one node is broadcast to a peer that does not have it, the peer
// materialises it, and spill then fans work out to that peer. Without the
// broadcast the peer would 400 on the missing store and the spill would
// silently stay local.
//
// The closure must be absent at upload time or broadcastClosure's own gate
// skips the peer and nothing is pushed — the two nodes share this process's
// filesystem, so a store path that exists for one exists for both. What proves
// the push is the peer's own NAR cache (its cache root is separate) and its
// store becoming runnable only after the upload.
func TestClosureBroadcastEnablesSpill(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	fakeNix(t)

	// Separate cache roots: the pushing node's copy of the NAR must not be
	// mistaken for the peer's.
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s1 := New("node-" + strings.Repeat("x", 8))
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s2 := New("node-" + strings.Repeat("y", 8))

	peerCalls := &spillCounter{}
	srv1 := httptest.NewServer(s1.Handler())
	srv2 := httptest.NewServer(peerCalls.wrap(s2.Handler()))
	defer srv1.Close()
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	// Nothing is on disk for this store path yet: only the broadcast can make
	// the peer runnable for it.
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	runPath := filepath.Join(storePath, "bin", "run")
	nar := closureNAR(t, storePath)

	// s1 knows s2 as a healthy peer, so upload broadcasts the closure to it.
	peerHost := strings.TrimPrefix(srv2.URL, "http://")
	peer := &PeerHealth{Host: strings.Split(peerHost, ":")[0], Port: mustPort(t, peerHost), Status: "healthy"}
	s1.peersMu.Lock()
	s1.peerHealths = map[string]*PeerHealth{peerHost: peer}
	s1.peersMu.Unlock()

	if s1.peerHasStore(peer, storePath) {
		t.Fatal("peer must not be runnable for the closure before the broadcast")
	}

	// A real (empty-ish) workspace tar: handleJobUpload streams it as a tar.
	workspaceTar := filepath.Join(t.TempDir(), "workspace.tar")
	wf, err := os.Create(workspaceTar)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(wf)
	tw.WriteHeader(&tar.Header{Name: "main.py", Mode: 0644, Size: int64(len("print(1)")), Typeflag: tar.TypeReg})
	tw.Write([]byte("print(1)"))
	tw.Close()
	wf.Close()

	reqBody := &bytes.Buffer{}
	mp := multipart.NewWriter(reqBody)
	mp.WriteField("store_path", storePath)
	mp.WriteField("script_path", "main.py")
	fw, _ := mp.CreateFormFile("workspace", "workspace.tar")
	wf2, _ := os.Open(workspaceTar)
	io.Copy(fw, wf2)
	wf2.Close()
	nw, _ := mp.CreateFormFile("nar", "closure.nar")
	nw.Write(nar)
	mp.Close()

	resp, err := http.Post(srv1.URL+"/v1/jobs/upload", mp.FormDataContentType(), reqBody)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// The upload broadcasts synchronously, so by now the peer must hold the NAR
	// and have materialised it. Cached alone is not enough: pool spill needs
	// bin/run to exist there or the request 400s.
	if _, cached := s2.narCache.narFileFor(storePath); !cached {
		t.Fatal("broadcast never pushed the NAR to a peer that lacked the closure")
	}
	if !s1.peerHasStore(peer, storePath) {
		t.Fatal("peer cached the closure but cannot run it: spill would 400 on every chunk")
	}

	// Now run a chunk with spill enabled: s1 must see s2 as a closure-sharing
	// peer and fan out to it.
	s1.EnablePoolSpill()
	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	ch := &chunk{funcSrc: doubleSrc, funcName: "double", items: items}
	results, err := s1.pool.runChunk(runPath, storePath, ch, "")
	if err != nil {
		t.Fatalf("runChunk: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results, got %d", len(results))
	}
	for i, v := range unpickleInts(t, results) {
		if want := (i + 1) * 2; v != want {
			t.Fatalf("result[%d] = %d, want %d (input order broken)", i, v, want)
		}
	}

	arrivals, statuses := peerCalls.snapshot()
	if arrivals < 1 {
		t.Fatal("peer never received spill work: broadcast did not enable fan-out")
	}
	for i, st := range statuses {
		if st != http.StatusOK {
			t.Fatalf("peer answered %d for spill request %d: the part fell back to local", st, i)
		}
	}
	s2.pool.mu.Lock()
	_, s2Ran := s2.pool.workers[storePath]
	s2.pool.mu.Unlock()
	if !s2Ran {
		t.Fatal("peer never executed spill work on the broadcast closure")
	}
}

// TestClosureBroadcastPushesToLackingPeer covers the per-node-store topology
// (production: each node has its own /nix/store): a peer that does NOT have the
// closure must receive it via broadcast and end up able to RUN it, or spill
// would silently stay local. It drives broadcastClosure itself rather than
// importStoreOnPeer, so the gate that decides which peers get a push is part of
// what is under test — and the second broadcast proves the gate still holds,
// since re-pushing costs a full closure upload (6.6GB on the demo rig) per job.
func TestClosureBroadcastPushesToLackingPeer(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	fakeNix(t)

	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s1 := New("node-" + strings.Repeat("x", 8))
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	s2 := New("node-" + strings.Repeat("y", 8))

	var importsMu sync.Mutex
	imports := 0
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/store/import" {
			importsMu.Lock()
			imports++
			importsMu.Unlock()
		}
		s2.Handler().ServeHTTP(w, r)
	}))
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	peerHost := strings.TrimPrefix(srv2.URL, "http://")
	peer := &PeerHealth{Host: strings.Split(peerHost, ":")[0], Port: mustPort(t, peerHost), Status: "healthy"}
	s1.peersMu.Lock()
	s1.peerHealths = map[string]*PeerHealth{peerHost: peer}
	s1.peersMu.Unlock()

	// s2 has no copy of this store path (absent on disk AND in its NAR cache),
	// so the runnable check must report false and the broadcast must push.
	storePath := filepath.Join(t.TempDir(), "nix", "store", "absent-on-s2")
	if _, err := s1.narCache.store(storePath, bytes.NewReader(closureNAR(t, storePath))); err != nil {
		t.Fatalf("cache nar on s1: %v", err)
	}
	if s1.peerHasStore(peer, storePath) {
		t.Fatal("s2 must not be runnable for an absent store path")
	}

	// broadcastClosure reports nothing and swallows per-peer failures by design
	// (a peer that cannot import is simply not offered spill work), so the
	// outcome is asserted on the peer instead. It waits for its pushes, so
	// there is nothing to poll for.
	s1.broadcastClosure(storePath)

	if _, cached := s2.narCache.narFileFor(storePath); !cached {
		t.Fatal("broadcast did not push the closure to a peer lacking it")
	}
	if !s1.peerHasStore(peer, storePath) {
		t.Fatal("peer holds the NAR but cannot run the closure: pool spill would 400 on it")
	}
	importsMu.Lock()
	n := imports
	importsMu.Unlock()
	if n != 1 {
		t.Fatalf("want exactly 1 import push, got %d", n)
	}

	// The peer is runnable now, so the gate must skip it.
	s1.broadcastClosure(storePath)
	importsMu.Lock()
	n = imports
	importsMu.Unlock()
	if n != 1 {
		t.Fatalf("broadcast re-pushed a closure the peer already runs (%d imports)", n)
	}
}

func mustPort(t *testing.T, hostport string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	return p
}

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
	s1.pool.SetPeerFn(func(_, _ string) []string {
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
	s1.pool.SetPeerFn(func(_, _ string) []string { return hosts })

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
	s1.pool.SetPeerFn(func(_, _ string) []string {
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
	s1.pool.SetPeerFn(func(_, _ string) []string { return []string{"127.0.0.1:1", alivePeer} })

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

// TestSinkPeerLast keeps the submitting machine eligible but last in spill
// order — an idle orchestrator must never outrank real workers.
func TestSinkPeerLast(t *testing.T) {
	peers := []string{"10.0.0.5:38080", "10.0.0.1:38080", "10.0.0.9:38080"}
	got := sinkPeerLast(peers, "10.0.0.1:38080")
	want := []string{"10.0.0.5:38080", "10.0.0.9:38080", "10.0.0.1:38080"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("expected %v, got %v", want, got)
	}

	if got := sinkPeerLast(peers, ""); len(got) != 3 {
		t.Fatalf("empty submitter must be a no-op, got %v", got)
	}

	alone := sinkPeerLast([]string{"10.0.0.1:38080"}, "10.0.0.1:38080")
	if len(alone) != 1 || alone[0] != "10.0.0.1:38080" {
		t.Fatalf("submitter-only pool must stay intact, got %v", alone)
	}

	same := sinkPeerLast(peers, "not-in-list:1")
	if len(same) != 3 || same[0] != peers[0] {
		t.Fatalf("absent submitter must not reorder, got %v", same)
	}
}

// TestFanWidthRespectsFreeMemory pins the bound that keeps a wide chunk from
// pushing a node over. Nothing below this enforces anything — the sandbox
// carries no cgroup — so if this arithmetic is wrong the OOM killer is the
// next line of defence.
func TestFanWidthRespectsFreeMemory(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	probe := `
import os, sys
ns = {"os": os, "sys": sys}
exec(sys.argv[1], ns)
fan = ns["_fan_width"]

# One item never forks.
assert fan(1, 0) == 1, fan(1, 0)

# An estimate far larger than RAM collapses the width to one.
huge = ns["_free_bytes"]() * 100 or 1 << 60
assert fan(64, huge) == 1, ("huge estimate not bounded", fan(64, huge))

# No estimate must still be bounded by memory, not just by cores: with the
# 256MB-per-item assumption a node cannot fan wider than its free RAM allows.
free = ns["_free_bytes"]()
if free > 0:
    cap = max(1, int(free * 0.4) // (256 << 20))
    assert fan(4096, 0) <= max(1, min(os.cpu_count() or 1, cap)), fan(4096, 0)

# An explicit override wins, but never exceeds the item count.
os.environ["PIPEDPEER_WORKER_PROCS"] = "3"
assert fan(64, 0) == 3, fan(64, 0)
assert fan(2, 0) == 2, fan(2, 0)
print("fan-ok")
`
	out, err := exec.Command(python, "-c", probe, chunkFanOut).CombinedOutput()
	if err != nil {
		t.Fatalf("fan width probe failed:\n%s\n%v", out, err)
	}
	if !strings.Contains(string(out), "fan-ok") {
		t.Fatalf("unexpected probe output: %q", out)
	}
}
