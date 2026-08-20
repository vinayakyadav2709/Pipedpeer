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

// TestRunPoolChunk exercises the full worker path: pickled func + items in,
// base64-pickled results out, matching what the sitecustomize shim sends and
// expects.
func TestRunPoolChunk(t *testing.T) {
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
		if _, err := pm.runChunk(runPath, store, &chunk{pickledFunc: string(pickled), items: items}); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}

	invBytes, _ := os.ReadFile(inv)
	if n := len(strings.Fields(string(invBytes))); n != 1 {
		t.Fatalf("warm worker reused: expected 1 closure spawn across 3 chunks, got %d", n)
	}
}

// TestMultiNodeSpill verifies a chunk is split across local + a peer when a
// peer is available, with results merged in order. It stands up two servers,
// each with its own warm worker, and points one at the other via peerFn.
func TestMultiNodeSpill(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	// Build a shared pickled func (both nodes import the same worker_mod).
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

	// Two nodes, each with their own warm-worker pool.
	s1 := New("node-" + strings.Repeat("x", 8))
	s2 := New("node-" + strings.Repeat("y", 8))
	srv1 := httptest.NewServer(s1.Handler())
	srv2 := httptest.NewServer(s2.Handler())
	defer srv1.Close()
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
	s1.pool.SetPeerFn(func(_ string) []string { return []string{peerHost} })

	// 16 items: >= minSplit, so it fans out local + peer.
	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}

	results, err := s1.pool.runChunk(runPath, storePath, &chunk{pickledFunc: string(pickled), items: items})
	if err != nil {
		t.Fatalf("runChunk spill: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results, got %d", len(results))
	}
	// Spot check values are doubles of 1..16 (order may be non-deterministic
	// across the fan-out).
	seen := map[int]bool{}
	for _, r := range results {
		m, _ := r.(map[string]string)
		blob, _ := base64.StdEncoding.DecodeString(m["pickle"])
		unpickle := `import pickle,sys;sys.stdout.write(str(pickle.loads(open(sys.argv[1],"rb").read())))`
		bp := filepath.Join(t.TempDir(), "p")
		os.WriteFile(bp, blob, 0644)
		out, err := exec.Command(python, "-c", unpickle, bp).Output()
		if err != nil {
			t.Fatalf("unpickle: %v", err)
		}
		var v int
		fmt.Sscan(string(out), &v)
		seen[v] = true
	}
	for i := 2; i <= 32; i += 2 {
		if !seen[i] {
			t.Fatalf("missing result %d in %v", i, seen)
		}
	}
}

// TestPoolSpillDeadPeerFallsBackToLocal ensures that when a peer is unreachable
// the chunk still completes locally instead of failing (D2/D3: a remote node
// never subtracts capacity).
func TestPoolSpillDeadPeerFallsBackToLocal(t *testing.T) {
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

	s := New("fallback-node-xxxxxxxx")
	defer s.StopWarmWorkers()
	// Point at an address that refuses connections.
	s.pool.SetPeerFn(func(_ string) []string { return []string{"127.0.0.1:1"} })

	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	results, err := s.pool.runChunk(runPath, storePath, &chunk{pickledFunc: string(pickled), items: items})
	if err != nil {
		t.Fatalf("chunk failed though a peer was down: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results after local fallback, got %d", len(results))
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

// TestClosureBroadcastEnablesSpill is the end-to-end multi-node path: a
// closure uploaded to one node is broadcast to healthy peers, and the peer
// then accepts pool spill work for that store. Without the broadcast the peer
// would 400 on the missing store and the spill would silently stay local.
func TestClosureBroadcastEnablesSpill(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	// Shared closure layout on both nodes: <store>/bin/run.
	storePath := filepath.Join(t.TempDir(), "nix", "store", "fake")
	os.MkdirAll(filepath.Join(storePath, "bin"), 0755)
	os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\nexec "+python+" \"$@\"\n"), 0755)
	runPath := filepath.Join(storePath, "bin", "run")

	// A pickled function both nodes can import.
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

	s1 := New("node-" + strings.Repeat("x", 8))
	s2 := New("node-" + strings.Repeat("y", 8))
	srv1 := httptest.NewServer(s1.Handler())
	srv2 := httptest.NewServer(s2.Handler())
	defer srv1.Close()
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	// s1 knows s2 as a healthy peer, so upload broadcasts the closure to it.
	peerHost := strings.TrimPrefix(srv2.URL, "http://")
	s1.peersMu.Lock()
	s1.peerHealths = map[string]*PeerHealth{
		peerHost: {Host: strings.Split(peerHost, ":")[0], Port: mustPort(t, peerHost), Status: "healthy"},
	}
	s1.peersMu.Unlock()

	// Simulate a submitter uploading the NAR closure to s1.
	narPath := filepath.Join(t.TempDir(), "closure.nar")
	os.WriteFile(narPath, []byte("fake-nar-bytes"), 0644)

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
	nw.Write([]byte("fake-nar-bytes"))
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

	// This test's two nodes share one store path on the host filesystem, so s2
	// is already runnable for it. A broadcast must NOT push the NAR again — the
	// shared-store shortcut (D) is exactly what kills the redundant 6.6GB push
	// on the demo rig. Assert the broadcast skipped s2 and the spill still fans
	// out to it.
	if !s2.peerHasStore(&PeerHealth{Host: strings.Split(peerHost, ":")[0], Port: mustPort(t, peerHost)}, storePath) {
		t.Fatal("s2 should be runnable for the shared store path")
	}
	if _, cached := s2.narCache.narFileFor(storePath); cached {
		t.Fatal("broadcast pushed the NAR to a peer that already holds the closure")
	}

	// Now run a chunk with spill enabled: s1 must see s2 as a closure-sharing
	// peer and fan out to it.
	s1.EnablePoolSpill()
	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	results, err := s1.pool.runChunk(runPath, storePath, &chunk{pickledFunc: string(pickled), items: items})
	if err != nil {
		t.Fatalf("runChunk: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results, got %d", len(results))
	}

	// At least one result must have come from s2's warm worker, proving the
	// fan-out reached the peer. Count s2 worker spawns via its invocation log
	// (the store's bin/run is shared; the pool writes no per-node log). Instead
	// assert spill happened by asking s2's pool for worker count: s2 ran its
	// part only if a worker exists there.
	s2.pool.mu.Lock()
	_, s2Ran := s2.pool.workers[storePath]
	s2.pool.mu.Unlock()
	if !s2Ran {
		t.Fatal("peer never executed spill work: broadcast did not enable fan-out")
	}
}

// TestClosureBroadcastPushesToLackingPeer covers the per-node-store topology
// (production: each node has its own /nix/store): a peer that does NOT have the
// closure must still receive it via broadcast, or spill would silently stay
// local. The shared-store shortcut only applies when bin/run already exists.
func TestClosureBroadcastPushesToLackingPeer(t *testing.T) {
	s1 := New("node-" + strings.Repeat("x", 8))
	s2 := New("node-" + strings.Repeat("y", 8))
	srv1 := httptest.NewServer(s1.Handler())
	srv2 := httptest.NewServer(s2.Handler())
	defer srv1.Close()
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	peerHost := strings.TrimPrefix(srv2.URL, "http://")
	s1.peersMu.Lock()
	s1.peerHealths = map[string]*PeerHealth{
		peerHost: {Host: strings.Split(peerHost, ":")[0], Port: mustPort(t, peerHost), Status: "healthy"},
	}
	s1.peersMu.Unlock()

	// s2 has no copy of this store path (absent on disk AND in its NAR cache),
	// so the runnable check must report false and the broadcast must push.
	storePath := filepath.Join(t.TempDir(), "nix", "store", "absent-on-s2")
	narPath := filepath.Join(t.TempDir(), "closure.nar")
	os.WriteFile(narPath, []byte("fake-nar-bytes"), 0644)

	if s2.peerHasStore(&PeerHealth{Host: strings.Split(peerHost, ":")[0], Port: mustPort(t, peerHost)}, storePath) {
		t.Fatal("s2 must not be runnable for an absent store path")
	}

	// handleStoreImport caches the NAR before materialising it, so a push is
	// proven by the cache landing even though the fake NAR cannot be imported.
	_ = importStoreOnPeer(&PeerHealth{Host: strings.Split(peerHost, ":")[0], Port: mustPort(t, peerHost)}, storePath, narPath)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, cached := s2.narCache.narFileFor(storePath); cached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("broadcast did not push the closure to a peer lacking it")
		}
		time.Sleep(50 * time.Millisecond)
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
	s1.pool.SetPeerFn(func(_ string) []string {
		return []string{strings.TrimPrefix(peerSrv.URL, "http://")}
	})

	// 12 items: split into part 0 (items 1..6 → slow peer) and part 1
	// (items 7..12 → fast local).
	var items []json.RawMessage
	for i := 1; i <= 12; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	results, err := s1.pool.runChunk(runPath, storePath, &chunk{pickledFunc: string(pickled), items: items})
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

	// Three fake peers; each records which items it received.
	type peerRec struct {
		mu    sync.Mutex
		items []int
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
			rec.mu.Unlock()
			res := make([]map[string]string, 0, len(items))
			for _, it := range items {
				res = append(res, map[string]string{"pickle": pickles[strconv.Itoa(it*2)]})
			}
			writeJSON(w, http.StatusOK, map[string]any{"results": res})
		}))
		defer srvs[i].Close()
	}

	s1 := New("node-" + strings.Repeat("x", 8))
	defer s1.StopWarmWorkers()
	s1.pool.SetPeerFn(func(_ string) []string {
		hosts := make([]string, 3)
		for i := range srvs {
			hosts[i] = strings.TrimPrefix(srvs[i].URL, "http://")
		}
		return hosts
	})

	// 4 items over 3 peers: part 0→peer0, part 1→peer1, part 2→peer2,
	// part 3 wraps to peer0 (3 % 3 == 0).
	items := []json.RawMessage{
		json.RawMessage(`1`), json.RawMessage(`2`),
		json.RawMessage(`3`), json.RawMessage(`4`),
	}
	results, err := s1.pool.runChunk(runPath, storePath, &chunk{items: items, noSplit: true})
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
}
func TestPoolSpillNextBestOnFailure(t *testing.T) {
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

	s1 := New("node-" + strings.Repeat("x", 8))
	s2 := New("node-" + strings.Repeat("y", 8))
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()
	defer s1.StopWarmWorkers()
	defer s2.StopWarmWorkers()

	alivePeer := strings.TrimPrefix(srv2.URL, "http://")
	// Best first, but the best is dead (port 1 refuses connections).
	s1.pool.SetPeerFn(func(_ string) []string { return []string{"127.0.0.1:1", alivePeer} })

	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	results, err := s1.pool.runChunk(runPath, storePath, &chunk{pickledFunc: string(pickled), items: items})
	if err != nil {
		t.Fatalf("chunk failed: %v", err)
	}
	if len(results) != 16 {
		t.Fatalf("want 16 results, got %d", len(results))
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
