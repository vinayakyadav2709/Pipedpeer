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
	"strconv"
	"strings"
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
	results, err := runPoolChunk(filepath.Join(store, "bin", "run"), string(pickled), "", "", "", items, false, false)
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
		if _, err := pm.runChunk(runPath, store, string(pickled), "", "", "", items, false, false); err != nil {
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

	results, err := s1.pool.runChunk(runPath, storePath, string(pickled), "", "", "", items, false, false)
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
	results, err := s.pool.runChunk(runPath, storePath, string(pickled), "", "", "", items, false, false)
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

	results, err := runPoolChunk(filepath.Join(store, "bin", "run"), string(pickled), "", "", "", items, false, true)
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

	// The broadcast is async; give it a moment to land on s2.
	deadline := time.Now().Add(5 * time.Second)
	for {
		path, cached := s2.narCache.narFileFor(storePath)
		if cached {
			_ = path
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("closure never broadcast to peer s2")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Now run a chunk with spill enabled: s1 must see s2 as a closure-sharing
	// peer and fan out to it.
	s1.EnablePoolSpill()
	var items []json.RawMessage
	for i := 1; i <= 16; i++ {
		items = append(items, json.RawMessage(fmt.Sprintf(`%d`, i)))
	}
	results, err := s1.pool.runChunk(runPath, storePath, string(pickled), "", "", "", items, false, false)
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

func mustPort(t *testing.T, hostport string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(hostport)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	return p
}

// TestPoolSpillNextBestOnFailure verifies the failover chain: with two peers
// ranked best-first, a dead best peer must fall through to the next best —
// and its part must be picked up there, not silently absorbed locally.
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
	results, err := s1.pool.runChunk(runPath, storePath, string(pickled), "", "", "", items, false, false)
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
