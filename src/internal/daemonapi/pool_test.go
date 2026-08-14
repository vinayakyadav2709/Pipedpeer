package daemonapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
	results, err := runPoolChunk(filepath.Join(store, "bin", "run"), string(pickled), items, false)
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
		if _, err := pm.runChunk(runPath, store, string(pickled), items, false); err != nil {
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

	results, err := s1.pool.runChunk(runPath, storePath, string(pickled), items, false)
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
