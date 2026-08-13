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
