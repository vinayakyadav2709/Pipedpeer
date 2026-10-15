package phone

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var oldPyVersionOf = pyVersionOf

func stub(t *testing.T, pool bool, version string) {
	t.Helper()
	oldPool, oldLook, oldVer := hostChunks, lookPython, pyVersionOf
	t.Cleanup(func() {
		hostChunks, lookPython, pyVersionOf = oldPool, oldLook, oldVer
		pyOnce = sync.Once{}
	})
	pyOnce = sync.Once{}
	hostChunks = func() bool { return pool }
	lookPython = func() (string, error) { return "/termux/bin/python3", nil }
	pyVersionOf = func(string) (string, error) { return version, nil }
}

func TestIsTermux(t *testing.T) {
	t.Setenv("TERMUX_VERSION", "")
	t.Setenv("PREFIX", "/usr")
	if IsTermux() {
		t.Fatal("plain linux taken for Termux")
	}
	t.Setenv("PREFIX", "/data/data/com.termux/files/usr")
	if !IsTermux() {
		t.Fatal("Termux PREFIX not detected")
	}
	t.Setenv("PREFIX", "")
	t.Setenv("TERMUX_VERSION", "0.118.1")
	if !IsTermux() {
		t.Fatal("TERMUX_VERSION not detected")
	}
}

func storeWithRun(t *testing.T) string {
	store := t.TempDir()
	if err := os.MkdirAll(filepath.Join(store, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "bin", "run"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestPoolRunPath(t *testing.T) {
	store := storeWithRun(t)
	missing := filepath.Join(t.TempDir(), "nix-store-abc")

	stub(t, false, "3.12")
	if p, err := PoolRunPath(store); err != nil || p != filepath.Join(store, "bin", "run") {
		t.Fatalf("closure present: got %q, %v", p, err)
	}
	if _, err := PoolRunPath(missing); err == nil {
		t.Fatal("full node without the closure must refuse")
	}

	stub(t, true, "3.12")
	if p, err := PoolRunPath(store); err != nil || p != filepath.Join(store, "bin", "run") {
		t.Fatalf("pool-only with closure should still prefer bin/run: %q, %v", p, err)
	}
	if p, err := PoolRunPath(missing); err != nil || p != "/termux/bin/python3" {
		t.Fatalf("pool-only without closure: got %q, %v", p, err)
	}
}

func TestCanRun(t *testing.T) {
	store := storeWithRun(t)
	missing := filepath.Join(t.TempDir(), "nix-store-abc")
	hdr := func(v string) http.Header {
		h := http.Header{}
		if v != "" {
			h.Set(PythonHeader, v)
		}
		return h
	}

	stub(t, false, "3.12")
	if !CanRun(store, hdr("")) {
		t.Fatal("materialised closure must be runnable")
	}
	if CanRun(missing, hdr("3.12")) {
		t.Fatal("full node without the closure must answer not runnable")
	}

	stub(t, true, "3.12")
	for v, want := range map[string]bool{"3.12": true, "3.12.4": true, "3.11.9": false, "": false, "3": false} {
		if got := CanRun(missing, hdr(v)); got != want {
			t.Errorf("pool-only, header %q: got %v want %v", v, got, want)
		}
	}
	// func_src is re-compiled on the phone: a minor mismatch is fine.
	if !CanRun(missing, Need("3.14", true)) || !CanRun(missing, Need("", true)) {
		t.Error("pool-only must take a func_src chunk from another minor version")
	}
	if CanRun(missing, Need("3.14", false)) {
		t.Error("pool-only must refuse by-value bytecode from another minor version")
	}
}

// TestTermuxPython runs only inside Termux (the termux-docker image under
// qemu, or a real phone): the host python is found, reports a minor version,
// and has numpy.
func TestTermuxPython(t *testing.T) {
	if !IsTermux() {
		t.Skip("not in Termux")
	}
	stub(t, true, "")
	lookPython = func() (string, error) { return exec.LookPath("python3") }
	pyVersionOf = oldPyVersionOf
	py, v, err := hostPython()
	if err != nil || minor(v) == "" {
		t.Fatalf("host python: %q %q %v", py, v, err)
	}
	if !CanRun("/nix/store/none", http.Header{PythonHeader: {v}}) {
		t.Fatalf("pool-only Termux node refuses its own python %s", v)
	}
	if out, err := exec.Command(py, "-c", "import numpy; print(numpy.__version__)").CombinedOutput(); err != nil {
		t.Fatalf("numpy: %v %s", err, out)
	}
}
