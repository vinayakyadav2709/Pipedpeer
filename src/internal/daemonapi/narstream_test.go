package daemonapi

import (
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// TestAStoreImportStreamsTheArchive.
//
// ParseMultipartForm held up to 512 MiB of an archive in memory; a multi-GB
// CUDA closure took receivers to ~1 GB RSS and the kernel killed them
// mid-broadcast. A 96 MiB archive must now cost far less than its own size.
func TestAStoreImportStreamsTheArchive(t *testing.T) {
	const size = 96 << 20
	const bound = 32 << 20

	dir := t.TempDir()
	s := New("test-node")
	s.narCache = &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}
	// A store path whose entrypoint exists, so materializeClosure has nothing
	// to import and the test needs no nix.
	storePath := filepath.Join(dir, "store-root")
	if err := os.MkdirAll(filepath.Join(storePath, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storePath, "bin", "run"), nil, 0o755); err != nil {
		t.Fatal(err)
	}

	pr, pw := io.Pipe()
	mp := multipart.NewWriter(pw)
	go func() {
		_ = mp.WriteField("store_path", storePath)
		fw, err := mp.CreateFormFile("nar", "closure.nar")
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(fw, io.LimitReader(zeroReader{}, size)); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.CloseWithError(mp.Close())
	}()

	req := httptest.NewRequest(http.MethodPost, "/v1/store/import", pr)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	rec := httptest.NewRecorder()

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	s.handleStoreImport(rec, req)
	runtime.ReadMemStats(&after)

	if rec.Code != http.StatusOK {
		t.Fatalf("import returned %d: %s", rec.Code, rec.Body.String())
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > bound {
		t.Errorf("importing a %d MiB archive allocated %d MiB; want under %d MiB", size>>20, alloc>>20, bound>>20)
	}
	path, ok := s.narCache.narFileFor(storePath)
	if !ok {
		t.Fatal("the archive was not cached")
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() != size {
		t.Errorf("cached archive: %v, size %v; want %d bytes", err, fi, size)
	}
}

func TestAClosureNeedsAGPUOnlyWhenItCarriesCUDA(t *testing.T) {
	cases := []struct {
		paths []string
		want  bool
	}{
		{[]string{"/nix/store/aaa-python3-3.12.8", "/nix/store/bbb-torch-cu126-site"}, true},
		{[]string{"/nix/store/aaa-torch-cu126-wheels"}, true},
		{[]string{"/nix/store/aaa-cuda_cudart-12.6.77"}, true},
		{[]string{"/nix/store/aaa-cudatoolkit-12.6"}, true},
		{[]string{"/nix/store/aaa-python3.12-torch-2.5.1", "/nix/store/bbb-curl-8.11", "/nix/store/ccc-cups-2.4"}, false},
		{[]string{"/nix/store/aaa-python3.12-cupy-13.3"}, false},
		{nil, false},
	}
	for _, c := range cases {
		if got := closureNeedsGPU(c.paths); got != c.want {
			t.Errorf("closureNeedsGPU(%v) = %v, want %v", c.paths, got, c.want)
		}
	}
}

func TestAGPUClosureGoesOnlyToGPUPeers(t *testing.T) {
	peers := []PeerHealth{
		{Host: "10.0.0.1", Port: 1, GPU: "nvidia"},
		{Host: "10.0.0.2", Port: 1},
		{Host: "10.0.0.3", Port: 1, GPU: "amd"}, // cannot use CUDA torch
	}
	keep, skipped := gpuPeersOnly(peers)
	if len(keep) != 1 || keep[0].Host != "10.0.0.1" || len(skipped) != 2 {
		t.Errorf("gpuPeersOnly kept %v, skipped %v; want only 10.0.0.1, 2 skipped", keep, skipped)
	}
}
