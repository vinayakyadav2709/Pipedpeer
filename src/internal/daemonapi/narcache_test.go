package daemonapi

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNarCacheContentAddressed(t *testing.T) {
	dir := t.TempDir()
	c := &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}

	src, err := os.CreateTemp(dir, "nar-*.nar")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.WriteString("closure-bytes"); err != nil {
		t.Fatal(err)
	}
	src.Close()

	f, _ := os.Open(src.Name())
	p1, err := c.store("/nix/store/abc", f)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Second store for the same path returns the same file without rewriting.
	f2, _ := os.Open(src.Name())
	p2, err := c.store("/nix/store/abc", f2)
	f2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatalf("expected same cached path, got %s vs %s", p1, p2)
	}

	// Different store path gets a different cache entry.
	f3, _ := os.Open(src.Name())
	p3, err := c.store("/nix/store/xyz", f3)
	f3.Close()
	if err != nil {
		t.Fatal(err)
	}
	if p3 == p2 {
		t.Fatal("different store paths must not share a cache entry")
	}

	if _, ok := c.narFileFor("/nix/store/abc"); !ok {
		t.Fatal("expected abc to be cached")
	}
	if _, ok := c.narFileFor("/nix/store/missing"); ok {
		t.Fatal("missing path should not be cached")
	}
}

// TestEnsureLocalExportsWhenNothingWasUploaded covers the hole that made pool
// spill impossible on the demo rig: the NAR cache is only filled by an upload,
// and the submitter skips uploading whenever the target already has the
// closure — which is always true when the job lands on the machine that built
// it. That node then held the closure and no NAR, so broadcastClosure returned
// immediately, no peer ever became eligible for spill, and every chunk ran at
// home while the run still looked healthy.
func TestEnsureLocalExportsWhenNothingWasUploaded(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}
	// A nix stand-in: -qR lists the closure, --export emits its bytes. The
	// real one only ever works against /nix/store, which a test cannot use.
	binDir := t.TempDir()
	// argv[0] is "nix-store" (the nix binary dispatches on it), so the flag
	// is $1 and the paths follow.
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  -qR) echo \"$2\" ;;\n" +
		"  --export) shift; echo \"NAR-OF $*\" ;;\n" +
		"esac\n"
	if err := os.WriteFile(filepath.Join(binDir, "nix"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	storePath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(storePath, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storePath, "bin", "run"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	c := &narCache{dir: filepath.Join(t.TempDir(), "nar-cache"), byID: map[string]string{}}

	if _, ok := c.narFileFor(storePath); ok {
		t.Fatal("cache should start empty")
	}
	path, ok := c.ensureLocal(storePath)
	if !ok {
		t.Fatal("ensureLocal did not export a closure this node holds; peers can never be seeded")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading exported NAR: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("exported NAR is empty")
	}
	// Cached, so the export cost is paid once.
	again, ok := c.ensureLocal(storePath)
	if !ok || again != path {
		t.Errorf("second call re-exported instead of using the cache: %q vs %q", again, path)
	}

	// A store path this node does not hold must not be invented.
	if _, ok := c.ensureLocal(filepath.Join(t.TempDir(), "absent")); ok {
		t.Error("ensureLocal claimed a closure that is not materialised here")
	}
}
