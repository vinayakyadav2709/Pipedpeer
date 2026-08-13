package daemonapi

import (
	"os"
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
