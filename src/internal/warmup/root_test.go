package warmup

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nixstore"
)

// stubAddRoot replaces nix-store: it makes link a symlink to store and counts calls.
func stubAddRoot(t *testing.T, fail error) *int {
	t.Helper()
	n := 0
	old := addRoot
	t.Cleanup(func() { addRoot = old })
	addRoot = func(link, store string) error {
		n++
		if fail != nil {
			return fail
		}
		return os.Symlink(store, link)
	}
	return &n
}

func linkCount(t *testing.T) int {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(userdirState(), "warmup-env*"))
	return len(m)
}

func userdirState() string { return filepath.Dir(RootPath()) }

func TestRegisterRootCreatesAndReplaces(t *testing.T) {
	setLog(t, nil, 0)
	if err := os.MkdirAll(userdirState(), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(RootEnv, RootPath())
	a, b := t.TempDir(), t.TempDir()
	stubAddRoot(t, nil)
	RegisterRoot(a)
	if got, _ := os.Readlink(RootPath()); got != a {
		t.Fatalf("root -> %q, want %q", got, a)
	}
	RegisterRoot(b)
	if got, _ := os.Readlink(RootPath()); got != b {
		t.Fatalf("second warm-up: root -> %q, want %q", got, b)
	}
	if n := linkCount(t); n != 1 {
		t.Fatalf("%d root links, want 1", n)
	}
}

func TestOrdinaryRunAddsNoRoot(t *testing.T) {
	setLog(t, nil, 0)
	t.Setenv(RootEnv, "")
	n := stubAddRoot(t, nil)
	RegisterRoot(t.TempDir())
	if *n != 0 || linkCount(t) != 0 {
		t.Fatalf("ordinary run added a root (calls %d)", *n)
	}
}

func TestDoneNeedsLiveRoot(t *testing.T) {
	body := header("v1", "CPU") + "done\n"
	setLog(t, &body, 0)
	if !Done("v1", false) {
		t.Fatal("log done + link ok: not Done")
	}
	// link to a deleted store path
	target := t.TempDir()
	_ = os.Remove(RootPath())
	if err := os.Symlink(target, RootPath()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if Done("v1", false) {
		t.Fatal("link to a deleted path: still Done")
	}
	// no link (none could be registered): the log decides
	if err := os.Remove(RootPath()); err != nil {
		t.Fatal(err)
	}
	if !Done("v1", false) {
		t.Fatal("no link: log done must count")
	}
}

func TestRootFailureIsAWarningOnly(t *testing.T) {
	setLog(t, nil, 0)
	if err := os.MkdirAll(userdirState(), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(RootEnv, RootPath())
	n := stubAddRoot(t, errors.New("nix-store: boom"))
	RegisterRoot(t.TempDir()) // must not panic or return an error
	if *n != 1 {
		t.Fatalf("addRoot called %d times", *n)
	}
}

// A private store's link names /nix/store/... as seen inside the namespace;
// from outside it lives under the private NixDir.
func TestDoneResolvesPrivateStoreLink(t *testing.T) {
	body := header("v1", "CPU") + "done\n"
	setLog(t, &body, 0)
	t.Setenv("PIPEDPEER_FORCE_PRIVATE_NIX", "1")
	nixDir := nixstore.NixDir()
	_ = os.Remove(RootPath())
	if err := os.Symlink("/nix/store/zzz-env", RootPath()); err != nil {
		t.Fatal(err)
	}
	if nixstore.HostPath("/nix/store/zzz-env") == "/nix/store/zzz-env" {
		t.Skip("store already resolved as system; cannot force private here")
	}
	if Done("v1", false) {
		t.Fatal("private link to a path absent under NixDir: Done")
	}
	if err := os.MkdirAll(filepath.Join(nixDir, "store", "zzz-env"), 0o755); err != nil {
		t.Fatal(err)
	}
	if !Done("v1", false) {
		t.Fatal("private link to a present path: not Done")
	}
}
