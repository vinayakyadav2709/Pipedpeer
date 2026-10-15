package daemonapi

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/gpu"
)

func tarOf(t *testing.T, files map[string]string) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		tw.Write([]byte(body))
	}
	tw.Close()
	return &buf
}

func shaOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// A second upload after editing one file needs one file: the CAS answers
// for the rest and fills them into the next job's work dir.
func TestWorkspaceSecondUploadSendsOneFile(t *testing.T) {
	cas := filepath.Join(t.TempDir(), "cas")
	first := map[string]string{"./a.py": "print(1)", "./b.py": "print(2)", "./data.csv": "x,y\n1,2\n"}
	if err := extractWorkspaceTar(tarOf(t, first), t.TempDir(), map[string]FileStamp{}, cas); err != nil {
		t.Fatal(err)
	}

	second := map[string]string{"./a.py": "print(1)", "./b.py": "print(3)", "./data.csv": "x,y\n1,2\n"}
	var shas []string
	for _, body := range second {
		shas = append(shas, shaOf(body))
	}
	missing := casMissing(cas, shas)
	if len(missing) != 1 || missing[0] != shaOf("print(3)") {
		t.Fatalf("missing = %v, want only the edited file", missing)
	}

	work := t.TempDir()
	uploaded := map[string]FileStamp{}
	if err := extractWorkspaceTar(tarOf(t, map[string]string{"./b.py": "print(3)"}), work, uploaded, cas); err != nil {
		t.Fatal(err)
	}
	fromCAS := []casEntry{{Name: "./a.py", SHA: shaOf("print(1)"), Mode: 0o644}, {Name: "./data.csv", SHA: shaOf("x,y\n1,2\n"), Mode: 0o644}}
	if err := materializeFromCAS(cas, work, fromCAS, uploaded); err != nil {
		t.Fatal(err)
	}
	for name, body := range second {
		got, err := os.ReadFile(filepath.Join(work, name))
		if err != nil || string(got) != body {
			t.Errorf("%s = %q (%v), want %q", name, got, err, body)
		}
	}
	if len(uploaded) != 3 {
		t.Errorf("uploaded stamps = %v, want 3", uploaded)
	}
	// A copy, not a link: editing the job's file leaves the CAS object alone.
	os.WriteFile(filepath.Join(work, "a.py"), []byte("changed"), 0o644)
	if got, _ := os.ReadFile(filepath.Join(cas, shaOf("print(1)"))); string(got) != "print(1)" {
		t.Errorf("CAS object changed through the work dir: %q", got)
	}
}

func TestMaterializeFromCASRefusesEscapes(t *testing.T) {
	err := materializeFromCAS(t.TempDir(), t.TempDir(), []casEntry{{Name: "../evil", SHA: shaOf("x")}}, map[string]FileStamp{})
	if err == nil {
		t.Fatal("an entry outside the work dir was accepted")
	}
}

func TestPruneCASDropsLeastRecentlyUsed(t *testing.T) {
	dir := t.TempDir()
	old, recent := filepath.Join(dir, "old"), filepath.Join(dir, "recent")
	os.WriteFile(old, make([]byte, 100), 0o644)
	os.WriteFile(recent, make([]byte, 100), 0o644)
	past := time.Now().Add(-time.Hour)
	os.Chtimes(old, past, past)
	pruneCAS(dir, 150)
	if _, err := os.Stat(old); err == nil {
		t.Error("least recently used object survived")
	}
	if _, err := os.Stat(recent); err != nil {
		t.Error("recent object pruned")
	}
}

// The device a job sees follows the lease, for AMD as for NVIDIA.
func TestGPUVisibleEnvFollowsLease(t *testing.T) {
	amd := gpu.Info{Vendor: gpu.VendorAMD, Best: 1}
	if env := gpuVisibleEnv(amd, "2"); !slices.Contains(env, "ROCR_VISIBLE_DEVICES=2") {
		t.Errorf("leased AMD env = %v", env)
	}
	if env := gpuVisibleEnv(amd, ""); !slices.Contains(env, "ROCR_VISIBLE_DEVICES=1") {
		t.Errorf("unleased AMD env = %v, want Best", env)
	}
	nv := gpu.Info{Vendor: gpu.VendorNVIDIA, Best: 0}
	if env := gpuVisibleEnv(nv, "3"); !slices.Contains(env, "NVIDIA_VISIBLE_DEVICES=3") {
		t.Errorf("leased NVIDIA env = %v", env)
	}
	if env := gpuVisibleEnv(gpu.Info{Vendor: gpu.VendorNone, Best: -1}, ""); env != nil {
		t.Errorf("no GPU env = %v", env)
	}
}

// A cancelled import never leaves nix running: it fails at once.
func TestImportNARStopsOnCancel(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "nar")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := importNAR(ctx, f); err == nil {
		t.Fatal("import with a cancelled context succeeded")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Errorf("cancelled import took %v", d)
	}
}

// Paths the binary cache serves are not asked of the sender; the node fetches
// them itself, and only when the asker is about to send.
func TestSubstitutablePathsNotSent(t *testing.T) {
	onCache, private := "/nix/store/aaaa-python", "/nix/store/bbbb-env"
	var mu sync.Mutex
	var fetched []string
	oldSub, oldReal := substitutable, realise
	t.Cleanup(func() { substitutable, realise = oldSub, oldReal })
	substitutable = func(paths []string) map[string]bool { return map[string]bool{onCache: true} }
	realise = func(_ context.Context, paths []string) error {
		mu.Lock()
		fetched = append(fetched, paths...)
		mu.Unlock()
		return nil
	}
	s := &Server{narCache: &narCache{dir: filepath.Join(t.TempDir(), "nar-cache")}}

	ask := func(substitute bool) []string {
		body, _ := json.Marshal(map[string]any{"paths": []string{onCache, private}, "substitute": substitute})
		rec := httptest.NewRecorder()
		s.handleStoreMissing(rec, httptest.NewRequest(http.MethodPost, "/v1/store/missing", strings.NewReader(string(body))))
		var out struct{ Missing []string }
		json.NewDecoder(rec.Body).Decode(&out)
		return out.Missing
	}
	if got := ask(false); len(got) != 2 {
		t.Fatalf("dry question missing = %v, want both", got)
	}
	if got := ask(true); len(got) != 1 || got[0] != private {
		t.Fatalf("missing = %v, want only %s", got, private)
	}
	if err := awaitFetches(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fetched) != 1 || fetched[0] != onCache {
		t.Errorf("fetched = %v, want %s", fetched, onCache)
	}
}

// A fetch slower than the sender is cancelled once the import has waited
// substituteWait; the import says errFetchSlow (409) and the next question
// asks the sender for every missing path.
func TestSlowFetchCancelledAndSenderAsked(t *testing.T) {
	onCache := "/nix/store/aaaa-python"
	oldSub, oldReal, oldWait := substitutable, realise, substituteWait
	t.Cleanup(func() {
		substitutable, realise, substituteWait = oldSub, oldReal, oldWait
		fetches.Lock()
		fetches.offUntil = time.Time{}
		fetches.Unlock()
	})
	substituteWait = 200 * time.Millisecond
	substitutable = func(paths []string) map[string]bool { return map[string]bool{onCache: true} }
	cancelled := make(chan struct{})
	realise = func(ctx context.Context, _ []string) error {
		<-ctx.Done() // a download that never finishes
		close(cancelled)
		return ctx.Err()
	}
	if got := substituteMissing([]string{onCache}); len(got) != 0 {
		t.Fatalf("first ask = %v, want the path fetched here", got)
	}
	if err := awaitFetches(context.Background()); !errors.Is(err, errFetchSlow) {
		t.Fatalf("await = %v, want errFetchSlow", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("the slow fetch was not cancelled before the import went on")
	}
	if got := substituteMissing([]string{onCache}); len(got) != 1 {
		t.Fatalf("ask after a slow fetch = %v, want the sender asked for it", got)
	}
}
