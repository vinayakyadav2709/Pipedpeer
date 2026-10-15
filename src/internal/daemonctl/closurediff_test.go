package daemonctl

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// stubNode answers the two questions a sender asks before shipping a closure,
// and records what it was sent.
type stubNode struct {
	missing   []string // what it will claim to lack
	answers   bool     // false: an older daemon that does not know the question
	imported  bool
	partial   string
	importErr bool
	errBody   string // the body a failed import answers with
}

func (n *stubNode) serve(t *testing.T) (host string, port int, close func()) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/store/missing", func(w http.ResponseWriter, r *http.Request) {
		if !n.answers {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"missing": n.missing})
	})
	mux.HandleFunc("/v1/store/import", func(w http.ResponseWriter, r *http.Request) {
		if n.importErr {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(n.errBody))
			return
		}
		r.ParseMultipartForm(1 << 20)
		n.imported = true
		n.partial = r.FormValue("partial")
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return u.Hostname(), p, srv.Close
}

// TestNodeThatCannotAnswerGetsTheWholeArchive. An older daemon does not know
// the question, and "no answer" must never be read as "has everything" - that
// would ship a job to a node with no environment to run it in.
func TestNodeThatCannotAnswerGetsTheWholeArchive(t *testing.T) {
	n := &stubNode{answers: false}
	host, port, done := n.serve(t)
	defer done()

	_, ok := askMissing(host, port, []string{"/nix/store/aaa", "/nix/store/bbb"}, false)
	if ok {
		t.Error("a node that refused the question was treated as having answered")
	}
	if n.imported {
		t.Error("a fragment was sent to a node that never said what it lacked")
	}
}

// TestFragmentIsMarkedPartial. The receiving node caches whole archives under
// the closure's key and forwards them to third nodes. A fragment filed there
// would be handed on as if it were complete, leaving that node with something
// it cannot import - so the flag is not cosmetic.
func TestFragmentIsMarkedPartial(t *testing.T) {
	n := &stubNode{answers: true}
	host, port, done := n.serve(t)
	defer done()

	nar := t.TempDir() + "/frag.nar"
	if err := writeFile(nar, "not really a nar"); err != nil {
		t.Fatal(err)
	}
	if _, err := postPartial(host, port, "/nix/store/xxx-run", fromFile(nar)); err != nil {
		t.Fatalf("postPartial: %v", err)
	}
	if !n.imported {
		t.Fatal("nothing arrived")
	}
	if n.partial != "1" {
		t.Errorf("partial flag was %q; the fragment would be cached as a whole "+
			"closure and forwarded to a third node", n.partial)
	}
}

// TestRejectedImportIsReported, so the caller falls back to the whole archive
// instead of running a job against an environment that never arrived.
func TestRejectedImportIsReported(t *testing.T) {
	n := &stubNode{answers: true, importErr: true}
	host, port, done := n.serve(t)
	defer done()

	nar := t.TempDir() + "/frag.nar"
	if err := writeFile(nar, "x"); err != nil {
		t.Fatal(err)
	}
	_, err := postPartial(host, port, "/nix/store/xxx-run", fromFile(nar))
	if err == nil {
		t.Fatal("a rejected import was reported as success")
	}
	if !strings.Contains(err.Error(), "closure import rejected") {
		t.Errorf("error %q does not say what failed", err)
	}
	var refused *ClosureRefusedError
	if errors.As(err, &refused) {
		t.Errorf("an unexplained failure (an older node, say) was taken for a refusal: %v", err)
	}
}

// TestSignatureRefusalIsNotResent. A node that unpacked the signed closure and
// refused it (V2 45-friend: a multi-user nix without the sender's key) would
// refuse the unsigned whole closure too; resending it cost 3.8 GB and ten
// minutes, and the reschedule a third time. The refusal must come back typed.
func TestSignatureRefusalIsNotResent(t *testing.T) {
	n := &stubNode{answers: true, importErr: true,
		errBody: `{"error":"nix copy from a peer's closure: exit status 1: error: cannot add path '/nix/store/809x-pip' because it lacks a signature by a trusted key"}`}
	host, port, done := n.serve(t)
	defer done()

	nar := t.TempDir() + "/closure.tar"
	if err := writeFile(nar, "x"); err != nil {
		t.Fatal(err)
	}
	_, err := postPartial(host, port, "/nix/store/xxx-run", fromFile(nar))
	var refused *ClosureRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("a signature refusal came back as %v; the caller would resend the whole closure", err)
	}
	if !strings.Contains(refused.Error(), "lacks a signature by a trusted key") || strings.Contains(refused.Error(), `{"error"`) {
		t.Errorf("refusal %q should carry nix's reason, without the JSON wrapping", refused)
	}
}

// fromFile packs by copying a file, standing in for narpack.PackSigned.
func fromFile(path string) func(io.Writer) error {
	return func(w io.Writer) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	}
}

// TestStreamedPackFailureIsReported: a pack that fails mid-stream fails the
// send, so the caller falls back instead of the node importing half an archive.
func TestStreamedPackFailureIsReported(t *testing.T) {
	n := &stubNode{answers: true}
	host, port, done := n.serve(t)
	defer done()
	_, err := postPartial(host, port, "/nix/store/xxx-run", func(w io.Writer) error {
		w.Write([]byte("half"))
		return errors.New("pack broke")
	})
	if err == nil {
		t.Fatal("a broken pack was reported as sent")
	}
}

// TestWarmReseedTakesMilliseconds: a node that already holds the closure gets
// nothing but the workspace, and the upload costs round trips, not packing.
func TestWarmReseedTakesMilliseconds(t *testing.T) {
	var imported bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/store", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"cached": true})
	})
	mux.HandleFunc("/v1/store/import", func(w http.ResponseWriter, r *http.Request) { imported = true })
	mux.HandleFunc("/v1/jobs/upload", func(w http.ResponseWriter, r *http.Request) {
		r.ParseMultipartForm(1 << 20)
		if _, _, err := r.FormFile("nar"); err == nil {
			imported = true
		}
		json.NewEncoder(w).Encode(map[string]any{"job_id": "j1", "node_id": "node-x"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	ws := filepath.Join(t.TempDir(), "ws.tar")
	if err := writeFile(ws, ""); err != nil {
		t.Fatal(err)
	}
	// Warm the identity key once so the measured run is the seed alone.
	if _, err := UploadJob(context.Background(), u.Hostname(), port, ws, "/unused.nar", "/nix/store/xxx-run", "main.py", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resp, err := UploadJob(context.Background(), u.Hostname(), port, ws, "/unused.nar", "/nix/store/xxx-run", "main.py", UploadOptions{})
	took := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if imported || resp.Closure != "on-node" {
		t.Errorf("closure sent to a node that had it (closure=%s)", resp.Closure)
	}
	if took > 100*time.Millisecond {
		t.Errorf("warm re-seed took %v, want < 100 ms", took)
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
