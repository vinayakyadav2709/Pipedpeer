package daemonctl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
)

// stubNode answers the two questions a sender asks before shipping a closure,
// and records what it was sent.
type stubNode struct {
	missing   []string // what it will claim to lack
	answers   bool     // false: an older daemon that does not know the question
	imported  bool
	partial   string
	importErr bool
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

	_, ok := askMissing(host, port, []string{"/nix/store/aaa", "/nix/store/bbb"})
	if ok {
		t.Error("a node that refused the question was treated as having answered")
	}
	if n.imported {
		t.Error("a fragment was sent to a node that never said what it lacked")
	}
}

// TestNothingInCommonIsNotWorthADiff. Two unrelated closures share no paths,
// and a diff there is the whole archive plus two round trips.
func TestNothingInCommonIsNotWorthADiff(t *testing.T) {
	paths := []string{"/nix/store/aaa", "/nix/store/bbb"}
	n := &stubNode{answers: true, missing: paths}
	host, port, done := n.serve(t)
	defer done()

	missing, ok := askMissing(host, port, paths)
	if !ok || len(missing) != len(paths) {
		t.Fatalf("stub answered %v, ok=%v", missing, ok)
	}
	// seedClosure declines this case before touching nix; asserting through
	// askMissing keeps the test off a real store.
	if n.imported {
		t.Error("a fragment was sent when the node had nothing in common")
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
	if err := postPartial(host, port, "/nix/store/xxx-run", nar); err != nil {
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
	err := postPartial(host, port, "/nix/store/xxx-run", nar)
	if err == nil {
		t.Fatal("a rejected import was reported as success")
	}
	if !strings.Contains(err.Error(), "partial import rejected") {
		t.Errorf("error %q does not say what failed", err)
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
