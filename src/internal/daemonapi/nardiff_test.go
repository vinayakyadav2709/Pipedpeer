package daemonapi

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// TestStoreMissingReportsOnlyAbsentPaths covers the question a sender asks
// before shipping a closure. Transfers are whole-closure today, keyed on the
// top-level store path, so two environments differing by one package share
// none of the archive and re-send the entire interpreter tree.
func TestStoreMissingReportsOnlyAbsentPaths(t *testing.T) {
	s := New("test-node")

	present := t.TempDir()
	absent := filepath.Join(t.TempDir(), "not-here")

	body, _ := json.Marshal(map[string]any{"paths": []string{present, absent}})
	req := httptest.NewRequest(http.MethodPost, "/v1/store/missing", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleStoreMissing(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Missing []string `json:"missing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Missing) != 1 || out.Missing[0] != absent {
		t.Errorf("missing = %v, want just %q", out.Missing, absent)
	}
}

// TestPartialImportIsNotCachedAsTheClosure guards the hazard the diff
// introduces. A partial archive holds only what one node was missing; cached
// under the closure's key it would later be forwarded to a third node as if
// it were complete, leaving that node with an unimportable fragment and no
// way to tell.
func TestPartialImportIsNotCachedAsTheClosure(t *testing.T) {
	dir := t.TempDir()
	s := New("test-node")
	s.narCache = &narCache{dir: filepath.Join(dir, "nar-cache"), byID: map[string]string{}}

	storePath := filepath.Join(dir, "store", "closure")

	post := func(partial bool) *httptest.ResponseRecorder {
		var buf bytes.Buffer
		mp := multipart.NewWriter(&buf)
		_ = mp.WriteField("store_path", storePath)
		if partial {
			_ = mp.WriteField("partial", "1")
		}
		fw, _ := mp.CreateFormFile("nar", "closure.nar")
		_, _ = io.Copy(fw, bytes.NewReader([]byte("not a real nar")))
		mp.Close()

		req := httptest.NewRequest(http.MethodPost, "/v1/store/import", &buf)
		req.Header.Set("Content-Type", mp.FormDataContentType())
		rec := httptest.NewRecorder()
		s.handleStoreImport(rec, req)
		return rec
	}

	// The import itself fails here (the payload is not a real NAR and there
	// is no nix); what matters is that nothing was cached under the closure
	// key either way.
	post(true)
	if path, ok := s.narCache.narFileFor(storePath); ok {
		t.Errorf("partial archive was cached as the closure at %q", path)
	}
	// And it did not leave its scratch copy behind.
	entries, err := os.ReadDir(s.narCache.dir)
	if err == nil {
		for _, e := range entries {
			if filepath.Ext(e.Name()) == ".nar" && len(e.Name()) > 8 && e.Name()[:8] == "partial-" {
				t.Errorf("partial archive left behind: %s", e.Name())
			}
		}
	}
}
