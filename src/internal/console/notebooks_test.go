package console

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func contentPath(p string) string {
	return "/api/notebooks/content?path=" + url.QueryEscape(p)
}

func TestNotebookPathsCannotEscapeRoot(t *testing.T) {
	ts := newTestServer(t, Options{})
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.ipynb")
	if err := os.WriteFile(secret, []byte(`{"secret": true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(ts.root, "linkdir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(ts.root, "linkfile.ipynb")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "not-yet.ipynb"), filepath.Join(ts.root, "dangling.ipynb")); err != nil {
		t.Fatal(err)
	}

	escapes := []string{
		"../secret.ipynb",
		"a/../../secret.ipynb",
		secret,
		"linkdir/secret.ipynb",
		"linkdir/new.ipynb",
		"linkfile.ipynb",
		"dangling.ipynb",
		`..\secret.ipynb`,
		"notes.txt",
		".ipynb",
		"",
	}
	for _, p := range escapes {
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			resp, body := ts.do(t, method, contentPath(p), `{}`, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s %q: status %d, want 400 (body %s)", method, p, resp.StatusCode, body)
			}
		}
		resp, body := ts.do(t, http.MethodPost, "/api/notebooks/rename", mustJSON(NotebookRename{From: p, To: "x.ipynb"}), nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("rename from %q: status %d, want 400 (body %s)", p, resp.StatusCode, body)
		}
		if p == "" {
			continue // an empty create path means "pick a name"
		}
		resp, body = ts.do(t, http.MethodPost, "/api/notebooks", mustJSON(NotebookPath{Path: p}), nil)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("create %q: status %d, want 400 (body %s)", p, resp.StatusCode, body)
		}
	}

	if got, _ := os.ReadFile(secret); string(got) != `{"secret": true}` {
		t.Errorf("file outside the root was modified: %s", got)
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 1 {
		t.Errorf("files were created outside the root: %v", entries)
	}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestNotebookRoundTrip(t *testing.T) {
	ts := newTestServer(t, Options{})

	var created NotebookPath
	for _, want := range []string{"Untitled.ipynb", "Untitled1.ipynb"} {
		resp, body := ts.do(t, http.MethodPost, "/api/notebooks", `{}`, nil)
		wantStatus(t, resp, body, http.StatusOK)
		if err := json.Unmarshal(body, &created); err != nil || created.Path != want {
			t.Fatalf("create untitled = %s, want path %s", body, want)
		}
	}

	resp, body := ts.do(t, http.MethodPost, "/api/notebooks", `{"path":"demo/a.ipynb"}`, nil)
	wantStatus(t, resp, body, http.StatusOK)
	resp, body = ts.do(t, http.MethodPost, "/api/notebooks", `{"path":"demo/a.ipynb"}`, nil)
	wantStatus(t, resp, body, http.StatusConflict)
	wantErrorBody(t, body)

	resp, body = ts.do(t, http.MethodGet, contentPath("demo/a.ipynb"), "", nil)
	wantStatus(t, resp, body, http.StatusOK)
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("GET content sent no ETag")
	}
	var nb struct {
		Cells         []any `json:"cells"`
		NBFormat      int   `json:"nbformat"`
		NBFormatMinor int   `json:"nbformat_minor"`
	}
	if err := json.Unmarshal(body, &nb); err != nil {
		t.Fatalf("new notebook is not JSON: %v", err)
	}
	if nb.NBFormat != 4 || nb.NBFormatMinor != 5 || nb.Cells == nil || len(nb.Cells) != 0 {
		t.Errorf("new notebook = %s, want empty nbformat 4.5", body)
	}

	edited := `{"cells":[{"cell_type":"code","id":"c1","metadata":{},"source":"print(1)","outputs":[],"execution_count":null}],"metadata":{},"nbformat":4,"nbformat_minor":5}`
	resp, body = ts.do(t, http.MethodPut, contentPath("demo/a.ipynb"), edited, map[string]string{"If-Match": etag})
	wantStatus(t, resp, body, http.StatusOK)
	var saved NotebookSaved
	if err := json.Unmarshal(body, &saved); err != nil || saved.ETag == "" || saved.ETag == etag || saved.Modified == 0 {
		t.Fatalf("PUT reply = %s, want a new etag and a modified time", body)
	}

	resp, body = ts.do(t, http.MethodGet, contentPath("demo/a.ipynb"), "", nil)
	if string(body) != edited {
		t.Errorf("content after PUT = %s", body)
	}
	if got := resp.Header.Get("ETag"); got != saved.ETag {
		t.Errorf("ETag after PUT = %s, want the one PUT returned (%s)", got, saved.ETag)
	}

	// Someone else edits the file; a save based on the old ETag must not win.
	external := `{"cells":[],"metadata":{"edited":"elsewhere"},"nbformat":4,"nbformat_minor":5}`
	if err := os.WriteFile(filepath.Join(ts.root, "demo", "a.ipynb"), []byte(external), 0o644); err != nil {
		t.Fatal(err)
	}
	resp, body = ts.do(t, http.MethodPut, contentPath("demo/a.ipynb"), edited, map[string]string{"If-Match": saved.ETag})
	wantStatus(t, resp, body, http.StatusPreconditionFailed)
	wantErrorBody(t, body)
	if got, _ := os.ReadFile(filepath.Join(ts.root, "demo", "a.ipynb")); string(got) != external {
		t.Errorf("a conflicting save overwrote the external edit: %s", got)
	}

	resp, body = ts.do(t, http.MethodPut, contentPath("demo/a.ipynb"), `{not json`, nil)
	wantStatus(t, resp, body, http.StatusBadRequest)

	resp, body = ts.do(t, http.MethodPost, "/api/notebooks/rename", `{"from":"demo/a.ipynb","to":"Untitled.ipynb"}`, nil)
	wantStatus(t, resp, body, http.StatusConflict)
	resp, body = ts.do(t, http.MethodPost, "/api/notebooks/rename", `{"from":"demo/a.ipynb","to":"moved/b.ipynb"}`, nil)
	wantStatus(t, resp, body, http.StatusOK)
	if err := json.Unmarshal(body, &created); err != nil || created.Path != "moved/b.ipynb" {
		t.Errorf("rename reply = %s", body)
	}
	resp, body = ts.do(t, http.MethodGet, contentPath("demo/a.ipynb"), "", nil)
	wantStatus(t, resp, body, http.StatusNotFound)
	resp, body = ts.do(t, http.MethodGet, contentPath("moved/b.ipynb"), "", nil)
	if string(body) != external {
		t.Errorf("renamed notebook content = %s", body)
	}

	resp, body = ts.do(t, http.MethodDelete, contentPath("moved/b.ipynb"), "", nil)
	wantStatus(t, resp, body, http.StatusNoContent)
	resp, body = ts.do(t, http.MethodDelete, contentPath("moved/b.ipynb"), "", nil)
	wantStatus(t, resp, body, http.StatusNotFound)

	if leftovers, _ := filepath.Glob(filepath.Join(ts.root, "demo", ".*")); len(leftovers) != 0 {
		t.Errorf("atomic write left temp files behind: %v", leftovers)
	}
}

func TestNotebookListSkipsHiddenAndVendored(t *testing.T) {
	ts := newTestServer(t, Options{})
	for _, p := range []string{
		"top.ipynb", "sub/deep/inner.ipynb",
		".hidden/x.ipynb", "node_modules/pkg/x.ipynb", ".pipedpeer/x.ipynb", "sub/.ipynb_checkpoints/top-checkpoint.ipynb",
		"sub/readme.md",
	} {
		abs := filepath.Join(ts.root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte(emptyNotebook), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var list []NotebookEntry
	ts.getJSON(t, "/api/notebooks", &list)
	var paths []string
	for _, e := range list {
		paths = append(paths, e.Path)
		if e.Name != filepath.Base(e.Path) || e.Size != int64(len(emptyNotebook)) || e.Modified == 0 {
			t.Errorf("entry %+v has wrong name, size or modified time", e)
		}
	}
	want := []string{"sub/deep/inner.ipynb", "top.ipynb"}
	if len(paths) != len(want) || paths[0] != want[0] || paths[1] != want[1] {
		t.Errorf("listed %v, want %v", paths, want)
	}
}

func TestNotebookListIsEmptyArray(t *testing.T) {
	ts := newTestServer(t, Options{})
	resp, body := ts.do(t, http.MethodGet, "/api/notebooks", "", nil)
	wantStatus(t, resp, body, http.StatusOK)
	if got := string(body); got != "[]\n" {
		t.Errorf("empty directory listed as %q, want []", got)
	}
}
