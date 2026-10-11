package daemonapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
)

// The page itself must load on a cluster with a secret - a browser cannot
// attach the header to the address-bar request - while the data behind it
// stays behind the secret like the rest of the API.
func TestUIPageLoadsWithoutTheSecretButStatsDoNot(t *testing.T) {
	if err := authtoken.Set("ui-test-token"); err != nil {
		t.Skipf("NOT VERIFIED: cannot set a token here: %v", err)
	}
	defer authtoken.Set("")

	s := New("node-ui")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/ui")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "<title>Pipedpeer cluster</title>") {
		t.Fatalf("/ui: status %d, want the page", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("/ui content type %q", ct)
	}
	if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "connect-src 'self'") {
		t.Errorf("/ui CSP %q does not confine the page to its own daemon", csp)
	}

	for _, path := range []string{"/v1/stats", "/ui/../v1/stats", "/v1/nodes"} {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s without the secret: %d, want 401", path, resp.StatusCode)
		}
	}

	req, _ := http.NewRequest("GET", ts.URL+"/v1/stats", nil)
	req.Header.Set(authtoken.Header, "ui-test-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/v1/stats with the secret: %d", resp.StatusCode)
	}
	var snap struct {
		Nodes []json.RawMessage `json:"nodes"`
		Jobs  []json.RawMessage `json:"jobs"`
		At    string            `json:"at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil || snap.At == "" {
		t.Fatalf("/v1/stats is not a snapshot: %v", err)
	}
}

// The page is one file with no outside resources: a daemon on a network with
// no route to a CDN would otherwise show a blank page.
func TestUIPageIsSelfContained(t *testing.T) {
	page := string(uiPage)
	for _, bad := range []string{"<script src", "<link ", "https://", "http://"} {
		if strings.Contains(page, bad) {
			t.Errorf("the page references an outside resource (%q)", bad)
		}
	}
}

// Errors are never silent: every catch on the page leaves a console line.
func TestUIPageCatchesLog(t *testing.T) {
	blocks := regexp.MustCompile(`catch\s*(\([^)]*\))?\s*\{([^{}]*)\}`).FindAllStringSubmatch(string(uiPage), -1)
	if len(blocks) == 0 {
		t.Fatal("found no catch blocks; the pattern no longer matches the page")
	}
	for _, b := range blocks {
		if !strings.Contains(b[2], "console.") {
			t.Errorf("catch block logs nothing: %s", b[0])
		}
	}
}

// The shim counts an unshippable tail in local_items already, so the page's
// totals must not add it again (jobhistory.Receipt Total/Raced do the same).
func TestUIPageCountsUnshippableOnce(t *testing.T) {
	if m := regexp.MustCompile(`local_items\s*\+\s*\(?\s*rc\.unshippable`).FindString(string(uiPage)); m != "" {
		t.Fatalf("page adds unshippable on top of local_items: %s", m)
	}
}
