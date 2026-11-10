package console

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// testServer is a console served on a real loopback port.
type testServer struct {
	*Server
	URL  string
	Host string
}

func newTestServer(t *testing.T, opts Options) *testServer {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	if opts.HistoryDir == "" {
		opts.HistoryDir = t.TempDir()
	}
	s, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewUnstartedServer(nil)
	ts.Config.Handler = s.Handler(ts.Listener.Addr().(*net.TCPAddr).Port)
	ts.Start()
	t.Cleanup(func() {
		ts.Close()
		s.kernels.shutdownAll()
	})
	return &testServer{Server: s, URL: ts.URL, Host: strings.TrimPrefix(ts.URL, "http://")}
}

// do sends an authenticated request and returns the status and body.
func (ts *testServer) do(t *testing.T, method, path, body string, header map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+ts.Token())
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp, raw
}

// getJSON GETs path and decodes a 200 response into out.
func (ts *testServer) getJSON(t *testing.T, path string, out any) {
	t.Helper()
	resp, raw := ts.do(t, http.MethodGet, path, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d: %s", path, resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("GET %s: %v in %s", path, err, raw)
	}
}

func wantStatus(t *testing.T, resp *http.Response, body []byte, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("%s %s: status %d, want %d (body %s)", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, want, body)
	}
}

// wantErrorBody checks the contract's error shape: {"error": "<message>"}.
func wantErrorBody(t *testing.T, body []byte) {
	t.Helper()
	var e errorBody
	if err := json.Unmarshal(body, &e); err != nil || e.Error == "" {
		t.Fatalf("error body = %s, want {\"error\": \"...\"}", body)
	}
}

const testTimeout = 30 * time.Second
