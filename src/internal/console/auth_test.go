package console

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

// noRedirect is a client that reports redirects instead of following them.
var noRedirect = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}}

func rawGet(t *testing.T, client *http.Client, rawURL string, edit func(*http.Request)) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(req)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, body
}

func TestAPIRequiresToken(t *testing.T) {
	ts := newTestServer(t, Options{})

	for _, path := range []string{"/api/overview", "/api/notebooks", "/api/kernel/ws?notebook=a.ipynb", "/api/no-such-route"} {
		resp, body := rawGet(t, http.DefaultClient, ts.URL+path, nil)
		wantStatus(t, resp, body, http.StatusUnauthorized)
		wantErrorBody(t, body)
	}

	resp, body := rawGet(t, http.DefaultClient, ts.URL+"/api/notebooks", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer not-the-token")
	})
	wantStatus(t, resp, body, http.StatusUnauthorized)

	resp, body = rawGet(t, http.DefaultClient, ts.URL+"/api/notebooks", func(r *http.Request) {
		r.AddCookie(&http.Cookie{Name: cookieName, Value: "not-the-token"})
	})
	wantStatus(t, resp, body, http.StatusUnauthorized)

	resp, body = rawGet(t, http.DefaultClient, ts.URL+"/api/notebooks", func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+ts.Token())
	})
	wantStatus(t, resp, body, http.StatusOK)
}

func TestTokenInURLBecomesCookie(t *testing.T) {
	ts := newTestServer(t, Options{})

	resp, body := rawGet(t, noRedirect, ts.URL+"/notebooks/a.ipynb?token="+ts.Token()+"&tab=2", nil)
	wantStatus(t, resp, body, http.StatusFound)
	if got := resp.Header.Get("Location"); got != "/notebooks/a.ipynb?tab=2" {
		t.Errorf("redirect Location = %q, want the same path without the token", got)
	}
	var session *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == cookieName {
			session = c
		}
	}
	if session == nil {
		t.Fatalf("no %s cookie set; headers: %v", cookieName, resp.Header)
	}
	if !session.HttpOnly || session.SameSite != http.SameSiteStrictMode {
		t.Errorf("cookie HttpOnly=%v SameSite=%v, want HttpOnly and Strict", session.HttpOnly, session.SameSite)
	}

	// The cookie the server issued is what authenticates the API.
	resp, body = rawGet(t, http.DefaultClient, ts.URL+"/api/notebooks", func(r *http.Request) { r.AddCookie(session) })
	wantStatus(t, resp, body, http.StatusOK)

	resp, body = rawGet(t, noRedirect, ts.URL+"/?token=wrong", nil)
	wantStatus(t, resp, body, http.StatusUnauthorized)
	wantErrorBody(t, body)
	if len(resp.Cookies()) != 0 {
		t.Errorf("a wrong token set cookies: %v", resp.Cookies())
	}
}

func TestBadHostRejected(t *testing.T) {
	ts := newTestServer(t, Options{})
	port := ts.Host[strings.LastIndex(ts.Host, ":"):]

	for _, host := range []string{"evil.example" + port, "evil.example", "127.0.0.1:1", "127.0.0.1.evil.example" + port} {
		for _, path := range []string{"/", "/api/notebooks"} {
			resp, body := rawGet(t, http.DefaultClient, ts.URL+path, func(r *http.Request) {
				r.Host = host
				r.Header.Set("Authorization", "Bearer "+ts.Token())
			})
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("Host %q GET %s: status %d, want 403 (body %s)", host, path, resp.StatusCode, body)
			}
		}
	}

	resp, body := rawGet(t, http.DefaultClient, ts.URL+"/api/notebooks", func(r *http.Request) {
		r.Host = "localhost" + port
		r.Header.Set("Authorization", "Bearer "+ts.Token())
	})
	wantStatus(t, resp, body, http.StatusOK)
}

func TestForeignOriginRejected(t *testing.T) {
	ts := newTestServer(t, Options{DevOrigin: "http://127.0.0.1:5173"})
	wsURL := "ws://" + ts.Host + "/api/kernel/ws?notebook=" + url.QueryEscape("a.ipynb")
	dial := func(origin string) (*websocket.Conn, *http.Response, error) {
		return websocket.DefaultDialer.Dial(wsURL, http.Header{
			"Authorization": {"Bearer " + ts.Token()},
			"Origin":        {origin},
		})
	}

	_, resp, err := dial("http://evil.example")
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("WebSocket from a foreign origin: err=%v resp=%v, want 403", err, resp)
	}

	for _, origin := range []string{ts.URL, "http://127.0.0.1:5173", "http://localhost:5173"} {
		conn, _, err := dial(origin)
		if err != nil {
			t.Errorf("WebSocket from %s refused: %v", origin, err)
			continue
		}
		conn.Close()
	}

	resp2, body := ts.do(t, http.MethodPost, "/api/notebooks", `{}`, map[string]string{"Origin": "http://evil.example"})
	wantStatus(t, resp2, body, http.StatusForbidden)
}

func TestPagesFallBackToShell(t *testing.T) {
	ts := newTestServer(t, Options{})

	for _, path := range []string{"/", "/nodes", "/jobs/123", "/notebooks/demo/a.ipynb"} {
		resp, body := rawGet(t, http.DefaultClient, ts.URL+path, nil)
		wantStatus(t, resp, body, http.StatusOK)
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Errorf("GET %s: Content-Type %q, want text/html", path, ct)
		}
		if !strings.Contains(strings.ToLower(string(body)), "<!doctype html>") {
			t.Errorf("GET %s: body is not an HTML document", path)
		}
	}

	resp, body := ts.do(t, http.MethodGet, "/api/no-such-route", "", nil)
	wantStatus(t, resp, body, http.StatusNotFound)
	wantErrorBody(t, body)
}

func TestServeRefusesNonLoopbackListener(t *testing.T) {
	s, err := New(Options{Dir: t.TempDir(), HistoryDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Serve(t.Context(), fakeListener{addr: "192.0.2.10:7878"}); err == nil {
		t.Fatal("Serve accepted a non-loopback listener")
	}
}
