package authtoken

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// withToken points the package at a scratch token for one test. The token is
// process-global by design (every call site reads the same one), so tests must
// reset it or they leak into each other.
func withToken(t *testing.T, tok string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dir)
	mu.Lock()
	cached, loaded = "", false
	mu.Unlock()
	if tok != "" {
		if err := Set(tok); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		mu.Lock()
		cached, loaded = "", false
		mu.Unlock()
	})
}

func TestMiddlewareOpenWhenNoTokenSet(t *testing.T) {
	withToken(t, "")
	// An existing cluster must keep working until its operator opts in;
	// silently locking everyone out on upgrade is not an acceptable default.
	rec := httptest.NewRecorder()
	Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("open daemon rejected a request: %d", rec.Code)
	}
}

func TestMiddlewareRejectsWithoutToken(t *testing.T) {
	withToken(t, "s3cret")

	served := false
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = true
		w.WriteHeader(http.StatusOK)
	}))

	cases := []struct {
		name   string
		header string
		value  string
		want   int
	}{
		{"no header", "", "", http.StatusUnauthorized},
		{"wrong token", Header, "wrong", http.StatusUnauthorized},
		{"right token", Header, "s3cret", http.StatusOK},
		{"bearer form", "Authorization", "Bearer s3cret", http.StatusOK},
		{"wrong bearer", "Authorization", "Bearer nope", http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			served = false
			req := httptest.NewRequest(http.MethodPost, "/v1/pool/map", nil)
			if tc.header != "" {
				req.Header.Set(tc.header, tc.value)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d", rec.Code, tc.want)
			}
			if tc.want == http.StatusUnauthorized && served {
				t.Fatal("handler ran despite a rejected request")
			}
		})
	}
}

// TestTransportOnlySendsTokenToDaemons is the one that matters for secrecy:
// a transport that attached the token to every outbound request would hand it
// to whatever host the process happened to talk to next.
func TestTransportOnlySendsTokenToDaemons(t *testing.T) {
	withToken(t, "s3cret")

	var seen string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get(Header)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := &http.Client{Transport: &transport{base: http.DefaultTransport}}

	for _, tc := range []struct {
		path string
		want string
	}{
		{"/v1/pool/map", "s3cret"},
		{"/health", "s3cret"},
		{"/some/other/thing", ""},
	} {
		seen = ""
		resp, err := client.Get(srv.URL + tc.path)
		if err != nil {
			t.Fatalf("%s: %v", tc.path, err)
		}
		resp.Body.Close()
		if seen != tc.want {
			t.Errorf("%s: token %q, want %q", tc.path, seen, tc.want)
		}
	}
}

func TestSetAndClearRoundTrip(t *testing.T) {
	withToken(t, "")
	if Current() != "" {
		t.Fatal("expected no token")
	}
	tok := Generate()
	if len(tok) != 64 {
		t.Fatalf("generated token is %d chars; too short to be worth having", len(tok))
	}
	if err := Set(tok); err != nil {
		t.Fatal(err)
	}
	if Current() != tok {
		t.Fatalf("Current()=%q want %q", Current(), tok)
	}
	if err := Set(""); err != nil {
		t.Fatal(err)
	}
	if Current() != "" {
		t.Fatal("clear did not remove the token")
	}
}

// TestTestBinaryIgnoresConfiguredToken is the guard that keeps a developer's
// real secret out of the test suite. Without it, every package whose tests
// start or call a daemon began failing on a machine where a token was
// configured — and in the coordinator package that surfaced as a ten-minute
// timeout rather than an error, which sends you looking at the wrong code.
func TestTestBinaryIgnoresConfiguredToken(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dir)

	// Write a token the way `pipedpeer auth set` would, then read it back as
	// a fresh process would.
	if err := Set("operator-secret"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	cached, loaded = "", false
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		cached, loaded = "", false
		mu.Unlock()
	})

	if !underTest() {
		t.Fatal("this is a test binary; underTest() should say so")
	}
	if got := Current(); got != "" {
		t.Errorf("test binary picked up the operator's token %q", got)
	}

	// An explicit Set still works, so tests that mean to exercise auth can.
	if err := Set("chosen-by-the-test"); err != nil {
		t.Fatal(err)
	}
	if got := Current(); got != "chosen-by-the-test" {
		t.Errorf("Current()=%q after an explicit Set", got)
	}
}
