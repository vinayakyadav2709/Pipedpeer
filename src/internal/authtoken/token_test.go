package authtoken

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	forgetProofs()
	t.Cleanup(forgetProofs)
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

func forgetProofs() {
	provenMu.Lock()
	proven, refused = map[string]string{}, map[string]bool{}
	provenMu.Unlock()
}

// remoteServer starts h on 127.0.0.2, which the transport treats as a remote
// host (only the literal loopback names are exempt, as in tlsid).
func remoteServer(t *testing.T, h http.Handler, withTLS bool) *httptest.Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Skipf("cannot listen on 127.0.0.2 (not Linux?): %v", err)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.Listener.Close()
	srv.Listener = ln
	if withTLS {
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)
	return srv
}

// insecureTLS stands in for tlsid's pinned client: httptest's certificate
// does not name 127.0.0.2, and identity is the pin's job, not this test's.
func insecureTLS() *http.Transport {
	return &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
}

// tokenLog records every token a server was handed.
type tokenLog struct {
	mu   sync.Mutex
	seen []string
}

func (l *tokenLog) note(r *http.Request) {
	if v := r.Header.Get(Header); v != "" {
		l.mu.Lock()
		l.seen = append(l.seen, v)
		l.mu.Unlock()
	}
}

func (l *tokenLog) got() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.seen...)
}

func TestTheTokenIsNeverSentToAHostThatCannotProve(t *testing.T) {
	withToken(t, "s3cret")
	var log tokenLog
	srv := remoteServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.note(r)
		w.WriteHeader(http.StatusOK) // answers like a daemon, proves nothing
	}), false)
	client := &http.Client{Transport: &transport{base: http.DefaultTransport}}

	_, err := client.Get(srv.URL + "/v1/pool/map")
	if err == nil || !strings.Contains(err.Error(), "could not prove it holds the cluster token; not sending it") {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if !Unproven(err) {
		t.Fatalf("err = %v: something answered, want Unproven", err)
	}
	// A token the caller set by hand is held to the same rule.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/anything", nil)
	req.Header.Set(Header, "s3cret")
	if _, err := client.Do(req); err == nil {
		t.Fatal("a hand-set token went to a host that proved nothing")
	}
	if got := log.got(); len(got) != 0 {
		t.Fatalf("host was handed the token: %v", got)
	}
}

func TestAProvenHostGetsTheToken(t *testing.T) {
	withToken(t, "s3cret")
	for _, withTLS := range []bool{false, true} {
		forgetProofs()
		var log tokenLog
		inner := 0
		srv := remoteServer(t, Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			inner++
			log.note(r)
			w.WriteHeader(http.StatusOK)
		})), withTLS)
		base := http.RoundTripper(http.DefaultTransport)
		if withTLS {
			base = insecureTLS()
		}
		client := &http.Client{Transport: &transport{base: base}}
		for i := 0; i < 2; i++ {
			resp, err := client.Get(srv.URL + "/v1/jobs")
			if err != nil {
				t.Fatalf("tls=%v: %v", withTLS, err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("tls=%v: status %d", withTLS, resp.StatusCode)
			}
		}
		// The challenge is answered by the middleware alone, and only once:
		// the proof is remembered.
		if inner != 2 || len(log.got()) != 2 {
			t.Fatalf("tls=%v: handler ran %d times with %d tokens, want 2 and 2", withTLS, inner, len(log.got()))
		}
	}
}

// TestAProofFromAnotherTLSSessionIsRefused: a host without the token relays
// the challenge to a real daemon over its own TLS session and passes the
// daemon's answer back. The answer is bound to the wrong session.
func TestAProofFromAnotherTLSSessionIsRefused(t *testing.T) {
	withToken(t, "s3cret")
	real := remoteServer(t, Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})), true)
	var log tokenLog
	relay := remoteServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.note(r)
		req, _ := http.NewRequest(http.MethodGet, real.URL+"/health", nil)
		req.Header.Set(ChallengeHeader, r.Header.Get(ChallengeHeader))
		resp, err := insecureTLS().RoundTrip(req)
		if err != nil {
			t.Errorf("relay: %v", err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		w.Header().Set(ProofHeader, resp.Header.Get(ProofHeader))
		w.WriteHeader(resp.StatusCode)
	}), true)

	client := &http.Client{Transport: &transport{base: insecureTLS()}}
	resp, err := client.Get(real.URL + "/v1/jobs")
	if err != nil {
		t.Fatalf("the real daemon should prove itself: %v", err)
	}
	resp.Body.Close()
	if _, err := client.Get(relay.URL + "/v1/jobs"); err == nil {
		t.Fatal("a relayed proof was accepted")
	}
	if got := log.got(); len(got) != 0 {
		t.Fatalf("relay was handed the token: %v", got)
	}
}

func TestTheChallengeIsAnsweredWithNothingElse(t *testing.T) {
	withToken(t, "s3cret")
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the handler ran for a challenge")
	}))
	for _, tc := range []struct {
		challenge string
		want      int
	}{
		{strings.Repeat("ab", 16), http.StatusNoContent},
		{"short", http.StatusUnauthorized},
		{strings.Repeat("zz", 16), http.StatusUnauthorized},
	} {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.Header.Set(ChallengeHeader, tc.challenge)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("challenge %q: status %d, want %d", tc.challenge, rec.Code, tc.want)
		}
		if tc.want == http.StatusNoContent && (rec.Header().Get(ProofHeader) == "" || rec.Body.Len() != 0) {
			t.Fatalf("challenge answer: proof %q, body %q", rec.Header().Get(ProofHeader), rec.Body.String())
		}
	}
}

// Nothing listening is not a refusal: connect must not blame the secret for it.
func TestNoAnswerIsNotUnproven(t *testing.T) {
	withToken(t, "s3cret")
	srv := remoteServer(t, http.NotFoundHandler(), false)
	addr := srv.Listener.Addr().String()
	srv.Close()
	err := prove(context.Background(), http.DefaultTransport, "http", addr, Current())
	if err == nil || Unproven(err) || !strings.Contains(err.Error(), "did not answer") {
		t.Fatalf("err = %v, want a no-answer that is not Unproven", err)
	}
}
