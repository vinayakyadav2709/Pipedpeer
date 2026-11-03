package daemonapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
)

// Case table for the router as the daemon serves it (X1 wiring): a token is
// configured, and each request says where it comes from and what it asks.
// The /nix-cache mount is open to loopback reads only; the peer routes under
// /v1/store stay token-gated; the UI page stays as it was.
func TestRouterWiring(t *testing.T) {
	if err := authtoken.Set("secret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authtoken.Set("") })
	h := New("wire-node").Handler()

	cases := []struct {
		name   string
		method string
		path   string
		remote string
		token  bool
		want   int
	}{
		{"1 nix-cache-info from loopback, no token", http.MethodGet, "/nix-cache/nix-cache-info", "127.0.0.1:5555", false, http.StatusOK},
		{"2 nix-cache-info from another host, no token", http.MethodGet, "/nix-cache/nix-cache-info", "10.0.0.2:5555", false, http.StatusUnauthorized},
		{"3 peer narinfo from loopback, no token", http.MethodGet, "/v1/store/narinfo/" + ncHash, "127.0.0.1:5555", false, http.StatusUnauthorized},
		{"4 peer nar from loopback, no token", http.MethodGet, "/v1/store/nar/" + ncHash, "127.0.0.1:5555", false, http.StatusUnauthorized},
		{"5 peer narinfo with token (absent path: gate passed, 404)", http.MethodGet, "/v1/store/narinfo/00000000000000000000000000000000", "127.0.0.1:5555", true, http.StatusNotFound},
		{"6 UI page with no token", http.MethodGet, "/ui", "10.0.0.2:5555", false, http.StatusOK},
		{"7 nodes list, no token", http.MethodGet, "/v1/nodes", "127.0.0.1:5555", false, http.StatusUnauthorized},
		{"8 nix-cache POST from loopback, no token", http.MethodPost, "/nix-cache/nix-cache-info", "127.0.0.1:5555", false, http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			r.RemoteAddr = tc.remote
			if tc.token {
				r.Header.Set(authtoken.Header, "secret")
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Errorf("%s %s from %s: got %d, want %d (body %q)", tc.method, tc.path, tc.remote, w.Code, tc.want, w.Body.String())
			}
		})
	}
}
