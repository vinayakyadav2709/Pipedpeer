// Package authtoken gates the daemon API behind a shared secret.
//
// Until this existed, anyone who could reach the daemon port could execute
// code as root on that machine, read job output and workspace files, and
// deregister nodes. That confined the whole system to a network the user
// already trusted, and made an overlay like Tailscale do double duty: not
// just the route between machines but the only thing standing between them
// and everyone else.
//
// A shared secret is not the end state — a per-node keypair and a real
// network identity are (see plan.md P3) — but it is the difference between
// "anyone who can reach the port" and "anyone holding the token", which is
// most of the distance for a fraction of the work.
//
// It is off unless a token is configured, so existing clusters keep working
// until their operator opts in.
package authtoken

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pipedpeer/pipedpeer/internal/userdir"
)

// Header is where the token travels. Not a cookie and not a query parameter:
// query strings end up in access logs and shell history.
const Header = "X-Pipedpeer-Token"

// EnvVar overrides the stored token, and is how a job's shim learns the
// token it must present when talking back to its own daemon.
const EnvVar = "PIPEDPEER_TOKEN"

var (
	mu     sync.RWMutex
	cached string
	loaded bool
)

func tokenPath() string {
	return filepath.Join(userdir.Data(), "auth_token")
}

// Current returns the configured token, or "" when the daemon is open.
func Current() string {
	mu.RLock()
	if loaded {
		defer mu.RUnlock()
		return cached
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()
	if loaded {
		return cached
	}
	loaded = true
	if v := strings.TrimSpace(os.Getenv(EnvVar)); v != "" {
		cached = v
		return cached
	}
	b, err := os.ReadFile(tokenPath())
	if err == nil {
		cached = strings.TrimSpace(string(b))
	}
	return cached
}

// Set writes a token, or clears it when tok is empty.
func Set(tok string) error {
	tok = strings.TrimSpace(tok)
	path := tokenPath()
	if tok == "" {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		// 0600: a token readable by every user on the box protects nothing.
		if err := os.WriteFile(path, []byte(tok+"\n"), 0o600); err != nil {
			return err
		}
	}
	mu.Lock()
	cached, loaded = tok, true
	mu.Unlock()
	return nil
}

// Generate returns a fresh random token.
func Generate() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// Path is where the token lives, for messages to the operator.
func Path() string { return tokenPath() }

// Middleware rejects requests without the token once one is configured.
// Comparison is constant-time: a byte-by-byte compare leaks the token one
// character at a time to anyone willing to measure.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := Current()
		if want == "" {
			next.ServeHTTP(w, r)
			return
		}
		got := r.Header.Get(Header)
		if got == "" {
			// Also accept the standard bearer form, so curl and any ordinary
			// HTTP client can talk to the daemon without a custom header.
			if a := r.Header.Get("Authorization"); strings.HasPrefix(a, "Bearer ") {
				got = strings.TrimPrefix(a, "Bearer ")
			}
		}
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"missing or invalid ` + Header + `"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// transport attaches the token to pipedpeer's own API calls.
type transport struct{ base http.RoundTripper }

// isDaemonAPI reports whether a request is going to a pipedpeer daemon.
// The token must never ride along on anything else: attaching a secret to
// every outbound request would hand it to whatever host the process happens
// to talk to next.
func isDaemonAPI(p string) bool {
	return p == "/health" || strings.HasPrefix(p, "/v1/")
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	tok := Current()
	if tok != "" && isDaemonAPI(r.URL.Path) && r.Header.Get(Header) == "" {
		r = r.Clone(r.Context())
		r.Header.Set(Header, tok)
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(r)
}

// Install makes every outbound daemon call carry the token. Wrapping the
// default transport rather than each call site means a new call site cannot
// forget to authenticate.
func Install() {
	if _, already := http.DefaultTransport.(*transport); already {
		return
	}
	http.DefaultTransport = &transport{base: http.DefaultTransport}
}
