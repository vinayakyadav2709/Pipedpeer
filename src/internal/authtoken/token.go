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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

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
	if underTest() {
		// A test binary must never pick up the operator's real secret. It
		// did: every package whose tests start or call a daemon began
		// getting 401s on a machine where a token was configured, and in the
		// coordinator that presented as a ten-minute timeout rather than an
		// error — a failure about the developer's machine, not the code.
		// Tests that mean to exercise auth call Set(), which fills the cache
		// directly and is unaffected by this.
		return cached
	}
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

// underTest reports whether this process is a `go test` binary. Those are
// linked as <package>.test; the testing flags are not registered yet during
// package initialisation, so the name is the reliable signal.
func underTest() bool {
	return strings.HasSuffix(os.Args[0], ".test") ||
		strings.Contains(os.Args[0], "/_test/") ||
		flag.Lookup("test.v") != nil
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
		if got == "" && r.Method == http.MethodGet && r.URL.Path == "/health" &&
			VerifyProof(want, r.Host, r.Header.Get(ProofHeader), time.Now()) {
			next.ServeHTTP(w, r)
			return
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

// ProofHeader carries a probe's proof of holding the token, in place of the
// token itself.
//
// LAN discovery asks every address on the local network whether a daemon
// answers there. Sending the token on those requests handed the cluster's
// secret to every host on the subnet, so a probe sends this instead: an HMAC,
// under the token, of the address it is talking to and the time. A host that
// is not a cluster member learns nothing from it it can use elsewhere, and
// the daemon accepts it for GET /health only.
const ProofHeader = "X-Pipedpeer-Proof"

// proofWindow is how far a proof's timestamp may be from the receiver's
// clock. Wide enough for unsynchronised clocks on a LAN, narrow enough that a
// captured proof is soon useless even against the host it was made for.
const proofWindow = 2 * time.Minute

// Proof is the proof for a request to target ("host:port") at time t.
func Proof(token, target string, t time.Time) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	return proofMAC(token, target, ts) + "." + ts
}

func proofMAC(token, target, ts string) string {
	m := hmac.New(sha256.New, []byte(token))
	m.Write([]byte("pipedpeer-probe|" + target + "|" + ts))
	return hex.EncodeToString(m.Sum(nil))
}

// VerifyProof checks a proof made for target, within proofWindow of now.
func VerifyProof(token, target, proof string, now time.Time) bool {
	if token == "" || proof == "" {
		return false
	}
	mac, ts, ok := strings.Cut(proof, ".")
	if !ok {
		return false
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	if d := now.Sub(time.Unix(sec, 0)); d > proofWindow || d < -proofWindow {
		return false
	}
	return hmac.Equal([]byte(mac), []byte(proofMAC(token, target, ts)))
}

// ProbeClient is an HTTP client for discovery probes: it proves holding the
// token without sending it, and never carries the token itself.
func ProbeClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: probeTransport{}}
}

type probeTransport struct{}

func (probeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Del(Header)
	if tok := Current(); tok != "" {
		r.Header.Set(ProofHeader, Proof(tok, r.URL.Host, time.Now()))
	}
	return bareTransport.RoundTrip(r)
}

// bareTransport is the standard transport as it was before Install wrapped
// it, so a probe cannot pick the token up on its way out.
var bareTransport = http.DefaultTransport
