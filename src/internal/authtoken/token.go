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
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/rs/zerolog/log"
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
	c, err := clustercfg.Load()
	if err != nil {
		log.Warn().Err(err).Msg("authtoken: cannot read the cluster config; running without a token")
		return cached
	}
	cached = strings.TrimSpace(c.Token)
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

// Set writes a token into the cluster config, or clears it when tok is empty.
func Set(tok string) error {
	tok = strings.TrimSpace(tok)
	c, err := clustercfg.Load()
	if err != nil {
		return err
	}
	c.Token = tok
	// 0600 (clustercfg.Save): a token readable by every user on the box protects nothing.
	if err := clustercfg.Save(c); err != nil {
		return err
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
func Path() string { return clustercfg.Path() }

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
		if got == "" && r.Method == http.MethodGet && r.URL.Path == "/health" {
			if c := r.Header.Get(ChallengeHeader); validChallenge(c) {
				p, err := proof(want, c, r.TLS)
				if err == nil {
					// Nothing but the proof: an asker without the token learns
					// only that this host holds the one it challenged with.
					w.Header().Set(ProofHeader, p)
					w.WriteHeader(http.StatusNoContent)
					return
				}
				log.Warn().Err(err).Str("remote", r.RemoteAddr).Msg("cannot bind the token proof to this TLS session")
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
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	// A challenge is the proof exchange itself; it must leave bare.
	if r.Header.Get(ChallengeHeader) != "" {
		return base.RoundTrip(r)
	}
	if tok := Current(); tok != "" && isDaemonAPI(r.URL.Path) && r.Header.Get(Header) == "" {
		r = r.Clone(r.Context())
		r.Header.Set(Header, tok)
	}
	// Whoever set the token, the host gets it only once it has shown it
	// already holds it. Answering on the daemon port is not enough: the LAN
	// sweep and a stale address book both reach hosts nobody vetted.
	if sent := r.Header.Get(Header); sent != "" {
		if err := prove(r.Context(), base, r.URL.Scheme, r.URL.Host, sent); err != nil {
			if r.Body != nil {
				_ = r.Body.Close() // RoundTrip owns the body, even on error
			}
			return nil, err
		}
	}
	return base.RoundTrip(r)
}

// ChallengeHeader and ProofHeader carry the exchange in which a host proves
// it holds the token before being sent it.
const (
	ChallengeHeader = "X-Pipedpeer-Challenge"
	ProofHeader     = "X-Pipedpeer-Proof"
	proofLabel      = "EXPORTER-pipedpeer-token-proof"
)

// proof is the answer to a challenge. Over TLS it is bound to the session's
// exporter, so a host that merely relays the challenge to a real daemon
// cannot pass that daemon's answer off as its own. Over plain HTTP there is
// no session to bind to, and the binding is "".
func proof(tok, challenge string, cs *tls.ConnectionState) (string, error) {
	ekm := ""
	if cs != nil {
		b, err := cs.ExportKeyingMaterial(proofLabel, nil, 32)
		if err != nil {
			return "", err
		}
		ekm = hex.EncodeToString(b)
	}
	m := hmac.New(sha256.New, []byte(tok))
	m.Write([]byte("pipedpeer-proof/1|" + challenge + "|" + ekm))
	return hex.EncodeToString(m.Sum(nil)), nil
}

func validChallenge(c string) bool {
	if len(c) != 32 {
		return false
	}
	_, err := hex.DecodeString(c)
	return err == nil
}

// isLoopback matches the hosts tlsid never upgrades: the job's own daemon.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
}

var (
	provenMu sync.Mutex
	proven   = map[string]string{} // "host:port" -> the token it proved it holds
	refused  = map[string]bool{}   // "host:port" -> refusal already logged
)

// Prove checks that the daemon at addr ("host:port") holds this process's
// token, for callers that attach the token outside http.DefaultTransport
// (the websocket dialer). Success is remembered per addr; failure is not, so
// a peer that was down is asked again next time.
func Prove(ctx context.Context, addr string) error {
	return prove(ctx, http.DefaultTransport, "http", addr, Current())
}

// ponytail: a proof is remembered for the life of the process. Over TLS the
// pin keeps it honest; over plain HTTP a host that takes the address over
// later inherits it. Expire entries if that matters.
func prove(ctx context.Context, rt http.RoundTripper, scheme, addr, tok string) error {
	if tok == "" || isLoopback(addr) {
		return nil
	}
	provenMu.Lock()
	ok := proven[addr] == tok
	provenMu.Unlock()
	if ok {
		return nil
	}
	err := askProof(ctx, rt, scheme, addr, tok)
	provenMu.Lock()
	defer provenMu.Unlock()
	if err == nil {
		proven[addr] = tok
		delete(refused, addr)
		return nil
	}
	err = fmt.Errorf("%s could not prove it holds the cluster token; not sending it: %w", addr, err)
	if !refused[addr] {
		refused[addr] = true
		log.Warn().Err(err).Str("addr", addr).Msg("withholding the cluster token")
	}
	return err
}

func askProof(ctx context.Context, rt http.RoundTripper, scheme, addr, tok string) error {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Errorf("making a challenge: %w", err)
	}
	challenge := hex.EncodeToString(b)
	if scheme == "" {
		scheme = "http"
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+addr+"/health", nil)
	if err != nil {
		return err
	}
	req.Header.Set(ChallengeHeader, challenge)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	got := resp.Header.Get(ProofHeader)
	if got == "" {
		return fmt.Errorf("its reply carried no proof (HTTP %d)", resp.StatusCode)
	}
	want, err := proof(tok, challenge, resp.TLS)
	if err != nil {
		return fmt.Errorf("binding the proof to the TLS session: %w", err)
	}
	if !hmac.Equal([]byte(got), []byte(want)) {
		return errors.New("its proof does not match")
	}
	return nil
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
