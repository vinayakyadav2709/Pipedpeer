package tlsid

import (
	"net/http"
	"strings"
	"sync"
)

// Outbound calls prefer TLS and remember what each peer answered, so a
// cluster mid-upgrade keeps working in both directions.
//
// The alternative — a cluster-wide switch — means every daemon changes scheme
// at once and anything missed goes silent. Preferring https and falling back
// on a handshake failure lets nodes be updated one at a time.
//
// The fallback is a migration aid, not a security posture: an attacker who
// can break the TLS connection can force plain HTTP. What stops that
// mattering is the pin — once a peer has answered TLS it is remembered, so
// the window is the first contact only, and the token gates that. Removing
// the fallback entirely is what a "require TLS" mode would do, and is worth
// having once no unupgraded daemons remain.
type schemeMemo struct {
	mu  sync.RWMutex
	tls map[string]bool // "host:port" -> speaks TLS
}

var schemes = &schemeMemo{tls: map[string]bool{}}

func (m *schemeMemo) get(addr string) (bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.tls[addr]
	return v, ok
}

func (m *schemeMemo) set(addr string, isTLS bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tls[addr] = isTLS
}

// URL builds a request URL for a peer, choosing the scheme it is known to
// speak. Unknown peers get https; Do falls back if that turns out wrong.
func URL(addr, path string) string {
	scheme := "https"
	if speaks, known := schemes.get(addr); known && !speaks {
		scheme = "http"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return scheme + "://" + addr + path
}

// Client returns an http.Client that verifies peers by pinned fingerprint.
func Client(addr string, base *http.Client) *http.Client {
	c := &http.Client{}
	if base != nil {
		*c = *base
	}
	c.Transport = &http.Transport{
		TLSClientConfig:     ClientConfig(addr),
		ForceAttemptHTTP2:   true,
		MaxIdleConnsPerHost: 4,
	}
	return c
}

// Note downgrades a peer to plain HTTP after a failed TLS attempt, so the
// next call skips straight there rather than paying for the handshake again.
func Note(addr string, speaksTLS bool) { schemes.set(addr, speaksTLS) }

// transport upgrades outbound daemon calls to TLS without touching the
// dozens of places that build "http://host:port/..." URLs. A call site that
// has to remember to be secure eventually forgets.
type transport struct {
	base   http.RoundTripper
	secure sync.Map // addr -> *http.Client with the pin for that peer
	baseMu sync.Mutex
}

func (t *transport) clientFor(addr string) *http.Client {
	if c, ok := t.secure.Load(addr); ok {
		return c.(*http.Client)
	}
	c := Client(addr, nil)
	actual, _ := t.secure.LoadOrStore(addr, c)
	return actual.(*http.Client)
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !t.eligible(r) {
		return t.base.RoundTrip(r)
	}
	addr := r.URL.Host
	if speaks, known := schemes.get(addr); known && !speaks {
		return t.base.RoundTrip(r)
	}

	secure := r.Clone(r.Context())
	secure.URL.Scheme = "https"
	resp, err := t.clientFor(addr).Do(secure)
	if err == nil {
		Note(addr, true)
		return resp, nil
	}
	// A pin mismatch is a refusal, not a reason to try again in the clear:
	// falling back there would hand the request to exactly the party the pin
	// was protecting against.
	if strings.Contains(err.Error(), "certificate for") {
		return nil, err
	}
	Note(addr, false)
	return t.base.RoundTrip(r)
}

// eligible reports whether a request should be tried over TLS first.
func (t *transport) eligible(r *http.Request) bool {
	if r.URL.Scheme != "http" {
		return false
	}
	if !(r.URL.Path == "/health" || strings.HasPrefix(r.URL.Path, "/v1/")) {
		return false
	}
	// Loopback is the job's own daemon. Encrypting a connection that never
	// leaves the machine costs handshakes and would mean teaching the shim
	// to trust this node's certificate, for no attacker it excludes.
	if host := r.URL.Hostname(); host == "127.0.0.1" || host == "::1" || host == "localhost" {
		return false
	}
	// A body that cannot be replayed cannot be retried, and the fallback
	// needs a retry. Those requests use whatever scheme is already known,
	// which the constant health polling establishes within seconds.
	if r.Body != nil && r.GetBody == nil {
		return false
	}
	return true
}

// Install makes outbound daemon calls prefer TLS, verified by pin.
func Install() {
	if _, already := http.DefaultTransport.(*transport); already {
		return
	}
	http.DefaultTransport = &transport{base: http.DefaultTransport}
}
