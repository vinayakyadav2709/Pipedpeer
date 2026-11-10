package console

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

const (
	loopbackIP  = "127.0.0.1"
	cookieName  = "pp_console"
	tokenBytes  = 32
	tokenParam  = "token"
	bearerLabel = "Bearer "
)

func newToken() (string, error) {
	b := make([]byte, tokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// guard holds what a request must match to be served: the token, the Host
// values this server answers to, and the Origins a browser may call from.
type guard struct {
	token   string
	hosts   map[string]bool
	origins map[string]bool
}

func newGuard(token string, port int, devOrigin string) *guard {
	g := &guard{token: token, hosts: map[string]bool{}, origins: map[string]bool{}}
	g.allowLoopback(fmt.Sprint(port))
	if devOrigin == "" {
		return g
	}
	// A dev server proxying /api may forward its own Host, so in dev mode its
	// address is accepted as a Host as well as an Origin.
	u, err := url.Parse(devOrigin)
	if err != nil || u.Host == "" {
		return g
	}
	if name := u.Hostname(); name == loopbackIP || name == "localhost" {
		g.allowLoopback(u.Port())
		return g
	}
	g.hosts[u.Host] = true
	g.origins[u.Scheme+"://"+u.Host] = true
	return g
}

func (g *guard) allowLoopback(port string) {
	for _, name := range []string{loopbackIP, "localhost"} {
		host := name + ":" + port
		g.hosts[host] = true
		g.origins["http://"+host] = true
	}
}

// checkHost rejects requests addressed to any other name. A page on a hostile
// site that rebinds its DNS name to 127.0.0.1 still sends its own name here.
func (g *guard) checkHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.hosts[r.Host] {
			writeError(w, http.StatusForbidden, "host not allowed")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// checkOrigin requires a browser-supplied Origin to be this server (or the dev
// origin). Requests without an Origin come from non-browser clients, which
// still need the token.
func (g *guard) checkOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && !g.origins[origin] {
			writeError(w, http.StatusForbidden, "origin not allowed")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (g *guard) valid(candidate string) bool {
	return candidate != "" && subtle.ConstantTimeCompare([]byte(candidate), []byte(g.token)) == 1
}

func (g *guard) authenticated(r *http.Request) bool {
	if c, err := r.Cookie(cookieName); err == nil && g.valid(c.Value) {
		return true
	}
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, bearerLabel) {
		return g.valid(strings.TrimPrefix(auth, bearerLabel))
	}
	return false
}

func (g *guard) requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.authenticated(r) {
			writeError(w, http.StatusUnauthorized, "missing or invalid console token")
			return
		}
		// State-changing requests from a browser must come from our own page.
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if origin := r.Header.Get("Origin"); origin != "" && !g.origins[origin] {
				writeError(w, http.StatusForbidden, "origin not allowed")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// exchangeToken turns a valid ?token= on a page GET into the session cookie
// and redirects to the same path without it, so the token does not stay in the
// address bar or history.
func (g *guard) exchangeToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.Method != http.MethodGet || !q.Has(tokenParam) {
			next.ServeHTTP(w, r)
			return
		}
		if !g.valid(q.Get(tokenParam)) {
			writeError(w, http.StatusUnauthorized, "invalid console token")
			return
		}
		http.SetCookie(w, &http.Cookie{
			Name:     cookieName,
			Value:    g.token,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		})
		q.Del(tokenParam)
		target := r.URL.EscapedPath()
		if rest := q.Encode(); rest != "" {
			target += "?" + rest
		}
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, target, http.StatusFound)
	})
}
