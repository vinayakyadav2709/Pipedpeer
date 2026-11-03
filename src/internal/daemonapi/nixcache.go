package daemonapi

import (
	"io"
	"net"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	"github.com/pipedpeer/pipedpeer/internal/direct"
	"github.com/pipedpeer/pipedpeer/internal/peercache"
)

// The local nix binary cache (/nix-cache) is what this machine's nix is told
// to use as an extra substituter. It owns no store paths: each answer is
// fetched from a peer's token-gated /v1/store routes (peerstore.go), and those
// never fan out, so a lookup cannot loop through the cluster.

// maxFanOuts caps the lookups asking peers at once. Nix asks up to
// http-connections (25) narinfos together, so a lookup over the cap waits
// fanOutWait for a slot and then answers 503. Never 404: nix remembers a 404
// (narinfo-cache-negative-ttl, an hour by default), while it retries a 5xx
// and does not cache it.
const maxFanOuts = 16

// fanOutWait is a variable so tests can shorten it.
var fanOutWait = narinfoBudget

var fanOutSem = make(chan struct{}, maxFanOuts)

const (
	narinfoBudget   = 3 * time.Second  // all peers, one narinfo
	hitTTL          = 10 * time.Minute // remembered answers
	missTTL         = 30 * time.Second // remembered all-404 answers
	cacheMaxEntries = 4096
)

// cacheNow is the clock for the hit and miss maps; tests move it.
var cacheNow = time.Now

// exceptLocalNixCache applies auth to everything but GET/HEAD on /nix-cache/
// from this machine. Nix cannot send the token. A loopback caller is this
// machine's nix or another local user. A connection the QUIC splice opened for
// a peer (direct.Spliced) also arrives from loopback, but nothing says which
// process on the far machine opened it, so it is not exempt and needs the
// token like any other remote request.
func exceptLocalNixCache(auth func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		guarded := auth(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := r.URL.Path
			// The Host check keeps a web page that rebinds its name to
			// 127.0.0.1 from reading the cache through the browser.
			// A browser request carries Origin or Sec-Fetch-Site; nix sends
			// neither. A page could otherwise make this machine fan out.
			if strings.HasPrefix(p, peercache.Route+"/") && path.Clean(p) == p &&
				isLoopback(r.RemoteAddr) && !direct.Spliced(r.RemoteAddr) && loopbackHost(r.Host) &&
				r.Header.Get("Origin") == "" && r.Header.Get("Sec-Fetch-Site") == "" {
				if r.Method != http.MethodGet && r.Method != http.MethodHead {
					w.Header().Set("Allow", "GET, HEAD")
					http.Error(w, "the cache is read-only", http.StatusMethodNotAllowed)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			guarded.ServeHTTP(w, r)
		})
	}
}

// loopbackHost reports whether a Host header names this machine's loopback.
func loopbackHost(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = strings.Trim(hostport, "[]")
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type cacheHit struct {
	body    []byte       // the winning peer's narinfo, unchanged
	peers   []PeerHealth // winner first, then peers that may also have it
	expires time.Time
	// busy marks an answer that is no answer: a slot was not free here or at
	// a peer. It is never remembered and goes to nix as 503.
	busy bool
}

// busyReply tells nix to try again later (5xx is retried, a 404 is cached).
func busyReply(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	http.Error(w, "busy, retry", http.StatusServiceUnavailable)
}

// nixCache is the per-server lookup state behind the /nix-cache router.
type nixCache struct {
	s      *Server
	flight singleflight.Group
	mu     sync.Mutex
	hits   map[string]cacheHit
	misses map[string]time.Time
}

// nixCacheRouter returns the router X1 mounts at peercache.Route.
func (s *Server) nixCacheRouter() http.Handler {
	c := &nixCache{s: s, hits: map[string]cacheHit{}, misses: map[string]time.Time{}}
	r := chi.NewRouter()
	for _, m := range []string{http.MethodGet, http.MethodHead} {
		r.MethodFunc(m, "/nix-cache-info", c.handleInfo)
		r.MethodFunc(m, "/nar/{name}", c.handleNar)
		r.MethodFunc(m, "/{name}", c.handleNarinfo)
	}
	return r
}

func (c *nixCache) handleInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/x-nix-cache-info")
	_, _ = io.WriteString(w, peercache.CacheInfo)
}

func (c *nixCache) handleNarinfo(w http.ResponseWriter, r *http.Request) {
	hash, ok := strings.CutSuffix(chi.URLParam(r, "name"), ".narinfo")
	if !ok {
		// .ls files and anything else this cache does not serve.
		http.NotFound(w, r)
		return
	}
	if !peercache.ValidHash(hash) {
		http.Error(w, "bad store hash", http.StatusBadRequest)
		return
	}
	hit, ok := c.lookup(r, hash)
	if hit.busy {
		busyReply(w)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/x-nix-narinfo")
	if r.Method != http.MethodHead {
		_, _ = w.Write(hit.body)
	}
}

func (c *nixCache) handleNar(w http.ResponseWriter, r *http.Request) {
	hash, ok := strings.CutSuffix(chi.URLParam(r, "name"), ".nar.zst")
	if !ok || !peercache.ValidHash(hash) {
		http.Error(w, "bad store hash", http.StatusBadRequest)
		return
	}
	hit, ok := c.lookup(r, hash)
	if hit.busy {
		busyReply(w)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Type", "application/zstd")
		return
	}
	busy := false
	for _, p := range hit.peers {
		resp, err := peerGet(r.Context(), p, "/v1/store/nar/"+hash)
		if err != nil {
			warnPeer(p, err, "nix cache: NAR fetch failed", map[string]any{"hash": hash})
			continue
		}
		if resp.StatusCode != http.StatusOK {
			warnPeer(p, nil, "nix cache: peer refused the NAR", map[string]any{"hash": hash, "status": resp.StatusCode})
			busy = busy || resp.StatusCode == http.StatusServiceUnavailable
			resp.Body.Close()
			continue
		}
		w.Header().Set("Content-Type", "application/zstd")
		if _, err := io.Copy(w, resp.Body); err != nil {
			// Headers are gone; nix sees a short body, rejects it and falls back.
			log.Warn().Err(err).Str("hash", hash).Str("peer", p.NodeID).Msg("nix cache: NAR stream broke")
			if r.Context().Err() == nil { // not nix hanging up
				c.dropSource(hash, p)
			}
		}
		resp.Body.Close()
		return
	}
	// Every source failed: forget the answer so the next ask looks again.
	// A source that was only busy is a 503, which nix retries; a 404 on a
	// NAR whose narinfo it has fails the build.
	c.forget(hash)
	if busy {
		busyReply(w)
		return
	}
	http.NotFound(w, r)
}

// dropSource removes a peer whose NAR stream broke from the remembered hit,
// and the hit with it when no source is left.
func (c *nixCache) dropSource(hash string, p PeerHealth) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.hits[hash]
	if !ok {
		return
	}
	var left []PeerHealth
	for _, q := range h.peers {
		if peerKey(q) != peerKey(p) {
			left = append(left, q)
		}
	}
	if len(left) == 0 {
		delete(c.hits, hash)
		return
	}
	h.peers = left
	c.hits[hash] = h
}

// addSource adds a peer that answered after the winner with the same NarHash.
func (c *nixCache) addSource(hash string, p PeerHealth) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.hits[hash]
	if !ok {
		return
	}
	for _, q := range h.peers {
		if peerKey(q) == peerKey(p) {
			return
		}
	}
	h.peers = append(slices.Clone(h.peers), p)
	c.hits[hash] = h
}

func (c *nixCache) forget(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.hits, hash)
}

// lookup returns the narinfo for hash from the remembered answers or by
// asking the peers. Concurrent lookups of one hash share a single fan-out.
func (c *nixCache) lookup(r *http.Request, hash string) (cacheHit, bool) {
	if h, ok := c.remembered(hash); ok {
		return h, h.body != nil
	}
	v, _, _ := c.flight.Do(hash, func() (any, error) {
		if h, ok := c.remembered(hash); ok {
			return h, nil
		}
		peers := c.s.cachePeers()
		if len(peers) == 0 {
			log.Debug().Str("hash", hash).Msg("nix cache: no peers to ask")
			return cacheHit{}, nil
		}
		wait := time.NewTimer(fanOutWait)
		defer wait.Stop()
		select {
		case fanOutSem <- struct{}{}:
			defer func() { <-fanOutSem }()
		case <-wait.C:
			warnPeer(PeerHealth{NodeID: "local"}, nil, "nix cache: too many lookups in flight, answering 503", map[string]any{"hash": hash})
			return cacheHit{busy: true}, nil
		}
		h, clean, rest := fanOutNarinfo(peers, hash)
		c.remember(hash, h, clean)
		// The slower peers' answers are collected after the slot is free.
		go rest(func(p PeerHealth) { c.addSource(hash, p) })
		return h, nil
	})
	h := v.(cacheHit)
	return h, h.body != nil
}

// remembered returns a live hit, or a zero hit with ok for a live miss.
func (c *nixCache) remembered(hash string) (cacheHit, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := cacheNow()
	if h, ok := c.hits[hash]; ok {
		if now.Before(h.expires) {
			return h, true
		}
		delete(c.hits, hash)
	}
	if until, ok := c.misses[hash]; ok {
		if now.Before(until) {
			return cacheHit{}, true
		}
		delete(c.misses, hash)
	}
	return cacheHit{}, false
}

// remember stores a result. A miss is kept only when every peer answered
// cleanly; a timeout says nothing about whether the peer has the path.
func (c *nixCache) remember(hash string, h cacheHit, clean bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := cacheNow()
	if len(c.hits)+len(c.misses) >= cacheMaxEntries {
		for k, v := range c.hits {
			if !now.Before(v.expires) {
				delete(c.hits, k)
			}
		}
		for k, v := range c.misses {
			if !now.Before(v) {
				delete(c.misses, k)
			}
		}
		for len(c.hits)+len(c.misses) >= cacheMaxEntries {
			for k := range c.hits {
				delete(c.hits, k)
				break
			}
			for k := range c.misses {
				delete(c.misses, k)
				break
			}
		}
	}
	if h.body != nil {
		h.expires = now.Add(hitTTL)
		c.hits[hash] = h
	} else if clean {
		c.misses[hash] = now.Add(missTTL)
	}
}
