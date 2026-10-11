package daemonapi

import (
	_ "embed"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clusterstats"
)

// uiPage is the cluster page served at /ui: one self-contained file with no
// external resources, because a daemon is often on a network with no route
// to a CDN, and a page that cannot load its script shows nothing at all.
//
//go:embed ui/index.html
var uiPage []byte

// exceptUIPage applies auth to everything but the page itself. The page is
// static and carries no data; it asks for the token and sends it with every
// /v1/stats request, which stays behind auth like the rest of the API. A
// browser cannot attach a header to the address-bar request, so without this
// the page could never load on a cluster with a secret set.
func exceptUIPage(auth func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		guarded := auth(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/ui" {
				next.ServeHTTP(w, r)
				return
			}
			guarded.ServeHTTP(w, r)
		})
	}
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:")
	_, _ = w.Write(uiPage)
}

// statsCache keeps one snapshot for a moment. The page polls every two
// seconds, and each snapshot asks every node for its counters; a few open
// tabs should not multiply that.
type statsCache struct {
	mu   sync.Mutex
	at   time.Time
	snap clusterstats.Snapshot
}

const statsTTL = time.Second

// handleStats serves the cluster snapshot: the same data as
// `pipedpeer stats --json`, built by the same package.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.stats.mu.Lock()
	defer s.stats.mu.Unlock()
	if time.Since(s.stats.at) > statsTTL {
		nodes, err := s.nodeList()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		limit := 15
		if v, err := strconv.Atoi(r.URL.Query().Get("jobs")); err == nil && v > 0 && v <= 100 {
			limit = v
		}
		s.stats.snap = clusterstats.Collect(toStatsNodes(s.withUnreachable(nodes)), limit, nil)
		s.stats.at = time.Now()
	}
	writeJSON(w, http.StatusOK, s.stats.snap)
}

func toStatsNodes(nodes []nodeResp) []clusterstats.Node {
	out := make([]clusterstats.Node, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, clusterstats.Node{
			NodeID: n.NodeID, Host: hostOf(n.SSHEndpoint), Port: n.DaemonPort,
			State: n.State, Source: n.Source, Capabilities: n.Capabilities, Load: n.Load,
		})
	}
	return out
}

// hostOf pulls the host out of "user@host:port".
func hostOf(endpoint string) string {
	if i := strings.Index(endpoint, "@"); i >= 0 {
		endpoint = endpoint[i+1:]
	}
	if h, _, err := net.SplitHostPort(endpoint); err == nil {
		return h
	}
	return endpoint
}
