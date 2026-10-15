package daemonapi

import (
	_ "embed"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clusterstats"
	"github.com/pipedpeer/pipedpeer/internal/jobhistory"
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
		out = append(out, clusterstats.NodeFromAPI(n.NodeID, n.SSHEndpoint, n.DaemonPort,
			n.State, n.Source, n.Capabilities, n.Load))
	}
	return out
}

// handleHistory (C16) serves this machine's own runs. Without id it is a
// clusterstats.History the other nodes' pages merge; with ?id=<job> it is that
// job's detailed run record (steps.json beside the receipt, C24), sent as
// stored so the daemon needs no knowledge of its fields.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Query().Get("id"); id != "" {
		if id != filepath.Base(id) || id == "." || id == ".." {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad job id"})
			return
		}
		if node := r.URL.Query().Get("node"); node != "" {
			s.proxyHistory(w, node, id)
			return
		}
		_, dir, err := jobhistory.ReadRecord(id)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such run: " + id})
			return
		}
		b, err := os.ReadFile(filepath.Join(dir, "steps.json"))
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "this run has no detailed steps"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
		return
	}
	limit := 15
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 100 {
		limit = v
	}
	origin, err := os.Hostname()
	if err != nil {
		log.Printf("daemonapi: /v1/history cannot read the hostname, runs are sent without an origin: %v", err)
	}
	jobs := clusterstats.RecentJobs(limit)
	for i := range jobs {
		jobs[i].Origin = origin
	}
	writeJSON(w, http.StatusOK, clusterstats.History{Origin: origin, Jobs: jobs})
}

// proxyHistory fetches another node's steps.json for the page, which may only
// talk to its own daemon (CSP connect-src 'self'). The call goes out like
// Collect's, so the cluster secret rides along as on every peer call. The
// body is returned unchanged.
func (s *Server) proxyHistory(w http.ResponseWriter, nodeID, id string) {
	nodes, err := s.nodeList()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for _, n := range nodes {
		if n.NodeID != nodeID {
			continue
		}
		cn := clusterstats.NodeFromAPI(n.NodeID, n.SSHEndpoint, n.DaemonPort, n.State, n.Source, nil, n.Load)
		u := fmt.Sprintf("http://%s:%d/v1/history?id=%s", cn.Host, cn.Port, url.QueryEscape(id))
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(u)
		if err != nil {
			log.Printf("daemonapi: /v1/history cannot reach node %s for run %s: %v", nodeID, id, err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "that node did not answer"})
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("daemonapi: /v1/history copying node %s's steps for run %s: %v", nodeID, id, err)
		}
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such node: " + nodeID})
}
