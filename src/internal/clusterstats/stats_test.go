package clusterstats

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/registry"
)

func TestCollectAsksEachNodeAndKeepsTheSilentOnes(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/pool/stats":
			_ = json.NewEncoder(w).Encode(PoolCounters{ChunksReceived: 7, ItemsExecuted: 120})
		case "/v1/jobs":
			_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []Lease{{LeaseID: "l1", JobName: "train"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	snap := Collect([]Node{
		{NodeID: "up", Host: host, Port: port, State: "healthy"},
		{NodeID: "gone", Host: "127.0.0.1", Port: 1, State: "healthy"},
	}, 5, nil)

	if len(snap.Nodes) != 2 {
		t.Fatalf("got %d nodes, want 2 (a silent node is kept)", len(snap.Nodes))
	}
	byID := map[string]NodeStats{}
	for _, n := range snap.Nodes {
		byID[n.NodeID] = n
	}
	up := byID["up"]
	if !up.Reachable || up.Pool == nil || up.Pool.ItemsExecuted != 120 || len(up.Leases) != 1 {
		t.Errorf("answering node = %+v", up)
	}
	if byID["gone"].Reachable {
		t.Error("a node that did not answer is marked reachable")
	}
}

// The CLI's `stats` and the daemon's /ui build their rows here, so both read
// a node's daemon at the same address.
func TestNodeFromAPIParsesTheHost(t *testing.T) {
	for endpoint, want := range map[string]string{
		"root@10.0.1.5:22": "10.0.1.5",
		"10.0.1.5:22":      "10.0.1.5",
		"box":              "box",
		"me@[::1]:22":      "[::1]", // bracketed, so http://[::1]:port works
		"":                 "127.0.0.1",
	} {
		n := NodeFromAPI("id", endpoint, 38080, "healthy", "mdns", nil, registry.LoadInfo{})
		if n.Host != want {
			t.Errorf("NodeFromAPI(%q).Host = %q, want %q", endpoint, n.Host, want)
		}
	}
	n := NodeFromAPI("id", "u@h:22", 1, "s", "src", map[string]string{"a": "b"}, registry.LoadInfo{ActiveJobs: 2})
	if n.NodeID != "id" || n.Port != 1 || n.State != "s" || n.Source != "src" || n.Capabilities["a"] != "b" || n.Load.ActiveJobs != 2 {
		t.Fatalf("fields not carried: %+v", n)
	}
}

func TestCollectMergesRunsFromEveryNodeByStartTime(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	mk := func(origin string, starts ...string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/pool/stats" {
				_ = json.NewEncoder(w).Encode(PoolCounters{})
				return
			}
			if r.URL.Path != "/v1/history" {
				http.NotFound(w, r)
				return
			}
			h := History{Origin: origin}
			for _, s := range starts {
				h.Jobs = append(h.Jobs, JobStats{ID: origin + s, StartedAt: s})
			}
			_ = json.NewEncoder(w).Encode(h)
		}))
	}
	a := mk("alpha", "2026-10-15T10:00:00Z", "2026-10-15T12:00:00Z")
	b := mk("beta", "2026-10-15T11:00:00Z")
	defer a.Close()
	defer b.Close()
	node := func(id string, srv *httptest.Server) Node {
		host, portStr, _ := net.SplitHostPort(srv.Listener.Addr().String())
		port, _ := strconv.Atoi(portStr)
		return Node{NodeID: id, Host: host, Port: port, State: "healthy"}
	}
	snap := Collect([]Node{node("a", a), node("b", b)}, 2, nil)
	if len(snap.Jobs) != 2 || snap.Jobs[0].ID != "alpha2026-10-15T12:00:00Z" ||
		snap.Jobs[1].ID != "beta2026-10-15T11:00:00Z" || snap.Jobs[1].Origin != "beta" {
		t.Fatalf("merged runs = %+v", snap.Jobs)
	}
}

func TestMergeJobsNewestFirstAndLimited(t *testing.T) {
	got := MergeJobs([]JobStats{
		{ID: "a1", Origin: "alpha", StartedAt: "2026-10-15T10:00:00Z"},
		{ID: "b1", Origin: "beta", StartedAt: "2026-10-15T11:00:00Z"},
		{ID: "a2", Origin: "alpha", StartedAt: "2026-10-15T12:00:00Z"},
	}, 2)
	if len(got) != 2 || got[0].ID != "a2" || got[1].ID != "b1" || got[1].Origin != "beta" {
		t.Fatalf("merge = %+v", got)
	}
}
