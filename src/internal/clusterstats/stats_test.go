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
