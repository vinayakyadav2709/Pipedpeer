package clusterstats

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
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
