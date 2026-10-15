package daemonapi

import (
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nodestore"
)

// A stored 'healthy' row is only as fresh as the last poll that wrote it. When
// a peer's daemon dies nothing rewrites the row, and while the local daemon is
// down nothing polls at all, so the state outlives the node. Everything that
// reads /v1/nodes filters on State == "healthy" — including placement — so the
// endpoint has to apply the last_seen cutoff itself.
func TestToNodeRespDowngradesStaleHealthy(t *testing.T) {
	tests := []struct {
		name  string
		state string
		age   time.Duration
		want  string
	}{
		{"fresh healthy stays healthy", "healthy", 5 * time.Second, "healthy"},
		{"healthy just inside cutoff", "healthy", staleAfter - time.Second, "healthy"},
		{"healthy past cutoff goes stale", "healthy", staleAfter + time.Second, "stale"},
		{"long dead goes stale", "healthy", 6 * time.Hour, "stale"},
		{"unreachable is left alone", "unreachable", 6 * time.Hour, "unreachable"},
		{"unknown is left alone", "unknown", 6 * time.Hour, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toNodeResp(nodestore.Node{
				NodeID:   "n1",
				Host:     "10.0.0.5",
				Port:     4242,
				State:    tt.state,
				LastSeen: time.Now().Add(-tt.age).Unix(),
			})
			if got.State != tt.want {
				t.Fatalf("state %q last seen %s ago: got %q, want %q",
					tt.state, tt.age, got.State, tt.want)
			}
		})
	}
}

// /v1/nodes keeps every old field (ssh_endpoint stays "root@<host>:22" for its
// parsers) and adds host, path and reason from the active path.
func TestToNodeRespCarriesHostPathReason(t *testing.T) {
	got := toNodeResp(nodestore.Node{
		NodeID: "n1", Host: "127.0.0.1", Port: 40001, Source: "internet",
		State: "unreachable", Reason: "connection refused",
	})
	if got.SSHEndpoint != "root@127.0.0.1:22" || got.Host != "127.0.0.1" || got.DaemonPort != 40001 ||
		got.Path != "internet" || got.Capabilities[PathCapability] != "internet" || got.Reason != "connection refused" {
		t.Fatalf("got %+v", got)
	}
	if empty := toNodeResp(nodestore.Node{NodeID: "n2"}); empty.Path != "" || empty.SSHEndpoint != "" || empty.State != "unknown" {
		t.Fatalf("row with no address: got %+v, want no path, no endpoint, state unknown", empty)
	}
}
