package daemonapi

import (
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/nodestore"
)

// TestHowAPeerIsReachedIsVisible.
//
// A relay-free design routes around a peer it cannot reach, and a machine
// that quietly stops serving then looks exactly like one that was never
// there. The route has to be visible, and where it cannot be known it must
// say so rather than guess.
func TestHowAPeerIsReachedIsVisible(t *testing.T) {
	paths := map[string]string{
		"127.0.0.1:33709": "punched",
		"127.0.0.1:41000": "mapped",
	}

	cases := []struct {
		name string
		node nodestore.Node
		want string
		why  string
	}{
		{
			name: "a punched internet peer",
			node: nodestore.Node{Host: "127.0.0.1", Port: 33709, Source: "manual"},
			want: "punched",
			why:  "the manager that made the forwarder is the only thing that knows",
		},
		{
			name: "a peer on a mapped port",
			node: nodestore.Node{Host: "127.0.0.1", Port: 41000, Source: "manual"},
			want: "mapped",
			why:  "a router granted this one; no punching was needed",
		},
		{
			name: "this machine",
			node: nodestore.Node{Host: "127.0.0.1", Port: 38080, Source: "self"},
			want: "self",
			why:  "there is no path to itself",
		},
		{
			name: "a LAN peer",
			node: nodestore.Node{Host: "192.168.0.5", Port: 38080, Source: "discovery"},
			want: "lan",
			why:  "a routable address is talked to directly by construction",
		},
		{
			name: "a forwarder whose link has gone",
			node: nodestore.Node{Host: "127.0.0.1", Port: 55555, Source: "manual"},
			want: "",
			why:  "no manager claims it, and naming a route here would invent one",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pathFor(tc.node, paths); got != tc.want {
				t.Errorf("pathFor = %q, want %q — %s", got, tc.want, tc.why)
			}
		})
	}
}

// With nothing joined over the internet there is no reporter, and every
// routable peer is still reached directly.
func TestPathsWithoutInternetMode(t *testing.T) {
	if got := pathFor(nodestore.Node{Host: "10.0.0.4", Port: 38080}, nil); got != "lan" {
		t.Errorf("pathFor = %q, want lan", got)
	}
	if got := pathFor(nodestore.Node{Host: "127.0.0.1", Port: 1, Source: "manual"}, nil); got != "" {
		t.Errorf("pathFor = %q, want empty: nothing knows how that is reached", got)
	}
}
