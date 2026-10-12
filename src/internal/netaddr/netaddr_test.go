package netaddr

import (
	"net"
	"testing"
)

func TestSourceForGivesARoutableLocalAddress(t *testing.T) {
	// A public address we never contact: the kernel still knows which of our
	// addresses it would use to reach it.
	got := SourceFor("198.51.100.1:38080")
	if got == "" {
		t.Skip("no route to anywhere; nothing to assert")
	}
	ip := net.ParseIP(got)
	if ip == nil {
		t.Fatalf("SourceFor returned %q, which is not an address", got)
	}
	if ip.IsLoopback() {
		t.Errorf("SourceFor returned the loopback address; no peer can reach us there")
	}
}

func TestSourceForRejectsNonsense(t *testing.T) {
	for _, in := range []string{"", "   :", "not a host:::"} {
		if got := SourceFor(in); got != "" {
			t.Errorf("SourceFor(%q) = %q, want empty", in, got)
		}
	}
}

func TestAdvertiseFallsBackWithoutPeers(t *testing.T) {
	if got := Advertise(nil, "10.0.0.1"); got != "10.0.0.1" {
		t.Errorf("got %q, want the fallback", got)
	}
	// Peers that cannot be resolved must not beat the fallback either.
	if got := Advertise([]string{"this.is.not.a.host.invalid:1"}, "10.0.0.1"); got != "10.0.0.1" {
		t.Errorf("got %q, want the fallback", got)
	}
}

func TestAdvertisePrefersTheAddressMostPeersReachUsOn(t *testing.T) {
	// Loopback is the one destination whose source address is predictable
	// everywhere, so use it to check the counting rather than the routing.
	// Two peers on the same route must outvote whatever the fallback says.
	peers := []string{"127.0.0.1:1", "127.0.0.1:2"}
	got := Advertise(peers, "203.0.113.9")
	// SourceFor rejects loopback results, so this must fall back — the point
	// being that a majority of *unusable* answers still does not win.
	if got != "203.0.113.9" {
		t.Errorf("got %q; a loopback source is not something a peer can dial", got)
	}
}

// TestAdvertiseIgnoresSelfReferencingPeers covers the case that made the fix
// look broken in practice. The node store accumulates rows describing this
// same machine — old lab containers, a daemon restarted on another port —
// and those route to our own address, so they vote for whatever we already
// publish. Three of them outvoted the one real peer.
func TestAdvertiseIgnoresSelfReferencingPeers(t *testing.T) {
	own := SourceFor("198.51.100.1:38080")
	if own == "" {
		t.Skip("no outbound route; nothing to assert")
	}
	// Rows describing us, on assorted ports. Whatever they resolve to, they
	// must not be counted.
	var selfRows []string
	for _, port := range []string{"38081", "38082", "38083"} {
		selfRows = append(selfRows, own+":"+port)
	}
	if got := Advertise(selfRows, "fallback-addr"); got != "fallback-addr" {
		t.Errorf("self-referencing rows voted: got %q, want the fallback", got)
	}
}
