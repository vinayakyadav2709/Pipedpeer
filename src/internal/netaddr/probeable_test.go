package netaddr

import (
	"net"
	"testing"
)

// E6: another member's private address is probed only when it is on one of
// this machine's networks; public, overlay (100.64/10) and names always are.
func TestOnlyPrivateAddressesOffOurNetworksAreNotProbed(t *testing.T) {
	_, lan, _ := net.ParseCIDR("10.88.0.5/24")
	addrs := []net.Addr{lan}
	if !onNetworks(net.ParseIP("10.88.0.9"), addrs) {
		t.Error("an address on our own /24 is not probed")
	}
	if onNetworks(net.ParseIP("192.168.0.3"), addrs) {
		t.Error("a private address on none of our networks is probed")
	}
	for _, h := range []string{"203.0.113.7", "100.101.102.103", "peer.example.org"} {
		if !Probeable(h) {
			t.Errorf("Probeable(%q) = false; only private addresses are filtered", h)
		}
	}
}
