package main

import (
	"net"
	"strings"
	"testing"
)

// TestAddressIsChosenPerPeer.
//
// detectLocalIP picks one address for everybody and prefers a private one. A
// machine on both a home LAN and a Tailscale network has two, of which only
// one reaches a given peer: measured, a node advertised its stale LAN address
// 192.168.0.201 to a peer reachable only over Tailscale. DDP worked anyway
// because the lead rank happened to be the reachable node; with the roles
// reversed the sync URL is an address the other rank cannot open and the run
// hangs at the first barrier.
func TestAddressIsChosenPerPeer(t *testing.T) {
	// A route that certainly exists and is not loopback: whatever the kernel
	// uses to reach a public address.
	got := localIPFor("1.1.1.1")
	if got == "" {
		t.Fatal("no address chosen for a routable peer")
	}
	ip := net.ParseIP(got)
	if ip == nil {
		t.Fatalf("chose %q, which is not an address", got)
	}
	if ip.IsLoopback() {
		t.Errorf("chose the loopback address %s for a remote peer; no other "+
			"machine can reach us there", got)
	}
}

// TestNoPeerFallsBackToTheOldBehaviour, so self-advertisement with nobody in
// particular in mind keeps working exactly as it did.
func TestNoPeerFallsBackToTheOldBehaviour(t *testing.T) {
	if got, want := localIPFor(""), detectLocalIP(); got != want {
		t.Errorf("with no peer, chose %q; want detectLocalIP's %q", got, want)
	}
}

// TestAnUnroutablePeerStillYieldsAnAddress rather than an empty string that
// would be stamped into a URL as "://:38080".
func TestAnUnroutablePeerStillYieldsAnAddress(t *testing.T) {
	for _, target := range []string{"not a host", "192.0.2.1:38080", ":::::"} {
		got := localIPFor(target)
		if got == "" {
			t.Errorf("target %q produced no address at all", target)
		}
		if strings.Contains(got, ":") && net.ParseIP(got) == nil {
			t.Errorf("target %q produced %q, which is not an address", target, got)
		}
	}
}

// TestHostPortAndBareHostAgree. Callers pass whichever they have, and the
// route depends on the host alone.
func TestHostPortAndBareHostAgree(t *testing.T) {
	if a, b := localIPFor("1.1.1.1"), localIPFor("1.1.1.1:38080"); a != b {
		t.Errorf("bare host gave %q and host:port gave %q", a, b)
	}
}

// TestReachingOurselfDoesNotAdvertiseLoopback. A peer on the same machine is
// reached over lo, and telling a second machine to find us at 127.0.0.1 is
// the one answer guaranteed to be wrong.
func TestReachingOurselfDoesNotAdvertiseLoopback(t *testing.T) {
	got := localIPFor("127.0.0.1")
	if net.ParseIP(got) != nil && net.ParseIP(got).IsLoopback() && detectLocalIP() != "127.0.0.1" {
		t.Errorf("chose %q for a loopback peer though this machine has a real "+
			"address (%q)", got, detectLocalIP())
	}
}
