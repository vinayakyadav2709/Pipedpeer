package main

import (
	"net"
	"strings"
	"testing"
)

// TestTheRouteDecidesTheAddress.
//
// This is the whole mechanism: which of our addresses a peer sees depends on
// which of them reaches that peer. detectLocalIP picks one for everybody and
// prefers a private one, so a machine on both a home LAN and a Tailscale
// network advertises the LAN address to a peer only reachable over Tailscale -
// measured, 192.168.0.201, and the run hangs at the first barrier.
//
// Asserted against loopback, which every machine has a distinct route to:
// reaching 127.0.0.1 must not give the same source address as reaching the
// outside world. An earlier version asked only whether the result was a
// non-loopback address, which detectLocalIP also satisfies - so it passed
// with the whole per-peer mechanism removed, and the audit caught it.
func TestTheRouteDecidesTheAddress(t *testing.T) {
	loop, ok := routeSourceIP("127.0.0.1")
	if !ok {
		t.Skip("NOT VERIFIED: no route to loopback on this machine")
	}
	out, ok := routeSourceIP("1.1.1.1")
	if !ok {
		t.Skip("NOT VERIFIED: this machine has no route off itself")
	}
	if loop.Equal(out) {
		t.Errorf("reaching loopback and reaching the internet both gave %s; "+
			"the address is not being chosen per peer", loop)
	}
	if !loop.IsLoopback() {
		t.Errorf("the route to 127.0.0.1 gave %s, not a loopback address", loop)
	}
	if out.IsLoopback() {
		t.Errorf("the route to the internet gave the loopback address %s", out)
	}
}

// TestLoopbackIsNeverAdvertised. A peer on this machine is reached over lo,
// and telling a second machine to find us at 127.0.0.1 is the one answer
// guaranteed to be wrong - so the policy layer overrides the route there.
func TestLoopbackIsNeverAdvertised(t *testing.T) {
	if detectLocalIP() == "127.0.0.1" {
		t.Skip("NOT VERIFIED: this machine has no non-loopback address")
	}
	got := localIPFor("127.0.0.1")
	if ip := net.ParseIP(got); ip != nil && ip.IsLoopback() {
		t.Errorf("advertised %s for a peer reached over loopback", got)
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
