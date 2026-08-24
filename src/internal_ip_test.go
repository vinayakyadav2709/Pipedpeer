package main

import (
	"net"
	"testing"
)

// detectLocalIP is what this node advertises as its own endpoint and what a
// DDP rank 0 hands out as MASTER_ADDR. An address peers cannot dial makes
// every rank hang at rendezvous, so the bar is "reachable", not "non-empty".
func TestDetectLocalIPIsPeerReachable(t *testing.T) {
	got := detectLocalIP()
	if got == "127.0.0.1" {
		t.Skip("host has no usable non-loopback interface")
	}

	ip := net.ParseIP(got)
	if ip == nil {
		t.Fatalf("not an IP: %q", got)
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		t.Fatalf("advertised an undialable address: %s", got)
	}
	// 198.18.0.0/15 is the RFC 2544 benchmark range. Tailscale parks a /32
	// from it on lo, where IsLoopback does not catch it — the original bug.
	if v4 := ip.To4(); v4 != nil && v4[0] == 198 && (v4[1] == 18 || v4[1] == 19) {
		t.Fatalf("advertised a benchmark-range address parked on loopback: %s", got)
	}

	// It must actually belong to an up, non-loopback interface.
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Skipf("cannot enumerate interfaces: %v", err)
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok && n.IP.Equal(ip) {
				return
			}
		}
	}
	t.Fatalf("%s is not on any up, non-loopback interface", got)
}
