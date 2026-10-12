// Package netaddr answers "which of this machine's addresses can a peer
// actually reach us on".
//
// Guessing from the interface list gets this wrong the moment a machine has
// more than one route to the world. The previous rule preferred any RFC1918
// address, so a laptop with a stale 192.168.x lease advertised that while its
// peers were reachable only over an overlay — Tailscale's 100.64/10 is CGNAT,
// which Go's IsPrivate does not count as private, so it always lost. Nothing
// failed loudly: the node published an address no peer could dial, and DDP
// happened to work only because the reachable machine drew rank 0.
//
// The kernel already knows the answer. Asking it which source address it
// would use for a given destination is exact, and costs nothing: a UDP
// "connection" sends no packets.
package netaddr

import (
	"net"
	"sort"
)

// SourceFor returns the local address the kernel would use to reach peer
// ("host:port" or bare host), or "" if it has no route.
func SourceFor(peer string) string {
	host := peer
	if h, _, err := net.SplitHostPort(peer); err == nil {
		host = h
	}
	if host == "" {
		return ""
	}
	// Port is irrelevant to route selection; UDP dial is connectionless, so
	// nothing is sent and no peer is contacted.
	conn, err := net.Dial("udp", net.JoinHostPort(host, "9"))
	if err != nil {
		return ""
	}
	defer conn.Close()
	local, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		return ""
	}
	ip := net.ParseIP(local)
	if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
		return ""
	}
	return local
}

// Advertise picks the address to publish to peers: the one most of them would
// reach us on, falling back to the caller's guess when there are no peers yet
// or none is routable.
//
// Majority rather than first, so one unreachable straggler in the node store —
// and those accumulate, since manual entries never expire — cannot drag the
// whole cluster onto an address the others cannot use.
func Advertise(peers []string, fallback string) string {
	counts := map[string]int{}
	for _, p := range peers {
		src := SourceFor(p)
		if src == "" {
			continue
		}
		// A "peer" that is really this machine under another port routes to
		// our own address, so it would vote for whatever we already publish.
		// The node store accumulates those - old lab containers, a daemon
		// restarted on a different port - and three of them outvoted the one
		// real peer, which is exactly the stale answer this is meant to fix.
		host := p
		if h, _, err := net.SplitHostPort(p); err == nil {
			host = h
		}
		if host == src {
			continue
		}
		counts[src]++
	}
	if len(counts) == 0 {
		return fallback
	}
	type pair struct {
		addr string
		n    int
	}
	var ranked []pair
	for a, n := range counts {
		ranked = append(ranked, pair{a, n})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].n != ranked[j].n {
			return ranked[i].n > ranked[j].n
		}
		return ranked[i].addr < ranked[j].addr // stable when tied
	})
	return ranked[0].addr
}
