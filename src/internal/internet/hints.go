package internet

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/direct"
	"github.com/pipedpeer/pipedpeer/internal/portmap"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
	"github.com/quic-go/quic-go"
)

// attempt starts a connection to node unless one is up, in flight, or under a
// penalty for the same addresses.
func (m *Manager) attempt(ctx context.Context, node string) {
	now := time.Now()
	m.mu.Lock()
	if m.peers[node] != nil || m.trying[node] {
		m.mu.Unlock()
		return
	}
	cands := m.candidatesLocked(node, now)
	back := m.backoff[node]
	if !shouldTry(back, cands, now) {
		m.mu.Unlock()
		return
	}
	m.trying[node] = true
	m.mu.Unlock()

	go func() {
		defer m.doneTrying(node)
		cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		if err := m.connect(cctx, node, cands); err != nil {
			m.noPath(node, err, candKey(cands))
		}
	}()
}

// candidatesLocked is what to try for node: what it published through an
// introducer, what it sent with a punch request, where its last connection
// was seen, then what other members vouched for. One entry per address.
func (m *Manager) candidatesLocked(node string, now time.Time) []string {
	var out []string
	seen := map[string]bool{}
	add := func(list ...string) {
		for _, c := range list {
			if pc, err := direct.ParseCandidate(c); err == nil && !seen[pc.Addr.String()] {
				seen[pc.Addr.String()] = true
				out = append(out, c)
			}
		}
	}
	add(m.cands[node]...)
	add(m.invited[node]...)
	if o := m.observed[node]; o != "" {
		add(o)
	}
	if h, ok := m.hints[node]; ok && now.Sub(h.at) <= hintTTL {
		add(h.cands...)
	}
	return out
}

// observeLocked records where a peer's connection is.
func (m *Manager) observeLocked(node string, conn *quic.Conn) {
	if conn == nil || m.observed == nil {
		return
	}
	ua, ok := conn.RemoteAddr().(*net.UDPAddr)
	if !ok {
		return
	}
	ap := ua.AddrPort()
	ap = netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	m.observed[node] = string(direct.KindReflex) + ":" + ap.String()
}

// Hint offers addresses for node that another member vouched for (signed
// gossip). They join the candidates tried for it, and an unlinked node is
// tried at once: this is how members find each other again when no
// introducer can introduce them (F34). Each candidate is the wire form
// ("lan:10.0.0.2:4243") or a bare host:port, taken as gossip.
func (m *Manager) Hint(node string, candidates []string) {
	if node == "" || node == m.node() {
		return
	}
	var cands []string
	for _, c := range candidates {
		if _, err := direct.ParseCandidate(c); err == nil {
			cands = append(cands, c)
			continue
		}
		if ap, err := netip.ParseAddrPort(c); err == nil {
			cands = append(cands, string(direct.KindGossip)+":"+ap.String())
			continue
		}
		m.cfg.Log("ignoring the hinted address %q for %s: not host:port", c, short(node))
	}
	if len(cands) == 0 {
		return
	}
	m.mu.Lock()
	m.hints[node] = hint{cands: cands, at: time.Now()}
	ctx := m.ctx
	m.mu.Unlock()
	if ctx != nil {
		m.attempt(ctx, node)
	}
}

// sync registers with every introducer and retries vouched-for peers.
func (m *Manager) sync(ctx context.Context) {
	m.mu.Lock()
	reflex := m.reflex
	keeper := m.keeper
	m.mu.Unlock()

	var mapped netip.AddrPort
	if keeper == nil {
		// Still asking the router (mapPort); register without a mapping.
	} else if mp := keeper.Current(); mp.Kind != portmap.KindNone && mp.Public {
		// The router's word, checked against what a public server actually
		// sees. A range check catches the obvious double-NAT cases, and not
		// this one: a phone tether here granted a UPnP mapping on
		// 172.168.3.211 while the world saw this machine as 103.57.97.77.
		// That address is outside every private range, so it looked
		// publishable, and it belongs to somebody else entirely - publishing
		// it would have peers send unsolicited probes to an uninvolved host.
		//
		// Only skipped when the reflexive address is not known yet, on the
		// first poll, where the mapping is the better guess of the two.
		if mappingIsOurs(mp.External, reflex) {
			mapped = mp.External
		} else {
			m.cfg.Log("router claims %s but the introducer sees %s; not publishing "+
				"the mapping - there is another NAT above this router",
				mp.External.Addr(), reflex.Addr())
		}
	}
	cands := direct.Gather(m.endpoint.Port(), mapped, reflex)

	pkt, err := rendezvous.RegisterPacket(m.cfg.Cluster, m.node(), direct.Strings(cands))
	if err != nil {
		m.cfg.Log("building the introducer registration: %v", err)
		return
	}
	now := time.Now()
	m.mu.Lock()
	intros := append([]*introducer(nil), m.intros...)
	m.mu.Unlock()
	for _, in := range intros {
		if in.hidden {
			continue
		}
		at, err := resolve(in.addr)
		m.mu.Lock()
		if err == nil {
			in.at = at
		}
		if in.waiting.IsZero() {
			in.waiting = now
		}
		m.mu.Unlock()
		if err != nil {
			m.introducerFailed(in, err.Error())
			continue
		}
		if _, err := m.endpoint.Conn().WriteToUDP(pkt, net.UDPAddrFromAddrPort(at)); err != nil {
			m.introducerFailed(in, err.Error())
		}
	}
	m.checkSilence(now)
	m.retryHinted(ctx, now)
}

func resolve(addr string) (netip.AddrPort, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return netip.AddrPort{}, err
	}
	ap := ua.AddrPort()
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()), nil
}

// introducerFailed records why an introducer could not be reached, logging
// only when the reason changes.
func (m *Manager) introducerFailed(in *introducer, reason string) {
	m.mu.Lock()
	changed := in.lastErr != reason
	in.lastErr = reason
	m.mu.Unlock()
	if changed {
		m.cfg.Log("introducer %s: %s", in.addr, reason)
	}
}

// checkSilence reports introducers that stopped answering, once per change
// (F36/F37: a registration that is never answered used to go unnoticed).
func (m *Manager) checkSilence(now time.Time) {
	var silent []*introducer
	m.mu.Lock()
	for _, in := range m.intros {
		if in.hidden || in.silent || in.waiting.IsZero() || now.Sub(in.waiting) < silentAfter {
			continue
		}
		in.silent = true
		if in.lastErr == "" {
			in.lastErr = fmt.Sprintf("no answer for %s", silentAfter)
		}
		silent = append(silent, in)
	}
	m.mu.Unlock()
	for _, in := range silent {
		m.cfg.Log("introducer %s has not answered for %s", in.addr, silentAfter)
	}
}

// retryHinted tries vouched-for peers no introducer lists; listed ones are
// tried from the listing.
func (m *Manager) retryHinted(ctx context.Context, now time.Time) {
	var nodes []string
	m.mu.Lock()
	for node, h := range m.hints {
		if now.Sub(h.at) > hintTTL {
			delete(m.hints, node)
			continue
		}
		if !m.listed[node] {
			nodes = append(nodes, node)
		}
	}
	m.mu.Unlock()
	for _, node := range nodes {
		m.attempt(ctx, node)
	}
}

// expired names the peers that should lose their local port.
//
// A live connection is never expired, whatever the introducer says. That is
// the whole point of not relaying: once two daemons are talking directly the
// introducer is not part of the path, and a link that survives it going away
// is the property this was built for.
//
// It was measured failing. With the introducer stopped, two machines with a
// working punched connection tore it down sixty seconds later - "peer
// stopped checking in" - because the address book had stopped listing them.
// The connection was fine; nothing had stopped except the introducer, which
// is exactly what is supposed to be survivable. A QUIC connection knows
// perfectly well whether it is alive, and its keepalive says so every twenty
// seconds.
//
// For a peer without a live connection the grace period still applies: a
// single lost registration is not a machine going away - the address book is
// UDP, and a dropped packet looks exactly like a departure.
func expired(peers map[string]*peerLink, live map[string]bool, now time.Time, poll time.Duration) []string {
	var gone []string
	for node, link := range peers {
		if live[node] {
			continue
		}
		if link.alive() {
			continue
		}
		if now.Sub(link.lastSeen) > 3*poll {
			gone = append(gone, node)
		}
	}
	return gone
}

// alive reports whether this link's connection is still up.
func (l *peerLink) alive() bool {
	if l == nil || l.conn == nil {
		return false
	}
	select {
	case <-l.conn.Context().Done():
		return false
	default:
		return true
	}
}

func short(node string) string {
	if len(node) > 8 {
		return node[:8]
	}
	return node
}

// mappingIsOurs reports whether a router's claimed external address is really
// this machine's.
//
// A range check catches the obvious double-NAT cases and misses this one: a
// phone tether granted a UPnP mapping on 172.168.3.211 while a public server
// saw the machine as 103.57.97.77. That is outside every private range, so it
// looks publishable, and it is somebody else's address. Comparing it against
// what the introducer actually sees settles it.
//
// True when there is no reflexive address yet, on the first poll: the mapping
// is then the better of two guesses, and one wasted probe costs less than
// publishing nothing at all.
func mappingIsOurs(mapped, reflex netip.AddrPort) bool {
	if !reflex.IsValid() {
		return true
	}
	return reflex.Addr() == mapped.Addr()
}

// shouldTry decides whether a peer under penalty is worth trying again.
//
// Yes once the penalty has run out, and yes immediately when the peer is
// offering different addresses than the ones that earned it: a daemon that
// restarts gets a new mapping and republishes, and that is a new situation
// about which the previous failure says nothing. Making it serve out the old
// penalty is how a restarted peer sat unreachable for minutes while both ends
// were perfectly able to connect.
func shouldTry(back backoffEntry, cands []string, now time.Time) bool {
	if !now.Before(back.until) {
		return true
	}
	return back.tried != candKey(cands)
}

// candKey identifies a set of candidate addresses.
//
// Order-independent, because two registrations listing the same addresses in
// a different order describe the same situation and should not read as a new
// one.
func candKey(cands []string) string {
	sorted := append([]string(nil), cands...)
	sort.Strings(sorted)
	return strings.Join(sorted, "|")
}
