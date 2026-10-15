package internet

import (
	"context"
	"net"
	"net/netip"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/direct"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
)

// serveIntroducer runs this cluster's address book inside the daemon.
//
// Any member can introduce the others (ADR-0004: refused across clusters), so
// the cluster has no special machine and no outside server; a member whose
// introducer port is reachable is enough for the rest to meet.
func (m *Manager) serveIntroducer(ctx context.Context) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{Port: m.cfg.IntroducerPort})
	if err != nil {
		m.cfg.Log("cannot introduce this cluster's members on udp port %d: %v", m.cfg.IntroducerPort, err)
		return
	}
	srv := rendezvous.NewClusterServer(0, m.cfg.Cluster)
	srv.Log = m.cfg.Log
	go func() {
		if err := srv.Serve(pc, ctx.Done()); err != nil && ctx.Err() == nil {
			m.cfg.Log("this cluster's introducer stopped: %v", err)
		}
	}()
	m.cfg.Log("introducing this cluster's members on udp port %d", m.cfg.IntroducerPort)
}

// readIntroducer dispatches datagrams that are neither QUIC nor probes.
//
// The registration replies and the forwarded punch requests both land here.
// A punch request is the important one: it means a peer is trying to reach us
// right now, and answering by racing back is what makes the punch
// simultaneous.
func (m *Manager) readIntroducer(ctx context.Context) {
	// direct.Endpoint hands over what its QUIC transport and prober did not
	// want, which is exactly this.
	for {
		payload, from, err := m.endpoint.NextOther(ctx)
		if err != nil {
			return
		}
		if p, ok := rendezvous.ParsePunch(payload); ok {
			m.onPunchRequest(ctx, p)
			continue
		}
		if you, peers, ok := rendezvous.ParseRegister(payload); ok {
			m.onRegistered(ctx, m.introducerAt(from), you, peers)
			continue
		}
	}
}

// introducerAt is the introducer a reply came from. An address nobody
// registered with gets a hidden entry of its own: its listing is still used,
// and the configured address it probably stands for stays reported as silent,
// which is what it is.
func (m *Manager) introducerAt(from netip.AddrPort) *introducer {
	from = netip.AddrPortFrom(from.Addr().Unmap(), from.Port())
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, in := range m.intros {
		if in.at == from {
			return in
		}
	}
	in := &introducer{addr: from.String(), at: from, hidden: true}
	m.intros = append(m.intros, in)
	m.cfg.Log("introducer reply from %s, an address this node did not register with; using it", from)
	return in
}

// onPunchRequest answers a peer that wants to reach us.
//
// Racing back does two things: it opens this router to that peer's address,
// and it finds the path from this side. Without it the far side punches alone
// and a port-restricted router here drops every packet.
//
// Answered even when we hold a link to that peer: a peer asks only when it
// has none to us, which means it moved or lost the link, and our side's link
// is the stale one (F33). The newer connection replaces it in linkUp.
func (m *Manager) onPunchRequest(ctx context.Context, p rendezvous.Punch) {
	if p.From == m.node() {
		return
	}
	m.mu.Lock()
	m.invited[p.From] = p.Candidates
	busy := m.trying[p.From]
	if !busy {
		m.trying[p.From] = true
	}
	m.mu.Unlock()
	if busy {
		return
	}

	m.cfg.Log("peer %s is trying to reach us; punching back", short(p.From))
	go func() {
		defer m.doneTrying(p.From)
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := m.connect(cctx, p.From, p.Candidates); err != nil {
			m.cfg.Log("could not meet %s: %v", short(p.From), err)
		}
	}()
}

func (m *Manager) doneTrying(node string) {
	m.mu.Lock()
	delete(m.trying, node)
	m.mu.Unlock()
}

// onRegistered records what an introducer said and connects to anyone new.
func (m *Manager) onRegistered(ctx context.Context, in *introducer, you string, peers []rendezvous.Peer) {
	now := time.Now()
	self := m.node()
	listed := map[string]bool{}
	for _, p := range peers {
		if p.Node != self {
			listed[p.Node] = true
		}
	}

	m.mu.Lock()
	wasSilent := in.silent
	in.lastReply = now
	in.waiting = time.Time{}
	in.lastErr = ""
	in.silent = false
	in.members = len(listed)
	in.peers = listed
	if ap, err := netip.ParseAddrPort(you); err == nil {
		in.you = netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	before := m.reflex
	m.reflex = m.bestReflexLocked(now)
	changed := m.reflex.IsValid() && m.reflex != before
	m.mu.Unlock()

	if wasSilent {
		m.cfg.Log("introducer %s is answering again (%d other members)", in.addr, len(listed))
	}
	// Re-register before asking anyone to punch. A peer we ask is woken
	// with the candidates we REGISTERED, and the first registration goes
	// out before the introducer has told us our reflexive address, so it
	// carries LAN addresses only. The woken peer then punched back at a
	// private address on another network and never at our public one,
	// and a port-restricted router on our side dropped everything it
	// sent: in the NAT lab two cone sites never linked by an ordinary
	// punch, only by collision (FIX-NAT2). Sent first, on the same
	// socket, so it reaches the introducer before the punch requests.
	if changed {
		m.sync(ctx)
	}

	for _, p := range peers {
		if p.Node == self {
			continue
		}
		cands := p.Candidates
		// The reflexive address the introducer saw is a candidate too, and
		// on an old daemon that publishes none it is the only one.
		if p.Addr != "" {
			cands = append(append([]string{}, cands...), string(direct.KindReflex)+":"+p.Addr)
		}
		m.mu.Lock()
		m.cands[p.Node] = cands
		if link := m.peers[p.Node]; link != nil {
			link.lastSeen = now
		}
		m.mu.Unlock()
		m.attempt(ctx, p.Node)
	}

	// Peers no fresh introducer lists have stopped checking in. Their
	// local port is kept for one further pass rather than closed at once,
	// because a single missed registration is not a machine going away and
	// tearing the node out makes every job in flight against it fail.
	m.mu.Lock()
	m.listed = m.listedLocked(now)
	gone := expired(m.peers, m.listed, now, m.cfg.Poll)
	m.mu.Unlock()
	for _, node := range gone {
		m.dropLink(node)
	}
}

// fresh reports whether an introducer's last reply still counts.
func (m *Manager) fresh(in *introducer, now time.Time) bool {
	return !in.lastReply.IsZero() && now.Sub(in.lastReply) <= 3*m.cfg.Poll
}

// listedLocked is every peer some fresh introducer lists.
func (m *Manager) listedLocked(now time.Time) map[string]bool {
	out := map[string]bool{}
	for _, in := range m.intros {
		if !m.fresh(in, now) {
			continue
		}
		for node := range in.peers {
			out[node] = true
		}
	}
	return out
}

// bestReflexLocked picks this node's reflexive address from the introducers'
// fresh answers: a public one over a private one, the most recent among
// equals, never loopback. Our own introducer sees us on loopback, and an
// introducer on the LAN sees our LAN address; neither is what a peer on
// another network needs.
func (m *Manager) bestReflexLocked(now time.Time) netip.AddrPort {
	var best netip.AddrPort
	var bestAt time.Time
	bestPublic := false
	for _, in := range m.intros {
		if !m.fresh(in, now) || !in.you.IsValid() || in.you.Addr().IsLoopback() {
			continue
		}
		pub := isPublic(in.you.Addr())
		if !best.IsValid() || (pub && !bestPublic) || (pub == bestPublic && in.lastReply.After(bestAt)) {
			best, bestAt, bestPublic = in.you, in.lastReply, pub
		}
	}
	return best
}

func isPublic(a netip.Addr) bool {
	return a.IsGlobalUnicast() && !a.IsPrivate()
}
