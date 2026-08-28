// Package internet joins a daemon to peers it cannot reach on a local
// network, and does it without anything in the middle carrying the traffic.
//
// It used to relay everything. That worked everywhere and cost whoever ran
// the relay: every byte of every closure crossed their machine, was billed as
// their egress, and was bounded by their uplink. Worse, it did so even for
// two machines on the same Wi-Fi, whose packets went out to the internet and
// back for no reason at all.
//
// So the rendezvous is an introducer now and nothing else. It carries a few
// hundred bytes to tell two daemons where each other might be and to make
// them punch at the same moment; after that they talk directly and it can go
// away entirely. What passes through it per connection is measured in
// kilobytes, which is the difference between a $3 box and a bandwidth bill.
//
// Each peer still gets a local port that forwards to its daemon, which is
// what keeps the rest of the system ignorant: health polling, closure upload,
// pool spill and gradient sync all speak HTTP to a host and a port, and none
// of them has to learn what carries it.
//
// # When there is no path
//
// Some pairs cannot connect. Two machines both behind NATs that allocate a
// fresh port per destination have no address to exchange, and no protocol
// solves that - which is why every system that punches also ships a relay.
// This one does not use it: a peer that cannot be reached is recorded as
// unreachable WITH THE REASON, and the scheduler routes around it. That is
// available here and not to a video call, because a job needs *a* machine
// rather than *that* machine. A peer that vanishes silently would be the
// worst of both, so the reason is carried all the way to `pipedpeer nodes`.
package internet

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/direct"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/portmap"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
	"github.com/quic-go/quic-go"
)

// DefaultPort is the shared UDP port everything direct goes over.
//
// Fixed rather than ephemeral: a port mapping and a peer's cached address are
// both worth keeping across a restart, and neither survives a port that
// changes every time the daemon starts.
const DefaultPort = 38447

// Config is what a daemon needs to join over the internet.
type Config struct {
	// Rendezvous is the introducer, host:port. Empty disables all of this.
	Rendezvous string
	// Cluster is derived from the shared token; peers in different clusters
	// never see each other.
	Cluster string
	// Key identifies this node. The address peers reach it at is this key's
	// fingerprint.
	Key identity.KeyPair
	// Cert is this node's TLS certificate for the QUIC listener. Self-signed:
	// identity is proved by the signed hello, not by a chain.
	Cert *tls.Certificate
	// DaemonPort is the local daemon peers' streams are handed to.
	DaemonPort int
	// DirectPort is the shared UDP port. Zero means DefaultPort.
	DirectPort int
	// Poll is how often to re-register and look for new peers. Also what
	// keeps this node's own NAT mapping alive, so it must stay comfortably
	// under a NAT's idle timeout - commonly thirty seconds.
	Poll time.Duration
	// OnPeer is called with a local address once a peer is reachable there.
	OnPeer func(node, localAddr string)
	// OnPeerGone is called when a peer stops answering.
	OnPeerGone func(node, localAddr string)
	// OnUnreachable is called when a peer exists but no direct path to it
	// does, with a reason fit to show a user. The scheduler routes around
	// such peers; this is how they stay visible while doing it.
	OnUnreachable func(node, reason string)
	// Log receives progress.
	Log func(format string, args ...any)
}

// Manager keeps the cluster's remote members reachable.
type Manager struct {
	cfg      Config
	endpoint *direct.Endpoint
	keeper   *portmap.Keeper

	mu    sync.Mutex
	peers map[string]*peerLink
	// reflex is this node's address as the introducer last saw it.
	reflex netip.AddrPort
	// backoff is when to next try a peer that had no path, and which
	// addresses earned the penalty.
	backoff map[string]backoffEntry
	// waiting are peers that asked us to punch, so an incoming request is
	// answered by racing rather than ignored.
	invited map[string][]string
}

// backoffEntry records a failed attempt.
//
// tried is what was tried, because a penalty belongs to a SITUATION rather
// than to a peer. When a daemon restarts its socket gets a new mapping and it
// republishes different candidates: that is a new situation, and making it
// serve out a penalty earned by addresses that no longer exist is how a
// restarted peer sat unreachable for minutes while both machines were
// perfectly able to connect. Retrying on change costs nothing extra - the
// addresses changed, so the previous failure says nothing about these ones.
type backoffEntry struct {
	until time.Time
	tried string
}

type peerLink struct {
	listener net.Listener
	addr     string
	conn     *quic.Conn
	stop     context.CancelFunc
	lastSeen time.Time
	// path says how this peer is reached, for `pipedpeer nodes`.
	path string
}

func New(cfg Config) *Manager {
	if cfg.Poll <= 0 {
		// Under a NAT's usual thirty-second idle timeout: the registration
		// is also what keeps this node's own mapping alive, so polling more
		// slowly than that loses the mapping between polls.
		cfg.Poll = 20 * time.Second
	}
	if cfg.DirectPort == 0 {
		cfg.DirectPort = DefaultPort
	}
	if cfg.Log == nil {
		cfg.Log = func(string, ...any) {}
	}
	return &Manager{
		cfg:     cfg,
		peers:   map[string]*peerLink{},
		backoff: map[string]backoffEntry{},
		invited: map[string][]string{},
	}
}

// Run joins the cluster and keeps it joined until ctx ends.
//
// Failures are never fatal: a machine with no internet, or an introducer that
// is down, must still run local jobs. Everything is retried on the next tick.
func (m *Manager) Run(ctx context.Context) {
	if m.cfg.Rendezvous == "" {
		return
	}

	ep, err := direct.Listen(direct.Config{
		Port:    m.cfg.DirectPort,
		Key:     m.cfg.Key,
		Cluster: m.cfg.Cluster,
		Cert:    m.cfg.Cert,
		Local:   direct.LocalDialer(fmt.Sprintf("127.0.0.1:%d", m.cfg.DaemonPort)),
		Log:     m.cfg.Log,
		// A peer that dialled us is just as reachable as one we dialled, and
		// for a pair where one side cannot be dialled at all it is the only
		// way a link is ever made. Without this the connection was
		// authenticated and then left unused while the far side reconnected
		// every few seconds - seen against the two real machines.
		OnInbound: func(node string, conn *quic.Conn) {
			if err := m.linkUp(ctx, node, conn, "punched-in"); err != nil {
				m.cfg.Log("could not use the connection from %s: %v", short(node), err)
			}
		},
	})
	if err != nil {
		m.cfg.Log("cannot listen for direct connections: %v", err)
		return
	}
	m.endpoint = ep
	defer ep.Close()
	go func() { _ = ep.Serve(ctx) }()

	// Ask the router for a port. When it grants one this node is reachable
	// without any punching at all, which is the best case and the cheapest.
	m.keeper = portmap.NewKeeper(ctx, ep.Port(), m.cfg.Log)
	defer m.keeper.Close()
	if mp := m.keeper.Current(); mp.Kind != portmap.KindNone {
		m.cfg.Log("router granted %s", mp)
	}

	// The introducer's replies and peers' punch requests arrive on the same
	// socket as everything else, so one reader dispatches them.
	go m.readIntroducer(ctx)

	ticker := time.NewTicker(m.cfg.Poll)
	defer ticker.Stop()
	defer m.closeAll()

	for {
		m.sync(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
		_ = from
		if p, ok := rendezvous.ParsePunch(payload); ok {
			m.onPunchRequest(ctx, p)
			continue
		}
		if you, peers, ok := rendezvous.ParseRegister(payload); ok {
			m.onRegistered(ctx, you, peers)
			continue
		}
	}
}

// onPunchRequest answers a peer that wants to reach us.
//
// Racing back does two things: it opens this router to that peer's address,
// and it finds the path from this side. Without it the far side punches alone
// and a port-restricted router here drops every packet.
func (m *Manager) onPunchRequest(ctx context.Context, p rendezvous.Punch) {
	m.mu.Lock()
	m.invited[p.From] = p.Candidates
	if p.You != "" {
		if ap, err := netip.ParseAddrPort(p.You); err == nil {
			m.reflex = ap
		}
	}
	_, already := m.peers[p.From]
	m.mu.Unlock()
	if already {
		return
	}

	m.cfg.Log("peer %s is trying to reach us; punching back", short(p.From))
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := m.connect(cctx, p.From, p.Candidates); err != nil {
			m.cfg.Log("could not meet %s: %v", short(p.From), err)
		}
	}()
}

// onRegistered records what the introducer said and connects to anyone new.
func (m *Manager) onRegistered(ctx context.Context, you string, peers []rendezvous.Peer) {
	if ap, err := netip.ParseAddrPort(you); err == nil {
		m.mu.Lock()
		m.reflex = ap
		m.mu.Unlock()
	}

	live := map[string]bool{}
	for _, p := range peers {
		if p.Node == m.node() {
			continue
		}
		live[p.Node] = true

		m.mu.Lock()
		link, connected := m.peers[p.Node]
		if connected {
			link.lastSeen = time.Now()
		}
		back := m.backoff[p.Node]
		m.mu.Unlock()
		if connected {
			continue
		}

		cands := p.Candidates
		// The reflexive address the introducer saw is a candidate too, and
		// on an old daemon that publishes none it is the only one.
		if p.Addr != "" {
			cands = append(append([]string{}, cands...), string(direct.KindReflex)+":"+p.Addr)
		}

		if !shouldTry(back, cands, time.Now()) {
			continue
		}
		go func(node string, cands []string) {
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			if err := m.connect(cctx, node, cands); err != nil {
				m.noPath(node, err, candKey(cands))
			}
		}(p.Node, cands)
	}

	// Peers the introducer no longer lists have stopped checking in. Their
	// local port is kept for one further pass rather than closed at once,
	// because a single missed registration is not a machine going away and
	// tearing the node out makes every job in flight against it fail.
	m.mu.Lock()
	now := time.Now()
	gone := expired(m.peers, live, now, m.cfg.Poll)
	m.mu.Unlock()
	for _, node := range gone {
		m.dropLink(node)
	}
}

// sync registers with the introducer and asks it to wake anyone we want to
// reach.
func (m *Manager) sync(ctx context.Context) {
	m.mu.Lock()
	reflex := m.reflex
	m.mu.Unlock()

	var mapped netip.AddrPort
	if mp := m.keeper.Current(); mp.Kind != portmap.KindNone && mp.Public {
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
		return
	}
	addr, err := net.ResolveUDPAddr("udp", m.cfg.Rendezvous)
	if err != nil {
		m.cfg.Log("introducer address %q: %v", m.cfg.Rendezvous, err)
		return
	}
	if _, err := m.endpoint.Conn().WriteToUDP(pkt, addr); err != nil {
		m.cfg.Log("introducer %s: %v", m.cfg.Rendezvous, err)
	}
}

// connect races a peer's candidates and, on success, gives it a local port.
func (m *Manager) connect(ctx context.Context, node string, candidates []string) error {
	cands := direct.ParseCandidates(candidates)
	if len(cands) == 0 {
		return &direct.Unreachable{Peer: node, Reason: direct.ReasonNoCandidates}
	}

	// Tell the introducer to wake the peer, so both punch at once. Its own
	// probes are what open its router to us.
	nonce := make([]byte, 8)
	_, _ = rand.NewChaCha8([32]byte{}).Read(nonce)
	_ = rendezvous.RequestPunch(m.endpoint.Conn(), m.cfg.Rendezvous,
		m.cfg.Cluster, m.node(), node, hex.EncodeToString(nonce))

	at, err := m.raceOrCollide(ctx, node, cands)
	if err != nil {
		return err
	}

	conn, err := m.endpoint.Dial(ctx, node, at)
	if err != nil {
		if direct.IsAlreadyConnected(err) {
			// Both ends raced and both won. One connection has to lose, and
			// losing is not a failure to reach the peer.
			return nil
		}
		return err
	}

	kind := "punched"
	for _, c := range cands {
		if c.Addr == at {
			kind = string(c.Kind)
			break
		}
	}
	return m.linkUp(ctx, node, conn, kind)
}

// raceOrCollide finds an address that answers, by the ordinary punch or, when
// that fails, by collision.
//
// The ordinary punch handles every pair where at least one router keeps a
// predictable external port. The collision is for the pair where neither
// does, or where one side allocates a fresh port per destination and the
// other accepts packets only from an address it has already written to -
// which is this project's own two machines, and where a plain punch moved 400
// packets each way and received nothing.
//
// PIPEDPEER_FORCE_BIRTHDAY=1 skips the ordinary punch. That exists to
// exercise the collision on hardware where the plain punch happens to
// succeed: otherwise the path can only ever be tested against a simulation,
// and a mechanism that has never run in the field is one nobody should rely
// on.
func (m *Manager) raceOrCollide(ctx context.Context, node string, cands []direct.Candidate) (netip.AddrPort, error) {
	if os.Getenv("PIPEDPEER_FORCE_BIRTHDAY") != "1" {
		// A bounded window of its own, not the caller's whole budget: the
		// race used to run until the connect context expired, so the
		// collision inherited a context that was already done and failed
		// instantly while logging that it had tried.
		//
		// Fifteen seconds, not six. A punch between these two networks has
		// been observed taking well over six - both sides have to keep
		// sending until each router has seen the other - and cutting the
		// window to six stopped a pair connecting that had been connecting
		// reliably. The collision gets its own window after this one.
		raceCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		at, err := m.endpoint.Prober().Race(raceCtx, cands, 250*time.Millisecond)
		cancel()
		if err == nil {
			return at, nil
		}
	} else {
		m.cfg.Log("PIPEDPEER_FORCE_BIRTHDAY=1: skipping the ordinary punch to %s", short(node))
	}

	// Nothing answered where it was expected. If the peer has a routable
	// address at all, its router may simply be allocating unpredictably - so
	// stop aiming and collide instead.
	var ip netip.Addr
	var known netip.AddrPort
	for _, c := range cands {
		if c.Addr.Addr().IsPrivate() || c.Addr.Addr().IsLoopback() {
			continue
		}
		ip = c.Addr.Addr()
		known = c.Addr
		break
	}
	if !ip.IsValid() {
		return netip.AddrPort{}, &direct.Unreachable{
			Peer: node, Reason: direct.ReasonTimeout, Tried: len(cands),
		}
	}

	m.cfg.Log("no answer from %s where expected; colliding across its port space", short(node))

	// Both halves at once, so neither side has to know which kind of NAT it
	// is behind. The side that cannot be aimed at needs many mappings open
	// for the other's probes to hit; the side that can be aimed at needs to
	// spray. Running both means whichever role this machine turns out to
	// have, it is already playing it.
	// Detached from the caller's deadline for the same reason: the collision
	// needs a window of its own, and the two sides have to be spraying at the
	// same moment for it to mean anything.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 12*time.Second)
	defer cancel()

	type found struct {
		at   netip.AddrPort
		conn *net.UDPConn // set when OUR socket was hit
	}
	hits := make(chan found, 2)

	go func() {
		// The known address goes in even under the forcing flag. It is half
		// the mechanism rather than a shortcut past it: the spray opens the
		// far side's filter for our mapping, and this is the packet that
		// arrives through it. Suppressing it made the forced run test
		// something that could never work.
		at, err := m.endpoint.Prober().SprayToward(cctx, ip, known, 10*time.Second)
		if err == nil {
			hits <- found{at: at}
		}
	}()
	go func() {
		if !known.IsValid() {
			return
		}
		conn, from, err := direct.OpenMany(cctx, known, m.node(), 10*time.Second)
		if err == nil {
			hits <- found{at: from, conn: conn}
		}
	}()

	select {
	case h := <-hits:
		if h.conn != nil {
			// Their probe landed on one of our sockets. That socket's
			// mapping is the reachable one, so it has to carry the
			// connection - and they will dial it, not the shared port.
			if err := m.endpoint.Adopt(ctx, h.conn); err != nil {
				_ = h.conn.Close()
				return netip.AddrPort{}, err
			}
			m.cfg.Log("collision: %s reached us on a second socket; listening there too", short(node))
			// Their dial is what completes this; nothing to connect to from
			// here, so report it as not-yet rather than as failure.
			return netip.AddrPort{}, &direct.Unreachable{
				Peer: node, Reason: direct.ReasonTimeout, Tried: len(cands),
			}
		}
		m.cfg.Log("collision found %s at %s", short(node), h.at)
		return h.at, nil
	case <-cctx.Done():
		return netip.AddrPort{}, &direct.Unreachable{
			Peer: node, Reason: direct.ReasonBothSymmetric, Tried: len(cands),
		}
	}
}

// linkUp gives a connected peer a local port and tells the daemon about it.
func (m *Manager) linkUp(ctx context.Context, node string, conn *quic.Conn, path string) error {
	// Port zero: the kernel picks. Fixed ports would collide as soon as a
	// second peer appeared, and the number is never typed by anybody - it
	// goes straight to the node store.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		conn.CloseWithError(1, "")
		return fmt.Errorf("no local port for peer %s: %w", short(node), err)
	}
	// Detached from the caller's deadline, and this is load-bearing.
	//
	// An outbound link is established inside the connect budget - the
	// candidate race plus the collision window - so a context derived from
	// the caller's carried that deadline. The forwarder then closed seconds
	// after the link came up, while the QUIC connection stayed perfectly
	// healthy, and every HTTP request to that peer failed with the manager
	// still reporting a direct path. It matched the symptom exactly: a peer
	// showing "punched" was broken and one showing "punched-in" worked,
	// because the inbound path is handed the manager's own context and the
	// outbound path was not.
	//
	// Shutdown still closes it: closeAll calls stop and closes the listener.
	linkCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	link := &peerLink{
		listener: ln, addr: ln.Addr().String(), conn: conn,
		stop: cancel, lastSeen: time.Now(), path: path,
	}

	m.mu.Lock()
	if old := m.peers[node]; old != nil {
		// Already connected by the other side's punch, which is ordinary:
		// both ends race at once and both can win.
		m.mu.Unlock()
		cancel()
		_ = ln.Close()
		conn.CloseWithError(0, "already connected")
		return nil
	}
	m.peers[node] = link
	delete(m.backoff, node)
	m.mu.Unlock()

	go m.serveLink(linkCtx, node, conn, ln)
	m.cfg.Log("peer %s reachable at %s (%s)", short(node), link.addr, path)
	if m.cfg.OnPeer != nil {
		m.cfg.OnPeer(node, link.addr)
	}
	return nil
}

// noPath records that a peer cannot be reached, and why.
func (m *Manager) noPath(node string, err error, tried string) {
	reason := err.Error()
	if u, ok := direct.IsUnreachable(err); ok {
		reason = string(u.Reason)
	}

	// Backed off, and further each time: a peer behind a firewall that drops
	// UDP will never answer, and retrying it every twenty seconds forever is
	// a punch burst per peer per poll for nothing.
	m.mu.Lock()
	wait := 30 * time.Second
	if prev, ok := m.backoff[node]; ok {
		if d := time.Until(prev.until); d > 0 {
			wait = d * 2
		}
	}
	if wait > 5*time.Minute {
		wait = 5 * time.Minute
	}
	m.backoff[node] = backoffEntry{until: time.Now().Add(wait), tried: tried}
	m.mu.Unlock()

	m.cfg.Log("no direct path to %s: %s (retrying in %s)", short(node), reason, wait)
	if m.cfg.OnUnreachable != nil {
		m.cfg.OnUnreachable(node, reason)
	}
}

func (m *Manager) serveLink(ctx context.Context, node string, conn *quic.Conn, ln net.Listener) {
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	// A connection that dies takes its streams with it; drop the link so the
	// next poll reconnects rather than forwarding into nothing.
	if conn != nil {
		go func() {
			<-conn.Context().Done()
			m.dropLink(node)
		}()
	}
	// When the forwarder stops accepting, this link cannot carry anything
	// more, and the manager has to be told. Returning quietly left the link
	// in m.peers: Paths() went on reporting a route, the node table went on
	// showing "punched", every poll went on failing, and nothing ever tried
	// to reconnect - a cluster that had silently stopped distributing while
	// claiming a direct path. Observed after a large closure transfer, and it
	// did not recover in ten minutes; only a restart brought it back.
	defer m.dropLink(node)
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func(local net.Conn) {
			defer local.Close()
			st, err := conn.OpenStreamSync(ctx)
			if err != nil {
				return
			}
			defer st.Close()
			done := make(chan struct{}, 2)
			go func() {
				_, _ = io.Copy(st, local)
				// Half-close: the far daemon sees the end of the request and
				// answers, instead of waiting for a body that is not coming.
				_ = st.Close()
				done <- struct{}{}
			}()
			go func() {
				_, _ = io.Copy(local, st)
				done <- struct{}{}
			}()
			<-done
			<-done
		}(c)
	}
}

func (m *Manager) dropLink(node string) {
	m.mu.Lock()
	link := m.peers[node]
	delete(m.peers, node)
	m.mu.Unlock()
	if link == nil {
		return
	}
	link.stop()
	_ = link.listener.Close()
	if link.conn != nil {
		link.conn.CloseWithError(0, "")
	}
	m.cfg.Log("peer %s stopped checking in; released %s", short(node), link.addr)
	if m.cfg.OnPeerGone != nil {
		m.cfg.OnPeerGone(node, link.addr)
	}
}

func (m *Manager) closeAll() {
	m.mu.Lock()
	links := m.peers
	m.peers = map[string]*peerLink{}
	m.mu.Unlock()
	for _, link := range links {
		link.stop()
		_ = link.listener.Close()
		if link.conn != nil {
			link.conn.CloseWithError(0, "")
		}
	}
}

func (m *Manager) node() string { return m.cfg.Key.Fingerprint() }

// Peers reports the local address each known peer is reachable at.
func (m *Manager) Peers() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.peers))
	for node, link := range m.peers {
		out[node] = link.addr
	}
	return out
}

// Paths reports how each peer is reached, keyed by the local address it is
// reachable at.
//
// Keyed by address rather than by node, because the two halves of the system
// name a peer differently: this manager knows it by the fingerprint of its
// key, and the node store knows it by the UUID its daemon reports. The
// forwarder address is the one thing both have, so it is what joins them.
func (m *Manager) Paths() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.peers))
	for _, link := range m.peers {
		out[link.addr] = link.path
	}
	return out
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

// trimKind strips a candidate's tag, for logging.
func trimKind(s string) string {
	if _, addr, ok := strings.Cut(s, ":"); ok {
		return addr
	}
	return s
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
