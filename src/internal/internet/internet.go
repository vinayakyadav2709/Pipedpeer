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
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
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
// changes every time the daemon starts. Part of the 4242 block (4242 API and
// LAN discovery, 4243 direct, 4244 introducer).
const DefaultPort = 4243

// Config is what a daemon needs to join over the internet.
type Config struct {
	// Introducers are the cluster's introducers, host:port. This node
	// registers with every one of them, so one that is down or has moved
	// costs nothing while another still answers.
	Introducers []string
	// ServeIntroducer runs this cluster's introducer inside the daemon, on
	// IntroducerPort, answering this cluster id only. Any member can then
	// introduce the others; the node registers with its own over loopback.
	ServeIntroducer bool
	// IntroducerPort is where ServeIntroducer listens. Zero means
	// clustercfg.DefaultPort.
	IntroducerPort int
	// Cluster is derived from the shared token; peers in different clusters
	// never see each other.
	Cluster string
	// Secret is the cluster token; proves membership in the direct hello.
	Secret string
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

// IntroducerState is one configured introducer as this node last saw it.
type IntroducerState struct {
	Addr      string    `json:"addr"`
	LastReply time.Time `json:"last_reply"`
	LastErr   string    `json:"last_err,omitempty"`
	// Members is how many other members it listed in its last reply.
	Members int `json:"members"`
}

// Tunables, variables so tests can shorten them.
var (
	// silentAfter is how long an introducer may go unanswered before it is
	// reported. Three missed polls: one lost datagram is not an outage.
	silentAfter = 60 * time.Second
	// raceWindow is how long a fresh link is defended against a second
	// connection from the same peer by the deterministic tie-break. Both
	// ends punching at once makes two connections within a second or two;
	// a second connection after this is the peer coming back from somewhere
	// else, and the newer one wins (F33).
	raceWindow = 10 * time.Second
	// ifacePoll is how often this machine's own addresses are checked.
	ifacePoll = 5 * time.Second
	// localAddrs lists this machine's addresses, for noticing a change.
	localAddrs = interfaceAddrs
	// newKeeper asks the router for a port; tests stub it rather than map a
	// port on whatever router the test machine sits behind.
	newKeeper = func(ctx context.Context, port uint16, log func(string, ...any)) portMapper {
		return portmap.NewKeeper(ctx, port, log)
	}
)

// portMapper is what the manager needs of a portmap.Keeper; a seam so tests
// can use a slow or granting fake instead of the machine's real router.
type portMapper interface {
	Current() portmap.Mapping
	Close()
}

const (
	// hintTTL is how long a gossiped address stays worth trying. Gossip
	// repeats every minute, so ten minutes is many missed rounds.
	hintTTL = 10 * time.Minute
	// migrateTimeout bounds the path probe after an address change.
	migrateTimeout = 5 * time.Second
	// pathInbound is the path of a link the peer dialled.
	pathInbound = "punched-in"
	// codeReplaced closes a connection that lost to another one between the
	// same two nodes. The far end keeps its link for replaceGrace instead of
	// dropping it: the winner is usually still finishing its handshake there.
	codeReplaced quic.ApplicationErrorCode = 2
	replaceGrace                           = 5 * time.Second
)

// Manager keeps the cluster's remote members reachable.
type Manager struct {
	cfg      Config
	endpoint *direct.Endpoint
	// keeper is nil until mapPort has asked the router.
	keeper portMapper

	mu sync.Mutex
	// ctx is Run's, for work started from outside it (Hint).
	ctx context.Context
	// intros are the introducers, configured ones first.
	intros []*introducer
	peers  map[string]*peerLink
	// reflex is this node's address as the introducers see it, the best of
	// their fresh answers.
	reflex netip.AddrPort
	// backoff is when to next try a peer that had no path, and which
	// addresses earned the penalty.
	backoff map[string]backoffEntry
	// waiting are peers that asked us to punch, so an incoming request is
	// answered by racing rather than ignored.
	invited map[string][]string
	// listed are the peers some fresh introducer reply lists, so a peer that
	// stopped registering everywhere is no longer reported as unreachable.
	listed map[string]bool
	// cands is each peer's latest published candidates.
	cands map[string][]string
	// hints are addresses other members vouched for (signed gossip).
	hints map[string]hint
	// trying are peers with a connection attempt in flight.
	trying map[string]bool
}

// introducer is one address book this node registers with.
type introducer struct {
	addr string
	// self is this daemon's own introducer, reached over loopback; it is
	// not reported, as nobody configured it.
	self bool
	// hidden is an address that answered without being one we registered
	// with (a multi-homed introducer answering from another address).
	hidden bool
	// at is addr as last resolved; replies are matched against it.
	at netip.AddrPort
	// waiting is the first registration sent since the last reply.
	waiting   time.Time
	lastReply time.Time
	lastErr   string
	members   int
	you       netip.AddrPort
	peers     map[string]bool
	// silent is set once "has not answered" has been logged.
	silent bool
}

type hint struct {
	cands []string
	at    time.Time
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
	// reason is why the last attempt failed, fit to show a user.
	reason string
}

type peerLink struct {
	listener net.Listener
	addr     string
	// conn is swapped, under Manager.mu, when a newer connection from the
	// same key replaces it; the forwarder and its address stay.
	conn     *quic.Conn
	stop     context.CancelFunc
	lastSeen time.Time
	// since is when conn was set, dialled whether this end dialled it.
	since   time.Time
	dialled bool
	// path says how this peer is reached, for `pipedpeer nodes`.
	path string
	// announced is done once OnPeer has returned. OnPeerGone waits for it:
	// a link that died while OnPeer was still registering it otherwise had
	// its leave run first, and the join then left a node row at a forwarder
	// port nobody listens on (FIX-NAT5).
	announced sync.WaitGroup
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
	if cfg.IntroducerPort == 0 {
		cfg.IntroducerPort = clustercfg.DefaultPort
	}
	if cfg.Log == nil {
		cfg.Log = func(string, ...any) {}
	}
	m := &Manager{
		cfg:     cfg,
		peers:   map[string]*peerLink{},
		backoff: map[string]backoffEntry{},
		invited: map[string][]string{},
		cands:   map[string][]string{},
		hints:   map[string]hint{},
		trying:  map[string]bool{},
	}
	seen := map[string]bool{}
	for _, a := range cfg.Introducers {
		a = strings.TrimSpace(a)
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		m.intros = append(m.intros, &introducer{addr: a})
	}
	if cfg.ServeIntroducer {
		m.intros = append(m.intros, &introducer{
			addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.IntroducerPort)), self: true,
		})
	}
	return m
}

// Run joins the cluster and keeps it joined until ctx ends.
//
// Failures are never fatal: a machine with no internet, or an introducer that
// is down, must still run local jobs. Everything is retried on the next tick.
func (m *Manager) Run(ctx context.Context) {
	if len(m.intros) == 0 {
		m.cfg.Log("no introducer configured and none served here; internet links are off")
		return
	}

	ep, err := direct.Listen(direct.Config{
		Port:    m.cfg.DirectPort,
		Key:     m.cfg.Key,
		Cluster: m.cfg.Cluster,
		Secret:  m.cfg.Secret,
		Cert:    m.cfg.Cert,
		Local:   direct.LocalDialer(fmt.Sprintf("127.0.0.1:%d", m.cfg.DaemonPort)),
		Log:     m.cfg.Log,
		// A peer that dialled us is just as reachable as one we dialled, and
		// for a pair where one side cannot be dialled at all it is the only
		// way a link is ever made. Without this the connection was
		// authenticated and then left unused while the far side reconnected
		// every few seconds - seen against the two real machines.
		OnInbound: func(node string, conn *quic.Conn) {
			if err := m.linkUp(ctx, node, conn, pathInbound); err != nil {
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
	go func() {
		if err := ep.Serve(ctx); err != nil && ctx.Err() == nil {
			m.cfg.Log("direct connections stopped being accepted: %v", err)
		}
	}()

	if m.cfg.ServeIntroducer {
		m.serveIntroducer(ctx)
	}

	m.mu.Lock()
	m.ctx = ctx
	m.mu.Unlock()

	// The introducer's replies and peers' punch requests arrive on the same
	// socket as everything else, so one reader dispatches them.
	go m.readIntroducer(ctx)
	go m.watchInterfaces(ctx)
	go m.mapPort(ctx, ep.Port())

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

// mapPort asks the router for a port, in the background, and re-registers
// when one is granted so peers learn the mapped address.
//
// In the background because UPnP and NAT-PMP discovery take about seven
// seconds on a router that speaks neither, and while it ran nothing read the
// introducer's replies: connecting took 8.2 s where 3 s was the target
// (FIX-E2). A granted port is the best path there is - reachable with no
// punching at all - but it is a later improvement, not a precondition.
func (m *Manager) mapPort(ctx context.Context, port uint16) {
	k := newKeeper(ctx, port, m.cfg.Log)
	defer k.Close()
	m.mu.Lock()
	m.keeper = k
	m.mu.Unlock()
	if mp := k.Current(); mp.Kind != portmap.KindNone && ctx.Err() == nil {
		m.cfg.Log("router granted %s", mp)
		m.sync(ctx)
	}
	<-ctx.Done()
}

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

// candidatesLocked is what to try for node: what it published, then what
// other members vouched for.
func (m *Manager) candidatesLocked(node string, now time.Time) []string {
	out := append([]string(nil), m.cands[node]...)
	if h, ok := m.hints[node]; ok && now.Sub(h.at) <= hintTTL {
		seen := map[string]bool{}
		for _, c := range out {
			if pc, err := direct.ParseCandidate(c); err == nil {
				seen[pc.Addr.String()] = true
			}
		}
		for _, c := range h.cands {
			if pc, err := direct.ParseCandidate(c); err == nil && !seen[pc.Addr.String()] {
				seen[pc.Addr.String()] = true
				out = append(out, c)
			}
		}
	}
	return out
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

// watchInterfaces notices this machine's addresses changing, re-registers at
// once and moves the links: a laptop that changes networks would otherwise
// wait a poll to be findable and an idle timeout to notice its links are dead.
func (m *Manager) watchInterfaces(ctx context.Context) {
	last, err := localAddrs()
	lastErr := ""
	if err != nil {
		lastErr = err.Error()
		m.cfg.Log("listing this machine's addresses: %v", err)
	}
	t := time.NewTicker(ifacePoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cur, err := localAddrs()
		if err != nil {
			if err.Error() != lastErr {
				lastErr = err.Error()
				m.cfg.Log("listing this machine's addresses: %v", err)
			}
			continue
		}
		lastErr = ""
		if cur == last {
			continue
		}
		m.cfg.Log("this machine's addresses changed (%s -> %s); re-registering and moving links",
			orNone(last), orNone(cur))
		last = cur
		m.sync(ctx)
		m.moveLinks(ctx)
	}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// interfaceAddrs is this machine's non-loopback addresses, sorted.
func interfaceAddrs() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var out []string
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(ipnet.IP)
		if !ok {
			continue
		}
		ip = ip.Unmap()
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			continue
		}
		out = append(out, ip.String())
	}
	sort.Strings(out)
	return strings.Join(out, ","), nil
}

// moveLinks carries every link over an address change. A connection this end
// dialled migrates (QUIC path probe, then switch); one the peer dialled cannot
// be migrated from here (RFC 9000: only the client moves), so it is redialled
// and the new connection replaces it. A failed migration redials too.
func (m *Manager) moveLinks(ctx context.Context) {
	type move struct {
		node    string
		conn    *quic.Conn
		dialled bool
	}
	var moves []move
	m.mu.Lock()
	for node, l := range m.peers {
		if l.conn != nil {
			moves = append(moves, move{node, l.conn, l.dialled})
		}
	}
	m.mu.Unlock()
	for _, mv := range moves {
		go func() {
			if mv.dialled {
				err := m.endpoint.Migrate(ctx, mv.conn, migrateTimeout)
				if err == nil {
					m.cfg.Log("moved the link to %s onto the new address", short(mv.node))
					return
				}
				m.cfg.Log("could not move the link to %s (%v); redialling", short(mv.node), err)
			}
			m.redial(ctx, mv.node)
		}()
	}
}

// redial makes a fresh connection to a linked peer; linkUp swaps it in.
func (m *Manager) redial(ctx context.Context, node string) {
	m.mu.Lock()
	cands := m.candidatesLocked(node, time.Now())
	busy := m.trying[node]
	if !busy {
		m.trying[node] = true
	}
	m.mu.Unlock()
	if busy {
		return
	}
	defer m.doneTrying(node)
	if len(cands) == 0 {
		m.cfg.Log("no known address to redial %s; waiting for its next registration", short(node))
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := m.connect(cctx, node, cands); err != nil {
		m.cfg.Log("redialling %s: %v; the old link stays until it times out", short(node), err)
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
	// The nonce is only an attempt id. crypto/rand.Read never returns an
	// error (Go 1.24+); it crashes the process instead.
	nonce := make([]byte, 8)
	_, _ = rand.Read(nonce)
	// Every introducer, since the peer may be registered with only some.
	m.mu.Lock()
	var ats []netip.AddrPort
	for _, in := range m.intros {
		if in.at.IsValid() {
			ats = append(ats, in.at)
		}
	}
	m.mu.Unlock()
	for _, at := range ats {
		if err := rendezvous.RequestPunch(m.endpoint.Conn(), at.String(),
			m.cfg.Cluster, m.node(), node, hex.EncodeToString(nonce)); err != nil {
			m.cfg.Log("asking the introducer %s to wake %s: %v", at, short(node), err)
		}
	}

	at, err := m.raceOrCollide(ctx, node, cands)
	if err != nil {
		return err
	}
	// The race answers with an IPv4-mapped address while candidates are plain
	// IPv4: unmap it, or nothing ever matches and every ordinary punch is
	// labelled "punched".
	at = netip.AddrPortFrom(at.Addr().Unmap(), at.Port())

	conn, err := m.endpoint.Dial(ctx, node, at)
	if err != nil {
		if direct.IsAlreadyConnected(err) {
			// Both ends raced and both won. One connection has to lose, and
			// losing is not a failure to reach the peer.
			return nil
		}
		return err
	}

	return m.linkUp(ctx, node, conn, pathKind(cands, at))
}

// pathKind names how at was reached: the kind of the candidate it is, or
// "punched" when it is none of them (a collision found it).
func pathKind(cands []direct.Candidate, at netip.AddrPort) string {
	at = netip.AddrPortFrom(at.Addr().Unmap(), at.Port())
	for _, c := range cands {
		if c.Addr == at {
			return string(c.Kind)
		}
	}
	return "punched"
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
		} else if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			m.cfg.Log("collision toward %s: %v", short(node), err)
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
			// here, so report it as not-yet rather than as failure. Its
			// linkUp clears the backoff and the reason.
			return netip.AddrPort{}, &direct.Unreachable{
				Peer: node, Reason: direct.ReasonAwaitingDial, Tried: len(cands),
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
//
// A second authenticated connection from a key that already has a link
// replaces the old connection (F33): a peer that changed address comes back
// from the new one while this side still holds the old, which used to be
// kept - refusing the new one as "already connected" - until its idle timeout
// ran out, about three minutes. The forwarder and its address stay, so the
// daemon sees no leave and join. Inside raceWindow of the link being made,
// both ends punching at once is the likelier story; then both ends keep the
// connection dialled by the lower fingerprint, so they agree on one.
func (m *Manager) linkUp(ctx context.Context, node string, conn *quic.Conn, path string) error {
	// A link to ourselves. Two networks that both use 192.168.0.0/24 make a
	// peer's LAN candidate one of this machine's own addresses; dialling it
	// lands on our own endpoint, which authenticates as our own key. Linking
	// that made a JOIN and a LEAVE of this machine every two seconds (V2,
	// evidence 38-*). Decided by key, never by address: an internet peer
	// is a 127.0.0.1 forwarder like any other.
	if node == m.node() {
		if conn != nil {
			conn.CloseWithError(0, "this is ourselves")
		}
		m.cfg.Log("dropped a connection to ourselves (%s path); one of a peer's addresses is also ours", path)
		return nil
	}
	now := time.Now()
	dialled := path != pathInbound

	m.mu.Lock()
	if old := m.peers[node]; old != nil {
		if conn != nil && old.conn == conn {
			m.mu.Unlock()
			return nil
		}
		if old.alive() && now.Sub(old.since) < raceWindow && !m.newWins(node, dialled) {
			// Both ends raced and both won; this one loses on both ends.
			m.mu.Unlock()
			if conn != nil {
				conn.CloseWithError(codeReplaced, "already connected")
			}
			return nil
		}
		prev := old.conn
		old.conn, old.dialled, old.path = conn, dialled, path
		old.since, old.lastSeen = now, now
		delete(m.backoff, node)
		m.mu.Unlock()
		if prev != nil {
			prev.CloseWithError(codeReplaced, "replaced by a newer connection")
		}
		if conn != nil {
			go m.watchConn(node, conn)
		}
		m.cfg.Log("peer %s: a newer connection replaced the old one (%s), still at %s", short(node), path, old.addr)
		return nil
	}
	m.mu.Unlock()

	// Port zero: the kernel picks. Fixed ports would collide as soon as a
	// second peer appeared, and the number is never typed by anybody - it
	// goes straight to the node store.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if conn != nil {
			conn.CloseWithError(1, "")
		}
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
		stop: cancel, lastSeen: now, since: now, dialled: dialled, path: path,
	}
	link.announced.Add(1)
	defer link.announced.Done()

	m.mu.Lock()
	if m.peers[node] != nil {
		// Another connection linked while the port was being opened; let
		// the replacement rule above decide between them.
		m.mu.Unlock()
		cancel()
		_ = ln.Close()
		return m.linkUp(ctx, node, conn, path)
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

// newWins is the tie-break for two live connections made at once: the one
// dialled by the lower fingerprint stays, decided the same way on both ends.
func (m *Manager) newWins(node string, dialled bool) bool {
	self := m.node()
	dialler := node
	if dialled {
		dialler = self
	}
	return dialler == min(self, node)
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
	m.backoff[node] = backoffEntry{until: time.Now().Add(wait), tried: tried, reason: reason}
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
		go m.watchConn(node, conn)
	}
	// When the forwarder stops accepting, this link cannot carry anything
	// more, and the manager has to be told. Returning quietly left the link
	// in m.peers: Paths() went on reporting a route, the node table went on
	// showing "punched", every poll went on failing, and nothing ever tried
	// to reconnect - a cluster that had silently stopped distributing while
	// claiming a direct path. Observed after a large closure transfer, and it
	// did not recover in ten minutes; only a restart brought it back.
	defer m.dropLinkIf(node, ln)
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go m.forward(ctx, node, ln, c)
	}
}

// forward carries one local connection over the link's current connection,
// which may have been replaced since the forwarder started.
func (m *Manager) forward(ctx context.Context, node string, ln net.Listener, local net.Conn) {
	defer local.Close()
	m.mu.Lock()
	var conn *quic.Conn
	if link := m.peers[node]; link != nil && link.listener == ln {
		conn = link.conn
	}
	m.mu.Unlock()
	if conn == nil {
		m.cfg.Log("peer %s: no connection behind its forwarder; dropping a local request", short(node))
		return
	}
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		m.cfg.Log("peer %s: opening a stream: %v", short(node), err)
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
}

// watchConn drops node's link when conn dies, unless conn was replaced.
//
// A connection the far end closed because it kept another one of this pair
// is given replaceGrace for that other one to link here too. Dropping at once
// tore the link down between the two - the far end links the winner the
// moment it accepts it, this end only when its own dial returns - and the
// daemon saw a leave and a join for a peer that never went anywhere.
func (m *Manager) watchConn(node string, conn *quic.Conn) {
	<-conn.Context().Done()
	var ae *quic.ApplicationError
	if errors.As(context.Cause(conn.Context()), &ae) && ae.Remote && ae.ErrorCode == codeReplaced {
		time.Sleep(replaceGrace)
	}
	m.dropWhere(node, func(l *peerLink) bool { return l.conn == conn })
}

func (m *Manager) dropLink(node string) { m.dropWhere(node, nil) }

// dropLinkIf drops node's link only if it is the one serving ln (nil: any).
// A dead link's watchers fire after a reconnect may have replaced it, and
// must not tear down the replacement.
func (m *Manager) dropLinkIf(node string, ln net.Listener) {
	m.dropWhere(node, func(l *peerLink) bool { return ln == nil || l.listener == ln })
}

// dropWhere drops node's link when match accepts it (nil: always).
func (m *Manager) dropWhere(node string, match func(*peerLink) bool) {
	m.mu.Lock()
	link := m.peers[node]
	if link == nil || (match != nil && !match(link)) {
		m.mu.Unlock()
		return
	}
	delete(m.peers, node)
	conn := link.conn
	m.mu.Unlock()
	link.stop()
	_ = link.listener.Close()
	if conn != nil {
		conn.CloseWithError(0, "")
	}
	m.cfg.Log("peer %s stopped checking in; released %s", short(node), link.addr)
	m.gone(node, link)
}

// gone reports a released link, after its join has been reported.
func (m *Manager) gone(node string, link *peerLink) {
	link.announced.Wait()
	if m.cfg.OnPeerGone != nil {
		m.cfg.OnPeerGone(node, link.addr)
	}
}

func (m *Manager) closeAll() {
	m.mu.Lock()
	links := m.peers
	m.peers = map[string]*peerLink{}
	m.mu.Unlock()
	for node, link := range links {
		link.stop()
		_ = link.listener.Close()
		if link.conn != nil {
			link.conn.CloseWithError(0, "")
		}
		// The forwarder port dies with this run; its node row must too, or
		// every restart leaves one more row for a port nobody listens on.
		m.gone(node, link)
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

// Paths reports how each peer is reached, by node id (the key fingerprint).
func (m *Manager) Paths() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.peers))
	for node, link := range m.peers {
		out[node] = link.path
	}
	return out
}

// Unreachable reports, by node id, why each peer that an introducer still
// lists, or another member vouches for, has no direct path. A peer drops out
// once it links or stops being listed and vouched for.
func (m *Manager) Unreachable() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	out := map[string]string{}
	for node, b := range m.backoff {
		h, hinted := m.hints[node]
		known := m.listed[node] || (hinted && now.Sub(h.at) <= hintTTL)
		if known && m.peers[node] == nil {
			out[node] = b.reason
		}
	}
	return out
}

// Introducers reports every configured introducer: when it last answered,
// why it is failing, and how many other members it listed.
func (m *Manager) Introducers() []IntroducerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []IntroducerState
	for _, in := range m.intros {
		if in.self || in.hidden {
			continue
		}
		out = append(out, IntroducerState{
			Addr: in.addr, LastReply: in.lastReply, LastErr: in.lastErr, Members: in.members,
		})
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
