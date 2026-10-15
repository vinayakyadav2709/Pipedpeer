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
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/clustercfg"
	"github.com/pipedpeer/pipedpeer/internal/direct"
	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/portmap"
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
	// lostAfter is how long a registration may go unanswered before an
	// introducer that has answered earlier counts as lost. A lost one is
	// re-registered with every lostAfter instead of every poll, and its next
	// answer retries every unlinked peer at once (FIX-RELINK: after B's
	// router came back, B sat out a 20 s poll and a 30 s penalty).
	lostAfter = 5 * time.Second
	// kickStale is how old an attempt must be for a sign of life from the
	// peer to restart it. Younger ones already carry fresh candidates and a
	// fresh punch request; restarting those on every probe would never let
	// one finish.
	kickStale = 5 * time.Second
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
	// inflight are the attempts started by attempt, so a sign of life from
	// the peer can restart a stale one (kick).
	inflight map[string]*inflight
	// observed is the address each peer's last connection came from or went
	// to. A peer that punched in through a hint may appear in no introducer
	// listing; without this, its link could not be redialled after this
	// machine moved (FIX-E3B: "no known address to redial").
	observed map[string]string
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

// inflight is one attempt started by attempt.
type inflight struct {
	cancel  context.CancelFunc
	started time.Time
	// kicked: cancelled by kick, to be started again rather than penalised.
	kicked bool
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
		cfg:      cfg,
		peers:    map[string]*peerLink{},
		backoff:  map[string]backoffEntry{},
		invited:  map[string][]string{},
		cands:    map[string][]string{},
		hints:    map[string]hint{},
		trying:   map[string]bool{},
		inflight: map[string]*inflight{},
		observed: map[string]string{},
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
		// A peer punching at us is reachable again: drop its penalty.
		OnProbe: func(from string) { m.probedBy(ctx, from) },
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
	fast := time.NewTicker(lostAfter)
	defer fast.Stop()
	defer m.closeAll()

	for {
		m.sync(ctx)
		// Every poll, and every lostAfter while an introducer is lost, so the
		// first registration after an outage goes out within seconds.
		for again := false; !again; {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				again = true
			case <-fast.C:
				again = m.introducerLost(time.Now())
			}
		}
	}
}

// introducerLost reports whether an introducer that has answered before is
// leaving a registration unanswered for lostAfter or more.
func (m *Manager) introducerLost(now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, in := range m.intros {
		if in.lostLocked(now) {
			return true
		}
	}
	return false
}

func (in *introducer) lostLocked(now time.Time) bool {
	return !in.hidden && !in.lastReply.IsZero() && !in.waiting.IsZero() && now.Sub(in.waiting) >= lostAfter
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
