// Package internet joins a daemon to peers it cannot reach on a local network.
//
// Everything the manual commands do - register with a rendezvous, learn who
// else is in the cluster, open a path to each of them, register that path as a
// node - is done here on a timer, so a machine joins by being told one address
// rather than by being walked through four steps.
//
// The path is a relay. Measurement decided that: one of the two machines this
// was built against is on a mobile carrier's NAT, which hands out a different
// external port per destination, so nothing can be told where to reach it and
// no hole punch to it has ever succeeded. Direct connections remain the better
// answer where they work, and `pipedpeer net-check` reports whether they do;
// what this needs is a path that always exists.
//
// Each peer gets a local port that forwards to its daemon, which is what keeps
// the rest of the system ignorant: health polling, closure upload, pool spill
// and gradient sync all speak HTTP to a host and a port, and none of them has
// to learn that the host is on the other side of the world.
package internet

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/relay"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
)

// Config is what a daemon needs to join over the internet.
type Config struct {
	// Rendezvous is the address book, host:port. Empty disables all of this.
	Rendezvous string
	// Relay carries the traffic. Defaults to the rendezvous host on the relay
	// port, since they are normally the same machine.
	Relay string
	// Cluster is derived from the shared token; peers in different clusters
	// never see each other.
	Cluster string
	// Key identifies this node. The address peers reach it at is this key's
	// fingerprint.
	Key identity.KeyPair
	// DaemonPort is the local daemon relayed streams are handed to.
	DaemonPort int
	// Poll is how often to re-register and look for new peers. Re-registering
	// is also what keeps the router's mapping alive.
	Poll time.Duration
	// VerifyRelay checks the relay's certificate. Required: the relay is
	// somebody else's machine.
	VerifyRelay func(rawCerts [][]byte, chains [][]*x509.Certificate) error
	// OnPeer is called with a local address once a peer is reachable there, so
	// the daemon can register it as an ordinary node.
	OnPeer func(node, localAddr string)
	// OnPeerGone is called when a peer stops answering.
	OnPeerGone func(node, localAddr string)
	// Log receives progress, so the caller decides the logging library.
	Log func(format string, args ...any)
}

// Manager keeps the cluster's remote members reachable.
type Manager struct {
	cfg Config

	mu    sync.Mutex
	peers map[string]*peerLink // node fingerprint -> local forwarder
}

type peerLink struct {
	listener net.Listener
	addr     string
	stop     context.CancelFunc
	lastSeen time.Time
}

func New(cfg Config) *Manager {
	if cfg.Poll <= 0 {
		cfg.Poll = 20 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = func(string, ...any) {}
	}
	return &Manager{cfg: cfg, peers: map[string]*peerLink{}}
}

// Run joins the cluster and keeps it joined until ctx ends.
//
// Failures here are never fatal to the daemon: a machine with no internet, or
// a rendezvous that is down, must still run local jobs. Everything is retried
// on the next tick.
func (m *Manager) Run(ctx context.Context) {
	if m.cfg.Rendezvous == "" {
		return
	}
	relayAddr := m.cfg.Relay
	if relayAddr == "" {
		host, _, err := net.SplitHostPort(m.cfg.Rendezvous)
		if err != nil {
			m.cfg.Log("rendezvous address %q is not host:port", m.cfg.Rendezvous)
			return
		}
		relayAddr = net.JoinHostPort(host, DefaultRelayPort)
	}

	ticker := time.NewTicker(m.cfg.Poll)
	defer ticker.Stop()
	defer m.closeAll()

	var client *relay.Client
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	for {
		if client == nil {
			c, err := relay.Dial(ctx, relayAddr, m.cfg.Cluster, m.cfg.Key,
				m.cfg.VerifyRelay, relay.LocalDialer(fmt.Sprintf("127.0.0.1:%d", m.cfg.DaemonPort)))
			if err != nil {
				m.cfg.Log("relay %s unreachable (%v); retrying", relayAddr, err)
			} else {
				client = c
				m.cfg.Log("joined cluster %s as %s via relay %s",
					m.cfg.Cluster, c.Node(), relayAddr)
				go func(c *relay.Client) {
					// Serving is how other peers reach this daemon. When it
					// returns the connection is gone, and the next tick
					// redials.
					_ = c.Serve(ctx)
				}(c)
			}
		}
		if client != nil {
			m.sync(ctx, client)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// A connection that has died takes its streams with it; drop it so the
		// next pass reconnects rather than opening streams into nothing.
		if client != nil && client.Closed() {
			m.cfg.Log("relay connection dropped; reconnecting")
			client = nil
			m.closeAll()
		}
	}
}

// DefaultRelayPort is where `pipedpeer rendezvous` puts the relay.
const DefaultRelayPort = "38446"

// sync registers with the address book and makes sure every peer it names has
// a local port pointing at it.
func (m *Manager) sync(ctx context.Context, client *relay.Client) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return
	}
	defer conn.Close()

	_, peers, err := rendezvous.Register(conn, m.cfg.Rendezvous, m.cfg.Cluster,
		client.Node(), 5*time.Second)
	if err != nil {
		m.cfg.Log("rendezvous %s: %v", m.cfg.Rendezvous, err)
		return
	}

	live := map[string]bool{}
	for _, p := range peers {
		if p.Node == client.Node() {
			continue
		}
		live[p.Node] = true
		m.ensureLink(ctx, client, p.Node)
	}

	// Peers the address book no longer lists have stopped checking in. Their
	// local port is kept for one further pass rather than closed at once,
	// because a single missed registration is not the same as a machine going
	// away, and tearing the node out of the store makes every job in flight
	// against it fail.
	m.mu.Lock()
	now := time.Now()
	for node, link := range m.peers {
		if live[node] {
			link.lastSeen = now
		}
	}
	gone := expired(m.peers, live, now, m.cfg.Poll)
	m.mu.Unlock()
	for _, node := range gone {
		m.dropLink(node)
	}
}

// ensureLink gives a peer a local port, if it does not have one.
func (m *Manager) ensureLink(ctx context.Context, client *relay.Client, node string) {
	m.mu.Lock()
	if _, ok := m.peers[node]; ok {
		m.peers[node].lastSeen = time.Now()
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	// Port zero: the kernel picks. Fixed ports would collide as soon as a
	// second peer appeared, and the number is never typed by anybody - it is
	// handed straight to the node store.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		m.cfg.Log("no local port for peer %s: %v", node, err)
		return
	}
	linkCtx, cancel := context.WithCancel(ctx)
	link := &peerLink{listener: ln, addr: ln.Addr().String(), stop: cancel, lastSeen: time.Now()}

	m.mu.Lock()
	m.peers[node] = link
	m.mu.Unlock()

	go m.serveLink(linkCtx, client, node, ln)
	m.cfg.Log("peer %s reachable at %s", node, link.addr)
	if m.cfg.OnPeer != nil {
		m.cfg.OnPeer(node, link.addr)
	}
}

func (m *Manager) serveLink(ctx context.Context, client *relay.Client, node string, ln net.Listener) {
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func(local net.Conn) {
			defer local.Close()
			st, err := client.Open(ctx, node)
			if err != nil {
				return
			}
			defer st.Close()
			done := make(chan struct{}, 2)
			go func() {
				_, _ = io.Copy(st, local)
				// Half-close: the far daemon sees the end of the request and
				// answers, instead of waiting for a body that is not coming.
				_ = st.Stream.Close()
				done <- struct{}{}
			}()
			go func() {
				_, _ = io.Copy(local, st)
				done <- struct{}{}
			}()
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
	m.cfg.Log("peer %s stopped checking in; released %s", node, link.addr)
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
	}
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

// expired names the peers that should lose their local port.
//
// A grace period rather than dropping on the first miss. A single lost
// registration is not a machine going away - the address book is UDP, and a
// dropped packet looks exactly like a departure - and tearing the node out of
// the store makes every job in flight against it fail. Three polls is long
// enough that a real departure is noticed within a minute and short enough
// that a dead peer is not offered work indefinitely.
func expired(peers map[string]*peerLink, live map[string]bool, now time.Time, poll time.Duration) []string {
	var gone []string
	for node, link := range peers {
		if live[node] {
			continue
		}
		if now.Sub(link.lastSeen) > 3*poll {
			gone = append(gone, node)
		}
	}
	return gone
}
