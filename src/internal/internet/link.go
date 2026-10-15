package internet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/direct"
	"github.com/quic-go/quic-go"
)

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
		m.observeLocked(node, conn)
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
	m.observeLocked(node, conn)
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
	st, err := m.openStream(ctx, node, ln)
	if err != nil {
		m.cfg.Log("peer %s: dropping a local request: %v", short(node), err)
		reset(local)
		return
	}
	defer st.Close()
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(st, local)
		// Half-close: the far daemon sees the end of the request and
		// answers, instead of waiting for a body that is not coming.
		_ = st.Close()
		done <- struct{}{}
	}()
	if _, err := io.Copy(local, st); err != nil {
		// The connection under the stream went away - most often a newer
		// connection from the same peer replaced it. Reset the local side
		// at once: waiting for the request half here held the caller until
		// its own timeout, 3 s on connect's first /health (FIX-E2c), and a
		// reset is what tells it to retry, which lands on the new connection.
		reset(local)
		st.CancelRead(0)
		return
	}
	// The response is complete; the request half ends with the caller.
	<-done
}

// openStream opens a stream on the link's current connection. A connection
// that is already dead, or dies as the stream opens, is waited past for up to
// a second: a swap to a newer connection is usually under way, and a new
// request should ride that one rather than fail.
func (m *Manager) openStream(ctx context.Context, node string, ln net.Listener) (*quic.Stream, error) {
	var lastErr error
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		var conn *quic.Conn
		if link := m.peers[node]; link != nil && link.listener == ln {
			conn = link.conn
		}
		m.mu.Unlock()
		if conn == nil {
			return nil, errors.New("no connection behind its forwarder")
		}
		if conn.Context().Err() == nil {
			st, err := conn.OpenStreamSync(ctx)
			if err == nil {
				return st, nil
			}
			lastErr = err
		} else {
			lastErr = context.Cause(conn.Context())
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return nil, fmt.Errorf("opening a stream: %w", lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// reset closes a local connection with a TCP reset rather than a clean end,
// so the caller sees a failed request and not a short answer.
func reset(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = c.Close()
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
