package internet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/direct"
	"github.com/pipedpeer/pipedpeer/internal/netaddr"
	"github.com/pipedpeer/pipedpeer/internal/rendezvous"
	"github.com/quic-go/quic-go"
)

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
	addrs, err := netaddr.InterfaceAddrs()
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
