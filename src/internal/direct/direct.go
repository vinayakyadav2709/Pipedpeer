// Package direct connects two daemons to each other without anything in the
// middle carrying the traffic.
//
// The relay works everywhere and costs whoever runs it: every byte of every
// closure crosses somebody else's machine, is billed as their egress, and is
// bounded by their uplink. It should be what happens when nothing else can,
// not what happens.
//
// What replaces it is the technique every mature peer-to-peer system
// converged on. Each side collects the addresses it might be reachable at -
// its LAN addresses, a port its router agreed to forward, the address a
// public server sees it as - and the two exchange those through an
// introducer that carries kilobytes and then gets out of the way. Both sides
// then probe every combination at once and keep whichever answers first.
//
// # One socket
//
// Everything here shares a single UDP socket: the QUIC listener, the probes,
// the introducer traffic. That is not tidiness, it is the mechanism. A
// router's mapping belongs to a socket, so the outbound probe that opens a
// path through it must come from the same socket the connection will later
// arrive on. Probing from one socket and listening on another produces a
// working punch to a port nothing is listening on.
//
// # Identity
//
// A punched connection carries exactly the authentication a relayed one
// does: the peer signs a fresh nonce with the key its address is the
// fingerprint of. This matters more here than on the relay, because a race
// between candidate addresses means talking to whoever answers first, and
// whoever answers first is not necessarily who was meant.
package direct

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// Candidate is one address a peer might be reachable at, and how it was
// learned - which is also roughly the order they are worth trying in.
type Candidate struct {
	Kind Kind
	Addr netip.AddrPort
}

// Kind orders the candidates by how likely they are to work and how little
// they cost when they do.
type Kind string

const (
	// KindLAN is an address on a network this machine is directly attached
	// to. When it works nothing leaves the switch, which is both the fastest
	// path available and the one no NAT can interfere with.
	KindLAN Kind = "lan"
	// KindMapped is a port a router agreed to forward. Stable, publishable,
	// and works through NATs that no amount of punching gets through.
	KindMapped Kind = "mapped"
	// KindReflex is the address a public server saw. Needs both sides to
	// punch at once, and fails when the far router allocates a fresh port
	// per destination.
	KindReflex Kind = "reflex"
)

// rank is the order candidates are tried in. Lower is better: a LAN path
// costs nothing and cannot be broken by a router, a mapped port is stable,
// and a reflexive address is a guess that needs the far side's cooperation.
func (k Kind) rank() int {
	switch k {
	case KindLAN:
		return 0
	case KindMapped:
		return 1
	case KindReflex:
		return 2
	default:
		return 3
	}
}

// String is the wire form: "lan:192.168.0.10:38447".
func (c Candidate) String() string { return string(c.Kind) + ":" + c.Addr.String() }

// ParseCandidate reads the wire form.
func ParseCandidate(s string) (Candidate, error) {
	kind, addr, ok := strings.Cut(s, ":")
	if !ok {
		return Candidate{}, fmt.Errorf("candidate %q has no kind", s)
	}
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return Candidate{}, fmt.Errorf("candidate %q: %w", s, err)
	}
	switch Kind(kind) {
	case KindLAN, KindMapped, KindReflex:
		return Candidate{Kind: Kind(kind), Addr: ap}, nil
	}
	return Candidate{}, fmt.Errorf("candidate %q has an unknown kind %q", s, kind)
}

// ParseCandidates reads a peer's list, keeping what it understands.
//
// A candidate this build does not recognise is not an error: a newer daemon
// may offer kinds this one has never heard of, and the right answer is to
// try the ones it does know rather than to refuse the peer.
func ParseCandidates(list []string) []Candidate {
	var out []Candidate
	for _, s := range list {
		if c, err := ParseCandidate(s); err == nil {
			out = append(out, c)
		}
	}
	sortByRank(out)
	return out
}

func sortByRank(cs []Candidate) {
	// Insertion sort: these lists are a handful of entries and this keeps
	// equal-ranked candidates in the order the peer offered them, which is
	// the order that peer thought best.
	for i := 1; i < len(cs); i++ {
		for j := i; j > 0 && cs[j].Kind.rank() < cs[j-1].Kind.rank(); j-- {
			cs[j], cs[j-1] = cs[j-1], cs[j]
		}
	}
}

// Reason says why no direct path was found, so the scheduler can report it
// and a reader can tell an unlucky moment from a network that will never
// work.
type Reason string

const (
	// ReasonBothSymmetric: neither router will say where the other should
	// send. No protocol solves this; the peer cannot serve work.
	ReasonBothSymmetric Reason = "both ends allocate a fresh port per destination"
	// ReasonNoCandidates: the peer offered nowhere to try.
	ReasonNoCandidates Reason = "the peer published no addresses to try"
	// ReasonTimeout: probes went out and nothing came back. Usually a
	// firewall dropping UDP, sometimes just a bad moment.
	ReasonTimeout Reason = "no answer from any address"
	// ReasonIdentity: something answered and could not prove it was the
	// peer. Worth reporting loudly - it is either a misconfiguration or
	// somebody else on the path.
	ReasonIdentity Reason = "the machine that answered could not prove its identity"
)

// Unreachable is the error returned when no direct path exists.
type Unreachable struct {
	Peer   string
	Reason Reason
	Tried  int
}

func (u *Unreachable) Error() string {
	return fmt.Sprintf("no direct path to %s after trying %d address(es): %s",
		short(u.Peer), u.Tried, u.Reason)
}

func short(node string) string {
	if len(node) > 8 {
		return node[:8]
	}
	return node
}

// IsUnreachable reports whether an error means no path exists, and why.
func IsUnreachable(err error) (*Unreachable, bool) {
	var u *Unreachable
	if errors.As(err, &u) {
		return u, true
	}
	return nil, false
}

// probeMagic marks this package's own datagrams on the shared socket.
//
// QUIC's first byte has its high bits set for long-header packets; these
// start with a byte that no QUIC packet can, so the read loop can tell them
// apart without parsing either.
var probeMagic = [4]byte{'P', 'P', 'd', 'r'}

const (
	probeRequest = 1
	probeReply   = 2
)

// probe is a punch packet: small, cheap to send hundreds of, and carrying
// just enough to tell whose it is.
type probe struct {
	kind  byte
	nonce [8]byte
	// from is the fingerprint prefix of the sender, so a reply can be
	// matched to the attempt that caused it without a full handshake.
	from [8]byte
}

func (p probe) marshal() []byte {
	out := make([]byte, 0, 4+1+8+8)
	out = append(out, probeMagic[:]...)
	out = append(out, p.kind)
	out = append(out, p.nonce[:]...)
	out = append(out, p.from[:]...)
	return out
}

func parseProbe(b []byte) (probe, bool) {
	if len(b) < 21 || [4]byte(b[0:4]) != probeMagic {
		return probe{}, false
	}
	var p probe
	p.kind = b[4]
	copy(p.nonce[:], b[5:13])
	copy(p.from[:], b[13:21])
	return p, true
}

// fingerprint8 is the first 8 bytes of a hex fingerprint, as bytes.
func fingerprint8(fp string) [8]byte {
	var out [8]byte
	copy(out[:], fp)
	return out
}

// newNonce is a fresh probe nonce.
func newNonce() ([8]byte, error) {
	var n [8]byte
	_, err := rand.Read(n[:])
	return n, err
}

// nonceHex is for logging an attempt without printing raw bytes.
func nonceHex(n [8]byte) string { return hex.EncodeToString(n[:]) }

// Gather collects the addresses this machine might be reachable at.
//
// The reflexive address is passed in because only the introducer can say
// what it is, and the mapped one because only the router can. What this adds
// is the LAN addresses, which cost nothing to collect and are the best
// candidate whenever they work: two machines on one network reach each other
// without a router being involved at all, which is the case a relay-only
// design serves worst and most often.
func Gather(port uint16, mapped netip.AddrPort, reflex netip.AddrPort) []Candidate {
	var out []Candidate

	ifaces, err := net.Interfaces()
	if err == nil {
		for _, ifc := range ifaces {
			if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := ifc.Addrs()
			if err != nil {
				continue
			}
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
				// Link-local v6 needs a zone to be usable and a bare one in
				// a candidate list is noise; loopback reaches only this
				// machine.
				if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
					continue
				}
				out = append(out, Candidate{Kind: KindLAN, Addr: netip.AddrPortFrom(ip, port)})
			}
		}
	}
	if mapped.IsValid() {
		out = append(out, Candidate{Kind: KindMapped, Addr: mapped})
	}
	if reflex.IsValid() {
		out = append(out, Candidate{Kind: KindReflex, Addr: reflex})
	}
	return dedupe(out)
}

func dedupe(cs []Candidate) []Candidate {
	seen := map[netip.AddrPort]bool{}
	var out []Candidate
	for _, c := range cs {
		if seen[c.Addr] {
			continue
		}
		seen[c.Addr] = true
		out = append(out, c)
	}
	sortByRank(out)
	return out
}

// Strings renders candidates for the introducer.
func Strings(cs []Candidate) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.String())
	}
	return out
}

// Prober owns the shared socket's probe traffic: it sends punches and
// notices the replies.
//
// The QUIC listener reads the same socket. Whoever owns the read loop hands
// non-QUIC datagrams here via Deliver.
type Prober struct {
	conn *net.UDPConn
	self [8]byte

	mu      sync.Mutex
	waiting map[[8]byte]chan netip.AddrPort
}

// NewProber wraps a socket. self is this node's fingerprint.
func NewProber(conn *net.UDPConn, self string) *Prober {
	return &Prober{
		conn:    conn,
		self:    fingerprint8(self),
		waiting: map[[8]byte]chan netip.AddrPort{},
	}
}

// Deliver handles one datagram that was not QUIC.
//
// Returns true when it was a probe and has been dealt with, so the caller
// knows whether to look at it further.
func (p *Prober) Deliver(payload []byte, from netip.AddrPort) bool {
	pr, ok := parseProbe(payload)
	if !ok {
		return false
	}
	switch pr.kind {
	case probeRequest:
		// Somebody is punching at us. Answering does two things: it tells
		// them a path exists, and - because our reply is outbound - it opens
		// our own router to their address.
		reply := probe{kind: probeReply, nonce: pr.nonce, from: p.self}
		_, _ = p.conn.WriteToUDPAddrPort(reply.marshal(), from)
	case probeReply:
		p.mu.Lock()
		ch := p.waiting[pr.nonce]
		p.mu.Unlock()
		if ch != nil {
			select {
			case ch <- from:
			default: // already answered by a faster candidate
			}
		}
	}
	return true
}

// Race probes every candidate at once and returns the first that answers.
//
// All at once rather than in order: the candidates are tried in rank order
// only in the sense that a LAN address usually answers first because it is
// nearer. Serialising them would add a timeout's delay per address that
// cannot work, and the ones that cannot work are exactly the ones that never
// answer.
func (p *Prober) Race(ctx context.Context, cands []Candidate, every time.Duration) (netip.AddrPort, error) {
	if len(cands) == 0 {
		return netip.AddrPort{}, fmt.Errorf("no candidates to try")
	}
	nonce, err := newNonce()
	if err != nil {
		return netip.AddrPort{}, err
	}
	ch := make(chan netip.AddrPort, 1)

	p.mu.Lock()
	p.waiting[nonce] = ch
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.waiting, nonce)
		p.mu.Unlock()
	}()

	msg := probe{kind: probeRequest, nonce: nonce, from: p.self}.marshal()
	if every <= 0 {
		every = 250 * time.Millisecond
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		// Repeated, not sent once: a punch works only when both sides are
		// sending, and the far side may not have been told to start yet.
		for _, c := range cands {
			_, _ = p.conn.WriteToUDPAddrPort(msg, c.Addr)
		}
		select {
		case addr := <-ch:
			return addr, nil
		case <-ticker.C:
		case <-ctx.Done():
			return netip.AddrPort{}, ctx.Err()
		}
	}
}
