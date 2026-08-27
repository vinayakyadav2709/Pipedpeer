// Package rendezvous introduces peers that cannot find each other.
//
// Two machines behind home routers can talk directly once each knows where the
// other appears from - measured at 0.2s to open such a path. Neither can
// discover that address for the other, because it is a property of the
// router, not of the machine, and it changes. Something on a public address
// has to hold the address book.
//
// It holds nothing else. Peers exchange their observed addresses through it
// and then talk directly, so it carries no job data, no gradients and no
// closures; the bandwidth bill of a relay is exactly what the measurement
// above makes avoidable.
//
// It speaks UDP, for two reasons. The address a peer needs is the one its UDP
// traffic presents, so registering over anything else would publish the wrong
// one; and the registration traffic doubles as the keepalive that stops the
// router discarding the mapping.
package rendezvous

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

// ClusterID is what the rendezvous files a node under.
//
// Derived from the cluster's shared token rather than being the token: knowing
// it is enough to be introduced to the cluster's members, which is the same
// trust the token already carries, but the server never holds the secret that
// authenticates against the daemons themselves. A rendezvous is somebody
// else's machine and should learn as little as the job allows.
func ClusterID(token string) string {
	if token == "" {
		return "public"
	}
	sum := sha256.Sum256([]byte("pipedpeer-rendezvous\x00" + token))
	return hex.EncodeToString(sum[:8])
}

// Peer is one member's address as the rendezvous last saw it.
type Peer struct {
	Node string `json:"node"`
	Addr string `json:"addr"`
	// Candidates is every address this peer might be reachable at, tagged
	// by how it was learned - "lan:", "mapped:", "reflex:". The reflexive
	// address in Addr is only one of them and is the one least likely to
	// work: a LAN address connects without troubling any router at all, and
	// a mapped one survives a NAT that reflexive addresses cannot.
	Candidates []string `json:"candidates,omitempty"`
	// AgeSec is how long ago it last checked in. A peer that stopped
	// registering has probably gone, and its address is probably stale, so
	// this travels with it rather than being silently trusted.
	AgeSec int `json:"age_sec"`
}

// request is what a node sends.
//
// Fields are only ever added, never removed or renamed: a new daemon must
// keep working against an old rendezvous and the other way round, and both
// sides ignore JSON they do not recognise.
type request struct {
	Op      string `json:"op"`
	Cluster string `json:"cluster"`
	Node    string `json:"node"`
	// Candidates travels with "register".
	Candidates []string `json:"candidates,omitempty"`
	// Peer and Nonce travel with "connect": which member to wake, and a
	// value it echoes so the two sides can tell one attempt from the next.
	Peer  string `json:"peer,omitempty"`
	Nonce string `json:"nonce,omitempty"`
}

// response is what it gets back.
//
// Also the shape of the forwarded "punch" datagram, which is not a reply to
// anything the receiver sent - hence Op, which is absent on an ordinary
// register response and tells the two apart.
type response struct {
	// You is the address the rendezvous saw, which is what a peer needs.
	You   string `json:"you"`
	Peers []Peer `json:"peers"`
	Error string `json:"error,omitempty"`

	// Op is "punch" on a forwarded connect, empty otherwise.
	Op string `json:"op,omitempty"`
	// From is the node that wants to be reached, Nonce its attempt id, and
	// Candidates every address it might be reachable at.
	From       string   `json:"from,omitempty"`
	Nonce      string   `json:"nonce,omitempty"`
	Candidates []string `json:"candidates,omitempty"`
}

type entry struct {
	addr       string
	seen       time.Time
	candidates []string
}

// Server is the address book.
// connectPerSec and connectBurst bound how often one source may ask the
// server to send a packet somewhere else. Four a second is far more than a
// daemon polling every twenty seconds needs, and far less than is useful to
// anyone trying to bounce traffic.
const (
	connectPerSec = 4.0
	connectBurst  = 8.0
)

type bucket struct {
	tokens float64
	last   time.Time
}

type Server struct {
	mu sync.Mutex
	// clusters is cluster -> node -> entry. Nothing is persisted: an address
	// that survived a restart of this process would be an address a router
	// has long since forgotten.
	clusters map[string]map[string]entry
	ttl      time.Duration
	// rate is per source host, for the connect op only.
	rate map[string]*bucket
}

// NewServer makes a rendezvous. ttl is how long an unrefreshed registration
// stays listed.
func NewServer(ttl time.Duration) *Server {
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	return &Server{clusters: map[string]map[string]entry{}, ttl: ttl}
}

// Handle processes one datagram and returns the reply, or nil to stay silent.
//
// Silence rather than an error reply for anything unparseable: this listens on
// a public address, and answering junk turns it into a way of bouncing traffic
// at somebody else.
// Forward is a datagram the server should send to a third address.
//
// The whole of what makes this an introducer rather than a relay: one packet
// telling a peer that somebody wants to reach it, so both sides punch at the
// same moment. Nothing else ever travels through here.
type Forward struct {
	To   string
	Body []byte
}

// Handle processes one datagram and returns the reply, or nil to stay silent.
func (s *Server) Handle(payload []byte, from net.Addr) []byte {
	reply, _ := s.handle(payload, from)
	return reply
}

func (s *Server) handle(payload []byte, from net.Addr) ([]byte, *Forward) {
	var req request
	if json.Unmarshal(payload, &req) != nil {
		return nil, nil
	}
	if req.Cluster == "" || req.Node == "" {
		return nil, nil
	}
	if req.Op == "connect" {
		return s.connect(req, from)
	}
	if req.Op != "register" {
		return nil, nil
	}

	s.mu.Lock()
	c := s.clusters[req.Cluster]
	if c == nil {
		c = map[string]entry{}
		s.clusters[req.Cluster] = c
	}
	c[req.Node] = entry{addr: from.String(), seen: time.Now(), candidates: req.Candidates}

	var peers []Peer
	for node, e := range c {
		if node == req.Node {
			continue // a node does not need introducing to itself
		}
		age := time.Since(e.seen)
		if age > s.ttl {
			delete(c, node)
			continue
		}
		peers = append(peers, Peer{
			Node: node, Addr: e.addr, AgeSec: int(age.Seconds()),
			Candidates: e.candidates,
		})
	}
	s.mu.Unlock()

	// Sorted so a caller polling repeatedly sees a stable order, and so tests
	// do not depend on map iteration.
	sort.Slice(peers, func(i, j int) bool { return peers[i].Node < peers[j].Node })

	out, err := json.Marshal(response{You: from.String(), Peers: peers})
	if err != nil {
		return nil, nil
	}
	return out, nil
}

// connect wakes a peer so it punches back at the same time.
//
// The rendezvous forwards; it never answers on the peer's behalf and never
// carries anything else. A misdescribed candidate is not a hole worth
// signing against: reaching the wrong machine fails the identity handshake
// that every direct connection performs before it will do anything, so the
// worst a lie achieves is a wasted probe.
func (s *Server) connect(req request, from net.Addr) ([]byte, *Forward) {
	if req.Peer == "" || req.Peer == req.Node {
		return nil, nil
	}
	// Rate limited, because this is the one operation that makes the server
	// send a packet somewhere the sender chose. Without a limit it is a way
	// of pointing this machine's traffic at a third party.
	if !s.allow(from.String()) {
		return nil, nil
	}

	s.mu.Lock()
	c := s.clusters[req.Cluster]
	target, ok := c[req.Peer]
	var mine entry
	if ok {
		mine = c[req.Node]
	}
	s.mu.Unlock()
	if !ok || time.Since(target.seen) > s.ttl {
		return nil, nil
	}

	// The candidates the caller registered, not any it names now: a node
	// speaks for itself, and only through a registration the server saw
	// arrive from its own address.
	body, err := json.Marshal(response{
		Op:    "punch",
		From:  req.Node,
		Nonce: req.Nonce,
		// The RECIPIENT's address, not the caller's: You means "you, as I
		// see you" everywhere else in this protocol, and the recipient
		// learning its own reflexive address here saves it a round trip.
		You:        target.addr,
		Candidates: mine.candidates,
	})
	if err != nil {
		return nil, nil
	}
	return nil, &Forward{To: target.addr, Body: body}
}

// allow is a small per-source token bucket over connectWindow.
func (s *Server) allow(src string) bool {
	host, _, err := net.SplitHostPort(src)
	if err != nil {
		host = src
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rate == nil {
		s.rate = map[string]*bucket{}
	}
	b := s.rate[host]
	if b == nil {
		b = &bucket{tokens: connectBurst, last: now}
		s.rate[host] = b
	}
	// Refill by elapsed time rather than on a timer: nothing to schedule and
	// nothing to stop.
	b.tokens += now.Sub(b.last).Seconds() * connectPerSec
	if b.tokens > connectBurst {
		b.tokens = connectBurst
	}
	b.last = now

	// Sources that have gone quiet are dropped, so a server left running does
	// not accumulate a bucket per address that ever spoke to it.
	if len(s.rate) > 4096 {
		for k, v := range s.rate {
			if now.Sub(v.last) > time.Minute {
				delete(s.rate, k)
			}
		}
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Serve runs the address book on an already-bound socket until ctx ends.
func (s *Server) Serve(pc net.PacketConn, stop <-chan struct{}) error {
	go func() {
		<-stop
		pc.Close()
	}()
	buf := make([]byte, 2048)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
			}
			continue
		}
		reply, fwd := s.handle(buf[:n], from)
		if reply != nil {
			_, _ = pc.WriteTo(reply, from)
		}
		if fwd != nil {
			if to, err := net.ResolveUDPAddr("udp", fwd.To); err == nil {
				_, _ = pc.WriteTo(fwd.Body, to)
			}
		}
	}
}

// Register tells a rendezvous where we are and asks who else is there.
//
// The same socket must later be used to reach those peers: the address the
// rendezvous reports belongs to this socket's mapping, and punching from
// another one arrives as an unrelated flow.
func Register(conn *net.UDPConn, server, cluster, node string, timeout time.Duration) (string, []Peer, error) {
	return RegisterWith(conn, server, cluster, node, nil, timeout)
}

// RegisterWith is Register, saying where else this node might be reachable.
//
// The reflexive address the rendezvous reports is only one candidate and the
// one least likely to work: a peer on the same network reaches a LAN address
// without troubling a router at all, and a mapped port survives a NAT that
// no reflexive address can get through.
func RegisterWith(conn *net.UDPConn, server, cluster, node string, candidates []string, timeout time.Duration) (string, []Peer, error) {
	addr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return "", nil, err
	}
	body, err := json.Marshal(request{
		Op: "register", Cluster: cluster, Node: node, Candidates: candidates,
	})
	if err != nil {
		return "", nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return "", nil, err
	}
	defer conn.SetDeadline(time.Time{})

	// Retried: this is UDP, and one lost packet is not a missing rendezvous.
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := conn.WriteToUDP(body, addr); err != nil {
			continue
		}
		buf := make([]byte, 8192)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		var resp response
		if json.Unmarshal(buf[:n], &resp) != nil {
			continue
		}
		if resp.Op == "punch" {
			// A peer asked to be punched while this registration was in
			// flight. Both arrive on the one socket by design, and dropping
			// it here would lose a connection attempt; callers that expect
			// punches own the read loop and use RegisterPacket instead.
			continue
		}
		if resp.Error != "" {
			return "", nil, fmt.Errorf("rendezvous: %s", resp.Error)
		}
		if resp.You == "" {
			continue
		}
		return resp.You, resp.Peers, nil
	}
	return "", nil, fmt.Errorf("no answer from rendezvous %s", server)
}

// Punch is a forwarded request from a peer that wants to be reached.
//
// It arrives unsolicited on the same socket everything else uses, which is
// the point: the packet that carries it also refreshes this node's mapping
// on its own router.
type Punch struct {
	// From is the node asking, Nonce its attempt id.
	From  string
	Nonce string
	// Candidates is every address that node might be reachable at.
	Candidates []string
	// You is this node's address as the rendezvous sees it, which arrives
	// free with the forward and saves a round trip to learn it.
	You string
}

// ParsePunch reports whether a datagram is a forwarded punch request.
//
// Used by callers that own their socket's read loop and must tell a
// registration reply, a punch, and a peer's own probe apart.
func ParsePunch(payload []byte) (Punch, bool) {
	var resp response
	if json.Unmarshal(payload, &resp) != nil {
		return Punch{}, false
	}
	if resp.Op != "punch" || resp.From == "" {
		return Punch{}, false
	}
	return Punch{
		From: resp.From, Nonce: resp.Nonce,
		Candidates: resp.Candidates, You: resp.You,
	}, true
}

// RegisterPacket is the registration datagram, for callers that own their
// own read loop and cannot let Register consume from the socket.
func RegisterPacket(cluster, node string, candidates []string) ([]byte, error) {
	return json.Marshal(request{
		Op: "register", Cluster: cluster, Node: node, Candidates: candidates,
	})
}

// ParseRegister reports whether a datagram is a registration reply.
func ParseRegister(payload []byte) (you string, peers []Peer, ok bool) {
	var resp response
	if json.Unmarshal(payload, &resp) != nil {
		return "", nil, false
	}
	if resp.Op != "" || resp.You == "" {
		return "", nil, false
	}
	return resp.You, resp.Peers, true
}

// RequestPunch asks the rendezvous to wake a peer.
//
// Write-only: the answer, if there is one, is the peer's own probes arriving
// on this socket, not a reply from the server.
func RequestPunch(conn *net.UDPConn, server, cluster, node, peer, nonce string) error {
	addr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return err
	}
	body, err := json.Marshal(request{
		Op: "connect", Cluster: cluster, Node: node, Peer: peer, Nonce: nonce,
	})
	if err != nil {
		return err
	}
	_, err = conn.WriteToUDP(body, addr)
	return err
}
