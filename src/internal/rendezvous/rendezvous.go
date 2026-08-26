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
	// AgeSec is how long ago it last checked in. A peer that stopped
	// registering has probably gone, and its address is probably stale, so
	// this travels with it rather than being silently trusted.
	AgeSec int `json:"age_sec"`
}

// request is what a node sends.
type request struct {
	Op      string `json:"op"`
	Cluster string `json:"cluster"`
	Node    string `json:"node"`
}

// response is what it gets back.
type response struct {
	// You is the address the rendezvous saw, which is what a peer needs.
	You   string `json:"you"`
	Peers []Peer `json:"peers"`
	Error string `json:"error,omitempty"`
}

type entry struct {
	addr string
	seen time.Time
}

// Server is the address book.
type Server struct {
	mu sync.Mutex
	// clusters is cluster -> node -> entry. Nothing is persisted: an address
	// that survived a restart of this process would be an address a router
	// has long since forgotten.
	clusters map[string]map[string]entry
	ttl      time.Duration
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
func (s *Server) Handle(payload []byte, from net.Addr) []byte {
	var req request
	if json.Unmarshal(payload, &req) != nil {
		return nil
	}
	if req.Cluster == "" || req.Node == "" {
		return nil
	}
	if req.Op != "register" {
		return nil
	}

	s.mu.Lock()
	c := s.clusters[req.Cluster]
	if c == nil {
		c = map[string]entry{}
		s.clusters[req.Cluster] = c
	}
	c[req.Node] = entry{addr: from.String(), seen: time.Now()}

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
		peers = append(peers, Peer{Node: node, Addr: e.addr, AgeSec: int(age.Seconds())})
	}
	s.mu.Unlock()

	// Sorted so a caller polling repeatedly sees a stable order, and so tests
	// do not depend on map iteration.
	sort.Slice(peers, func(i, j int) bool { return peers[i].Node < peers[j].Node })

	out, err := json.Marshal(response{You: from.String(), Peers: peers})
	if err != nil {
		return nil
	}
	return out
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
		if reply := s.Handle(buf[:n], from); reply != nil {
			_, _ = pc.WriteTo(reply, from)
		}
	}
}

// Register tells a rendezvous where we are and asks who else is there.
//
// The same socket must later be used to reach those peers: the address the
// rendezvous reports belongs to this socket's mapping, and punching from
// another one arrives as an unrelated flow.
func Register(conn *net.UDPConn, server, cluster, node string, timeout time.Duration) (string, []Peer, error) {
	addr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return "", nil, err
	}
	body, err := json.Marshal(request{Op: "register", Cluster: cluster, Node: node})
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
