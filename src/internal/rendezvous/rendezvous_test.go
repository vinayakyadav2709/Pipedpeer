package rendezvous

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

type fakeAddr string

func (f fakeAddr) Network() string { return "udp" }
func (f fakeAddr) String() string  { return string(f) }

func register(t *testing.T, s *Server, cluster, node, from string) response {
	t.Helper()
	body, _ := json.Marshal(request{Op: "register", Cluster: cluster, Node: node})
	raw := s.Handle(body, fakeAddr(from))
	if raw == nil {
		t.Fatalf("registration from %s was ignored", node)
	}
	var resp response
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("reply is not json: %v", err)
	}
	return resp
}

// TestPeersLearnEachOthersAddresses is the whole job. Neither machine can
// discover the address the other appears from - it belongs to the router, not
// the machine - so something on a public address has to hold it.
func TestPeersLearnEachOthersAddresses(t *testing.T) {
	s := NewServer(time.Minute)

	first := register(t, s, "c1", "alice", "203.0.113.5:41641")
	if first.You != "203.0.113.5:41641" {
		t.Errorf("alice was told she appears from %q", first.You)
	}
	if len(first.Peers) != 0 {
		t.Errorf("alice was given %d peer(s) before anyone else registered", len(first.Peers))
	}

	second := register(t, s, "c1", "bob", "198.51.100.9:45641")
	if len(second.Peers) != 1 || second.Peers[0].Node != "alice" {
		t.Fatalf("bob was given %+v, want alice", second.Peers)
	}
	if second.Peers[0].Addr != "203.0.113.5:41641" {
		t.Errorf("bob was given alice's address as %q", second.Peers[0].Addr)
	}

	// Alice re-registering must now see bob.
	again := register(t, s, "c1", "alice", "203.0.113.5:41641")
	if len(again.Peers) != 1 || again.Peers[0].Node != "bob" {
		t.Fatalf("alice was given %+v, want bob", again.Peers)
	}
}

// TestClustersAreIsolated. The cluster id is derived from the shared token, so
// leaking members across clusters would hand one user's machine list to
// another.
func TestClustersAreIsolated(t *testing.T) {
	s := NewServer(time.Minute)
	register(t, s, "c1", "alice", "203.0.113.5:1")
	got := register(t, s, "c2", "mallory", "198.51.100.9:2")
	if len(got.Peers) != 0 {
		t.Errorf("a node in another cluster was shown %+v", got.Peers)
	}
}

// TestStaleRegistrationsExpire. A router forgets a mapping within minutes of
// the traffic stopping, so an address from a peer that stopped checking in is
// not merely old, it is wrong - and handing it out sends the caller to punch
// at nothing.
func TestStaleRegistrationsExpire(t *testing.T) {
	s := NewServer(50 * time.Millisecond)
	register(t, s, "c1", "ghost", "203.0.113.5:1")
	time.Sleep(80 * time.Millisecond)

	got := register(t, s, "c1", "alice", "198.51.100.9:2")
	for _, p := range got.Peers {
		if p.Node == "ghost" {
			t.Errorf("a registration older than the ttl was still handed out: %+v", p)
		}
	}
}

// TestAgeTravelsWithTheAddress. Within the ttl an address may still be stale,
// and a caller deciding whether to punch or to give up needs to know how old
// it is rather than trusting it equally.
func TestAgeTravelsWithTheAddress(t *testing.T) {
	s := NewServer(time.Minute)
	register(t, s, "c1", "alice", "203.0.113.5:1")
	time.Sleep(1100 * time.Millisecond)
	got := register(t, s, "c1", "bob", "198.51.100.9:2")
	if len(got.Peers) != 1 {
		t.Fatalf("got %d peers", len(got.Peers))
	}
	if got.Peers[0].AgeSec < 1 {
		t.Errorf("age reported as %ds for a registration made over a second ago",
			got.Peers[0].AgeSec)
	}
}

// TestANodeIsNotIntroducedToItself. Punching at your own external address is
// at best a no-op and on some routers a loop.
func TestANodeIsNotIntroducedToItself(t *testing.T) {
	s := NewServer(time.Minute)
	register(t, s, "c1", "alice", "203.0.113.5:1")
	got := register(t, s, "c1", "alice", "203.0.113.5:1")
	for _, p := range got.Peers {
		if p.Node == "alice" {
			t.Error("alice was introduced to herself")
		}
	}
}

// TestJunkIsIgnoredSilently. This listens on a public address. Replying to
// anything unparseable turns it into a way of bouncing traffic at a third
// party who never asked for it.
func TestJunkIsIgnoredSilently(t *testing.T) {
	s := NewServer(time.Minute)
	for _, junk := range [][]byte{
		[]byte("hello"),
		[]byte("{}"),
		[]byte(`{"op":"register"}`),
		[]byte(`{"op":"register","cluster":"c"}`),
		[]byte(`{"op":"nonsense","cluster":"c","node":"n"}`),
		{},
	} {
		if reply := s.Handle(junk, fakeAddr("203.0.113.5:1")); reply != nil {
			t.Errorf("answered %q with %q; an unsolicited reply is an amplifier",
				junk, reply)
		}
	}
}

// TestClusterIDHidesTheToken. The rendezvous is somebody else's machine. It
// needs a key to file members under, and that key must not be the secret that
// authenticates against the daemons themselves.
func TestClusterIDHidesTheToken(t *testing.T) {
	const token = "s3cret-cluster-token"
	id := ClusterID(token)
	if strings.Contains(id, token) || id == token {
		t.Fatalf("cluster id %q contains the token", id)
	}
	if id == "" {
		t.Fatal("empty cluster id")
	}
	if ClusterID(token) != id {
		t.Error("cluster id is not stable across calls; members would never meet")
	}
	if ClusterID("different") == id {
		t.Error("two different tokens produced the same cluster id")
	}
	if ClusterID("") != "public" {
		t.Errorf("no token gave %q; an unauthenticated cluster should be named as one",
			ClusterID(""))
	}
}

// TestRegisterRoundTripsOverUDP checks the client against the server, so the
// two halves cannot drift apart.
func TestRegisterRoundTripsOverUDP(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	srv := NewServer(time.Minute)
	go func() { _ = srv.Serve(pc, stop) }()

	// A peer already listed, so the reply has something in it.
	body, _ := json.Marshal(request{Op: "register", Cluster: "c1", Node: "bob"})
	srv.Handle(body, fakeAddr("198.51.100.9:45641"))

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	you, peers, err := Register(conn, pc.LocalAddr().String(), "c1", "alice", 2*time.Second)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if you != conn.LocalAddr().String() {
		t.Errorf("told we appear from %q, want %q", you, conn.LocalAddr().String())
	}
	if len(peers) != 1 || peers[0].Node != "bob" {
		t.Errorf("got peers %+v, want bob", peers)
	}
}

func registerWith(t *testing.T, s *Server, cluster, node, from string, cands []string) response {
	t.Helper()
	body, _ := json.Marshal(request{Op: "register", Cluster: cluster, Node: node, Candidates: cands})
	raw := s.Handle(body, fakeAddr(from))
	if raw == nil {
		t.Fatalf("registration from %s was ignored", node)
	}
	var resp response
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("reply is not json: %v", err)
	}
	return resp
}

// TestCandidatesReachTheOtherPeer. The reflexive address the rendezvous sees
// is the one least likely to work: two machines on one network reach each
// other's LAN address without troubling a router, and a mapped port survives
// a NAT that no reflexive address gets through. A peer that is only ever told
// the reflexive address cannot try either.
func TestCandidatesReachTheOtherPeer(t *testing.T) {
	s := NewServer(time.Minute)
	registerWith(t, s, "c1", "alice", "203.0.113.1:5000",
		[]string{"lan:192.168.0.10:38447", "mapped:203.0.113.1:38447"})
	resp := registerWith(t, s, "c1", "bob", "198.51.100.2:6000", []string{"lan:10.0.0.5:38447"})

	if len(resp.Peers) != 1 {
		t.Fatalf("bob sees %d peers, want 1", len(resp.Peers))
	}
	got := resp.Peers[0]
	if got.Node != "alice" {
		t.Fatalf("peer is %s, want alice", got.Node)
	}
	if len(got.Candidates) != 2 {
		t.Fatalf("alice's candidates = %v, want both of them", got.Candidates)
	}
	if got.Candidates[0] != "lan:192.168.0.10:38447" || got.Candidates[1] != "mapped:203.0.113.1:38447" {
		t.Errorf("candidates = %v, want the LAN and mapped addresses alice registered", got.Candidates)
	}
	// The reflexive address must still travel: it is what the punch aims at.
	if got.Addr != "203.0.113.1:5000" {
		t.Errorf("Addr = %s, want the address the server saw", got.Addr)
	}
}

// TestConnectForwardsToThePeerAndNobodyElse. This is the one operation that
// makes the server send a packet somewhere the sender chose, so where it goes
// is worth pinning down.
func TestConnectForwardsToThePeerAndNobodyElse(t *testing.T) {
	s := NewServer(time.Minute)
	registerWith(t, s, "c1", "alice", "203.0.113.1:5000", []string{"lan:192.168.0.10:38447"})
	registerWith(t, s, "c1", "bob", "198.51.100.2:6000", nil)

	body, _ := json.Marshal(request{Op: "connect", Cluster: "c1", Node: "alice", Peer: "bob", Nonce: "n1"})
	reply, fwd := s.handle(body, fakeAddr("203.0.113.1:5000"))

	if reply != nil {
		t.Errorf("connect answered the caller (%s); it should only forward", reply)
	}
	if fwd == nil {
		t.Fatal("connect forwarded nothing, so bob never learns to punch back")
	}
	if fwd.To != "198.51.100.2:6000" {
		t.Errorf("forwarded to %s, want bob's registered address", fwd.To)
	}
	p, ok := ParsePunch(fwd.Body)
	if !ok {
		t.Fatalf("the forwarded datagram is not a punch: %s", fwd.Body)
	}
	if p.From != "alice" || p.Nonce != "n1" {
		t.Errorf("punch says from=%s nonce=%s, want alice/n1", p.From, p.Nonce)
	}
	// Alice's candidates must ride along, or bob has nothing to aim at.
	if len(p.Candidates) != 1 || p.Candidates[0] != "lan:192.168.0.10:38447" {
		t.Errorf("punch candidates = %v, want alice's registration", p.Candidates)
	}
	// And bob learns its own address free, saving a round trip.
	if p.You != "198.51.100.2:6000" {
		t.Errorf("punch You = %s, want bob's address", p.You)
	}
}

// TestConnectUsesRegisteredCandidatesNotClaimedOnes. A node speaks for itself,
// and only through a registration the server watched arrive from its own
// address. Letting connect carry arbitrary candidates would make the server
// repeat one machine's claims about another.
func TestConnectUsesRegisteredCandidatesNotClaimedOnes(t *testing.T) {
	s := NewServer(time.Minute)
	registerWith(t, s, "c1", "alice", "203.0.113.1:5000", []string{"lan:192.168.0.10:38447"})
	registerWith(t, s, "c1", "bob", "198.51.100.2:6000", nil)

	body, _ := json.Marshal(request{
		Op: "connect", Cluster: "c1", Node: "alice", Peer: "bob", Nonce: "n1",
		Candidates: []string{"lan:10.6.6.6:1"},
	})
	_, fwd := s.handle(body, fakeAddr("203.0.113.1:5000"))
	if fwd == nil {
		t.Fatal("nothing forwarded")
	}
	p, _ := ParsePunch(fwd.Body)
	for _, c := range p.Candidates {
		if strings.Contains(c, "10.6.6.6") {
			t.Errorf("a candidate named in the connect was forwarded: %v", p.Candidates)
		}
	}
}

// TestConnectCannotCrossClusters. Members of different clusters must never
// see each other, and the forward is a way of reaching an address directly.
func TestConnectCannotCrossClusters(t *testing.T) {
	s := NewServer(time.Minute)
	registerWith(t, s, "mine", "alice", "203.0.113.1:5000", nil)
	registerWith(t, s, "theirs", "victim", "198.51.100.2:6000", nil)

	body, _ := json.Marshal(request{Op: "connect", Cluster: "mine", Node: "alice", Peer: "victim"})
	if _, fwd := s.handle(body, fakeAddr("203.0.113.1:5000")); fwd != nil {
		t.Errorf("forwarded across clusters, to %s", fwd.To)
	}
}

// TestConnectIsRateLimited. Unbounded, this is a way of pointing the
// server's traffic at any address that has ever registered.
func TestConnectIsRateLimited(t *testing.T) {
	s := NewServer(time.Minute)
	registerWith(t, s, "c1", "alice", "203.0.113.1:5000", nil)
	registerWith(t, s, "c1", "bob", "198.51.100.2:6000", nil)

	body, _ := json.Marshal(request{Op: "connect", Cluster: "c1", Node: "alice", Peer: "bob"})
	forwarded := 0
	for i := 0; i < 50; i++ {
		if _, fwd := s.handle(body, fakeAddr("203.0.113.1:5000")); fwd != nil {
			forwarded++
		}
	}
	if forwarded > int(connectBurst)+1 {
		t.Errorf("forwarded %d of 50 back-to-back requests; the burst is %.0f", forwarded, connectBurst)
	}
	if forwarded == 0 {
		t.Error("forwarded none at all, so an ordinary first connect would be dropped")
	}

	// A different source has its own allowance: one noisy peer must not stop
	// everyone else connecting.
	registerWith(t, s, "c1", "carol", "192.0.2.7:7000", nil)
	other, _ := json.Marshal(request{Op: "connect", Cluster: "c1", Node: "carol", Peer: "bob"})
	if _, fwd := s.handle(other, fakeAddr("192.0.2.7:7000")); fwd == nil {
		t.Error("a second source was refused because the first exhausted its allowance")
	}
}

// TestConnectToAnAbsentPeerIsSilent. Answering would turn the server into a
// way of testing which nodes exist.
func TestConnectToAnAbsentPeerIsSilent(t *testing.T) {
	s := NewServer(time.Minute)
	registerWith(t, s, "c1", "alice", "203.0.113.1:5000", nil)

	body, _ := json.Marshal(request{Op: "connect", Cluster: "c1", Node: "alice", Peer: "ghost"})
	if reply, fwd := s.handle(body, fakeAddr("203.0.113.1:5000")); reply != nil || fwd != nil {
		t.Errorf("answered for a peer that is not there: reply=%s fwd=%v", reply, fwd)
	}
}

// TestAPunchIsNotMistakenForARegistration. Both arrive on one socket by
// design; a caller that confuses them either drops connections or treats a
// peer's punch as its own address.
func TestAPunchIsNotMistakenForARegistration(t *testing.T) {
	s := NewServer(time.Minute)
	registerWith(t, s, "c1", "alice", "203.0.113.1:5000", nil)
	registerWith(t, s, "c1", "bob", "198.51.100.2:6000", nil)

	regBody, _ := json.Marshal(request{Op: "connect", Cluster: "c1", Node: "alice", Peer: "bob"})
	_, fwd := s.handle(regBody, fakeAddr("203.0.113.1:5000"))
	if fwd == nil {
		t.Fatal("nothing forwarded")
	}
	if _, _, ok := ParseRegister(fwd.Body); ok {
		t.Error("a punch parsed as a registration reply")
	}

	reply := s.Handle(mustJSON(request{Op: "register", Cluster: "c1", Node: "bob"}), fakeAddr("198.51.100.2:6000"))
	if _, ok := ParsePunch(reply); ok {
		t.Error("a registration reply parsed as a punch")
	}
	if _, _, ok := ParseRegister(reply); !ok {
		t.Error("a registration reply did not parse as one")
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
