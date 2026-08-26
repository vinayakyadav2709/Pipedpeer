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
