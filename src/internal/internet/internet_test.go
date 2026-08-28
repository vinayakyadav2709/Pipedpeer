package internet

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/direct"
)

// TestOneMissedRegistrationDoesNotDropAPeer. The address book is UDP, so a
// lost packet looks exactly like a machine going away. Dropping the peer takes
// its local port with it, which fails every job in flight against that node -
// a far worse outcome than briefly offering work to something that has left.
func TestOneMissedRegistrationDoesNotDropAPeer(t *testing.T) {
	const poll = 20 * time.Second
	now := time.Now()
	peers := map[string]*peerLink{
		"busy-peer": {lastSeen: now.Add(-poll)}, // missed exactly one round
	}
	if gone := expired(peers, map[string]bool{}, now, poll); len(gone) != 0 {
		t.Errorf("dropped %v after a single missed registration", gone)
	}
}

// TestAPeerThatStopsAnsweringIsEventuallyDropped. The other half: a machine
// that has genuinely gone must stop being offered work, or every placement
// decision includes a node that will never answer.
func TestAPeerThatStopsAnsweringIsEventuallyDropped(t *testing.T) {
	const poll = 20 * time.Second
	now := time.Now()
	peers := map[string]*peerLink{
		"gone-peer": {lastSeen: now.Add(-4 * poll)},
	}
	gone := expired(peers, map[string]bool{}, now, poll)
	if len(gone) != 1 || gone[0] != "gone-peer" {
		t.Errorf("got %v, want the peer that stopped answering", gone)
	}
}

// TestALivePeerIsNeverDropped however old its last recorded sighting, because
// the address book has just named it.
func TestALivePeerIsNeverDropped(t *testing.T) {
	const poll = 20 * time.Second
	now := time.Now()
	peers := map[string]*peerLink{
		"here": {lastSeen: now.Add(-time.Hour)},
	}
	if gone := expired(peers, map[string]bool{"here": true}, now, poll); len(gone) != 0 {
		t.Errorf("dropped %v though the rendezvous just listed it", gone)
	}
}

// TestThePollKeepsTheMappingAlive.
//
// The registration is not only how peers are found: it is the packet that
// keeps this node's own mapping alive on its router. NATs commonly forget an
// idle UDP mapping after thirty seconds, so a poll at or above that loses the
// mapping between polls and the address published to peers stops working
// while the introducer still lists it.
func TestThePollKeepsTheMappingAlive(t *testing.T) {
	m := New(Config{Rendezvous: "203.0.113.9:38445"})
	if m.cfg.Poll <= 0 {
		t.Fatal("no default poll interval; the manager would spin")
	}
	if m.cfg.Poll >= 30*time.Second {
		t.Errorf("poll is %s, which is not under a NAT's usual 30s idle timeout; "+
			"the mapping lapses between polls", m.cfg.Poll)
	}
}

// TestTheDirectPortIsFixedByDefault. A mapping the router granted and an
// address a peer cached are both worth keeping across a restart, and neither
// survives a port that changes every time the daemon starts.
func TestTheDirectPortIsFixedByDefault(t *testing.T) {
	m := New(Config{Rendezvous: "203.0.113.9:38445"})
	if m.cfg.DirectPort != DefaultPort {
		t.Errorf("DirectPort = %d, want the fixed default %d", m.cfg.DirectPort, DefaultPort)
	}
	if DefaultPort == 0 {
		t.Error("the default port is ephemeral, so no mapping survives a restart")
	}
}

// TestAMappingThatIsNotOursIsNotPublished.
//
// Measured on this project's laptop: on a phone tether, UPnP granted a
// mapping on 172.168.3.211 while a public server saw the machine as
// 103.57.97.77. The range check for double NAT does not catch that - 172.168
// is outside RFC1918, which stops at 172.31 - so the address looked
// publishable and belongs to somebody else entirely. Publishing it sends
// every peer to probe an uninvolved host.
func TestAMappingThatIsNotOursIsNotPublished(t *testing.T) {
	cases := []struct {
		name    string
		mapped  string
		reflex  string
		publish bool
		why     string
	}{
		{
			name: "the router and the world agree", mapped: "203.0.113.9:38447",
			reflex: "203.0.113.9:41000", publish: true,
			why: "the mapping is genuinely this machine's",
		},
		{
			name: "the phone tether case", mapped: "172.168.3.211:38447",
			reflex: "103.57.97.77:44069", publish: false,
			why: "the router is behind another NAT and named a stranger",
		},
		{
			name: "no reflexive address yet", mapped: "203.0.113.9:38447",
			reflex: "", publish: true,
			why: "on the first poll the mapping is the better of two guesses",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reflex netip.AddrPort
			if tc.reflex != "" {
				reflex = netip.MustParseAddrPort(tc.reflex)
			}
			got := mappingIsOurs(netip.MustParseAddrPort(tc.mapped), reflex)
			if got != tc.publish {
				t.Errorf("publish = %v, want %v — %s", got, tc.publish, tc.why)
			}
		})
	}
}

// TestALiveLinkSurvivesTheIntroducerGoingAway.
//
// The property the whole batch exists for, and it was measured failing: with
// the introducer stopped, two machines holding a working punched connection
// tore it down sixty seconds later - "peer stopped checking in" - because the
// address book had stopped listing them. Nothing had stopped except the
// introducer, which is precisely what is meant to be survivable.
//
// A connection knows whether it is alive. The address book is how peers are
// found, not how they are kept.
func TestALiveLinkSurvivesTheIntroducerGoingAway(t *testing.T) {
	now := time.Now()
	poll := 20 * time.Second
	// Long past the grace period, and the introducer lists nobody.
	stale := now.Add(-10 * time.Minute)

	live := &peerLink{addr: "127.0.0.1:1", lastSeen: stale, conn: liveConn(t)}
	dead := &peerLink{addr: "127.0.0.1:2", lastSeen: stale}

	gone := expired(map[string]*peerLink{"live": live, "dead": dead},
		map[string]bool{}, now, poll)

	for _, n := range gone {
		if n == "live" {
			t.Error("a working direct connection was dropped because the " +
				"introducer stopped listing it; surviving that is the point")
		}
	}
	found := false
	for _, n := range gone {
		if n == "dead" {
			found = true
		}
	}
	if !found {
		t.Error("a peer with no connection and no listing was kept forever")
	}
}

// A link whose connection has closed is not alive, however recently the
// introducer mentioned it.
func TestAClosedConnectionIsNotAlive(t *testing.T) {
	conn := liveConn(t)
	l := &peerLink{conn: conn}
	if !l.alive() {
		t.Fatal("a fresh connection reports itself dead")
	}
	conn.CloseWithError(0, "")
	// The context closes asynchronously; give it a moment.
	for i := 0; i < 50 && l.alive(); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if l.alive() {
		t.Error("a closed connection still reports itself alive, so a dead peer " +
			"is kept and offered work forever")
	}
	if (&peerLink{}).alive() {
		t.Error("a link with no connection reports itself alive")
	}
}

// TestAPeerWithNoPathIsNeverHandedToTheDaemon.
//
// The routing-around contract: a peer that cannot be reached is reported
// with its reason and NOT registered as a node, so the scheduler never
// offers it work. If OnPeer fired for it, every job placed there would time
// out against a forwarder to nowhere - which is worse than the relay this
// design removed, not better.
func TestAPeerWithNoPathIsNeverHandedToTheDaemon(t *testing.T) {
	joined := make(chan string, 4)
	var unreachable []string
	m := New(Config{
		Rendezvous: "203.0.113.9:38445",
		OnPeer:     func(node, addr string) { joined <- node },
		OnUnreachable: func(node, reason string) {
			unreachable = append(unreachable, node+": "+reason)
		},
	})

	// A peer that published nowhere to try - the simplest unreachable case,
	// and the one an old daemon with no candidates produces.
	err := m.connect(context.Background(), "beefbeefbeefbeef", nil)
	u, ok := direct.IsUnreachable(err)
	if !ok {
		t.Fatalf("connect returned %v, not an Unreachable with a reason", err)
	}
	if u.Reason != direct.ReasonNoCandidates {
		t.Errorf("reason = %q, want no-candidates", u.Reason)
	}

	m.noPath("beefbeefbeefbeef", err, candKey(nil))
	select {
	case n := <-joined:
		t.Fatalf("OnPeer fired for unreachable peer %s; the scheduler would "+
			"place work on a forwarder to nowhere", n)
	default:
	}
	if len(unreachable) != 1 {
		t.Fatalf("OnUnreachable calls = %d, want 1: the reason is how a machine "+
			"that stopped serving stays visible", len(unreachable))
	}

	// And the failure sets a backoff, so the next poll does not punch-burst
	// at a peer that will not answer.
	m.mu.Lock()
	next, ok2 := m.backoff["beefbeefbeefbeef"]
	m.mu.Unlock()
	if !ok2 || !next.until.After(time.Now()) {
		t.Error("no backoff recorded; every poll would retry a dead peer forever")
	}
}

// TestARestartedPeerDoesNotServeTheOldPenalty.
//
// A peer that restarts gets a new socket, a new mapping and a different set
// of candidates. Making it wait out a backoff earned by addresses that no
// longer exist is how a restarted machine sat unreachable for minutes while
// both ends were perfectly able to connect - which is the restart-recovery
// window this exists to close.
//
// The penalty belongs to a situation, not to a peer.
func TestARestartedPeerDoesNotServeTheOldPenalty(t *testing.T) {
	m := New(Config{Rendezvous: "203.0.113.9:38445"})
	old := []string{"reflex:203.0.113.1:41000", "lan:192.168.0.5:38447"}

	m.noPath("beef", &direct.Unreachable{Peer: "beef", Reason: direct.ReasonTimeout}, candKey(old))

	m.mu.Lock()
	entry := m.backoff["beef"]
	m.mu.Unlock()
	if !entry.until.After(time.Now()) {
		t.Fatal("no penalty recorded")
	}

	// Same addresses: the penalty still applies, or a peer that cannot be
	// reached is punched at on every poll forever.
	if entry.tried != candKey(old) {
		t.Errorf("penalty recorded against %q, want the addresses that failed", entry.tried)
	}
	if candKey([]string{"lan:192.168.0.5:38447", "reflex:203.0.113.1:41000"}) != candKey(old) {
		t.Error("the same addresses in a different order read as a different situation")
	}

	now := time.Now()

	// Same addresses, penalty unexpired: wait, or a peer that cannot be
	// reached is punched at on every poll forever.
	if shouldTry(entry, old, now) {
		t.Error("retried the same addresses that just failed, before the penalty expired")
	}

	// Restarted: a new reflexive port, so a new situation and no wait.
	fresh := []string{"reflex:203.0.113.1:52111", "lan:192.168.0.5:38447"}
	if !shouldTry(entry, fresh, now) {
		t.Error("a restarted peer offering a new address was made to serve out a " +
			"penalty earned by an address that no longer exists")
	}

	// And once the penalty expires, the same addresses are fair game again.
	if !shouldTry(entry, old, entry.until.Add(time.Second)) {
		t.Error("the penalty never expires")
	}
}
