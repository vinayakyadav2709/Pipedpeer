package internet

import (
	"testing"
	"time"
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
