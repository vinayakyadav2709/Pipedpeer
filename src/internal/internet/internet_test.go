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

// TestRelayDefaultsToTheRendezvousHost. They are normally the same machine, and
// making the user give both addresses when one is derivable is exactly the
// setup this is meant to remove.
func TestRelayDefaultsToTheRendezvousHost(t *testing.T) {
	m := New(Config{Rendezvous: "203.0.113.9:38445"})
	if m.cfg.Poll <= 0 {
		t.Error("no default poll interval; the manager would spin")
	}
	// The derivation itself lives in Run; assert the port constant it uses is
	// the one `pipedpeer rendezvous` actually serves, since a mismatch would
	// mean a cluster that never connects and nothing saying why.
	if DefaultRelayPort != "38446" {
		t.Errorf("DefaultRelayPort is %s; `pipedpeer rendezvous` serves 38446",
			DefaultRelayPort)
	}
}
