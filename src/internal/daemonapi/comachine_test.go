package daemonapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/heartbeat"
)

// TestAPeerSharingThisMachineIsSubtracted.
//
// Free memory belongs to the machine, not to the daemon. Two daemons on one
// host each read the host's free memory and each subtract only their own
// reservations, so both conclude they have all of it and both admit work that
// between them the machine cannot hold. Measured before this: a forced spill
// drove a 14 GB machine to 194 MB free and the kernel killed its desktop
// shell.
func TestAPeerSharingThisMachineIsSubtracted(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id, so co-location " +
			"cannot be detected here at all")
	}
	s := New("self")

	before := s.AvailableForJob()

	s.peersMu.Lock()
	if s.peerHealths == nil {
		// The daemon creates this when it starts polling; these tests do not
		// start a poller, which would go looking for real peers on the LAN.
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["127.0.0.1:38091"] = &PeerHealth{
		Host: "127.0.0.1", Port: 38091, NodeID: "roommate",
		Status: "healthy", Machine: me, ReservedMem: 4 << 30,
	}
	s.peersMu.Unlock()
	forgetRoommates(s)

	after := s.AvailableForJob()
	if after >= before {
		t.Errorf("a peer on this machine reserved 4 GiB and this daemon still "+
			"offers %d bytes (was %d); both would admit work the machine "+
			"cannot hold", after, before)
	}
}

// TestAPeerOnAnotherMachineIsNotSubtracted. Its memory is its own; charging
// this node for it would shrink every cluster's usable memory.
func TestAPeerOnAnotherMachineIsNotSubtracted(t *testing.T) {
	s := New("self")
	before := s.AvailableForJob()

	s.peersMu.Lock()
	if s.peerHealths == nil {
		// The daemon creates this when it starts polling; these tests do not
		// start a poller, which would go looking for real peers on the LAN.
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["10.0.0.5:38080"] = &PeerHealth{
		Host: "10.0.0.5", Port: 38080, NodeID: "elsewhere",
		Status: "healthy", Machine: "a-different-boot-id", ReservedMem: 8 << 30,
	}
	s.peersMu.Unlock()
	forgetRoommates(s)

	after := s.AvailableForJob()
	// Free memory moves under a running test, so compare with a tolerance
	// well below the 8 GiB the peer reserved.
	if diff := before - after; diff > 1<<30 {
		t.Errorf("a peer on another machine cost this one %d bytes of headroom", diff)
	}
}

// TestUnknownMachineIsNotAssumedToBeThisOne. A peer that advertises nothing
// must not be guessed to be co-located: that would shrink every node's usable
// memory for no reason, on every cluster running a mixed set of versions.
func TestUnknownMachineIsNotAssumedToBeThisOne(t *testing.T) {
	s := New("self")
	before := s.AvailableForJob()

	s.peersMu.Lock()
	if s.peerHealths == nil {
		// The daemon creates this when it starts polling; these tests do not
		// start a poller, which would go looking for real peers on the LAN.
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["10.0.0.6:38080"] = &PeerHealth{
		Host: "10.0.0.6", Port: 38080, NodeID: "older-build",
		Status: "healthy", Machine: "", ReservedMem: 8 << 30,
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if diff := before - s.AvailableForJob(); diff > 1<<30 {
		t.Errorf("a peer that said nothing about its machine cost this one %d "+
			"bytes; silence is not agreement", diff)
	}
}

// TestAnUnreachablePeerIsNotHoldingMemory. A daemon that has stopped answering
// is not running anything, and continuing to charge for it would leave the
// machine unable to admit work after a peer died.
func TestAnUnreachablePeerIsNotHoldingMemory(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id")
	}
	s := New("self")
	before := s.AvailableForJob()

	s.peersMu.Lock()
	if s.peerHealths == nil {
		// The daemon creates this when it starts polling; these tests do not
		// start a poller, which would go looking for real peers on the LAN.
		s.peerHealths = map[string]*PeerHealth{}
	}
	s.peerHealths["127.0.0.1:38092"] = &PeerHealth{
		Host: "127.0.0.1", Port: 38092, NodeID: "dead",
		Status: "unreachable", Machine: me, ReservedMem: 8 << 30,
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if diff := before - s.AvailableForJob(); diff > 1<<30 {
		t.Errorf("a peer that stopped answering still held %d bytes", diff)
	}
}

// TestSameMachineNeedsBothSidesKnown. Two unknowns are not a match, or every
// node that cannot read a boot id would look co-located with every other.
func TestSameMachineNeedsBothSidesKnown(t *testing.T) {
	if heartbeat.SameMachine("", "") {
		t.Error("two unknown machines were called the same machine")
	}
	if heartbeat.SameMachine("abc", "") || heartbeat.SameMachine("", "abc") {
		t.Error("an unknown machine matched a known one")
	}
	if !heartbeat.SameMachine("abc", "abc") {
		t.Error("a machine did not match itself")
	}
}

// TestTheLiveFigureBeatsTheCachedOne.
//
// The health poller runs every ten seconds or so. A chunk arriving inside that
// window was admitted against a reservation figure from before the reservation
// it needed to see, so both daemons admitted in the same window and the
// machine went over anyway - measured, with the kernel taking a pipedpeer
// worker. A roommate is on the loopback, so it can simply be asked.
func TestTheLiveFigureBeatsTheCachedOne(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id")
	}

	var reserved int64 = 6 << 30
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(healthResponse{
			Status:      "ok",
			ReservedMem: atomic.LoadInt64(&reserved),
		})
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	s := New("self")
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{
		"roommate": {
			Host: u.Hostname(), Port: port, NodeID: "roommate",
			Status: "healthy", Machine: me,
			// Stale: what the poller last saw, before the peer reserved.
			ReservedMem: 0,
		},
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if got := s.reservedOnThisMachine(); got != 6<<30 {
		t.Errorf("read %d bytes reserved on this machine, want the live 6 GiB "+
			"rather than the poller's stale 0", got)
	}
}

// TestARoommateThatStopsAnsweringKeepsItsLastFigure. Treating silence as
// "reserved nothing" would hand out memory that is still spoken for, which is
// the failure this whole path exists to prevent.
func TestARoommateThatStopsAnsweringKeepsItsLastFigure(t *testing.T) {
	me := heartbeat.Machine()
	if me == "" {
		t.Skip("NOT VERIFIED: this kernel exposes no boot id")
	}
	s := New("self")
	s.peersMu.Lock()
	s.peerHealths = map[string]*PeerHealth{
		// Port 1 is not listening, so the live question fails.
		"gone": {
			Host: "127.0.0.1", Port: 1, NodeID: "gone",
			Status: "healthy", Machine: me, ReservedMem: 3 << 30,
		},
	}
	s.peersMu.Unlock()

	forgetRoommates(s)
	if got := s.reservedOnThisMachine(); got != 3<<30 {
		t.Errorf("read %d bytes; a roommate that did not answer should keep "+
			"its last known 3 GiB, not drop to nothing", got)
	}
}

// forgetRoommates drops the short-lived answer cache, standing in for the
// poller cycle that would separate two of these checks in a running daemon.
func forgetRoommates(s *Server) {
	s.coResMu.Lock()
	s.coResAt = time.Time{}
	s.coResMu.Unlock()
}
